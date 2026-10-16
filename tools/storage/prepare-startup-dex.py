#!/usr/bin/env python3
"""Copy only DEX recovery quotes from the existing sealed preparation snapshot.

Old services continue running. This does not activate producers, open history,
or replace any source table. The original broad preparation is left intact.
"""
import datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import subprocess
import urllib.parse
import urllib.request
import uuid

REPO = Path(__file__).resolve().parents[2]
PREP = REPO / "var/storage/bootstrap-preparation/20261013"
ROOT = PREP / "startup-dex"


def save(name, value):
    path = ROOT / name
    temporary = path.with_suffix(path.suffix + ".tmp")
    with os.fdopen(os.open(temporary, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, 0o600), "w") as f:
        json.dump(value, f, indent=2)
        f.write("\n")
    os.replace(temporary, path)


def query(sql, read_only=True):
    params = dict(query=sql + (" FORMAT JSONEachRow" if read_only else ""), max_threads=1,
                  max_memory_usage=128 << 20, max_execution_time=120,
                  max_block_size=500, max_insert_threads=1,
                  min_insert_block_size_rows=500,
                  log_comment="cmi-record-scoped-startup-quotes")
    if read_only:
        params["readonly"] = 2
    request = urllib.request.Request("http://127.0.0.1:8123/?" + urllib.parse.urlencode(params), data=b"")
    with urllib.request.urlopen(request, timeout=130) as response:
        return [json.loads(line) for line in response.read().splitlines()]


def main():
    ROOT.mkdir(mode=0o700, exist_ok=True)
    with (PREP / "finish.lock").open("r") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        manifest_bytes = (PREP / "snapshot-manifest.json").read_bytes()
        manifest = json.loads(manifest_bytes)
        tables = {t["table"]: t for t in manifest["tables"] if t["family"] == "crypto_market_info"}
        source = tables["dex_route_quote"]
        blocks = tables["dex_block"]
        audit = json.loads((REPO / "research/2026-10-14-startup-dependencies/dex-scope.json").read_text())
        manifest_hash = hashlib.sha256((REPO / "internal/dex/ethereum/manifest.json").read_bytes()).hexdigest()
        if audit["manifest_hash"] != manifest_hash:
            raise SystemExit("DEX identity changed; repeat the read-only scope audit")
        final = int(audit["anchors"]["finalized"])
        scope = (f"chain_id=1 AND manifest_hash=unhex('{manifest_hash}') AND committed=1 "
                 f"AND (block_number>={final} OR finality NOT IN ('finalized','orphaned') "
                 "OR (canonical=1 AND (log_coverage!='complete' OR receipt_coverage!='complete')))")
        if final <= 0 or scope != audit["selected_block_predicate"]:
            raise SystemExit("Audited block selection changed")
        identity_path = ROOT / "identity.json"
        if identity_path.exists():
            identity = json.loads(identity_path.read_text())
            if identity["source_manifest_sha256"] != hashlib.sha256(manifest_bytes).hexdigest():
                raise SystemExit("Original sealed snapshot changed")
        else:
            identity = dict(database="cmi_bootstrap_" + uuid.uuid4().hex[:16], run=str(uuid.uuid4()),
                            source_manifest_sha256=hashlib.sha256(manifest_bytes).hexdigest(),
                            selected_block_predicate=scope, selected_quote_rows=audit["selected_quote_rows"])
            save("identity.json", identity)
        if identity["selected_block_predicate"] != scope or identity["selected_quote_rows"] != audit["selected_quote_rows"]:
            raise SystemExit("Scoped copy identity changed")
        database = identity["database"]
        target = f"`{database}`.`recovery_quotes`"
        original = f'`{source["snapshot_database"]}`.`{source["snapshot_table"]}`'
        block_table = f'`{blocks["snapshot_database"]}`.`{blocks["snapshot_table"]}`'
        for t in (source, blocks):
            ddl = query(f"SELECT create_table_query FROM system.tables WHERE database='{t['snapshot_database']}' AND name='{t['snapshot_table']}'")[0]["create_table_query"]
            if hashlib.sha256(ddl.encode()).hexdigest() != t["snapshot_ddl_sha256"]:
                raise SystemExit("Sealed snapshot DDL changed")
        scoped_manifest = ROOT / "snapshot-manifest.json"
        if not scoped_manifest.exists():
            exists = query(f"SELECT count() AS n FROM system.tables WHERE database='{database}' AND name='recovery_quotes'")[0]["n"]
            if exists:
                raise SystemExit("Unsealed scoped snapshot exists; inspect before retrying")
            mapping = next(t for t in json.loads((REPO / "internal/storage/mysql/mapping.json").read_text())["tables"]
                           if t["database"] == "crypto_market_info" and t["table"] == "dex_route_quote")
            columns = ",".join("`" + c["name"] + "`" for c in mapping["columns"])
            query(f"CREATE DATABASE {database}", False)
            query(f"CREATE TABLE {target} AS {original}", False)
            keys = f"SELECT chain_id,manifest_hash,block_number,block_hash,batch_id FROM {block_table} FINAL WHERE {scope}"
            query(f"INSERT INTO {target} ({columns}) SELECT {columns} FROM {original} FINAL WHERE "
                  f"(chain_id,manifest_hash,block_number,block_hash,batch_id) IN ({keys})", False)
            count = int(query(f"SELECT count() AS n FROM {target} FINAL")[0]["n"])
            if count != audit["selected_quote_rows"]:
                raise SystemExit("Scoped quote count changed; snapshot remains unsealed")
            ddl = query(f"SELECT create_table_query FROM system.tables WHERE database='{database}' AND name='recovery_quotes'")[0]["create_table_query"]
            save("snapshot-manifest.json", dict(version=1, tables=[dict(
                family="crypto_market_info", table="dex_route_quote", snapshot_database=database,
                snapshot_table="recovery_quotes", source_ddl_sha256=source["source_ddl_sha256"],
                snapshot_ddl_sha256=hashlib.sha256(ddl.encode()).hexdigest(), rows=count)]))
        stage = ROOT / "staging"
        stage.mkdir(mode=0o700, exist_ok=True)
        common = ["--family", "crypto_market_info", "--target", "crypto_market_info",
                  "--table", "dex_route_quote", "--run", identity["run"],
                  "--source-snapshot-manifest", str(scoped_manifest),
                  "--socket", str(REPO / "var/storage/mysql/run/mysql.sock"),
                  "--user", "cmi_migrate", "--password-file",
                  "/home/ubuntu/.config/crypto-market-info-storage/cmi_migrate-password",
                  "--target-read-timeout", "2m", "--rows", "500", "--staging-dir", str(stage)]
        for phase in ("plan", "copy", "verify"):
            print("scoped DEX", phase, "quotes=", audit["selected_quote_rows"], flush=True)
            with (ROOT / (phase + ".log")).open("ab") as log:
                subprocess.run([str(PREP / "bin/storage-migrate"), phase] + common,
                               cwd=REPO, stdout=log, stderr=log, check=True)
        save("result.json", dict(completed_at_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                                 quote_rows=audit["selected_quote_rows"], selected_blocks=audit["selected_blocks"],
                                 copy_and_independent_row_verification_passed=True,
                                 startup_domain_recovery_checked=False,
                                 full_history_verified=False, services_switched=False,
                                 final_tail_refresh_required=True))


if __name__ == "__main__":
    main()
