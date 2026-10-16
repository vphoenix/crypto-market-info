#!/usr/bin/env python3
"""Plan/freeze/import finite startup dependencies after an old service exits.

Never activates a producer or changes a unit. Source snapshots and all original
data are preserved. Independent verification remains below history-complete.
"""
import argparse
import datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import subprocess
import urllib.parse
import urllib.request
import urllib.error
import uuid

REPO = Path(__file__).resolve().parents[2]
BASE = REPO / "var/storage/bootstrap-preparation/20261013"
SERVICES = {"collector": "crypto_market_info", "across": "crypto_market_info_across",
            "reserve": "crypto_market_info_reserve", "lst": "crypto_market_info_lst",
            "justlend-keeper": "crypto_market_info_justlend_keeper"}
MARKERS = {"across": "across_capture", "reserve": "reserve_capture",
           "lst": "lst_capture", "justlend-keeper": "jl_keeper_capture"}
catalog = {(t["database"], t["table"]): t for t in json.loads((REPO / "internal/storage/mysql/mapping.json").read_text())["tables"]}
baseline = {(t["family"], t["table"]): t for t in json.loads((BASE / "snapshot-manifest.json").read_text())["tables"]}


def save(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    with os.fdopen(os.open(temporary, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, 0o600), "w") as f:
        json.dump(value, f, indent=2)
        f.write("\n")
    os.replace(temporary, path)


def query(sql, read_only=True):
    params = dict(query=sql + (" FORMAT JSONEachRow" if read_only else ""),
                  max_threads=1, max_memory_usage=256 << 20, max_execution_time=120,
                  max_block_size=500, max_insert_threads=1,
                  min_insert_block_size_rows=500, min_insert_block_size_bytes=2 << 20,
                  max_local_read_bandwidth=16 << 20, log_comment="cmi-finite-startup-tail")
    if read_only:
        params["readonly"] = 2
    request = urllib.request.Request("http://127.0.0.1:8123/?" + urllib.parse.urlencode(params), data=b"")
    try:
        with urllib.request.urlopen(request, timeout=130) as response:
            body = response.read()
    except urllib.error.HTTPError as error:
        raise RuntimeError(error.read(4096).decode(errors="replace")) from None
    return [json.loads(line) for line in body.splitlines()]


def qualified(family, name):
    assert (family, name) in catalog
    return f"`{family}`.`{name}`"


def final(family, name):
    return " FINAL" if catalog[family, name]["source_engine"] == "ReplacingMergeTree" else ""


def columns(family, name):
    return ",".join("`" + c["name"] + "`" for c in catalog[family, name]["columns"])


def difference(family, name, filter_sql="1"):
    # Hash the same exact typed tuple on both sides inside ClickHouse. The
    # filter only selects changes; migration still checks full canonical typed
    # fields on write and independently rereads them. No JSON/float conversion.
    b = baseline[family, name]
    digest = f"SHA256(toString(tuple({columns(family, name)})))"
    original = f"`{b['snapshot_database']}`.`{b['snapshot_table']}`"
    return f"({filter_sql}) AND {digest} NOT IN (SELECT {digest} FROM {original}{final(family, name)})"


def selectors(service):
    family = SERVICES[service]
    selected = {}
    if service != "collector":
        marker = MARKERS[service]
        selected[marker] = difference(family, marker)
        parent = qualified(family, marker) + final(family, marker)
        ids = f"SELECT capture_id FROM {parent} WHERE {selected[marker]}"
        for f, name in catalog:
            if f != family or name == marker:
                continue
            cols = {c["name"] for c in catalog[f, name]["columns"]}
            if "capture_id" in cols:
                selected[name] = f"capture_id IN ({ids})"
            elif name == "instrument":
                selected[name] = difference(family, name)
        if service == "reserve":
            bounds = (f"SELECT min(from_block) AS lo,max(to_block) AS hi FROM {parent} "
                      f"WHERE {selected[marker]}")
            for name in ("dex_log", "dex_tx_receipt", "reserve_receipt_data"):
                selected[name] = (f"block_number>=(SELECT lo FROM ({bounds})) "
                                  f"AND block_number<=(SELECT hi FROM ({bounds}))")
            quotes = qualified(family, "reserve_route_quote") + final(family, "reserve_route_quote")
            selected["reserve_route_simulation"] = f"quote_id IN (SELECT quote_id FROM {quotes} WHERE capture_id IN ({ids}))"
        return selected
    for name in ("instrument", "derivative_contract_spec", "instrument_canonical_mapping", "yield_route"):
        selected[name] = difference(family, name)
    funding_cutoff = (datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=72)).strftime("%Y-%m-%d %H:%M:%S.%f")
    selected["funding_rate_hourly"] = difference(family, "funding_rate_hourly", f"funding_time>=toDateTime64('{funding_cutoff}',3,'UTC')")
    plan = qualified(family, "options_collection_plan") + final(family, "options_collection_plan")
    latest = f"(SELECT max(effective_minute) FROM {plan})"
    plans = f"SELECT plan_id FROM {plan} WHERE effective_minute={latest}"
    previous = f"SELECT previous_plan_id FROM {plan} WHERE effective_minute={latest}"
    selected["options_collection_plan"] = f"plan_id IN ({plans}) OR plan_id IN ({previous})"
    chosen = f"SELECT * FROM {plan} WHERE {selected['options_collection_plan']}"
    selected["options_live_run"] = f"run_id IN (SELECT arrayJoin(run_ids) FROM ({chosen}))"
    selected["options_catalog_scope_observation"] = f"observation_id IN (SELECT arrayJoin(observation_ids) FROM ({chosen}))"
    observations = qualified(family, "options_catalog_scope_observation") + final(family, "options_catalog_scope_observation")
    selected["derivative_trading_rule"] = ("trading_rule_id IN (SELECT arrayJoin(rule_ids) FROM "
                                            f"{observations} WHERE {selected['options_catalog_scope_observation']})")
    blocks = qualified(family, "dex_block") + final(family, "dex_block")
    mh = hashlib.sha256((REPO / "internal/dex/ethereum/manifest.json").read_bytes()).hexdigest()
    match = f"chain_id=1 AND manifest_hash=unhex('{mh}')"
    checkpoint = f"(SELECT max(block_number) FROM {blocks} WHERE {match} AND canonical=1 AND committed=1 AND finality='finalized')"
    recover = (f"{match} AND committed=1 AND (block_number>={checkpoint} "
               "OR finality NOT IN ('finalized','orphaned') "
               "OR (canonical=1 AND (log_coverage!='complete' OR receipt_coverage!='complete')))")
    selected["dex_block"] = difference(family, "dex_block")
    keys = f"SELECT chain_id,manifest_hash,block_number,block_hash,batch_id FROM {blocks} WHERE {recover}"
    predicate = f"(chain_id,manifest_hash,block_number,block_hash,batch_id) IN ({keys})"
    for name in ("dex_route_quote", "dex_log", "dex_sky_state"):
        selected[name] = predicate
    selected["dex_tx_receipt"] = f"block_hash IN (SELECT block_hash FROM {blocks} WHERE {recover})"
    return selected


def require_stopped(service):
    unit = "crypto-market-info-" + service + ".service"
    result = subprocess.run(["systemctl", "--user", "show", unit, "--property=ActiveState,MainPID"],
                            text=True, capture_output=True, check=True)
    fields = dict(line.split("=", 1) for line in result.stdout.splitlines())
    if fields != {"MainPID": "0", "ActiveState": "inactive"}:
        raise SystemExit("Old service must be fully stopped before freezing: " + unit)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=["plan", "freeze", "import"])
    parser.add_argument("service", choices=SERVICES)
    parser.add_argument("root", type=Path)
    args = parser.parse_args()
    root = args.root.resolve()
    if not root.is_relative_to(REPO / "var/storage"):
        parser.error("Use a private preparation directory under var/storage")
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    family = SERVICES[args.service]
    with (root / "lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        if args.mode != "plan":
            require_stopped(args.service)
        manifest_path = root / "snapshot-manifest.json"
        if args.mode in ("plan", "freeze") and not manifest_path.exists():
            filters = selectors(args.service)
            meta = []
            for name, where in filters.items():
                descriptor = catalog[family, name]
                ddl = query(f"SELECT create_table_query FROM system.tables WHERE database='{family}' AND name='{name}'")[0]["create_table_query"]
                if hashlib.sha256(ddl.encode()).hexdigest() != descriptor["source_ddl_sha256"]:
                    raise SystemExit("Source DDL changed: " + family + "." + name)
                count = int(query(f"SELECT count() AS n FROM {qualified(family, name)}{final(family, name)} WHERE {where}")[0]["n"])
                meta.append(dict(table=name, rows=count, selector=where))
                print(args.mode, family, name, count, flush=True)
            save(root / ("plan-" + args.mode + ".json"), dict(at_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(), family=family, tables=meta))
            if args.mode == "plan":
                return
            identity_path = root / "identity.json"
            if identity_path.exists():
                raise SystemExit("An incomplete frozen snapshot exists; inspect it instead of silently rebuilding")
            identity = dict(database="cmi_bootstrap_" + uuid.uuid4().hex[:16], run=str(uuid.uuid4()),
                            frozen_at_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(), service=args.service)
            save(identity_path, identity)
            database = identity["database"]
            query(f"CREATE DATABASE `{database}`", False)
            tables = []
            for i, entry in enumerate(meta):
                name, where = entry["table"], entry["selector"]
                clone = "s" + str(i).zfill(3)
                target = f"`{database}`.`{clone}`"
                query(f"CREATE TABLE {target} AS {qualified(family, name)}", False)
                query(f"INSERT INTO {target} ({columns(family, name)}) SELECT {columns(family, name)} FROM "
                      f"{qualified(family, name)}{final(family, name)} WHERE {where}", False)
                count = int(query(f"SELECT count() AS n FROM {target}{final(family, name)}")[0]["n"])
                if count != entry["rows"]:
                    raise SystemExit("Frozen row count changed: " + name)
                ddl = query(f"SELECT create_table_query FROM system.tables WHERE database='{database}' AND name='{clone}'")[0]["create_table_query"]
                tables.append(dict(family=family, table=name, snapshot_database=database, snapshot_table=clone,
                                   source_ddl_sha256=catalog[family, name]["source_ddl_sha256"],
                                   snapshot_ddl_sha256=hashlib.sha256(ddl.encode()).hexdigest(), rows=count))
                print("frozen", name, count, flush=True)
            save(manifest_path, dict(version=1, tables=tables))
        if args.mode != "import":
            return
        identity = json.loads((root / "identity.json").read_text())
        manifest = json.loads(manifest_path.read_text())
        if identity["service"] != args.service or any(t["family"] != family for t in manifest["tables"]):
            raise SystemExit("Frozen service identity changed")
        stage = root / "staging"
        stage.mkdir(mode=0o700, exist_ok=True)
        ordered = sorted(manifest["tables"], key=lambda t: (t["table"] in MARKERS.values() or t["table"] == "dex_block", t["rows"]))
        for phase in ("plan", "copy", "verify"):
            for t in ordered:
                print(phase, family, t["table"], t["rows"], flush=True)
                command = [str(BASE / "bin/storage-migrate"), phase, "--family", family, "--target", family,
                           "--table", t["table"], "--run", identity["run"], "--source-snapshot-manifest", str(manifest_path),
                           "--socket", str(REPO / "var/storage/mysql/run/mysql.sock"), "--user", "cmi_migrate",
                           "--password-file", "/home/ubuntu/.config/crypto-market-info-storage/cmi_migrate-password",
                           "--target-read-timeout", "2m", "--rows", "50" if t["table"] == "options_collection_plan" else "500",
                           "--staging-dir", str(stage), "--delay", "0s"]
                if phase == "verify":
                    command.append("--startup-rows-only")
                with (root / (phase + "-" + t["table"] + ".log")).open("ab") as log:
                    subprocess.run(command, cwd=REPO, stdout=log, stderr=log, check=True)
        save(root / "result.json", dict(at_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                                       startup_subset_fields_verified=True, history_complete=False,
                                       total_selected_rows=sum(t["rows"] for t in ordered), services_started=False))


if __name__ == "__main__":
    main()
