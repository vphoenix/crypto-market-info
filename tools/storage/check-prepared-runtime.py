#!/usr/bin/env python3
"""Read-only final checks after the owned bootstrap workflow releases its lock."""
import datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import subprocess

REPO = Path(__file__).resolve().parents[2]
ROOT = REPO / "var/storage/bootstrap-preparation/20261013"


def load(name):
    return json.loads((ROOT / name).read_text())


def save(name, value):
    temporary = ROOT / (name + ".tmp")
    with os.fdopen(os.open(temporary, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, 0o600), "w") as f:
        json.dump(value, f, indent=2)
        f.write("\n")
    os.replace(temporary, ROOT / name)


def main():
    with (ROOT / "finish.lock").open("r") as lock:
        fcntl.flock(lock, fcntl.LOCK_SH)
        workflow = load("workflow-status.json")
        if workflow.get("phase") != "validated_pending_final_runtime_checks" or workflow.get("buffer_pool_restored_bytes") != 536870912:
            raise SystemExit("Data validation incomplete; final checks did not run")
        data = load("bootstrap-check.json")
        recovery = load("startup-reader-recovery.json")
        if len(data["tables"]) != 59 or sum(t["rows"] for t in data["tables"]) != 22824530 or any(t["state"] != 2 for t in data["tables"]):
            raise SystemExit("Fixed snapshot validation incomplete")
        if data["active_producers"] or data["hot_book_rows"] or data["history_complete"] or data["services_switched"] or data["accepted_historical_batch_gaps"] != 8:
            raise SystemExit("Unexpected bootstrap scope change")
        if recovery["network_requests"] or recovery["production_writes"]:
            raise SystemExit("Recovery scope changed")
        command = [str(ROOT / "bin/storage-check"), "--all", "--socket", str(REPO / "var/storage/mysql/run/mysql.sock"),
                   "--user", "cmi_query", "--password-file", "/home/ubuntu/.config/crypto-market-info-storage/cmi_query-password",
                   "--redis-socket", str(REPO / "var/storage/redis/run/redis.sock"), "--spool", str(REPO / "var/storage/spool")]
        result = subprocess.run(command, cwd=REPO, capture_output=True, timeout=180)
        (ROOT / "workflow-final-health.json").write_bytes(result.stdout)
        (ROOT / "workflow-final-health-errors.log").write_bytes(result.stderr)
        result.check_returncode()
        health = json.loads(result.stdout)
        if len(health) != 12 or sum(len(h["tables"]) for h in health) != 125:
            raise SystemExit("Incomplete installed schema health check")
        for h in health:
            if not h["utc"] or h["RedoSync"] != 1 or h["BinlogSync"] != 1 or h["Doublewrite"] != "ON" or h["history_complete"]:
                raise SystemExit("Persistence, UTC or history state check failed")
        old = ["collector", "across", "reserve", "lst", "justlend-keeper", "clickhouse"]
        services = {}
        for name in old + ["storage-writer"]:
            unit = f"crypto-market-info-{name}.service"
            result = subprocess.run(["systemctl", "--user", "show", unit, "--property=ActiveState,MainPID,NRestarts"], capture_output=True, text=True, check=True, timeout=15)
            values = dict(line.split("=", 1) for line in result.stdout.splitlines() if "=" in line)
            services[name] = values
            expected = "inactive" if name == "storage-writer" else "active"
            if values["ActiveState"] != expected or (name == "storage-writer" and values["MainPID"] != "0"):
                raise SystemExit("Unexpected live service state: " + name)
        save("workflow-final-services.json", services)
        for line in (ROOT / "binaries.sha256").read_text().splitlines():
            expected, relative = line.split()
            digest = hashlib.sha256()
            with (ROOT / relative).open("rb") as f:
                while block := f.read(1 << 20):
                    digest.update(block)
            if digest.hexdigest() != expected:
                raise SystemExit("Staged binary hash changed: " + relative)
        now = datetime.datetime.now(datetime.timezone.utc).isoformat()
        summary = load("preparation-summary.json")
        summary.update(status="ready_for_review", completed_copy_tables=59, independent_all_table_verification_completed=True,
                       main_readonly_recovery_passed=True, final_runtime_checks_passed=True, completed_at_utc=now,
                       mysql_buffer_pool_temporarily_bytes=None, mysql_buffer_pool_restored_bytes=536870912)
        save("preparation-summary.json", summary)
        workflow.update(phase="ready_for_review", final_runtime_checks_passed=True, updated_at_utc=now)
        save("workflow-status.json", workflow)


if __name__ == "__main__":
    main()
