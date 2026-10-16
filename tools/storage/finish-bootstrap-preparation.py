#!/usr/bin/env python3
"""Finish the authorized, sealed bootstrap preparation; never switch services."""
import argparse
import datetime
import fcntl
import json
import os
from pathlib import Path
import signal
import subprocess
import time


REPO = Path(__file__).resolve().parents[2]
EXPECTED = REPO / "var/storage/bootstrap-preparation/20261013"
child = None


def utc_now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def write_json(path, value):
    temporary = path.with_name(path.name + ".tmp")
    with os.fdopen(os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600), "w") as f:
        json.dump(value, f, indent=2)
        f.write("\n")
        f.flush()
        os.fsync(f.fileno())
    os.replace(temporary, path)


def start_ticks(pid):
    try:
        return Path(f"/proc/{pid}/stat").read_text().split(") ", 1)[1].split()[19]
    except FileNotFoundError:
        return None


def interrupt(signum, frame):
    if child is not None and child.poll() is None:
        child.terminate()
    raise InterruptedError("Preparation finisher interrupted")


def run_phase(root, phase):
    global child
    with (root / f"workflow-{phase}.log").open("ab") as log:
        child = subprocess.Popen([str(root / "bin/bootstrap-prepare"), phase, str(root)], cwd=REPO, stdout=log, stderr=log)
        try:
            result = child.wait()
        finally:
            if child.poll() is None:
                child.terminate()
                child.wait()
            child = None
    if result:
        raise RuntimeError(f"{phase} failed ({result}); inspect workflow-{phase}.log")


def restore_pool(root):
    password = Path("/home/ubuntu/.config/crypto-market-info-storage/mysql-root-password").read_bytes().strip() + b"\n"
    sql = "SET GLOBAL innodb_buffer_pool_size=536870912; SHOW GLOBAL VARIABLES LIKE 'innodb_buffer_pool_size'; SHOW GLOBAL STATUS LIKE 'Innodb_buffer_pool_resize_status';"
    command = ["docker", "exec", "-i", "crypto-market-info-mysql", "sh", "-c",
               'read -r CMI_RESTORE_PASSWORD\nexport MYSQL_PWD="$CMI_RESTORE_PASSWORD"\nexec mysql --user=root --socket=/run/cmi/mysql.sock --batch -e "$1"', "sh", sql]
    result = subprocess.run(command, input=password, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=60)
    (root / "workflow-buffer-pool-restore.log").write_bytes(result.stdout + result.stderr)
    if result.returncode or b"536870912" not in result.stdout:
        raise RuntimeError("Original 512 MiB buffer pool restoration not confirmed")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("preparation", type=Path)
    parser.add_argument("--wait-pid", type=int, required=True)
    args = parser.parse_args()
    root = args.preparation.resolve()
    if root != EXPECTED or args.wait_pid < 1:
        parser.error("Only the reviewed 20261013 preparation and its current copy PID are supported")
    if (root / "startup-scope-review.json").exists():
        parser.error("The 59-table preload was superseded by a record-scoped startup plan; do not resume this workflow")
    lock = os.fdopen(os.open(root / "finish.lock", os.O_CREAT | os.O_RDWR, 0o600), "w")
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    ticks = start_ticks(args.wait_pid)
    if ticks is not None:
        arguments = Path(f"/proc/{args.wait_pid}/cmdline").read_bytes().split(b"\0")
        if len(arguments) < 3 or Path(os.fsdecode(arguments[0])).name != "bootstrap-prepare" or arguments[1] != b"copy" or os.fsdecode(arguments[2]) != str(root):
            parser.error("Wait PID is not the owned preparation copy")
    for signum in (signal.SIGTERM, signal.SIGINT):
        signal.signal(signum, interrupt)
    state = {"started_at_utc": utc_now(), "phase": "waiting_for_current_copy", "wait_pid": args.wait_pid,
             "services_switched": False, "formal_producers_activated": False, "full_history_enabled": False,
             "cutover_final_tail_refresh_required": True}
    write_json(root / "workflow-status.json", state)
    try:
        while ticks is not None and start_ticks(args.wait_pid) == ticks:
            time.sleep(5)
        # Reopen the same fixed runs: completed tables are skipped, and an
        # interrupted copy is resumed only after the original process exits.
        for phase in ("copy", "verify", "check", "recovery"):
            state.update(phase=phase, phase_started_at_utc=utc_now())
            write_json(root / "workflow-status.json", state)
            run_phase(root, phase)
        state["phase"] = "validated_pending_final_runtime_checks"
    except Exception as error:
        state.update(phase="failed", error=str(error))
    finally:
        # Never resize while the original copy still owns a transaction. If
        # this waiting supervisor is interrupted, leave restoration to the
        # operator who also stops that owned copy.
        original_running = ticks is not None and start_ticks(args.wait_pid) == ticks
        if original_running:
            state["buffer_pool_restoration_pending"] = True
        else:
            try:
                restore_pool(root)
                state["buffer_pool_restored_bytes"] = 536870912
            except Exception as error:
                state.update(phase="failed", restoration_error=str(error))
        state["updated_at_utc"] = utc_now()
        write_json(root / "workflow-status.json", state)
    if state["phase"] == "failed":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
