#!/usr/bin/env python3
"""Explicit per-service cutover actions with private backups and a MySQL fallback."""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import shlex
import shutil
import re
import subprocess
import time

REPO = Path(__file__).resolve().parents[2]
BASE = REPO / "var/storage/bootstrap-preparation/20261013"
ROOT = REPO / "var/storage/cutover/20261014"
SERVICES = {x["service"]: x for x in json.loads((BASE / "new-services.json").read_text())}


def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def save(path, value):
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with os.fdopen(os.open(temporary, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, 0o600), "w") as f:
        json.dump(value, f, indent=2)
        f.write("\n")
    os.replace(temporary, path)


def write_private(path, text):
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    with os.fdopen(os.open(path, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, 0o600), "w") as f:
        f.write(text)


def systemctl(*args):
    return subprocess.run(["systemctl", "--user", *args], capture_output=True, text=True, check=True)


def info(service):
    result = systemctl("show", SERVICES[service]["unit"], "--property=ActiveState,MainPID,NRestarts,FragmentPath,DropInPaths")
    return dict(line.split("=", 1) for line in result.stdout.splitlines() if "=" in line)


def executable(path):
    with Path(path).open("rb") as f:
        digest = hashlib.file_digest(f, "sha256").hexdigest()
    return digest


def prepare(service):
    s = SERVICES[service]
    live = info(service)
    if live["ActiveState"] != "active" or live["MainPID"] == "0":
        raise SystemExit("Expected an active old service before preparing " + service)
    current = systemctl("cat", s["unit"]).stdout
    reviewed = (BASE / "rollback" / s["unit"]).read_text()
    def normalized(text):
        text = re.sub(r"\x1b\[[0-9;]*m", "", text)
        return "\n".join(line.strip() for line in text.splitlines() if line.strip() and not line.lstrip().startswith("#"))
    if hashlib.sha256(reviewed.encode()).hexdigest() != s["original_unit_sha256"] or normalized(current) != normalized(reviewed):
        raise SystemExit("Live unit changed since preparation; inspect before switching " + service)
    if executable(s["executable"]) != s["binary_sha256"]:
        raise SystemExit("Staged binary changed: " + service)
    path = ROOT / service
    path.mkdir(mode=0o700, parents=True, exist_ok=True)
    write_private(path / "old-merged.service", current)
    fragment = Path(live["FragmentPath"])
    shutil.copy2(fragment, path / "old-fragment.service")
    dropins = []
    for value in shlex.split(live["DropInPaths"]):
        source = Path(value)
        backup = path / "old-dropins" / source.name
        backup.parent.mkdir(mode=0o700, exist_ok=True)
        shutil.copy2(source, backup)
        dropins.append(dict(path=str(source), backup=str(backup)))
    staged = Path(s["staged_unit"]).read_text()
    start = staged.rfind("\n[Unit]\n")
    if start < 0:
        raise SystemExit("Missing reviewed storage overlay")
    overlay = staged[start + 1:]
    original_commands = [line[10:] for line in current.splitlines() if line.startswith("ExecStart=") and line != "ExecStart="]
    if not original_commands:
        raise SystemExit("Old command absent")
    args = shlex.split(original_commands[-1])
    args[0] = s["executable"]
    overlay_lines = overlay.splitlines()
    for i, line in enumerate(overlay_lines):
        if line.startswith("ExecStart=") and line != "ExecStart=":
            overlay_lines[i] = "ExecStart=" + shlex.join(args)
    overlay = "\n".join(overlay_lines) + "\n"
    write_private(path / "mysql-overlay.conf", overlay)
    fallback = BASE / "rollback/binaries.before-startup-query-fix" / Path(s["executable"]).name
    if not fallback.is_file():
        raise SystemExit("MySQL-compatible fallback missing")
    fallback_overlay = overlay.replace(s["executable"], str(fallback))
    write_private(path / "mysql-fallback-overlay.conf", fallback_overlay)
    save(path / "state.json", dict(service=service, database=s["database"], phase="prepared",
                                   at_utc=now(), old_service=live, dropins=dropins,
                                   new_binary_sha256=s["binary_sha256"], mysql_fallback=str(fallback),
                                   mysql_fallback_sha256=executable(fallback), producer_activated=False,
                                   history_complete=False))
    print(service, "prepared; live source configuration retained; MySQL fallback saved")


def stop(service):
    path = ROOT / service
    state = json.loads((path / "state.json").read_text())
    if state["phase"] != "prepared":
        raise SystemExit("Service not ready for old stop")
    live = info(service)
    if live["MainPID"] != state["old_service"]["MainPID"]:
        raise SystemExit("Old process changed before stop")
    state.update(phase="stopping_old", stop_requested_at_utc=now())
    save(path / "state.json", state)
    result = systemctl("stop", SERVICES[service]["unit"])
    write_private(path / "stop-output.log", result.stdout + result.stderr)
    live = info(service)
    if live["MainPID"] != "0" or live["ActiveState"] != "inactive":
        raise SystemExit("Old service did not exit cleanly")
    state.update(phase="old_stopped_tail_required", old_stopped_at_utc=now(), stopped_service=live)
    # Capture current pending/cooldown/scan state only after its writer exits.
    dirs = {"lst": REPO / "var/lst/state", "justlend-keeper": REPO / "var/justlend-keeper/state"}
    if service in dirs:
        shutil.copytree(dirs[service], path / "state-after-stop", symlinks=True)
        state["state_backup"] = str(path / "state-after-stop")
    save(path / "state.json", state)
    print(service, "old process exited; fixed tail can now be synchronized")


def start(service):
    path = ROOT / service
    state = json.loads((path / "state.json").read_text())
    result = json.loads((path / "tail/result.json").read_text())
    if state["phase"] != "old_stopped_tail_required" or not result["startup_subset_fields_verified"]:
        raise SystemExit("Fixed startup tail not verified")
    recovery = json.loads((path / "startup-recovery.json").read_text())
    if not recovery.get("startup_recovery_passed") or recovery.get("public_api_requests", 0) or recovery.get("production_writes", 0):
        raise SystemExit("Actual startup recovery not verified")
    live = info(service)
    if live["MainPID"] != "0" or live["ActiveState"] != "inactive":
        raise SystemExit("Old writer still active")
    s = SERVICES[service]
    if executable(s["executable"]) != s["binary_sha256"]:
        raise SystemExit("New binary digest changed before activation")
    command = [str(BASE / "bin/storage-migrate"), "activate-producer", "--socket",
               str(REPO / "var/storage/mysql/run/mysql.sock"), "--user", "cmi_ddl", "--password-file",
               "/home/ubuntu/.config/crypto-market-info-storage/cmi_ddl-password", "--service", service,
               "--expected-generation", str(s["expected_generation"]), "--holder", s["holder"]]
    if not state.get("producer_activated"):
        activated = subprocess.run(command, capture_output=True, text=True, check=True)
        write_private(path / "activate-output.log", activated.stdout + activated.stderr)
        state.update(producer_activated=True, producer_activated_at_utc=now())
        save(path / "state.json", state)
    destination = Path(live["FragmentPath"]).with_name(s["unit"] + ".d") / "zz-redis-mysql-cutover.conf"
    write_private(destination, (path / "mysql-overlay.conf").read_text())
    systemctl("daemon-reload")
    # Other existing drop-ins may sort after numeric names (e.g. repair.conf).
    # Refuse to start unless systemd actually selected the reviewed executable.
    effective = systemctl("show", s["unit"], "--property=ExecStart", "--value").stdout
    if "path=" + s["executable"] + " ;" not in effective:
        raise SystemExit("Effective ExecStart differs from staged executable")
    systemctl("start", s["unit"])
    state.update(phase="new_started_validation_pending", new_started_at_utc=now(), new_service=info(service),
                 installed_overlay=str(destination))
    save(path / "state.json", state)
    print(service, "new process started; actual data validation pending")


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("action", choices=["prepare", "stop", "start"])
    p.add_argument("service", choices=SERVICES)
    args = p.parse_args()
    ROOT.mkdir(mode=0o700, parents=True, exist_ok=True)
    {"prepare": prepare, "stop": stop, "start": start}[args.action](args.service)


if __name__ == "__main__":
    main()
