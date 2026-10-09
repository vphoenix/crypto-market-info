#!/usr/bin/env python3
"""Install bounded syslog rotation and clear the explicitly approved log files.

Requires administrator authentication. Run only after the ClickHouse user unit
has been updated and its warning/non-console settings have been verified.
Market data, collector diagnostics, auth.log and other system logs are untouched.
"""

import argparse
import datetime
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import tempfile
import urllib.request


ROOT = Path(__file__).resolve().parents[2]
RECORD = ROOT / "var/deployments/clickhouse-logging-20261009"
TARGETS = tuple(Path("/var/log") / name for name in (
    "syslog", "syslog.1", "syslog.2.gz", "syslog.3.gz", "syslog.4.gz"
))


def run(*args):
    result = subprocess.run(args, capture_output=True, text=True, timeout=120)
    if result.returncode:
        raise RuntimeError(f"command failed: {args[0]}: {result.stderr[-2000:]}")
    return result


def atomic_write(path, data, mode=0o644):
    if path.is_symlink():
        raise RuntimeError(f"refusing symlink: {path}")
    fd, name = tempfile.mkstemp(prefix=path.name + ".", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as output:
            output.write(data)
            output.flush()
            os.fchmod(output.fileno(), mode)
            os.fsync(output.fileno())
        os.replace(name, path)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true", required=True)
    parser.parse_args()
    if os.geteuid() != 0:
        raise RuntimeError("administrator authentication required")

    # Refuse cleanup while the original noise source is still enabled.
    query = "SELECT name,value FROM system.server_settings WHERE name IN ('logger.level','logger.console') FORMAT JSON"
    request = urllib.request.Request("http://127.0.0.1:8123/?readonly=1", data=query.encode())
    with urllib.request.urlopen(request, timeout=15) as response:
        settings = {row["name"]: row["value"] for row in json.load(response)["data"]}
    if settings != {"logger.level": "warning", "logger.console": "0"}:
        raise RuntimeError(f"ClickHouse noise settings not yet disabled: {settings}")

    baseline = json.loads((RECORD / "before.json").read_text())
    known = {row["path"]: row for row in baseline["syslog_files"]}
    json.loads((RECORD / "diagnostics.json").read_text())
    for name in ("syslog.recent.before.gz", "syslog.old-tail.before.gz"):
        if (RECORD / name).stat().st_size == 0:
            raise RuntimeError("diagnostic backup is empty")

    # Check every target before any cleanup. A rotation changes the inode, so
    # do not silently delete a new file that was absent from the approved plan.
    present = {}
    for path in TARGETS:
        if not path.exists():
            continue
        info = path.lstat()
        original = known.get(str(path))
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise RuntimeError(f"refusing non-regular or linked file: {path}")
        if original is None or (info.st_ino, info.st_dev) != (original["inode"], original["device"]):
            raise RuntimeError(f"log was replaced since baseline; resample before cleanup: {path}")
        present[path] = info

    active = Path("/var/log/syslog")
    if active not in present:
        raise RuntimeError("active syslog is missing")

    backup = Path("/var/backups/crypto-market-info-clickhouse-logging-20261009")
    backup.mkdir(mode=0o700, parents=True, exist_ok=True)
    original_policy = Path("/etc/logrotate.d/rsyslog")
    policy = original_policy.read_bytes()
    saved_policy = backup / "rsyslog.before.conf"
    if not saved_policy.exists():
        atomic_write(saved_policy, policy, 0o600)
    lines = policy.decode().splitlines(keepends=True)
    matches = [line for line in lines if line.strip() == "/var/log/syslog"]
    dedicated = Path("/etc/logrotate.d/crypto-market-info-syslog")
    if len(matches) != 1:
        if len(matches) != 0 or not dedicated.exists():
            raise RuntimeError("unexpected existing rsyslog rotation policy")
    else:
        atomic_write(original_policy, "".join(line for line in lines if line.strip() != "/var/log/syslog").encode())
    atomic_write(dedicated, (ROOT / "deploy/logrotate/crypto-market-info-syslog").read_bytes())
    try:
        run("/usr/sbin/logrotate", "--debug", "/etc/logrotate.conf")
    except Exception:
        atomic_write(original_policy, policy)
        dedicated.unlink(missing_ok=True)
        raise

    for name in ("crypto-market-info-syslog-rotate.service", "crypto-market-info-syslog-rotate.timer"):
        atomic_write(Path("/etc/systemd/system") / name, (ROOT / "deploy/systemd" / name).read_bytes())
    run("systemctl", "daemon-reload")
    disk_before = shutil.disk_usage("/var/log")
    # Keep the live inode so rsyslog continues writing into the visible file.
    fd = os.open(active, os.O_WRONLY | os.O_NOFOLLOW | os.O_CLOEXEC)
    try:
        info = os.fstat(fd)
        if (info.st_ino, info.st_dev) != (present[active].st_ino, present[active].st_dev):
            raise RuntimeError("active syslog rotated before truncation")
        os.ftruncate(fd, 0)
        os.fsync(fd)
    finally:
        os.close(fd)
    removed = []
    for path, previous in present.items():
        if path == active:
            continue
        info = path.lstat()
        if (info.st_ino, info.st_dev) != (previous.st_ino, previous.st_dev):
            raise RuntimeError(f"rotated archive changed during cleanup: {path}")
        path.unlink()
        removed.append(str(path))
    disk_after = shutil.disk_usage("/var/log")
    result = {
        "completed_at_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "clickhouse_settings": settings,
        "truncated": str(active),
        "removed": removed,
        "cleared_logical_bytes": sum(info.st_size for info in present.values()),
        "cleared_allocated_bytes": sum(info.st_blocks * 512 for info in present.values()),
        "filesystem_free_before": disk_before.free,
        "filesystem_free_after": disk_after.free,
        "filesystem_bytes_recovered": disk_after.free - disk_before.free,
        "syslog_after_bytes": active.stat().st_size,
        "rotation_backup": str(saved_policy),
    }
    # Record deletion before starting the timer so an activation failure cannot
    # conceal a completed cleanup. The timer does not force a rotation.
    atomic_write(RECORD / "root-cleanup.json", json.dumps(result, indent=2).encode())
    run("systemctl", "enable", "--now", "crypto-market-info-syslog-rotate.timer")
    print(json.dumps(result))


if __name__ == "__main__":
    main()
