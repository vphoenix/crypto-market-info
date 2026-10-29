"""Read-only bounded MySQL access with a writer-backlog guard; results saved with SHA-256."""
import datetime as dt
import gzip
import hashlib
import json
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'tools/storage/backlog-validation'))
import mysql_client as mysql  # noqa: E402

WRITER_LOG = ROOT / 'var/storage/writer.log'


def writer_backlog():
    """Latest writer metrics line: (old_pending, unique_pending, local time)."""
    with open(WRITER_LOG, 'rb') as f:
        f.seek(0, 2)
        f.seek(max(0, f.tell() - 200_000))
        lines = f.read().decode(errors='replace').splitlines()
    for line in reversed(lines):
        if '"writer metrics"' in line:
            r = json.loads(line)
            return r['old_pending'], r['unique_pending'], r['time']
    return None, None, None


def guard(max_unique=300, max_wait=900):
    """Wait until the production writer has no old backlog and a small recent queue."""
    waited = 0
    while True:
        old, uniq, t = writer_backlog()
        if old == 0 and uniq is not None and uniq <= max_unique:
            return waited
        if waited >= max_wait:
            raise RuntimeError(f'writer backlog persists old={old} unique={uniq} at {t}')
        time.sleep(15)
        waited += 15


def run(sql, seconds=40):
    full = (f"SET SESSION time_zone='+00:00'; SET SESSION max_execution_time={seconds * 1000}; "
            f"START TRANSACTION READ ONLY; {sql}; ROLLBACK;")
    t0 = time.monotonic()
    r = subprocess.run(mysql.command('production') + ['--raw', '--skip-column-names', '-e', full],
                       env=mysql.environment(), capture_output=True, timeout=seconds + 20)
    if r.returncode:
        raise RuntimeError(r.stderr.decode(errors='replace')[:1500])
    return r.stdout, time.monotonic() - t0


def save(path, sql, raw, elapsed):
    path = Path(path)
    with gzip.open(path, 'wb') as f:
        f.write(raw)
    meta = {'sql': sql, 'sha256_uncompressed': hashlib.sha256(raw).hexdigest(), 'rows': len(raw.splitlines()),
            'elapsed_s': round(elapsed, 2), 'read_only': True,
            'saved_utc': dt.datetime.now(dt.timezone.utc).isoformat()}
    path.with_suffix('.meta.json').write_text(json.dumps(meta, indent=1) + '\n')
    return meta
