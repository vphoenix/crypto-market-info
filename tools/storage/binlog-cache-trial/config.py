#!/usr/bin/env python3
"""Persist or restore this trial's one reviewed configuration option."""
import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile

ROOT = Path('/home/ubuntu/crypto-market-info')
OUT = ROOT / 'var/storage/write-backlog-improvement/binlog-cache-4m'
CONFIG = ROOT / 'deploy/storage/mysql.cnf'

def sha(data): return hashlib.sha256(data).hexdigest()

def replace(path, data, mode=0o600):
    with tempfile.NamedTemporaryFile(dir=path.parent, prefix='.cmi-binlog-', delete=False) as stream:
        temp = Path(stream.name)
        stream.write(data)
        os.fchmod(stream.fileno(), mode)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temp, path)
    fd = os.open(path.parent, os.O_DIRECTORY)
    try: os.fsync(fd)
    finally: os.close(fd)

def save(name, value): replace(OUT / name, (json.dumps(value, indent=2)+'\n').encode())

def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=['keep', 'rollback'])
    args = parser.parse_args()
    state_path = OUT / 'config-prepared.json'
    if args.action == 'keep':
        assert not state_path.exists(), 'configuration trial already recorded'
        tool = json.loads((OUT / 'tool.json').read_text())
        binary = OUT / 'binlog-cache-trial'
        assert str(binary) == tool['binary'] and sha(binary.read_bytes()) == tool['sha256']
        current = json.loads(subprocess.check_output([str(binary), '--action', 'inspect'], timeout=45))
        assert set(current['Servers']) == {'production', 'isolated'}
        assert all(v['CacheBytes'] == 4 << 20 and v['FlushAtCommit'] == v['SyncBinlog'] == 1
                   and v['Doublewrite'] == 'ON' for v in current['Servers'].values())
        applied = json.loads((OUT / 'apply.json').read_text())
        assert dt.datetime.now(dt.timezone.utc) >= dt.datetime.fromisoformat(applied['FreshSessionsAfter'])
        old = CONFIG.read_bytes()
        assert b'binlog-cache-size' not in old and old.count(b'sync-binlog=1\n') == 1
        new = old.replace(b'sync-binlog=1\n', b'sync-binlog=1\n# Bounded whole-transaction cache; binlog durability remains synchronous.\nbinlog-cache-size=4M\n', 1)
        mode = CONFIG.stat().st_mode & 0o777
        state = {'path': str(CONFIG), 'before_sha256': sha(old), 'after_sha256': sha(new), 'mode': mode}
        replace(OUT / 'config-before.cnf', old)
        save('config-prepared.json', state)
        assert sha(CONFIG.read_bytes()) == state['before_sha256']
        replace(CONFIG, new, mode)
        save('config-kept.json', {'utc': dt.datetime.now(dt.timezone.utc).isoformat(), **state,
                                  'database_restarted': False, 'capacity_passed': False})
    else:
        state = json.loads(state_path.read_text())
        assert state['path'] == str(CONFIG)
        current = sha(CONFIG.read_bytes())
        assert current in [state['before_sha256'], state['after_sha256']], 'configuration changed outside this trial'
        old = (OUT / 'config-before.cnf').read_bytes()
        assert sha(old) == state['before_sha256']
        if current != state['before_sha256']: replace(CONFIG, old, state['mode'])
        save('config-rolled-back.json', {'utc': dt.datetime.now(dt.timezone.utc).isoformat(),
                                        'online_setting_rollback': 'run binlog-cache-trial --action rollback separately'})
    print(json.dumps({'action': args.action, 'configuration': str(CONFIG), 'database_restarted': False}))

if __name__ == '__main__': run()
