#!/usr/bin/env python3
"""Finite observation of the pinned trial; rollback on sustained redo pressure."""
import argparse
import datetime as dt
import json
from pathlib import Path
import subprocess
import time

BASE = Path('/home/ubuntu/crypto-market-info/var/storage/write-backlog-improvement/mysql-io-capacity-200')
BINARY = BASE / 'mysql-io-trial'

def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--until', required=True)
    args = parser.parse_args()
    deadline = dt.datetime.fromisoformat(args.until)
    assert deadline.tzinfo is not None
    before = json.loads((BASE / 'apply.json').read_text())
    failures = pressure = 0
    while dt.datetime.now(dt.timezone.utc) < deadline:
        row = {'utc': dt.datetime.now(dt.timezone.utc).isoformat(), 'deadline': deadline.isoformat()}
        try:
            result = subprocess.run([str(BINARY), '--action', 'inspect'], capture_output=True, timeout=45)
            if result.returncode:
                raise RuntimeError(f'bounded inspection failed ({result.returncode})')
            inspection = json.loads(result.stdout)
            row['inspection'] = inspection
            unsafe = False
            for name, state in inspection['Servers'].items():
                old = before['Servers'][name]
                assert state['UUID'] == old['UUID']
                assert state['Capacity'] == 200 and state['Maximum'] == 400
                counters = state['Counters']
                age = counters['Innodb_redo_log_current_lsn'] - counters['Innodb_redo_log_checkpoint_lsn']
                unsafe |= age > state['RedoCapacity'] * 3 // 4
                if counters['Innodb_log_waits'] > old['Counters']['Innodb_log_waits']:
                    pressure = 3
            failures = 0
            pressure = pressure + 1 if unsafe else (pressure if pressure >= 3 else 0)
        except Exception as exc:
            failures += 1
            row['error'] = type(exc).__name__
        row.update(failures=failures, pressure_samples=pressure)
        with (BASE / 'observations.jsonl').open('a') as stream:
            stream.write(json.dumps(row) + '\n')
        (BASE / 'observation-latest.json').write_text(json.dumps(row, indent=2) + '\n')
        if failures >= 3 or pressure >= 3:
            outcome = {'utc': dt.datetime.now(dt.timezone.utc).isoformat(), 'trigger': row, 'rollback_confirmed': False}
            try:
                result = subprocess.run([str(BINARY), '--action', 'rollback'], capture_output=True, timeout=45)
                outcome.update(returncode=result.returncode, rollback_confirmed=result.returncode == 0,
                               error=result.stderr.decode(errors='replace')[:2000])
            except subprocess.TimeoutExpired:
                outcome['error'] = 'rollback command exceeded its bound; inspect both instances'
            (BASE / 'automatic-rollback.json').write_text(json.dumps(outcome, indent=2) + '\n')
            raise SystemExit(1)
        time.sleep(min(30, max(0, (deadline - dt.datetime.now(dt.timezone.utc)).total_seconds())))
    (BASE / 'watch-ended.json').write_text(json.dumps({'utc': dt.datetime.now(dt.timezone.utc).isoformat(), 'capacity_accepted': False}) + '\n')

if __name__ == '__main__':
    run()
