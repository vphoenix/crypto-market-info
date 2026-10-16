#!/usr/bin/env python3
"""Resume the explicitly authorized 20261016 offline historical migration.

Uses the reviewed migration CLI without changing source data, conflict rules,
or completion gates. A failed table does not prevent independent tables copying.
"""
import datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import selectors
import signal
import subprocess
import sys
import time
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[2]
BASE = ROOT / 'var/storage/history-migration/20261016'
sys.path.insert(0, str(ROOT / 'tools/storage/backlog-validation'))
import mysql_client as mysql

SERVICES = ['collector', 'across', 'reserve', 'lst', 'justlend-keeper', 'storage-writer']
PRODUCTION = ['crypto_market_info', 'crypto_market_info_across',
              'crypto_market_info_reserve', 'crypto_market_info_lst',
              'crypto_market_info_justlend_keeper']
child = None


def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def save(path, data):
    tmp = path.with_suffix(path.suffix + '.tmp')
    with tmp.open('w') as f:
        json.dump(data, f, indent=2)
        f.write('\n')
        f.flush()
        os.fsync(f.fileno())
    tmp.replace(path)


def digest(path):
    with path.open('rb') as f:
        return hashlib.file_digest(f, 'sha256').hexdigest()


def source_rows(sql):
    params = urllib.parse.urlencode(dict(readonly=1, max_threads=1,
        max_execution_time=15, max_memory_usage=134217728))
    request = urllib.request.Request('http://127.0.0.1:8123/?' + params,
                                    data=(sql + ' FORMAT JSONEachRow').encode())
    with urllib.request.urlopen(request, timeout=20) as response:
        return [json.loads(line) for line in response.read().splitlines()]


def source_signature(databases):
    allowed = ','.join("'" + n + "'" for n in databases)
    # Ordinary merges preserve the upper insert block and mutation version.
    # Any insert or mutation in a frozen partition invalidates this run window.
    return {
        'blocks': source_rows('SELECT database,table,partition_id,max(max_block_number) AS last_block,'
            'max(data_version) AS data_version FROM system.parts WHERE active AND database IN ('
            + allowed + ') GROUP BY database,table,partition_id ORDER BY database,table,partition_id'),
        'ddl': source_rows('SELECT database,name,create_table_query FROM system.tables WHERE database IN ('
            + allowed + ') ORDER BY database,name'),
        'mutations': source_rows('SELECT database,table,mutation_id,is_done FROM system.mutations WHERE database IN ('
            + allowed + ') ORDER BY database,table,mutation_id'),
    }


def guard(identity, full=False):
    units = ['crypto-market-info-' + n + '.service' for n in SERVICES]
    result = subprocess.run(['systemctl', '--user', 'show', '--property=ActiveState', '--value'] + units,
                            capture_output=True, text=True, timeout=10, check=True)
    states = result.stdout.split()
    if len(states) != len(units) or any(s not in ('inactive', 'failed') for s in states):
        raise RuntimeError('collector/writer active or service status unknown; offline migration paused')
    fs = os.statvfs(BASE)
    if fs.f_bavail * fs.f_frsize < 50 * 2**30:
        raise RuntimeError('free disk below 50 GiB reserve; migration paused')
    if full:
        mysql.verify('production')
        if source_signature(identity['databases']) != identity['source_signature']:
            raise RuntimeError('frozen source insert/mutation/DDL boundary changed; migration paused')


def stop(signum, frame):
    raise InterruptedError('migration supervisor interrupted; committed checkpoints retained')


def command(identity, phase, table=None):
    args = [str(BASE / 'bin/storage-migrate'), phase, '--run', identity['run'],
            '--socket', str(mysql.SERVERS['production'][0]), '--user', 'cmi_migrate',
            '--password-file', '/home/ubuntu/.config/crypto-market-info-storage/cmi_migrate-password',
            '--target-read-timeout', '30s']
    if table:
        db, name = table['database'], table['table']
        stage = BASE / 'staging' / db / name
        stage.mkdir(parents=True, exist_ok=True)
        # A collection-plan row contains whole instrument/observation arrays;
        # use a smaller block to keep FINAL's merge buffers inside 128 MiB.
        rows = '64' if name == 'options_collection_plan' else '500'
        args += ['--source', '127.0.0.1:9000', '--family', db, '--target', db,
                 '--table', name, '--staging-dir', str(stage), '--rows', rows, '--delay', '250ms']
    return args


def run(identity, status, phase, table=None):
    global child
    guard(identity, full=True)
    label = table['database'] + '.' + table['table'] if table else 'run'
    status.update(phase=phase, current_table=label, updated_at_utc=now())
    save(BASE / 'status.json', status)
    print(now(), phase, label, flush=True)
    path = BASE / 'logs' / (label + '.' + phase + '.log')
    with path.open('ab', buffering=0) as output:
        output.write(('\n# attempt ' + now() + '\n').encode())
        child = subprocess.Popen(command(identity, phase, table), cwd=ROOT,
                                 stdout=subprocess.PIPE, stderr=output)
        os.set_blocking(child.stdout.fileno(), False)
        pending = b''
        last_guard = time.monotonic()
        last_full_guard = last_guard
        with selectors.DefaultSelector() as selector:
            selector.register(child.stdout, selectors.EVENT_READ)
            try:
                while selector.get_map():
                    for key, _ in selector.select(timeout=2):
                        data = os.read(key.fd, 65536)
                        if not data:
                            selector.unregister(key.fd)
                            continue
                        output.write(data)
                        pending += data
                        while b'\n' in pending:
                            line, pending = pending.split(b'\n', 1)
                            if phase == 'copy':
                                record = json.loads(line)
                                status['copy_chunk_events'] += 1
                                status['last_chunk'] = record
                                status['last_chunk_at_utc'] = now()
                    stamp = time.monotonic()
                    if stamp - last_guard >= 10:
                        full = stamp - last_full_guard >= 60
                        guard(identity, full=full)
                        last_guard = stamp
                        if full:
                            last_full_guard = stamp
                        status['updated_at_utc'] = now()
                        save(BASE / 'status.json', status)
                code = child.wait(timeout=40)
            finally:
                if child.poll() is None:
                    child.terminate()
                    try:
                        child.wait(timeout=45)
                    except subprocess.TimeoutExpired:
                        child.kill()
                        child.wait()
                child.stdout.close()
                child = None
    if code:
        status['errors'].append(dict(phase=phase, table=label, log=str(path), at_utc=now()))
    save(BASE / 'status.json', status)
    return code == 0


def main():
    os.umask(0o077)
    lock = (BASE / 'supervisor.lock').open('a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    identity = json.loads((BASE / 'identity.json').read_text())
    assert identity['mysql_uuid'] == mysql.SERVERS['production'][1]
    assert digest(BASE / 'bin/storage-migrate') == identity['binary_sha256']
    assert digest(ROOT / 'internal/storage/mysql/mapping.json') == identity['mapping_sha256']
    catalog = json.loads((ROOT / 'internal/storage/mysql/mapping.json').read_text())['tables']
    assert len(catalog) == 125 and sorted({t['database'] for t in catalog}) == identity['databases']
    inventory = json.loads((BASE / 'source-inventory.json').read_text())
    rows = {}
    for p in inventory['parts']:
        key = (p['database'], p['table'])
        rows[key] = rows.get(key, 0) + int(p['rows'])
    # Finish independent production families first; archived validation databases last.
    catalog.sort(key=lambda t: (t['database'] not in PRODUCTION,
        rows.get((t['database'], t['table']), 0), t['database'], t['table']))
    status = dict(run=identity['run'], started_at_utc=now(), phase='starting',
                  planned_tables=0, copied_tables=0, verified_tables=0, copy_chunk_events=0,
                  errors=[], full_history_enabled=False, collectors_started=False)
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, stop)
    try:
        planned = []
        for table in catalog:
            if run(identity, status, 'plan', table):
                planned.append(table)
                status['planned_tables'] += 1
        copied = []
        # Metadata counters are advisory; CLI/database checkpoints are authoritative
        # on every retry, including a supervisor killed after a committed chunk.
        for table in planned:
            if run(identity, status, 'copy', table):
                copied.append(table)
                status['copied_tables'] += 1
        for table in copied:
            if run(identity, status, 'verify', table):
                status['verified_tables'] += 1
        run(identity, status, 'report')
        if status['verified_tables'] == 125 and not status['errors']:
            # Existing accepted source gaps are an unconditional database-side
            # blocker; never bypass them or infer completion from row counts.
            status['full_history_enabled'] = run(identity, status, 'finalize')
        status['phase'] = 'complete' if status['full_history_enabled'] else 'finished_with_gaps_or_errors'
    except Exception as error:
        status.update(phase='paused', pause_reason=str(error))
        print(now(), 'paused:', error, flush=True)
    finally:
        status['updated_at_utc'] = now()
        save(BASE / 'status.json', status)
    return 0 if status['phase'] == 'complete' else 1


if __name__ == '__main__':
    sys.exit(main())
