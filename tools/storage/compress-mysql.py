#!/usr/bin/env python3
"""Explicit offline conversion of the pinned project's MySQL tables.

Native compressed pages preserve all logical values and the existing migration
mapping identity. No collectors, historical migration, or source cleanup run here.
"""
import datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import selectors
import signal
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[2]
BASE = ROOT / 'var/storage/mysql-compression/20261016'
sys.path.insert(0, str(ROOT / 'tools/storage/backlog-validation'))
import mysql_client as mysql

IDENT = re.compile(r'^[a-zA-Z0-9_]+$')
SERVICES = ['collector', 'across', 'reserve', 'lst', 'justlend-keeper',
            'storage-writer', 'history-migration-20261016']
child = None
status = {}


def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def save(path, value):
    temp = path.with_suffix(path.suffix + '.tmp')
    with temp.open('w') as f:
        json.dump(value, f, indent=2)
        f.write('\n')
        f.flush()
        os.fsync(f.fileno())
    temp.replace(path)


def quote(name):
    if not IDENT.fullmatch(name):
        raise ValueError('invalid SQL identifier')
    return '`' + name + '`'


def qualified(database, table):
    return quote(database) + '.' + quote(table)


def guard():
    units = ['crypto-market-info-' + s + '.service' for s in SERVICES]
    r = subprocess.run(['systemctl', '--user', 'show', '--property=ActiveState', '--value'] + units,
                       capture_output=True, text=True, timeout=10, check=True)
    states = r.stdout.split()
    if len(states) != len(units) or any(s not in ('inactive', 'failed') for s in states):
        raise RuntimeError('collectors, writer and historical migration must remain stopped')
    mysql.verify('production')
    if mysql.sql('production', 'SELECT @@log_bin,@@innodb_flush_log_at_trx_commit,@@innodb_doublewrite') != '0\t1\tON':
        raise RuntimeError('binary logging/durability configuration changed')
    fs = os.statvfs(BASE)
    if fs.f_bavail * fs.f_frsize < 50 * 2**30:
        raise RuntimeError('conversion paused: less than 50 GiB free')


def stream(sql, label, fingerprint=False):
    global child
    status.update(phase=label, updated_at_utc=now(), fingerprint_rows=0)
    save(BASE / 'status.json', status)
    digest = hashlib.sha256()
    count = total_bytes = 0
    captured = bytearray()
    last_guard = time.monotonic()
    started = time.monotonic()
    with (BASE / 'commands.log').open('a') as log:
        log.write(now() + ' ' + label + ' ' + status.get('current_table', '') + '\n')
    with (BASE / 'errors.log').open('ab', buffering=0) as errors:
        child = subprocess.Popen(mysql.command('production') + ['--quick', '--raw'],
                                 env=mysql.environment(), stdin=subprocess.PIPE,
                                 stdout=subprocess.PIPE, stderr=errors)
        child.stdin.write(sql.encode())
        child.stdin.close()
        os.set_blocking(child.stdout.fileno(), False)
        with selectors.DefaultSelector() as selector:
            selector.register(child.stdout, selectors.EVENT_READ)
            try:
                while selector.get_map():
                    for key, _ in selector.select(timeout=2):
                        raw = os.read(key.fd, 65536)
                        if not raw:
                            selector.unregister(key.fd)
                            continue
                        if fingerprint:
                            count += raw.count(b'\n')
                            total_bytes += len(raw)
                            digest.update(raw)
                        else:
                            captured.extend(raw)
                            if len(captured) > 65536:
                                raise RuntimeError('unexpected unbounded administrative output')
                    if time.monotonic() - last_guard >= 10:
                        guard()
                        status.update(fingerprint_rows=count, updated_at_utc=now())
                        save(BASE / 'status.json', status)
                        last_guard = time.monotonic()
                    if time.monotonic() - started > 4 * 3600:
                        raise RuntimeError('single operation exceeded four-hour budget')
                code = child.wait(timeout=30)
            finally:
                if child.poll() is None:
                    child.terminate()
                    try:
                        child.wait(timeout=30)
                    except subprocess.TimeoutExpired:
                        child.kill()
                        child.wait()
                child.stdout.close()
                child = None
    if code:
        raise RuntimeError('MySQL operation failed; inspect errors.log')
    if fingerprint and total_bytes != count * 65:
        raise RuntimeError('incomplete or invalid row fingerprint stream')
    return {'rows': count, 'sha256': digest.hexdigest(),
            'seconds': time.monotonic() - started,
            'output': captured.decode() if not fingerprint else ''}


def fingerprint(database, table, columns, keys, phase, sample=False):
    # Hash exact stored bytes (or exact scalar SQL representation), with an
    # explicit null marker. Fixed SHA-256 fields frame each nonnull column.
    # The ordered outer digest includes every row, including duplicates.
    fields = ["IF(" + quote(c) + " IS NULL,'N',CONCAT('V',SHA2(CAST(" + quote(c)
              + " AS BINARY),256)))" for c in columns]
    name = qualified(database, table)
    projection = 'SELECT SHA2(CONCAT(' + ','.join(fields) + '),256) FROM ' + name
    order = ','.join(quote(k) for k in keys)
    prefix = 'SET SESSION max_execution_time=14400000; '
    if not sample:
        return stream(prefix + projection + ' FORCE INDEX(PRIMARY) ORDER BY ' + order + ';', phase, fingerprint=True)
    # Native atomic ALTER copies every record. Verify exact total rows plus
    # complete values at the beginning, end and an interior key boundary;
    # avoid repeated full BLOB scans that would dominate the actual rebuild.
    total = stream(prefix + 'SELECT COUNT(*) FROM ' + name + ';', phase + '_count')
    count = int(total['output'].strip())
    queries = [projection + ' FORCE INDEX(PRIMARY) ORDER BY ' + order + ' LIMIT 256',
               projection + ' FORCE INDEX(PRIMARY) ORDER BY ' + ','.join(quote(k) + ' DESC' for k in keys) + ' LIMIT 256']
    kind = mysql.sql('production', "SELECT DATA_TYPE FROM information_schema.columns WHERE TABLE_SCHEMA='"
                     + database + "' AND TABLE_NAME='" + table + "' AND COLUMN_NAME='" + keys[0] + "'")
    key = quote(keys[0])
    boundary = None
    if kind in ('tinyint', 'smallint', 'mediumint', 'int', 'bigint') and count:
        pivot = mysql.sql('production', 'SELECT CAST(FLOOR((CAST(MIN(' + key
            + ') AS DECIMAL(65,0))+CAST(MAX(' + key + ') AS DECIMAL(65,0)))/2) AS CHAR) FROM ' + name)
        boundary = key + '>=' + str(int(pivot))
    elif kind in ('binary', 'varbinary') and count:
        low, high = mysql.sql('production', 'SELECT HEX(MIN(' + key + ')),HEX(MAX(' + key + ')) FROM ' + name).split('\t')
        if len(low) == len(high) and low:
            length = len(low) // 2
            pivot = ((int(low, 16) + int(high, 16)) // 2).to_bytes(length, 'big').hex()
            boundary = key + ">=UNHEX('" + pivot + "')"
    if boundary:
        queries.append(projection + ' FORCE INDEX(PRIMARY) WHERE ' + boundary + ' ORDER BY ' + order + ' LIMIT 256')
    result = stream(prefix + ';'.join(queries) + ';', phase, fingerprint=True)
    result.update(sample_rows=result['rows'], rows=count, scope='exact_total_count_and_bounded_full_column_samples')
    return result


def formats(database, table):
    prefix = database + '/' + table
    # Exact base name or its native partition suffix; underscores are literal.
    sql = "SELECT NAME,ROW_FORMAT,ZIP_PAGE_SIZE FROM information_schema.innodb_tables WHERE NAME='"
    sql += prefix + "' OR LEFT(NAME," + str(len(prefix) + 3) + ")='" + prefix + "#p#' ORDER BY NAME"
    rows = mysql.sql('production', sql).splitlines()
    if not rows:
        raise RuntimeError('missing InnoDB table/partition metadata')
    return [r.split('\t') for r in rows]


def compressed(database, table):
    return all(r[1] == 'Compressed' and r[2] == '8192' for r in formats(database, table))


def allocated(database, table):
    total = 0
    for path in (ROOT / 'var/storage/mysql/data' / database).iterdir():
        if path.name == table + '.ibd' or (path.name.startswith(table + '#p#') and path.suffix == '.ibd'):
            total += path.stat().st_blocks * 512
    return total


def interrupt(signum, frame):
    raise InterruptedError('compression interrupted; atomic DDL and saved fingerprints retained')


def main():
    global status
    os.umask(0o077)
    BASE.mkdir(parents=True, exist_ok=True)
    lock = (BASE / 'conversion.lock').open('a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    for signum in (signal.SIGTERM, signal.SIGINT):
        signal.signal(signum, interrupt)
    manifest = json.loads((BASE / 'manifest.json').read_text())
    assert manifest['mysql_uuid'] == mysql.SERVERS['production'][1]
    mapping = ROOT / 'internal/storage/mysql/mapping.json'
    assert hashlib.sha256(mapping.read_bytes()).hexdigest() == manifest['mapping_sha256']
    statefile = BASE / 'status.json'
    status = json.loads(statefile.read_text()) if statefile.exists() else {
        'started_at_utc': now(), 'tables': {}, 'history_migration_started': False}
    try:
        guard()
        for table in manifest['tables']:
            database, name = table['database'], table['table']
            identity = database + '.' + name
            status.update(current_table=identity, updated_at_utc=now())
            entry = status['tables'].setdefault(identity, {})
            if entry.get('verified'):
                assert compressed(database, name), 'compressed format changed after conversion'
                continue
            guard()
            print(now(), identity, flush=True)
            if 'before' not in entry:
                entry['allocated_before'] = allocated(database, name)
                entry['before'] = fingerprint(database, name, table['columns'], table['keys'], 'fingerprint_before', sample=True)
                save(statefile, status)
            if not compressed(database, name):
                fs = os.statvfs(BASE)
                if fs.f_bavail * fs.f_frsize < 50 * 2**30 + 2 * entry['allocated_before']:
                    raise RuntimeError('insufficient space for bounded table rebuild')
                # Offline native rebuild installs the completed table atomically;
                # one DDL thread and a small sort buffer bound its resource use.
                sql = 'SET SESSION lock_wait_timeout=10; SET SESSION innodb_ddl_threads=1; '
                sql += 'SET SESSION innodb_ddl_buffer_size=16777216; ALTER TABLE ' + qualified(database, name)
                sql += ' ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8, ALGORITHM=INPLACE, LOCK=EXCLUSIVE;'
                entry['alter'] = stream(sql, 'compress')
                save(statefile, status)
            assert compressed(database, name), 'physical compressed format not installed'
            entry['after'] = fingerprint(database, name, table['columns'], table['keys'], 'fingerprint_after', sample=True)
            if any(entry['before'][key] != entry['after'][key] for key in ('rows', 'sha256')):
                raise RuntimeError('logical row fingerprint changed across physical compression')
            entry.update(verified=True, allocated_after=allocated(database, name),
                         physical_formats=formats(database, name), verified_at_utc=now())
            status['verified_tables'] = sum(bool(t.get('verified')) for t in status['tables'].values())
            save(statefile, status)
        guard()
        status.update(phase='complete', finished_at_utc=now())
    except Exception as error:
        status.update(phase='paused', error=str(error))
        print(now(), str(error), flush=True)
    finally:
        status['updated_at_utc'] = now()
        save(statefile, status)
    return 0 if status['phase'] == 'complete' else 1


if __name__ == '__main__':
    sys.exit(main())
