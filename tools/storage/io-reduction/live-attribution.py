#!/usr/bin/env python3
"""Read-only host I/O attribution. Run on the host, not in a PID sandbox.

Writes diagnostic artifacts only. Never changes services, database settings,
queues, tracing configuration, or credentials. No process argv/SQL values saved.
"""
import argparse
import concurrent.futures
import datetime as dt
import importlib.util
import json
import os
from pathlib import Path
import sys
import time
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / 'tools/storage/backlog-validation'))
import mysql_client

_redis_spec = importlib.util.spec_from_file_location('io_reduction_redis', Path(__file__).with_name('observe.py'))
_redis = importlib.util.module_from_spec(_redis_spec)
_redis_spec.loader.exec_module(_redis)


def utc():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def snapshot(device):
    result = {'utc': utc(), 'clock': time.monotonic(), 'processes': {}, 'denied': 0,
              'disk': [int(x) for x in Path('/sys/block', device, 'stat').read_text().split()]}
    for path in Path('/proc').iterdir():
        if not path.name.isdigit():
            continue
        try:
            stat = (path / 'stat').read_text().rsplit(')', 1)[1].split()
            counters = {k: int(v) for k, v in
                        (line.split(':', 1) for line in (path / 'io').read_text().splitlines())}
            result['processes'][path.name] = {
                'name': (path / 'comm').read_text().strip(), 'ppid': int(stat[1]),
                'start': stat[19], 'state': stat[0], 'major_faults': int(stat[9]),
                'block_delay_ticks': int(stat[39]), **counters}
            if result['processes'][path.name]['name'] in ('mysqld', 'redis-server', 'codex', 'collector', 'storage-writer'):
                try:
                    result['processes'][path.name]['memory_KiB'] = {
                        key: int(value.split()[0])
                        for key, value in (line.split(':', 1) for line in (path / 'status').read_text().splitlines() if ':' in line)
                        if key in ('VmRSS', 'VmSwap')}
                except OSError:
                    pass
        except (OSError, ValueError, IndexError):
            result['denied'] += 1
    result['swap'] = {k: int(v) for k, v in
                      (line.split() for line in Path('/proc/vmstat').read_text().splitlines())
                      if k in ('pswpin', 'pswpout', 'pgmajfault')}
    result['io_pressure'] = Path('/proc/pressure/io').read_text()
    result['memory_KiB'] = {
        key: int(value.split()[0])
        for key, value in (line.split(':', 1) for line in Path('/proc/meminfo').read_text().splitlines())
        if key in ('MemTotal', 'MemAvailable', 'SwapTotal', 'SwapFree', 'SwapCached')}
    result['memory_pressure'] = Path('/proc/pressure/memory').read_text()
    return result


def database_snapshot():
    result = {'utc': utc()}
    # These are in-memory instrumentation tables, not historical fact scans.
    queries = {
        'files': "SELECT JSON_OBJECT('file',FILE_NAME,'writes',COUNT_WRITE,'write_bytes',SUM_NUMBER_OF_BYTES_WRITE,'reads',COUNT_READ,'read_bytes',SUM_NUMBER_OF_BYTES_READ,'read_ps',SUM_TIMER_READ,'write_ps',SUM_TIMER_WRITE) FROM performance_schema.file_summary_by_instance WHERE COUNT_READ>0 OR COUNT_WRITE>0",
        'waits': "SELECT JSON_OBJECT('event',EVENT_NAME,'count',COUNT_STAR,'wait_ps',SUM_TIMER_WAIT) FROM performance_schema.events_waits_summary_global_by_event_name WHERE COUNT_STAR>0 AND (EVENT_NAME LIKE 'wait/io/file/%' OR EVENT_NAME LIKE 'wait/lock/%')",
        'sessions': "SELECT JSON_OBJECT('id',ID,'user',USER,'database',DB,'command',COMMAND,'seconds',TIME,'state',STATE) FROM information_schema.PROCESSLIST WHERE COMMAND!='Sleep'",
    }
    for label, query in queries.items():
        try:
            raw = mysql_client.sql('production', 'SET SESSION max_execution_time=2000;' + query, limit=5)
            result[label] = [json.loads(line) for line in raw.splitlines()]
        except Exception as error:
            result[label + '_error'] = type(error).__name__
    try:
        persistence = dict(line.split(':', 1) for line in _redis.redis('INFO', 'persistence').splitlines() if ':' in line)
        result['redis'] = {key: persistence[key] for key in (
            'aof_enabled', 'aof_rewrites', 'aof_rewrite_in_progress',
            'aof_last_bgrewrite_status', 'aof_last_write_status',
            'aof_current_size', 'aof_base_size', 'aof_delayed_fsync') if key in persistence}
        result['redis']['config'] = _redis.redis('CONFIG', 'GET', 'auto-aof-rewrite-*', 'appendfsync')
    except Exception as error:
        result['redis_error'] = type(error).__name__
    return result


def clickhouse_snapshot():
    result = {'utc': utc()}
    query = ('SELECT database,table,elapsed,progress,rows_read,rows_written,'
             'bytes_read_uncompressed,bytes_written_uncompressed '
             'FROM system.merges LIMIT 32 FORMAT JSONEachRow')
    params = urllib.parse.urlencode({'readonly': 1, 'max_threads': 1,
                                    'max_execution_time': 2, 'max_memory_usage': 33554432})
    try:
        request = urllib.request.Request('http://127.0.0.1:8123/?' + params, data=query.encode())
        with urllib.request.urlopen(request, timeout=4) as response:
            raw = response.read(262144)
        result['merges'] = [json.loads(line) for line in raw.splitlines()]
    except Exception as error:
        result['error'] = type(error).__name__
    return result


def summarize(rows):
    first, last = rows[0], rows[-1]
    seconds = last['clock'] - first['clock']
    if seconds <= 0:
        return {'error': 'insufficient interval'}
    delta = [b-a for a, b in zip(first['disk'], last['disk'])]
    processes = []
    for pid, current in last['processes'].items():
        old = first['processes'].get(pid)
        if old is None or current['start'] != old['start']:
            continue
        row = {'pid': pid, 'name': current['name'], 'ppid': current['ppid']}
        for key in ('read_bytes', 'write_bytes', 'cancelled_write_bytes'):
            row[key + '_MiB_s'] = (current[key] - old[key]) / seconds / 2**20
        row['major_faults'] = current['major_faults'] - old['major_faults']
        if 'memory_KiB' in current:
            row['memory_KiB_start'] = old.get('memory_KiB', {})
            row['memory_KiB_end'] = current['memory_KiB']
        row['block_delay_seconds'] = (current['block_delay_ticks'] - old['block_delay_ticks']) / os.sysconf('SC_CLK_TCK')
        if row['read_bytes_MiB_s'] or row['write_bytes_MiB_s'] or row['major_faults']:
            processes.append(row)
    processes.sort(key=lambda p: p['read_bytes_MiB_s'] + p['write_bytes_MiB_s'], reverse=True)
    return {'start': first['utc'], 'end': last['utc'], 'seconds': seconds,
            'disk_busy_pct': delta[9] / seconds / 10,
            'disk_read_MiB_s': delta[2] * 512 / seconds / 2**20,
            'disk_write_MiB_s': delta[6] * 512 / seconds / 2**20,
            'read_await_ms': delta[3] / delta[0] if delta[0] else 0,
            'write_await_ms': delta[7] / delta[4] if delta[4] else 0,
            'queue_depth': delta[10] / seconds / 1000,
            'swap_in_KiB_s': (last['swap']['pswpin']-first['swap']['pswpin']) * os.sysconf('SC_PAGE_SIZE') / seconds / 1024,
            'swap_out_KiB_s': (last['swap']['pswpout']-first['swap']['pswpout']) * os.sysconf('SC_PAGE_SIZE') / seconds / 1024,
            'memory_KiB_start': first.get('memory_KiB', {}),
            'memory_KiB_end': last.get('memory_KiB', {}),
            'unreadable_processes': last['denied'], 'processes': processes[:25],
            'limitations': ['Process counters are not additive physical disk attribution.',
                            'Do not count reaped Redis children again in parent totals.',
                            'Cancelled writes and short-lived processes require interval evidence.',
                            'Rotating MySQL redo filenames cannot be differenced as stable files.']}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--seconds', type=int, default=120)
    parser.add_argument('--device', default='sda', choices=['sda'])
    args = parser.parse_args()
    if not 10 <= args.seconds <= 300:
        parser.error('seconds must be between 10 and 300')
    probe = snapshot(args.device)
    if not any(p['name'] == 'mysqld' for p in probe['processes'].values()):
        raise SystemExit('Host mysqld is not visible/readable. Run this command in the server host terminal; no diagnosis was performed.')
    out = ROOT / 'var/storage/io-attribution' / dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    out.mkdir(parents=True, exist_ok=False, mode=0o700)
    print('Output: ' + str(out), flush=True)
    rows, databases, merges = [probe], [], []
    start = time.monotonic()
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        db_future = pool.submit(database_snapshot)
        ch_future = pool.submit(clickhouse_snapshot)
        last_database, last_merge = start, start
        while time.monotonic() - start < args.seconds:
            time.sleep(1)
            rows.append(snapshot(args.device))
            now = time.monotonic()
            if db_future.done() and now - last_database >= 30:
                databases.append(db_future.result())
                db_future = pool.submit(database_snapshot)
                last_database = now
            if ch_future.done() and now - last_merge >= 10:
                merges.append(ch_future.result())
                ch_future = pool.submit(clickhouse_snapshot)
                last_merge = now
        databases.append(db_future.result())
        merges.append(ch_future.result())
    databases.append(database_snapshot())
    for name, value in [('samples', rows), ('mysql', databases), ('clickhouse', merges), ('summary', summarize(rows))]:
        (out / (name + '.json')).write_text(json.dumps(value, indent=2) + '\n')
    print(json.dumps(summarize(rows), indent=2), flush=True)
    print('Saved: ' + str(out), flush=True)


if __name__ == '__main__':
    main()
