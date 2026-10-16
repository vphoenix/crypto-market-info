#!/usr/bin/env python3
"""Finite 24-hour deployment observation; writes private, bounded evidence.

No database writes, source requests, service restarts or history imports.
"""
import argparse
import datetime as dt
import json
import math
import os
import re
from pathlib import Path
import subprocess
import time
from metrics_guards import metrics_current

ROOT = Path('/home/ubuntu/crypto-market-info')
OUT = ROOT / 'var/storage/write-backlog-improvement'
UNITS = ['collector', 'storage-writer', 'justlend-keeper', 'across', 'reserve', 'lst']

def latency_summary(lags, members, expected_options, now):
    expected_minutes = set(range((int(now) // 60 - 16) * 60, (int(now) // 60 - 1) * 60, 60))
    expected = expected_options if isinstance(expected_options, dict) else {m: expected_options for m in expected_minutes}
    complete = {m: lag for m, lag in lags.items() if m in expected_minutes and expected.get(m, 0) > 0
                and members.get(m, {}).get('cex') == 337
                and members.get(m, {}).get('options_catalog') == expected[m]}
    values = sorted(complete.values())
    return dict(closed_minute_count=len(values),
        last_shard_p99_seconds=values[math.ceil(len(values) * .99) - 1] if values else None,
        latency_scope_complete=set(complete) == expected_minutes,
        latency_scope=dict(expected_cex_members=337,
            expected_options_members={dt.datetime.fromtimestamp(m, dt.timezone.utc).isoformat(): expected.get(m)
                                     for m in sorted(expected_minutes)},
            incomplete_minutes=[dict(minute_utc=dt.datetime.fromtimestamp(m, dt.timezone.utc).isoformat(),
                members=members.get(m, {})) for m in sorted(expected_minutes - set(complete))],
            proof='log member counts; exact identities and plans require fixture verification'))

def save_snapshot(path, row):
    temp = path.with_suffix('.tmp')
    temp.write_text(json.dumps(row, indent=2) + '\n')
    os.chmod(temp, 0o600)
    os.replace(temp, path)

def low_frequency_pending(services):
    out = {}
    for name in ['across', 'reserve', 'lst', 'justlend-keeper']:
        entry = {'state': 'requires_source_commit_audit'}
        pid = int(services[name]['MainPID'])
        if not pid:
            out[name] = {'state': 'service_stopped'}
            continue
        try:
            # Select only the state path; never expose other command arguments.
            args = Path(f'/proc/{pid}/cmdline').read_bytes().decode().split('\0')
            selected = None
            flag = '--state' if name == 'justlend-keeper' else '--state-dir'
            for i, arg in enumerate(args):
                if arg == flag and i + 1 < len(args): selected = args[i + 1]
                elif arg.startswith(flag + '='): selected = arg.split('=', 1)[1]
            if name in ['lst', 'justlend-keeper']:
                selected = selected or ('var/lst/state' if name == 'lst' else 'var/justlend-keeper/state')
                directory = Path(selected)
                if not directory.is_absolute(): directory = Path(f'/proc/{pid}/cwd').resolve() / directory
                path = directory / ('pending-batch.gob' if name == 'lst' else 'state.gob')
                entry['checkpoint_path'] = str(path)
                try:
                    st = path.stat()
                    entry.update(bytes=st.st_size, modified_utc=dt.datetime.fromtimestamp(st.st_mtime, dt.timezone.utc).isoformat())
                    entry['state'] = 'pending_file_present' if name == 'lst' else 'pending_projection_in_runtime_audit'
                except FileNotFoundError:
                    entry['state'] = 'no_pending_file' if name == 'lst' else 'checkpoint_missing'
            else:
                # These two runners synchronously commit typed envelopes and
                # have no Redis/spool/frozen-write queue. Source repair work is
                # different and must still be reported, never called zero.
                entry.update(state='direct_mysql_transactions', durable_pending_queue='not_used',
                             source_recovery_pending='requires_source_audit')
                fd = Path(f'/proc/{pid}/fd/2').resolve()
                if fd.is_file():
                    with fd.open('rb') as stream:
                        stream.seek(max(0, fd.stat().st_size - (128 << 10)))
                        lines = stream.read().decode(errors='replace').splitlines()
                else:
                    # Unit-filtered journal scans exceeded the finite probe
                    # deadline on this host. Do not repeat those disk scans
                    # every minute or manufacture an empty source backlog.
                    lines = []
                    entry['source_work_snapshot'] = 'not available from a bounded file tail; source audit required'
                for line in lines:
                    if name == 'across':
                        match = re.search(r'discovery_queue=(\d+) receipt_backlog=(\d+) cycle_ms=(\d+)', line)
                        if match:
                            entry['source_work_snapshot'] = dict(discovery_queue=int(match[1]),
                                receipt_backlog=int(match[2]), cycle_ms=int(match[3]), log_time_prefix=line[:19])
                    elif name == 'reserve':
                        match = re.search(r'snapshot block=(\d+) states=(\d+) quotes=(\d+) coverage=([a-z_]+)', line)
                        if match:
                            entry['last_committed_snapshot'] = dict(block=int(match[1]), states=int(match[2]),
                                quotes=int(match[3]), coverage=match[4], log_time_prefix=line[:19])
                    # Raw messages, endpoint URLs and source responses are not
                    # retained in diagnostics.
        except (OSError, ValueError, subprocess.SubprocessError) as error:
            entry = {'state': 'inspection_failed', 'error': str(error)[:160]}
        out[name] = entry
    return out

def physical_space():
    result = {'mysql_tables': 0, 'mysql_binlog': 0, 'mysql_redo': 0, 'redis_aof': 0}
    mysql_dir = ROOT / 'var/storage/mysql/data'
    for entry in mysql_dir.iterdir():
        if entry.is_file() and entry.name.startswith('cmi-bin.'):
            result['mysql_binlog'] += entry.stat().st_size
        elif entry.is_dir() and (entry.name.startswith('crypto_market_info') or entry.name == 'cmi_storage'):
            result['mysql_tables'] += sum(p.stat().st_size for p in entry.glob('*.ibd'))
        elif entry.name == '#innodb_redo' and entry.is_dir():
            result['mysql_redo'] += sum(p.stat().st_size for p in entry.iterdir() if p.is_file())
    aof = ROOT / 'var/storage/redis/data/appendonlydir'
    for path in aof.iterdir():
        try:
            if path.is_file(): result['redis_aof'] += path.stat().st_size
        except FileNotFoundError: pass  # AOF rewrite may replace files atomically.
    return result

def snapshot():
    result = {'utc': dt.datetime.now(dt.timezone.utc).isoformat()}
    latest = None
    lags = {}
    members = {}
    seen_batches = set()
    now = time.time()
    # The diagnostics log is bounded to 64 MiB. Only read the last 8 MiB.
    path = ROOT / 'var/storage/writer.log'
    with path.open('rb') as stream:
        stream.seek(max(0, path.stat().st_size - (8 << 20)))
        if stream.tell(): stream.readline()
        for line in stream:
            try: row = json.loads(line)
            except (ValueError, UnicodeDecodeError): continue
            if row.get('msg') == 'writer metrics': latest = row
            elif row.get('msg') == 'batch committed':
                committed = dt.datetime.fromisoformat(row['time']).timestamp()
                minute = round((committed - row['lag_seconds'] - 60) / 60) * 60
                if now - 17 * 60 <= minute < now - 2 * 60:
                    lags[minute] = max(lags.get(minute, 0), row['lag_seconds'])
                    identity = (row.get('database'), row.get('kind'), row.get('batch_id'))
                    if identity not in seen_batches:
                        seen_batches.add(identity)
                        group = members.setdefault(minute, {})
                        group[row.get('kind')] = group.get(row.get('kind'), 0) + row.get('members', 0)
    age=None
    if latest is not None:
        try: age=time.time()-dt.datetime.fromisoformat(latest['time']).timestamp()
        except (KeyError,TypeError,ValueError): pass
    result['writer_metrics_age_seconds']=age
    result['writer'] = latest
    values = sorted(lags.values())
    result['observed_minute_count'] = len(values)
    result['observed_last_shard_p99_seconds'] = values[math.ceil(len(values) * .99) - 1] if values else None
    result['services'] = {}
    for name in UNITS:
        text = subprocess.check_output(['systemctl', '--user', 'show', f'crypto-market-info-{name}.service',
                                        '-p', 'MainPID', '-p', 'NRestarts', '-p', 'ActiveState'], timeout=10).decode()
        fields = dict(line.split('=', 1) for line in text.splitlines())
        pid = int(fields['MainPID'])
        if pid:
            try:
                status = Path(f'/proc/{pid}/status').read_text()
                fields['memory_kib'] = {k: int(v.split()[0]) for k, v in
                    (line.split(':', 1) for line in status.splitlines() if ':' in line)
                    if k in ['VmRSS', 'VmSwap', 'VmHWM']}
            except FileNotFoundError: fields['process_disappeared'] = True
        result['services'][name] = fields
    result['writer_metrics_fresh'] = metrics_current(result)
    stats = os.statvfs(ROOT)
    result['root_free_bytes'] = stats.f_bavail * stats.f_frsize
    result['host_memory_bytes'] = {key: int(value.split()[0]) * 1024 for key, value in
        (line.split(':', 1) for line in Path('/proc/meminfo').read_text().splitlines())
        if key in ['MemAvailable', 'SwapTotal', 'SwapFree']}
    # Count physical bytes, including headers and retained staging files. No
    # payload reads or database scans are needed for the quota forecast.
    spool_bytes = spool_files = pending_files = 0
    with os.scandir(ROOT / 'var/storage/spool') as entries:
        for entry in entries:
            if not entry.name.endswith(('.batch', '.pending')): continue
            try: size = entry.stat().st_size
            except FileNotFoundError: continue
            spool_bytes += size
            spool_files += entry.name.endswith('.batch')
            pending_files += entry.name.endswith('.pending')
    result['spool_physical'] = {'bytes': spool_bytes, 'batch_files': spool_files,
                                'staging_files': pending_files, 'quota_bytes': 20 << 30}
    result['spool_quota_alarm'] = ('critical' if spool_bytes >= 15 << 30 else
                                  'warning' if spool_bytes >= 10 << 30 else 'none')
    result['io_pressure'] = Path('/proc/pressure/io').read_text().strip()
    collector_log = Path('/home/ubuntu/.local/share/crypto-market-info-collector/logs/collector.log')
    source_events = []
    plan_path = OUT / 'observed-plan-counts.json'
    try: plans = json.loads(plan_path.read_text())
    except FileNotFoundError: plans = []
    by_id = {p['plan']: p for p in plans}
    with collector_log.open('rb') as stream:
        stream.seek(max(0, collector_log.stat().st_size - (8 << 20)))
        if stream.tell(): stream.readline()
        for line in stream:
            try: event = json.loads(line)
            except (ValueError, UnicodeDecodeError): continue
            message = event.get('msg', '')
            if message == 'options catalog plan published':
                by_id[event['plan']] = dict(plan=event['plan'], effective=event['effective'], books=event['books'])
            if message in ['OKX paired runtime health', 'OKX paired initial sampling starts with incomplete books',
                           'OKX paired catalog scheduled', 'options catalog runtime health']:
                # These structured counters contain no endpoint or credentials.
                source_events.append(event)
    result['source_health'] = source_events[-8:]
    # A minute with only one committed shard must not make the last-shard
    # latency appear healthy. This conservative count guard is additional to
    # the frozen fixture's exact plan/shard and durable-identity verification.
    plans = sorted(by_id.values(), key=lambda p: p['effective'])[-64:]
    save_snapshot(plan_path, plans)
    effective = {}
    for plan in plans:
        at = int(dt.datetime.fromisoformat(plan['effective']).timestamp())
        # Multiple published identities at the same effective minute cannot
        # be interpreted as a single valid plan by this lightweight observer.
        effective[at] = plan['books'] if at not in effective else 0
    expected_options = {}
    for minute in lags:
        earlier = [at for at in effective if at <= minute]
        if earlier: expected_options[minute] = effective[max(earlier)]
    result.update(latency_summary(lags, members, expected_options, now))
    result['low_frequency_pending'] = low_frequency_pending(result['services'])
    result['physical_storage_bytes'] = physical_space()
    device = os.stat(ROOT).st_dev
    for line in Path('/proc/diskstats').read_text().splitlines():
        parts = line.split()
        if (int(parts[0]), int(parts[1])) == (os.major(device), os.minor(device)):
            result['disk_counters'] = dict(device=parts[2], read_sectors=int(parts[5]),
                written_sectors=int(parts[9]), busy_ms=int(parts[12]))
            break
    result['cpu_counters'] = [int(v) for v in Path('/proc/stat').read_text().splitlines()[0].split()[1:9]]
    return result

def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--until', required=True, help='UTC ISO deadline, finite and at most 24 hours ahead')
    args = parser.parse_args()
    end = dt.datetime.fromisoformat(args.until).timestamp()
    assert 0 < end - time.time() <= 24 * 3600
    path = OUT / 'continuous-observation.jsonl'
    initial_restarts = None
    count = failures = 0
    history = []
    previous = None
    first_space = None
    last_audit = 0
    while True:
        try:
            row = snapshot()
            current = {k: v['NRestarts'] for k, v in row['services'].items()}
            if initial_restarts is None: initial_restarts = current
            row['restart_changed'] = current != initial_restarts
            row['deadline_utc'] = args.until
            if first_space is None: first_space = (time.time(), row['physical_storage_bytes'])
            seconds = time.time() - first_space[0]
            if seconds >= 60:
                row['space_net_bytes_since_observer_start'] = {k: v - first_space[1][k] for k, v in row['physical_storage_bytes'].items()}
                row['space_measurement_seconds'] = seconds
            if previous is not None:
                elapsed = time.time() - previous[0]
                old_disk, disk = previous[1].get('disk_counters'), row.get('disk_counters')
                if old_disk and disk and old_disk['device'] == disk['device']:
                    row['disk_rates'] = dict(read_bytes_per_second=(disk['read_sectors']-old_disk['read_sectors'])*512/elapsed,
                        write_bytes_per_second=(disk['written_sectors']-old_disk['written_sectors'])*512/elapsed,
                        busy_fraction=(disk['busy_ms']-old_disk['busy_ms'])/(1000*elapsed))
                cpu = [a-b for a,b in zip(row['cpu_counters'], previous[1]['cpu_counters'])]
                if sum(cpu) > 0: row['cpu_fractions'] = dict(idle=cpu[3]/sum(cpu), iowait=cpu[4]/sum(cpu))
            previous = (time.time(), row)
            history.append((time.time(), row['spool_physical']['bytes']))
            history = history[-6:]
            if len(history) >= 2:
                slope = (history[-1][1] - history[0][1]) / (history[-1][0] - history[0][0])
                row['spool_net_bytes_per_second'] = slope
                if slope > 0:
                    available = min(max(0, (20 << 30) - history[-1][1]), row['root_free_bytes'])
                    row['spool_remaining_seconds_at_net_growth'] = available / slope
                else:
                    row['spool_remaining_seconds_at_net_growth'] = None
        except Exception as error:
            row = {'utc': dt.datetime.now(dt.timezone.utc).isoformat(), 'observation_error': str(error)[:200]}
            failures += 1
        with path.open('a') as stream: stream.write(json.dumps(row) + '\n')
        os.chmod(path, 0o600)
        save_snapshot(OUT / 'observation-latest.json', row)
        if time.time() - last_audit >= 300:
            last_audit = time.time()
            try:
                subprocess.run(['/tmp/cmi-backlog-validation', '--mode', 'audit-runtime'], check=True,
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=35)
                audit = json.loads((OUT / 'runtime-audit-latest.json').read_text())
                with (OUT / 'runtime-audit-history.jsonl').open('a') as stream: stream.write(json.dumps(audit)+'\n')
                os.chmod(OUT / 'runtime-audit-history.jsonl', 0o600)
            except (OSError, ValueError, subprocess.SubprocessError) as error:
                with (OUT / 'runtime-audit-history.jsonl').open('a') as stream:
                    stream.write(json.dumps({'utc':dt.datetime.now(dt.timezone.utc).isoformat(), 'audit_error':str(error)[:160]})+'\n')
        count += 1
        if time.time() >= end: break
        time.sleep(min(60, max(1, end - time.time())))
    (OUT / 'observation-finished.json').write_text(json.dumps({
        'finished_at': dt.datetime.now(dt.timezone.utc).isoformat(), 'samples': count,
        'observer_errors': failures, 'deadline_utc': args.until,
        'automatic_acceptance': False,
        'review_required': 'review whole interval, capacity, low-frequency pending and daily space before stable claim'}, indent=2) + '\n')

if __name__ == '__main__': run()
