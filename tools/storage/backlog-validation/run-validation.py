#!/usr/bin/env python3
"""Finite, restart-observable isolated validation with a production lag guard."""
import datetime as dt
import json
import os
from pathlib import Path
import subprocess
import time
import uuid
import argparse
from metrics_guards import metrics_current

# The local daemon's unversioned negotiation endpoint stalls; its v1.45
# container inspection/exec API was verified against the pinned test instance.
os.environ.setdefault('DOCKER_API_VERSION', '1.45')

ROOT = Path('/home/ubuntu/crypto-market-info')
OUT = ROOT / 'var/storage/write-backlog-improvement'
BASE=OUT
BIN = '/tmp/cmi-backlog-validation'
ADMIN = '/tmp/cmi-backlog-storage-migrate'
# Last fully restored production scope before this finite capture: 337 CEX
# streams and 2,806 option/future members. A small startup plan is not R.
FULL_SCOPE_OPTIONS_MEMBERS = 2806

def fixture_has_full_scope(coverage, manifest):
    streams = coverage['minutes_per_stream']
    minutes = manifest['Minutes']
    return (coverage['distinct_streams'] >= 337
            and len(streams) >= 337 and min(streams.values()) >= 90
            and len(minutes) == 90
            and all(groups.get('cex', {}).get('Members', 0) >= 337
                    and groups.get('options_catalog', {}).get('Members', 0) >= FULL_SCOPE_OPTIONS_MEMBERS
                    for groups in minutes.values()))

def live_has_healthy_latency(row):
    w = row.get('writer') or {}
    if not metrics_current(row): return False
    p99 = row.get('last_shard_p99_seconds')
    return (w.get('spool_index_complete') is True and w.get('redis_index_complete') is True and w.get('old_pending') == 0
            and w.get('blocked') == 0 and row.get('closed_minute_count', 0) >= 10
            and row.get('latency_scope_complete') is True
            and p99 is not None and 0 <= p99 <= 30)

def live_has_full_scope(row):
    scope = row.get('latency_scope') or {}
    expected = scope.get('expected_options_members') or {}
    if scope.get('expected_cex_members') != 337 or not expected:
        return False
    if any(n is None or n < FULL_SCOPE_OPTIONS_MEMBERS for n in expected.values()):
        return False
    health = row.get('source_health') or []
    options = [r for r in health if r.get('msg') == 'options catalog runtime health']
    paired = [r for r in health if r.get('msg') == 'OKX paired runtime health']
    if not options or not paired:
        return False
    try:
        at = dt.datetime.fromisoformat(row['utc'])
        for event in [options[-1], paired[-1]]:
            age = (at - dt.datetime.fromisoformat(event['time'])).total_seconds()
            if not -5 <= age <= 150:
                return False
    except (KeyError, TypeError, ValueError):
        return False
    o, b = options[-1], paired[-1].get('books') or {}
    return (o.get('entries', 0) >= FULL_SCOPE_OPTIONS_MEMBERS
            and o.get('uncertain_scopes', 0) == 0 and o.get('protocol_scopes', 0) == 0
            and b.get('ReadyInstruments', 0) >= 332 and b.get('InvalidInstruments', 1) == 0)

def live_ready_for_isolated_load(row):
    return live_has_healthy_latency(row) and live_has_full_scope(row)

def status(phase, **fields):
    row = {'utc': dt.datetime.now(dt.timezone.utc).isoformat(), 'phase': phase,
           'history_import_allowed': False, **fields}
    path = OUT / 'validation-status.json'
    tmp = path.with_suffix('.tmp')
    tmp.write_text(json.dumps(row, indent=2) + '\n')
    os.chmod(tmp, 0o600)
    os.replace(tmp, path)

def command(args, name, limit):
    with (OUT / name).open('xb') as log:
        result = subprocess.run(args, cwd=ROOT, stdout=log, stderr=subprocess.STDOUT, timeout=limit)
    if result.returncode: raise RuntimeError(f'{name} exited {result.returncode}')

def latest():
    row = json.loads((BASE / 'observation-latest.json').read_text())
    stamp = dt.datetime.fromisoformat(row['utc']).timestamp()
    if time.time() - stamp > 150: raise RuntimeError('observation is stale')
    return row

def run():
    global OUT
    parser=argparse.ArgumentParser()
    parser.add_argument('--full-scope',action='store_true')
    parser.add_argument('--after-metadata-retry',action='store_true')
    parser.add_argument('--after-dex-indexes',action='store_true')
    parser.add_argument('--after-streaming-gap',action='store_true')
    parser.add_argument('--after-discovery-bulk',action='store_true')
    parser.add_argument('--after-cache-diagnostics',action='store_true')
    parser.add_argument('--after-publication-prefetch',action='store_true')
    parser.add_argument('--workers', type=int, choices=[1, 2], default=2)
    args=parser.parse_args()
    extra=[]
    if sum([args.after_metadata_retry, args.after_dex_indexes, args.after_streaming_gap, args.after_discovery_bulk,args.after_cache_diagnostics,args.after_publication_prefetch]) > 1:
        parser.error('choose exactly one capture stage')
    if args.after_metadata_retry or args.after_dex_indexes or args.after_streaming_gap or args.after_discovery_bulk or args.after_cache_diagnostics or args.after_publication_prefetch: args.full_scope=True
    if args.full_scope:
        stage = 'dex-index-validation' if args.after_dex_indexes else ('post-retry-validation' if args.after_metadata_retry else 'full-scope-validation')
        capture_stage = 'writer-dex-index-capture' if args.after_dex_indexes else ('writer-post-retry-capture' if args.after_metadata_retry else 'writer-full-scope-capture')
        if args.after_streaming_gap:
            stage, capture_stage = 'streaming-gap-validation', 'writer-streaming-gap-capture'
        if args.after_discovery_bulk:
            stage, capture_stage = 'discovery-bulk-validation', 'writer-discovery-bulk-capture'
        if args.after_cache_diagnostics:
            stage, capture_stage = 'cache-diagnostics-validation', 'writer-cache-diagnostics-capture'
        if args.after_publication_prefetch:
            stage, capture_stage = 'publication-prefetch-validation', 'writer-publication-prefetch-capture'
        OUT=BASE/stage;OUT.mkdir(mode=0o700,exist_ok=True)
        capture=json.loads((BASE/capture_stage/'deployment.json').read_text())
        extra=['--evidence-dir',str(OUT),'--frames',capture['frames'],'--from',capture['window_from_utc'],'--to',capture['window_to_utc']]
        end=dt.datetime.fromisoformat(capture['window_to_utc']).timestamp()+180
        status('capturing_fresh_full_scope_fixture',capture_from=capture['window_from_utc'],capture_to=capture['window_to_utc'])
        while time.time()<end:time.sleep(min(30,end-time.time()))
        if args.after_discovery_bulk or args.after_cache_diagnostics or args.after_publication_prefetch:
            status('waiting_for_healthy_live_before_fixture_checks')
            deadline=time.time()+3600
            while not live_ready_for_isolated_load(latest()):
                if time.time()>=deadline:
                    status('live_backlog_blocks_fixture_checks',stable=False,capacity_test_started=False)
                    return
                time.sleep(30)
        status('checking_original_fixture')
        for mode in ['refs','seal','cex-coverage']:
            command(['ionice','-c','2','-n','7',BIN,*extra,'--mode',mode],f'fixture-{mode}.log',1200)
    # Existing scans survive a chat interruption; never launch duplicate passes.
    status('waiting_for_fixture_seal')
    deadline = time.time() + 3600
    while not all((OUT / f).exists() for f in ['fixture-reference-filters.json', 'frozen-manifest.json']):
        if time.time() >= deadline: raise RuntimeError('fixture preparation did not finish within one hour')
        time.sleep(15)
    coverage = json.loads((OUT / 'frozen-cex-coverage.json').read_text())
    manifest = json.loads((OUT / 'frozen-manifest.json').read_text())
    observed_members = sum(coverage['minutes_per_stream'].values())
    if not fixture_has_full_scope(coverage, manifest):
        status('source_coverage_blocks_full_scope_capacity', stable=False,
               frozen_cex_streams=coverage['distinct_streams'],
               frozen_cex_member_minutes=observed_members,
               expected_cex_streams=337, capacity_test_started=False)
        return
    filters_path = OUT / 'fixture-reference-filters.json'
    filters = json.loads(filters_path.read_text())
    # The in-flight first scanner used this old name; normalize only that column.
    filters['derivative_trading_rule'] = [p.replace('rule_id IN (', 'trading_rule_id IN (', 1)
                                            if p.startswith('rule_id IN (') else p
                                            for p in filters.get('derivative_trading_rule', [])]
    filters_path.write_text(json.dumps(filters, indent=2) + '\n')
    status('copying_referenced_metadata')
    if not (OUT / 'bench-metadata-clone.json').exists():
        command(['/usr/bin/python3', str(ROOT / 'tools/storage/backlog-validation/clone-metadata.py'),
                 '--run-dir',str(OUT),*(['--reset-empty-bench'] if args.full_scope else [])],
                'bench-metadata-referenced-controller.log', 1200)
    status('activating_isolated_producer')
    command([ADMIN, 'activate-producer', '--socket', str(BASE / 'bench-mysql/run/mysql.sock'),
             '--user', 'root', '--password-file', '/home/ubuntu/.config/crypto-market-info-storage/mysql-root-password',
             '--target-read-timeout', '2m', '--service', 'collector', '--expected-generation', '0',
             '--holder', str(uuid.uuid4())], 'bench-producer.log', 150)
    status('waiting_for_stable_live_latency')
    deadline = time.time() + 3600
    while True:
        row = latest()
        if live_ready_for_isolated_load(row): break
        if time.time() >= deadline: raise RuntimeError('live latency did not meet 30-second gate; isolated load not started')
        time.sleep(30)
    status('replaying_90_minutes_at_1_5x', commit_workers=args.workers)
    with (OUT / 'bench-replay.log').open('xb') as log:
        proc = subprocess.Popen([BIN,*extra, '--mode', 'replay', '--workers', str(args.workers), '--speed', '1.5'],
                                cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
        deadline = time.time() + 75 * 60
        excessive = 0
        try:
            while proc.poll() is None:
                row = latest()
                available = int(next(line.split()[1] for line in Path('/proc/meminfo').read_text().splitlines()
                                     if line.startswith('MemAvailable:'))) * 1024
                if available < 1 << 30:
                    raise RuntimeError('host available memory fell below 1 GiB; isolated load stopped')
                if not live_ready_for_isolated_load(row): excessive += 1
                else: excessive = 0
                # Only the test process is stopped; accepted test copies remain.
                if excessive >= 3: raise RuntimeError('production full-scope latency/backlog gate failed for three observations; isolated replay stopped')
                if time.time() >= deadline: raise RuntimeError('isolated replay exceeded 75-minute bound')
                time.sleep(30)
            if proc.returncode: raise RuntimeError(f'isolated replay exited {proc.returncode}')
        except Exception:
            proc.terminate()
            try: proc.wait(timeout=45)
            except subprocess.TimeoutExpired: proc.kill(); proc.wait(timeout=10)
            raise
    status('checking_exact_content_and_replay')
    command(['ionice','-c','2','-n','7',BIN,*extra, '--mode', 'verify'], 'bench-exact-replay.log', 3600)
    status('capacity_and_replay_complete_pending_24h_review',
           capacity_complete=True, exact_replay_complete=True, stable=False)

if __name__ == '__main__':
    try:
        run()
    except Exception as error:
        status('validation_stopped', error=str(error)[:300], stable=False)
        raise
