#!/usr/bin/env python3
"""Deploy the already-tested wire-compatible decoder to the main collector."""
import datetime as dt
import argparse
import hashlib
import json
import os
import re
from pathlib import Path
import shutil
import subprocess
import time

ROOT = Path('/home/ubuntu/crypto-market-info')
PREP = ROOT / 'var/storage/bootstrap-preparation/20261013'
OUT = ROOT / 'var/storage/write-backlog-improvement/collector-decoder-deployment'
UNIT = 'crypto-market-info-collector.service'
TARGET = PREP / 'bin/collector'
CANDIDATE = Path('/tmp/cmi-collector-backlog-final')
EXPECTED = '728948cd8f93902fe3888ff591568dfeb90c272fdee744ee473cd3be433c3023'
BEFORE = '818e5c60ac44f1ba3f6d145c867068d03539f4f46c3340a38c281060190ea9ac'

def digest(path):
    with path.open('rb') as f: return hashlib.file_digest(f, 'sha256').hexdigest()

def state():
    output = subprocess.check_output(['systemctl', '--user', 'show', UNIT,
                                     '-p', 'MainPID', '-p', 'ActiveState', '-p', 'NRestarts'], timeout=15).decode()
    return dict(line.split('=', 1) for line in output.splitlines())

def now(): return dt.datetime.now(dt.timezone.utc).isoformat()

def wait_for_binary(expected):
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        current = state()
        if current['ActiveState'] == 'active' and current['MainPID'] != '0':
            proc = Path('/proc') / current['MainPID'] / 'exe'
            try:
                if proc.resolve() == TARGET and digest(proc) == expected:
                    return current
            except (FileNotFoundError, ProcessLookupError):
                pass
        time.sleep(0.2)
    raise RuntimeError('collector did not exec the expected binary within 20 seconds')

def run():
    global OUT,CANDIDATE,EXPECTED,BEFORE
    parser=argparse.ArgumentParser()
    parser.add_argument('--stage',choices=['decoder','paired-startup','resync-diag','quiet-book','metadata-retry','metadata-bulk','dex-indexes','metadata-preparation','rule-preparation','streaming-gap','run-group','identity-cache','discovery-bulk','delivery-budget','discovery-prefetch'],default='decoder')
    parser.add_argument('--candidate-sha256')
    args=parser.parse_args()
    if args.stage=='paired-startup':
        OUT=ROOT/'var/storage/write-backlog-improvement/collector-paired-startup-deployment'
        CANDIDATE=Path('/tmp/cmi-collector-paired-startup')
        BEFORE=EXPECTED
        EXPECTED='38ae3a8302497034c44f71071b6e2064c763c33b0980a5b3272001d3b6b40c07'
    if args.stage=='resync-diag':
        OUT=ROOT/'var/storage/write-backlog-improvement/collector-resync-diag-deployment'
        CANDIDATE=Path('/tmp/cmi-collector-resync-diag')
        BEFORE='38ae3a8302497034c44f71071b6e2064c763c33b0980a5b3272001d3b6b40c07'
        EXPECTED='3918f86ac55a708f58bd74007bad299b0e8dc1b1f8b8f18db85f9f49b8f93a69'
    if args.stage=='quiet-book':
        OUT=ROOT/'var/storage/write-backlog-improvement/collector-quiet-book-deployment'
        CANDIDATE=Path('/tmp/cmi-collector-quiet-book')
        BEFORE='3918f86ac55a708f58bd74007bad299b0e8dc1b1f8b8f18db85f9f49b8f93a69'
        EXPECTED='7dde2bc46ec0a793c9df9cf18340c3e38b2b1000abb0982e52a3deceec260898'
    if args.stage=='metadata-retry':
        OUT=ROOT/'var/storage/write-backlog-improvement/collector-metadata-retry-deployment'
        CANDIDATE=Path('/tmp/cmi-collector-metadata-retry')
        BEFORE='7dde2bc46ec0a793c9df9cf18340c3e38b2b1000abb0982e52a3deceec260898'
        EXPECTED='9276a28a87a4c89935b0ed9ff22457ea08b6c16503239baa6ea4dbab4a0cf657'
    if args.stage=='metadata-bulk':
        OUT=ROOT/'var/storage/write-backlog-improvement/collector-metadata-bulk-deployment'
        CANDIDATE=Path('/tmp/cmi-collector-metadata-bulk')
        BEFORE='9276a28a87a4c89935b0ed9ff22457ea08b6c16503239baa6ea4dbab4a0cf657'
        EXPECTED='1cdd92a04e648857812e08780f94594f8002bf64a01e4e49527c7b18ed6e1523'
    if args.stage=='dex-indexes':
        assert args.candidate_sha256 and re.fullmatch(r'[a-f0-9]{64}',args.candidate_sha256)
        OUT=ROOT/'var/storage/write-backlog-improvement/collector-dex-index-deployment'
        CANDIDATE=Path('/tmp/cmi-collector-dex-indexes')
        BEFORE='1cdd92a04e648857812e08780f94594f8002bf64a01e4e49527c7b18ed6e1523'
        EXPECTED=args.candidate_sha256
    if args.stage=='metadata-preparation':
        assert args.candidate_sha256 and re.fullmatch(r'[a-f0-9]{64}',args.candidate_sha256)
        OUT=ROOT/'var/storage/write-backlog-improvement/collector-metadata-preparation-deployment'
        CANDIDATE=Path('/tmp/cmi-collector-metadata-preparation')
        BEFORE='2ff3d61aef62113d4aab013fde18cd1059b0ccfc389cf39f181bc0d89f85d49b'
        EXPECTED=args.candidate_sha256
    if args.stage=='rule-preparation':
        assert args.candidate_sha256 and re.fullmatch(r'[a-f0-9]{64}',args.candidate_sha256)
        OUT=ROOT/'var/storage/write-backlog-improvement/collector-rule-preparation-deployment'
        CANDIDATE=Path('/tmp/cmi-collector-rule-preparation')
        BEFORE='1a429d8927ad0efe3b56c4c1b5f090749184fefcdea68b1c6e964eeb1167ca42'
        EXPECTED=args.candidate_sha256
    if args.stage=='streaming-gap':
        assert args.candidate_sha256 and re.fullmatch(r'[a-f0-9]{64}',args.candidate_sha256)
        OUT=ROOT/'var/storage/write-backlog-improvement/collector-streaming-gap-deployment'
        CANDIDATE=Path('/tmp/cmi-collector-streaming-gap')
        BEFORE='dc68416cfbbe918a6e7b0d9f0dba40bfe60ce7f318b418598f22810bbf0d43ce'
        EXPECTED=args.candidate_sha256
    if args.stage=='run-group':
        assert args.candidate_sha256 and re.fullmatch(r'[a-f0-9]{64}',args.candidate_sha256)
        OUT=ROOT/'var/storage/write-backlog-improvement/collector-run-group-deployment'
        CANDIDATE=Path('/tmp/cmi-collector-run-group')
        BEFORE='69c4d2e0571330b620245ad1014f94cde8ff028ee3421181fe78f5d133b8cc9e'
        EXPECTED=args.candidate_sha256
    if args.stage=='identity-cache':
        assert args.candidate_sha256 and re.fullmatch(r'[a-f0-9]{64}',args.candidate_sha256)
        OUT=ROOT/'var/storage/write-backlog-improvement/collector-identity-cache-deployment'
        CANDIDATE=Path('/tmp/cmi-collector-metadata-cache')
        BEFORE='362f2fca31844297ce3716a3f61c89f0ac8dbf9d1b2365b47267e3bcdb92973f'
        EXPECTED=args.candidate_sha256
    if args.stage=='discovery-bulk':
        assert args.candidate_sha256 and re.fullmatch(r'[a-f0-9]{64}',args.candidate_sha256)
        OUT=ROOT/'var/storage/write-backlog-improvement/collector-discovery-bulk-deployment'
        CANDIDATE=Path('/tmp/cmi-collector-discovery-bulk')
        BEFORE='a2174bcff5061d642c7013b4a31545628c07b8605104d2ea88c8f1690ac129df'
        EXPECTED=args.candidate_sha256
    if args.stage=='delivery-budget':
        assert args.candidate_sha256 and re.fullmatch(r'[a-f0-9]{64}',args.candidate_sha256)
        OUT=ROOT/'var/storage/write-backlog-improvement/collector-delivery-budget-deployment'
        CANDIDATE=Path('/tmp/cmi-collector-delivery-budget')
        BEFORE='bdd00c0577be8f1e1693ef1941139afb9a4b3e8739dc0306cb283845cc568180'
        EXPECTED=args.candidate_sha256
    if args.stage=='discovery-prefetch':
        assert args.candidate_sha256 and re.fullmatch(r'[a-f0-9]{64}',args.candidate_sha256)
        OUT=ROOT/'var/storage/write-backlog-improvement/collector-discovery-prefetch-deployment'
        CANDIDATE=Path('/tmp/cmi-collector-discovery-prefetch')
        BEFORE='7628e2bf53b92f9d0b40b15ac97896efeb41027f67feab42ef9611f871fbd1a7'
        EXPECTED=args.candidate_sha256
    assert digest(CANDIDATE) == EXPECTED and digest(TARGET) == BEFORE
    before = state()
    assert before['ActiveState'] == 'active' and int(before['MainPID']) > 0
    assert digest(Path('/proc') / before['MainPID'] / 'exe') == BEFORE
    OUT.mkdir(mode=0o700)
    shutil.copy2(TARGET, OUT / 'collector-before')
    shutil.copy2(PREP / 'new-services.json', OUT / 'new-services-before.json')
    shutil.copy2(PREP / 'binaries.sha256', OUT / 'binaries-before.sha256')
    report = {'stage':args.stage,'before': before, 'old_sha256': BEFORE, 'new_sha256': EXPECTED,
              'stop_requested_at': now(), 'generation_changed': False}
    (OUT / 'deployment.json').write_text(json.dumps(report, indent=2) + '\n')
    subprocess.run(['systemctl', '--user', 'stop', UNIT], check=True, timeout=60)
    stopped = state()
    assert stopped['ActiveState'] == 'inactive' and stopped['MainPID'] == '0'
    report.update(stopped_at=now(), stopped=stopped)
    temp = TARGET.with_suffix('.decoder-new')
    shutil.copyfile(CANDIDATE, temp)
    os.chmod(temp, 0o755)
    with temp.open('rb') as f: os.fsync(f.fileno())
    os.replace(temp, TARGET)
    fd = os.open(TARGET.parent, os.O_DIRECTORY)
    os.fsync(fd); os.close(fd)
    subprocess.run(['systemctl', '--user', 'start', UNIT], check=True, timeout=60)
    report['start_returned_at'] = now()
    after = wait_for_binary(EXPECTED)
    services = json.loads((PREP / 'new-services.json').read_text())
    for item in services:
        if item['service'] == 'collector': item['binary_sha256'] = EXPECTED
    (PREP / 'new-services.json').write_text(json.dumps(services, indent=2) + '\n')
    lines = (PREP / 'binaries.sha256').read_text().splitlines()
    lines = [EXPECTED + '  bin/collector' if line.split()[-1] == 'bin/collector' else line for line in lines]
    (PREP / 'binaries.sha256').write_text('\n'.join(lines) + '\n')
    report.update(after=after, verified_at=now(), sampling_warmup_pending=True)
    (OUT / 'deployment.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({'state': after, 'sha256': EXPECTED, 'sampling_warmup_pending': True}))

if __name__ == '__main__': run()
