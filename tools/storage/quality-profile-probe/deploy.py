#!/usr/bin/env python3
"""Deploy the quality extension after explicit correctness/performance validation.

Only the project's collector/writer are restarted. Queues and generations stay
intact. A write rollback must retain the new reader-capable binaries.
"""
import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys
import time

ROOT = Path('/home/ubuntu/crypto-market-info')
sys.path.insert(0, str(ROOT / 'tools/storage/backlog-validation'))
import mysql_client as mysql

UNITS = ['crypto-market-info-collector.service', 'crypto-market-info-storage-writer.service']
TARGETS = [ROOT / 'var/storage/bootstrap-preparation/20261013/bin/collector', ROOT / 'var/storage/bin/storage-writer']
DROPIN = Path('/home/ubuntu/.config/systemd/user/crypto-market-info-storage-writer.service.d/90-quality-storage.conf')


def digest(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def state(unit):
    raw = subprocess.check_output(['systemctl', '--user', 'show', unit, '-p', 'MainPID', '-p', 'ActiveState', '-p', 'NRestarts'], timeout=15).decode()
    return dict(line.split('=', 1) for line in raw.splitlines())


def now():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def generation():
    return mysql.sql('production', "SELECT JSON_OBJECT('service',CAST(service AS CHAR),'generation',generation,'holder',HEX(holder)) FROM cmi_storage.producer_generation ORDER BY service", limit=8)


def base_schema():
    return mysql.sql('production', "SELECT JSON_OBJECT('table',CAST(table_name AS CHAR),'version',version,'hash',HEX(ddl_hash)) FROM cmi_storage.schema_version WHERE database_name='crypto_market_info' AND table_name NOT IN ('derivative_book_quality_minute_v2','options_catalog_quality_profile_minute','options_catalog_quality_member_minute') ORDER BY table_name", limit=8)


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--collector-sha256', required=True)
    parser.add_argument('--writer-sha256', required=True)
    parser.add_argument('--admin-sha256', required=True)
    args = parser.parse_args()
    candidates = [Path('/tmp/cmi-collector-quality-v2'), Path('/tmp/cmi-storage-writer-quality-v2')]
    expected = [args.collector_sha256, args.writer_sha256]
    admin = Path('/tmp/cmi-storage-migrate-quality-v2')
    for candidate, sha in zip(candidates+[admin], expected+[args.admin_sha256]):
        assert digest(candidate) == sha, 'candidate changed'
    comparison = json.loads((ROOT / 'var/storage/quality-storage-20261019/comparison.json').read_text())
    assert [r['Variant'] for r in comparison] == ['baseline', 'minute-layout', 'shared-profile']
    assert comparison[-1]['TotalBytes'] < comparison[0]['TotalBytes'], 'physical write comparison failed'
    assert comparison[-1]['Members'] == comparison[0]['Members']
    validation = json.loads((ROOT / 'var/storage/quality-storage-20261019/validation-complete.json').read_text())
    assert validation['passed'] is True, 'final correctness gate missing'
    assert validation['candidates'] == {'collector': expected[0], 'writer': expected[1], 'admin': args.admin_sha256}, 'tested candidate set changed'
    mysql.verify('production')
    before = [state(unit) for unit in UNITS]
    assert all(s['ActiveState'] == 'active' and int(s['MainPID']) > 0 for s in before)
    previous = []
    for target, status in zip(TARGETS, before):
        sha = digest(target)
        assert digest(Path('/proc') / status['MainPID'] / 'exe') == sha, 'installed/process image mismatch'
        previous.append(sha)
    # Read the effective ExecStart without exposing Environment/credential values.
    text = subprocess.check_output(['systemctl', '--user', 'cat', UNITS[1]], timeout=15).decode()
    command = None
    for line in text.splitlines():
        if line.startswith('ExecStart='):
            command = shlex.split(line[len('ExecStart='):].replace('%h', '/home/ubuntu')) if line[len('ExecStart='):] else None
    assert command and command[0] == str(TARGETS[1]) and '--quality-storage-v2' not in command
    command.append('--quality-storage-v2')
    assert not DROPIN.exists(), 'existing quality deployment requires explicit review'
    out = ROOT / 'var/storage/quality-storage-20261019' / ('deploy-' + dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%SZ'))
    out.mkdir(mode=0o700)
    report = {'started_utc': now(), 'before': dict(zip(UNITS,before)), 'old_sha256': previous, 'new_sha256': expected, 'generations_before': generation(), 'base_schema_before': base_schema(), 'phase': 'prepared'}

    def save():
        (out / 'deployment.json').write_text(json.dumps(report, indent=2) + '\n')

    for target in TARGETS:
        shutil.copy2(target, out / (target.name + '-before'))
    (out / 'writer-effective-unit-before.txt').write_text(text)
    save()
    subprocess.run([str(admin), 'install-quality-storage', '--socket', str(mysql.SERVERS['production'][0]), '--user', 'root', '--password-file', '/home/ubuntu/.config/crypto-market-info-storage/mysql-root-password', '--family', 'crypto_market_info', '--target', 'crypto_market_info', '--target-read-timeout', '2m'], check=True, timeout=180)
    report.update(phase='schema_installed', schema_installed_utc=now()); save()
    for unit in UNITS:
        subprocess.run(['systemctl', '--user', 'stop', unit], check=True, timeout=120)
        assert state(unit)['MainPID'] == '0'
    report.update(phase='stopped', stopped_utc=now()); save()
    for target, candidate in zip(TARGETS,candidates):
        staged = target.with_name(target.name + '.quality-v2.new')
        shutil.copy2(candidate, staged)
        with staged.open('rb') as stream: os.fsync(stream.fileno())
        os.replace(staged, target)
        fd = os.open(target.parent, os.O_DIRECTORY)
        try: os.fsync(fd)
        finally: os.close(fd)
    DROPIN.parent.mkdir(exist_ok=True)
    DROPIN.write_text('[Service]\nExecStart=\nExecStart='+shlex.join(command)+'\n')
    subprocess.run(['systemctl', '--user', 'daemon-reload'], check=True, timeout=30)
    report.update(phase='replaced', replaced_utc=now()); save()
    for unit in reversed(UNITS):
        subprocess.run(['systemctl', '--user', 'start', unit], check=True, timeout=90)
    for unit, target, sha in zip(UNITS,TARGETS,expected):
        end = time.monotonic()+30
        while time.monotonic()<end:
            s = state(unit)
            if s['ActiveState']=='active' and s['MainPID']!='0':
                try:
                    if digest(Path('/proc')/s['MainPID']/'exe')==sha:break
                except OSError:pass
            time.sleep(0.5)
        else: raise RuntimeError(unit+' did not start the candidate image')
    report.update(phase='running', after={u:state(u) for u in UNITS}, running_utc=now(), generations_after=generation())
    assert report['generations_after']==report['generations_before'], 'producer generation changed'
    report['base_schema_after'] = base_schema()
    assert report['base_schema_after'] == report['base_schema_before'], 'frozen base schema changed'
    report['queues_deleted']=False
    save()
    print(json.dumps({'path':str(out), 'phase':report['phase'], 'after':report['after']}))


if __name__ == '__main__':
    run()
