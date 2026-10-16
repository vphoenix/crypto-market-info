#!/usr/bin/env python3
"""Replace only the writer after its in-flight work has stopped; retain queues."""
import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import shlex
import subprocess

ROOT = Path('/home/ubuntu/crypto-market-info')
OUT = ROOT / 'var/storage/write-backlog-improvement'
TARGET = ROOT / 'var/storage/bin/storage-writer'
UNIT = 'crypto-market-info-storage-writer.service'

def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()

def state():
    result = subprocess.check_output(['systemctl', '--user', 'show', UNIT,
        '-p', 'MainPID', '-p', 'ActiveState', '-p', 'NRestarts'], timeout=15).decode()
    return dict(line.split('=', 1) for line in result.splitlines())

def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('--candidate', required=True)
    parser.add_argument('--expected-current', required=True)
    parser.add_argument('--expected-candidate', required=True)
    parser.add_argument('--stage', required=True)
    parser.add_argument('--workers', type=int, choices=[1, 2])
    parser.add_argument('--cache-bytes',type=int,choices=[32<<20,64<<20,96<<20])
    parser.add_argument('--disable-archive',action='store_true')
    args = parser.parse_args()
    candidate = Path(args.candidate)
    assert candidate.parent == Path('/tmp') and candidate.name.startswith('cmi-storage-writer-')
    assert re.fullmatch(r'[a-z][a-z0-9-]{0,60}', args.stage)
    assert digest(candidate) == args.expected_candidate
    before = state()
    assert before['ActiveState'] == 'active'
    assert digest(TARGET) == args.expected_current
    assert digest(Path('/proc') / before['MainPID'] / 'exe') == args.expected_current
    backup = OUT / ('writer-' + args.stage + '-deployment')
    backup.mkdir(mode=0o700)
    shutil.copy2(TARGET, backup / 'writer-before')
    unit_path = Path('/home/ubuntu/.config/systemd/user') / UNIT
    shutil.copy2(unit_path, backup / 'unit-before.service')
    unit_text = unit_path.read_text()
    if args.workers is not None:
        unit_text, substitutions = re.subn(r'--workers [123](?=\s|$)', '--workers ' + str(args.workers), unit_text)
        assert substitutions == 1
    if args.cache_bytes is not None:
        unit_text,substitutions=re.subn(r'--cache-bytes [0-9]+(?=\s|$)','--cache-bytes '+str(args.cache_bytes),unit_text)
        assert substitutions==1
    if args.disable_archive:
        lines=unit_text.splitlines();changed=0
        for n,line in enumerate(lines):
            if not line.startswith('ExecStart='):continue
            command=shlex.split(line[len('ExecStart='):]);clean=[];i=0;removed=[]
            while i<len(command):
                flag=command[i].split('=',1)[0]
                if flag in ['--archive-dir','--archive-from','--archive-to']:
                    removed.append(flag);i+=1 if '=' in command[i] else 2;continue
                clean.append(command[i]);i+=1
            assert sorted(removed)==['--archive-dir','--archive-from','--archive-to']
            lines[n]='ExecStart='+shlex.join(clean);changed+=1
        assert changed==1
        unit_text='\n'.join(lines)+'\n'
    report = dict(before=before, old_sha256=args.expected_current,
        new_sha256=args.expected_candidate, generation_changed=False,
        stop_requested_at=dt.datetime.now(dt.timezone.utc).isoformat())
    def save():
        (backup / 'deployment.json').write_text(json.dumps(report, indent=2) + '\n')
    save()
    subprocess.run(['systemctl', '--user', 'stop', UNIT], check=True, timeout=65)
    assert state()['MainPID'] == '0'
    if args.workers is not None or args.cache_bytes is not None or args.disable_archive:
        unit_path.write_text(unit_text)
        subprocess.run(['systemctl', '--user', 'daemon-reload'], check=True, timeout=15)
        report['commit_workers'] = args.workers
        report['metadata_cache_bytes'] = args.cache_bytes
        report['archive_disabled'] = args.disable_archive
        save()
    staged = TARGET.with_suffix('.new')
    shutil.copy2(candidate, staged)
    with staged.open('rb') as stream:
        os.fsync(stream.fileno())
    os.replace(staged, TARGET)
    fd = os.open(TARGET.parent, os.O_DIRECTORY)
    os.fsync(fd)
    os.close(fd)
    subprocess.run(['systemctl', '--user', 'start', UNIT], check=True, timeout=60)
    report.update(after=state(), started_at=dt.datetime.now(dt.timezone.utc).isoformat())
    save()
    assert report['after']['ActiveState'] == 'active'
    assert digest(Path('/proc') / report['after']['MainPID'] / 'exe') == args.expected_candidate
    print(json.dumps(report))

if __name__ == '__main__':
    run()
