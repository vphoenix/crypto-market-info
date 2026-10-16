#!/usr/bin/env python3
"""Deploy the tested read-only Reserve capture pagination change; retain current state."""
import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import time

ROOT=Path('/home/ubuntu/crypto-market-info')
PREP=ROOT/'var/storage/bootstrap-preparation/20261013'
OUT=ROOT/'var/storage/write-backlog-improvement/reserve-cursor-deployment'
TARGET=PREP/'bin/reserve-data'
CANDIDATE=Path('/tmp/cmi-reserve-cursor')
BEFORE='5c9611bd9de682c462740c47967498439daa6862a510780f6361393af2dfabcf'
UNIT='crypto-market-info-reserve.service'

def now():return dt.datetime.now(dt.timezone.utc).isoformat()
def digest(path):
    with path.open('rb')as stream:return hashlib.file_digest(stream,'sha256').hexdigest()
def state():
    return dict(line.split('=',1)for line in subprocess.check_output(['systemctl','--user','show',UNIT,'-p','MainPID','-p','ActiveState','-p','NRestarts'],text=True,timeout=10).splitlines())
def wait(expected):
    until=time.monotonic()+20
    while time.monotonic()<until:
        current=state()
        if current['ActiveState']=='active' and current['MainPID']!='0':
            exe=Path('/proc')/current['MainPID']/'exe'
            try:
                if exe.resolve()==TARGET and digest(exe)==expected:return current
            except (FileNotFoundError,ProcessLookupError):pass
        time.sleep(.2)
    raise RuntimeError('Reserve did not exec expected binary within bound')
def replace(source):
    staged=TARGET.with_suffix('.priority-new')
    shutil.copyfile(source,staged);staged.chmod(0o755)
    with staged.open('rb')as stream:os.fsync(stream.fileno())
    os.replace(staged,TARGET)
    directory=os.open(TARGET.parent,os.O_DIRECTORY)
    try:os.fsync(directory)
    finally:os.close(directory)
def run():
    parser=argparse.ArgumentParser();parser.add_argument('--candidate-sha256',required=True);args=parser.parse_args()
    assert re.fullmatch('[a-f0-9]{64}',args.candidate_sha256)
    expected=args.candidate_sha256
    assert digest(CANDIDATE)==expected and digest(TARGET)==BEFORE
    before=state();assert before['ActiveState']=='active' and before['MainPID']!='0'
    assert digest(Path('/proc')/before['MainPID']/'exe')==BEFORE
    OUT.mkdir(mode=0o700)
    shutil.copy2(TARGET,OUT/'reserve-before')
    for name in ['new-services.json','binaries.sha256']:shutil.copy2(PREP/name,OUT/(name+'.before'))
    checkpoint=ROOT/'var/reserve/state/state.gob'
    if checkpoint.exists():shutil.copy2(checkpoint,OUT/'checkpoint-before.gob')
    report={'before':before,'old_sha256':BEFORE,'new_sha256':expected,'generation_changed':False,'stop_requested_at':now(),'checkpoint_restored':False}
    def save():(OUT/'deployment.json').write_text(json.dumps(report,indent=2)+'\n')
    save()
    try:
        subprocess.run(['systemctl','--user','stop',UNIT],check=True,timeout=65)
        assert state()['MainPID']=='0'
        report['stopped_at']=now();save()
        if checkpoint.exists():shutil.copy2(checkpoint,OUT/'checkpoint-after-stop.gob')
        replace(CANDIDATE)
        subprocess.run(['systemctl','--user','start',UNIT],check=True,timeout=60)
        report['start_returned_at']=now();report['after']=wait(expected)
        services=json.loads((PREP/'new-services.json').read_text());changed=0
        for item in services:
            if item['service']=='reserve':item['binary_sha256']=expected;changed+=1
        assert changed==1
        lines=(PREP/'binaries.sha256').read_text().splitlines();changed=0
        for n,line in enumerate(lines):
            if line.split()[-1]=='bin/reserve-data':lines[n]=expected+'  bin/reserve-data';changed+=1
        assert changed==1
        (PREP/'new-services.json').write_text(json.dumps(services,indent=2)+'\n')
        (PREP/'binaries.sha256').write_text('\n'.join(lines)+'\n')
        report['verified_at']=now();save();print(json.dumps(report))
    except Exception as exc:
        report['error']=type(exc).__name__;save()
        subprocess.run(['systemctl','--user','stop',UNIT],check=True,timeout=65)
        assert digest(TARGET)in [BEFORE,expected]
        replace(OUT/'reserve-before')
        for name in ['new-services.json','binaries.sha256']:shutil.copy2(OUT/(name+'.before'),PREP/name)
        subprocess.run(['systemctl','--user','start',UNIT],check=True,timeout=60)
        report['rolled_back_at']=now();report['rollback_state']=wait(BEFORE);save()
        raise

if __name__=='__main__':run()
