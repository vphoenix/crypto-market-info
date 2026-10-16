#!/usr/bin/env python3
"""Arm a finite fresh fixture window without changing producer identity."""
import datetime as dt
import argparse
import hashlib
import json
import os
import re
from pathlib import Path
import shlex
import shutil
import subprocess
from capture_guards import stable_full_scope

ROOT=Path('/home/ubuntu/crypto-market-info')
OUT=ROOT/'var/storage/write-backlog-improvement'
UNIT='crypto-market-info-storage-writer.service'
PATH=Path('/home/ubuntu/.config/systemd/user')/UNIT
EXPECTED='116d9042100c5124c09afd41a3bce885b1522cba3c25953e5fc992a53662591e'

def state():
    text=subprocess.check_output(['systemctl','--user','show',UNIT,'-p','MainPID','-p','NRestarts','-p','ActiveState'],timeout=15).decode()
    return dict(line.split('=',1) for line in text.splitlines())

def run():
    global EXPECTED
    parser=argparse.ArgumentParser()
    parser.add_argument('--after-metadata-retry',action='store_true')
    parser.add_argument('--after-dex-indexes',action='store_true')
    parser.add_argument('--after-streaming-gap',action='store_true')
    parser.add_argument('--after-discovery-bulk',action='store_true')
    parser.add_argument('--after-cache-diagnostics',action='store_true')
    parser.add_argument('--after-publication-prefetch',action='store_true')
    parser.add_argument('--candidate')
    parser.add_argument('--candidate-sha256')
    args=parser.parse_args()
    if sum([args.after_metadata_retry,args.after_dex_indexes,args.after_streaming_gap,args.after_discovery_bulk,args.after_cache_diagnostics,args.after_publication_prefetch])>1:
        parser.error('choose exactly one capture stage')
    stage='writer-full-scope-capture'
    frame_name='frozen-real-full-90m'
    if args.after_metadata_retry:
        EXPECTED='a2f486d7da51a6638a89090bbe5765eba7f690a5cd0016046eeeb64858633bb8'
        stage='writer-post-retry-capture'
        frame_name='frozen-real-post-retry-90m'
    if args.after_dex_indexes:
        assert not args.after_metadata_retry
        EXPECTED='531107de1b45b570e018db15a150c5af4ffb58fc5d9e404bb1c0662d1fc7ceb8'
        stage='writer-dex-index-capture'
        frame_name='frozen-real-dex-index-90m'
    if args.after_streaming_gap:
        assert not args.after_metadata_retry and not args.after_dex_indexes
        EXPECTED='d832574cf7fdf52525f7aadb548d67588ad18820d9555b5c3de14e3f425644ff'
        stage='writer-streaming-gap-capture'
        frame_name='frozen-real-streaming-gap-90m'
    if args.after_discovery_bulk:
        assert not args.after_metadata_retry and not args.after_dex_indexes and not args.after_streaming_gap
        EXPECTED='c88ac624372078d277ce922d48769f15142c9c335de7bb35acb7aaa9505c15e8'
        stage='writer-discovery-bulk-capture'
        frame_name='frozen-real-discovery-bulk-90m'
        observation=json.loads((OUT/'observation-latest.json').read_text())
        assert (dt.datetime.now(dt.timezone.utc)-dt.datetime.fromisoformat(observation['utc'])).total_seconds()<150, 'source observation is stale'
        collector=json.loads((OUT/'collector-discovery-bulk-deployment/deployment.json').read_text())
        assert observation['services']['collector']['MainPID']==collector['after']['MainPID'], 'observation belongs to an earlier collector'
        started=dt.datetime.fromisoformat(collector['start_returned_at'])
        options=[r for r in observation.get('source_health',[]) if r.get('msg')=='options catalog runtime health']
        paired=[r for r in observation.get('source_health',[]) if r.get('msg')=='OKX paired runtime health']
        assert options and options[-1]['entries']>=2806, 'full options scope has not recovered'
        assert paired and paired[-1]['books']['ReadyInstruments']>=332 and paired[-1]['books']['InvalidInstruments']==0, 'full paired source has not recovered'
        assert all(dt.datetime.fromisoformat(r['time'])>=started for r in [options[-1],paired[-1]]), 'source health belongs to an earlier collector'
    if args.after_cache_diagnostics or args.after_publication_prefetch:
        deployment=json.loads((OUT/('writer-publication-prefetch-deployment/deployment.json' if args.after_publication_prefetch else 'writer-process-metrics-deployment/deployment.json')).read_text())
        EXPECTED=deployment['new_sha256']
        stage='writer-cache-diagnostics-capture'
        frame_name='frozen-real-cache-diagnostics-90m'
        if args.after_publication_prefetch:
            stage='writer-publication-prefetch-capture'
            frame_name='frozen-real-publication-prefetch-90m'
        with (OUT/'continuous-observation.jsonl').open('rb') as history_file:
            history_file.seek(0,2);size=history_file.tell()
            history_file.seek(max(0,size-(4<<20)))
            if size>(4<<20):history_file.readline()
            history=[json.loads(line)for line in history_file if line.strip()]
        plans=json.loads((OUT/'observed-plan-counts.json').read_text())
        assert stable_full_scope(history,plans,dt.datetime.now(dt.timezone.utc)), 'ten continuous minutes of fresh full-scope observations required'
    before=state();assert before['ActiveState']=='active'
    with open('/proc/'+before['MainPID']+'/exe','rb') as f:assert hashlib.file_digest(f,'sha256').hexdigest()==EXPECTED
    target=ROOT/'var/storage/bin/storage-writer'
    candidate=None
    final_sha=EXPECTED
    if args.candidate:
        candidate=Path(args.candidate)
        assert candidate.parent==Path('/tmp') and candidate.name.startswith('cmi-storage-writer-')
        assert args.candidate_sha256 and re.fullmatch('[a-f0-9]{64}',args.candidate_sha256)
        with candidate.open('rb') as f:assert hashlib.file_digest(f,'sha256').hexdigest()==args.candidate_sha256
        final_sha=args.candidate_sha256
    else:
        assert not args.candidate_sha256
    backup=OUT/stage;backup.mkdir(mode=0o700)
    shutil.copy2(PATH,backup/'unit-before.service')
    if candidate:shutil.copy2(target,backup/'writer-before')
    start=dt.datetime.now(dt.timezone.utc).replace(second=0,microsecond=0)+dt.timedelta(minutes=2 if args.after_discovery_bulk or args.after_cache_diagnostics or args.after_publication_prefetch else 10)
    end=start+dt.timedelta(minutes=90)
    frames=OUT/frame_name;frames.mkdir(mode=0o700)
    text=PATH.read_text();lines=text.splitlines();replaced=0
    for i,line in enumerate(lines):
        if not line.startswith('ExecStart='):continue
        args=shlex.split(line[len('ExecStart='):]);filtered=[];n=0
        while n<len(args):
            arg=args[n]
            if arg.split('=',1)[0] in ['--archive-dir','--archive-from','--archive-to']:
                n+=1 if '=' in arg else 2;continue
            filtered.append(arg);n+=1
        filtered.extend(['--archive-dir',str(frames),'--archive-from',start.isoformat(),'--archive-to',end.isoformat()])
        lines[i]='ExecStart='+shlex.join(filtered);replaced+=1
    assert replaced==1
    report={'before':before,'window_from_utc':start.isoformat(),'window_to_utc':end.isoformat(),
            'frames':str(frames),'generation_changed':False,'old_sha256':EXPECTED,'new_sha256':final_sha,'stop_requested_at':dt.datetime.now(dt.timezone.utc).isoformat()}
    (backup/'deployment.json').write_text(json.dumps(report,indent=2)+'\n')
    subprocess.run(['systemctl','--user','stop',UNIT],check=True,timeout=60)
    assert state()['MainPID']=='0'
    if candidate:
        staged=target.with_suffix('.capture-new');shutil.copy2(candidate,staged)
        with staged.open('rb') as f:os.fsync(f.fileno())
        os.replace(staged,target)
        fd=os.open(target.parent,os.O_DIRECTORY);os.fsync(fd);os.close(fd)
    temp=PATH.with_suffix('.capture-new');temp.write_text('\n'.join(lines)+'\n');os.chmod(temp,0o600)
    with temp.open('rb') as f:os.fsync(f.fileno())
    os.replace(temp,PATH)
    subprocess.run(['systemctl','--user','daemon-reload'],check=True,timeout=20)
    subprocess.run(['systemctl','--user','start',UNIT],check=True,timeout=60)
    report.update(after=state(),start_returned_at=dt.datetime.now(dt.timezone.utc).isoformat())
    assert report['after']['ActiveState']=='active'
    with open('/proc/'+report['after']['MainPID']+'/exe','rb') as f:assert hashlib.file_digest(f,'sha256').hexdigest()==final_sha
    (backup/'deployment.json').write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({'capture_from':start.isoformat(),'capture_to':end.isoformat(),'writer':report['after']}))

if __name__=='__main__':run()
