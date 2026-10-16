#!/usr/bin/env python3
"""Reversible production-only 2 -> 2.5 GiB cache trial, with host headroom."""
import argparse
import datetime as dt
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess

ROOT=Path('/home/ubuntu/crypto-market-info')
OUT=ROOT/'var/storage/write-backlog-improvement/mysql-capacity-2560m'
NAME='crypto-market-info-mysql'
spec=importlib.util.spec_from_file_location('capacity_tools',Path(__file__).with_name('mysql-capacity-trial.py'))
previous=importlib.util.module_from_spec(spec);spec.loader.exec_module(previous)

def save(name,value):
    path=OUT/name;staged=path.with_suffix('.tmp')
    with staged.open('w')as stream:json.dump(value,stream,indent=2);stream.write('\n');stream.flush();os.fsync(stream.fileno())
    staged.chmod(0o600);os.replace(staged,path)
    directory=os.open(OUT,os.O_DIRECTORY)
    try:os.fsync(directory)
    finally:os.close(directory)

def current():
    value=previous.inspect(NAME)
    assert previous.sql(NAME,'SELECT @@binlog_cache_size,@@innodb_io_capacity,@@innodb_io_capacity_max')=='4194304\t10000\t20000'
    value['resize_status']=previous.sql(NAME,"SHOW GLOBAL STATUS LIKE 'Innodb_buffer_pool_resize_status'")
    result=previous.sql(NAME,"SHOW GLOBAL STATUS WHERE Variable_name IN ('Innodb_buffer_pool_pages_total','Innodb_buffer_pool_pages_data','Innodb_buffer_pool_pages_dirty','Innodb_buffer_pool_reads','Innodb_buffer_pool_read_requests','Innodb_buffer_pool_pages_flushed','Innodb_log_waits')")
    value['counters']={k:int(v)for k,v in(line.split('\t')for line in result.splitlines())}
    value['host_available_bytes']=int(next(l.split()[1]for l in Path('/proc/meminfo').read_text().splitlines()if l.startswith('MemAvailable:')))*1024
    return value

def run():
    parser=argparse.ArgumentParser();parser.add_argument('action',choices=['start','inspect','keep','rollback','restore-limits']);action=parser.parse_args().action
    if action=='start':
        value=current()
        assert value['buffer']==(2048<<20) and value['memory']==(3584<<20) and value['swap']==(4096<<20)
        assert value['host_available_bytes']>=(1800<<20),'insufficient host headroom'
        OUT.mkdir(mode=0o700)
        save('before.json',value)
        for n,path in enumerate(previous.CONFIG_FILES):shutil.copy2(path,OUT/f'config-before-{n}')
        save('started.json',{'utc':dt.datetime.now(dt.timezone.utc).isoformat(),'scope':'production only; idle isolated database stopped with data retained','capacity_passed':False})
        previous.update_limits(NAME,4096<<20,4608<<20)
        previous.sql(NAME,'SET GLOBAL innodb_buffer_pool_size=2684354560')
        save('online.json',current())
    elif action=='inspect':
        value=current();save('latest.json',{'utc':dt.datetime.now(dt.timezone.utc).isoformat(),**value});print(json.dumps(value));return
    elif action=='keep':
        value=current();assert value['buffer']==(2560<<20) and value['memory']==(4096<<20) and value['swap']==(4608<<20)
        assert value['counters']['Innodb_buffer_pool_pages_total']==163840 and 'Completed resizing buffer pool' in value['resize_status']
        assert not(OUT/'config-prepared.json').exists()
        prepared=[]
        for n,path in enumerate(previous.CONFIG_FILES):
            old=(OUT/f'config-before-{n}').read_bytes();assert path.read_bytes()==old
            text=old.decode()
            if path.suffix=='.cnf':
                assert text.count('innodb-buffer-pool-size=2048M')==1;text=text.replace('innodb-buffer-pool-size=2048M','innodb-buffer-pool-size=2560M')
            else:
                assert text.count('--memory 3584m --memory-swap 4096m')==1;text=text.replace('--memory 3584m --memory-swap 4096m','--memory 4096m --memory-swap 4608m')
            prepared.append({'path':str(path),'before':hashlib.sha256(old).hexdigest(),'after':hashlib.sha256(text.encode()).hexdigest(),'text':text})
        save('config-prepared.json',prepared)
        for item in prepared:previous.replace_config(Path(item['path']),item['text'])
        subprocess.run(['systemctl','--user','daemon-reload'],check=True,timeout=15)
        save('kept.json',{'utc':dt.datetime.now(dt.timezone.utc).isoformat(),'database_restarted':False,'capacity_passed':False,'current':value})
    elif action=='rollback':
        before=json.loads((OUT/'before.json').read_text());current();previous.sql(NAME,f"SET GLOBAL innodb_buffer_pool_size={before['buffer']}")
        if(OUT/'config-prepared.json').exists():
            for n,item in enumerate(json.loads((OUT/'config-prepared.json').read_text())):
                path=Path(item['path']);assert path in previous.CONFIG_FILES
                digest=hashlib.sha256(path.read_bytes()).hexdigest();assert digest in[item['before'],item['after']]
                if digest==item['after']:previous.replace_config(path,(OUT/f'config-before-{n}').read_text())
            subprocess.run(['systemctl','--user','daemon-reload'],check=True,timeout=15)
        save('rollback.json',{'utc':dt.datetime.now(dt.timezone.utc).isoformat(),'limit_restore_waits_for_resize':True})
    else:
        value=current();before=json.loads((OUT/'before.json').read_text());assert value['buffer']==before['buffer'] and value['counters']['Innodb_buffer_pool_pages_total']==131072 and 'Completed resizing buffer pool' in value['resize_status']
        previous.update_limits(NAME,before['memory'],before['swap'])
        save('limits-restored.json',{'utc':dt.datetime.now(dt.timezone.utc).isoformat()})
    print(json.dumps({'action':action,'evidence':str(OUT)}))

if __name__=='__main__':run()
