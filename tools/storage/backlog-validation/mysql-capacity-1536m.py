#!/usr/bin/env python3
"""Reversible extra 512 MiB for the production historical B-trees."""
import argparse
import datetime as dt
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess

ROOT = Path('/home/ubuntu/crypto-market-info')
OUT = ROOT / 'var/storage/write-backlog-improvement/mysql-capacity-1536m'
NAME = 'crypto-market-info-mysql'
spec = importlib.util.spec_from_file_location('previous_trial', Path(__file__).with_name('mysql-capacity-trial.py'))
previous = importlib.util.module_from_spec(spec)
spec.loader.exec_module(previous)

def save(name, value):
    p = OUT / name
    staged = p.with_suffix('.tmp')
    with staged.open('w') as stream:
        json.dump(value, stream, indent=2);stream.write('\n');stream.flush();os.fsync(stream.fileno())
    os.chmod(staged, 0o600);os.replace(staged, p)
    fd = os.open(OUT, os.O_DIRECTORY)
    try: os.fsync(fd)
    finally: os.close(fd)

def current():
    value = previous.inspect(NAME)
    assert previous.sql(NAME, 'SELECT @@binlog_cache_size') == '4194304'
    value['resize_status'] = previous.sql(NAME, "SHOW GLOBAL STATUS LIKE 'Innodb_buffer_pool_resize_status'")
    status=previous.sql(NAME,"SHOW GLOBAL STATUS WHERE Variable_name IN ('Innodb_buffer_pool_pages_total','Innodb_buffer_pool_pages_data','Innodb_buffer_pool_pages_free','Innodb_buffer_pool_reads','Innodb_buffer_pool_read_requests','Innodb_buffer_pool_pages_flushed','Innodb_os_log_written','Innodb_log_waits')")
    value['engine_counters']={name:int(number) for name,number in (line.split('\t') for line in status.splitlines())}
    value['host_available_bytes'] = int(next(x.split()[1] for x in Path('/proc/meminfo').read_text().splitlines() if x.startswith('MemAvailable:'))) * 1024
    return value

def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=['start','grow-2g','inspect','keep','rollback','restore-limits'])
    action = parser.parse_args().action
    if action == 'start':
        OUT.mkdir(mode=0o700)
        before = current()
        assert before['buffer'] == 1024 << 20 and before['memory'] == 2560 << 20 and before['swap'] == 3072 << 20
        assert before['host_available_bytes'] >= 2 << 30, 'insufficient host headroom'
        save('before.json', before)
        for number,path in enumerate(previous.CONFIG_FILES):
            shutil.copy2(path, OUT / f'config-before-{number}')
        save('started.json', {'utc':dt.datetime.now(dt.timezone.utc).isoformat(),'scope':'production only; isolated buffer remains 1 GiB','durability_unchanged':True})
        previous.update_limits(NAME,3072<<20,3584<<20)
        previous.sql(NAME,'SET GLOBAL innodb_buffer_pool_size=1610612736')
        save('online.json',current())
    elif action == 'grow-2g':
        value=current()
        assert value['buffer']==1536<<20 and value['memory']==3072<<20 and value['swap']==3584<<20
        assert value['host_available_bytes']>=1800<<20, 'insufficient host headroom for another 512 MiB'
        assert not (OUT/'grow-2g.json').exists()
        save('grow-2g.json',{'utc':dt.datetime.now(dt.timezone.utc).isoformat(),'before':value,'target_buffer':2<<30,'target_memory':3584<<20,'target_swap':4096<<20})
        previous.update_limits(NAME,3584<<20,4096<<20)
        previous.sql(NAME,'SET GLOBAL innodb_buffer_pool_size=2147483648')
        save('online-2g.json',current())
    elif action == 'inspect':
        value = current();save('latest.json',{'utc':dt.datetime.now(dt.timezone.utc).isoformat(),**value});print(json.dumps(value));return
    elif action == 'keep':
        value = current()
        large=(OUT/'online-2g.json').exists()
        buffer_mib,memory_mib,swap_mib=(2048,3584,4096) if large else (1536,3072,3584)
        assert value['buffer']==buffer_mib<<20 and value['memory']==memory_mib<<20 and value['swap']==swap_mib<<20
        assert 'Completed resizing buffer pool' in value['resize_status']
        assert not (OUT/'config-prepared.json').exists()
        prepared=[]
        for number,path in enumerate(previous.CONFIG_FILES):
            old=(OUT/f'config-before-{number}').read_bytes()
            assert path.read_bytes()==old, 'configuration changed since trial start'
            text=old.decode()
            if path.suffix=='.cnf':
                assert text.count('innodb-buffer-pool-size=1024M')==1
                text=text.replace('innodb-buffer-pool-size=1024M',f'innodb-buffer-pool-size={buffer_mib}M')
            else:
                assert text.count('--memory 2560m --memory-swap 3072m')==1
                text=text.replace('--memory 2560m --memory-swap 3072m',f'--memory {memory_mib}m --memory-swap {swap_mib}m')
            prepared.append({'path':str(path),'before':hashlib.sha256(old).hexdigest(),'after':hashlib.sha256(text.encode()).hexdigest(),'text':text})
        save('config-prepared.json',prepared)
        for item in prepared: previous.replace_config(Path(item['path']),item['text'])
        subprocess.run(['systemctl','--user','daemon-reload'],check=True,timeout=15)
        save('kept.json',{'utc':dt.datetime.now(dt.timezone.utc).isoformat(),'current':value,'database_restarted':False,'capacity_passed':False})
    elif action == 'rollback':
        before=json.loads((OUT/'before.json').read_text());current()
        previous.sql(NAME,f"SET GLOBAL innodb_buffer_pool_size={before['buffer']}")
        if (OUT/'config-prepared.json').exists():
            for number,item in enumerate(json.loads((OUT/'config-prepared.json').read_text())):
                path=Path(item['path']);assert path in previous.CONFIG_FILES
                digest=hashlib.sha256(path.read_bytes()).hexdigest();assert digest in [item['before'],item['after']]
                if digest==item['after']:previous.replace_config(path,(OUT/f'config-before-{number}').read_text())
            subprocess.run(['systemctl','--user','daemon-reload'],check=True,timeout=15)
        save('rollback.json',{'utc':dt.datetime.now(dt.timezone.utc).isoformat(),'limit_restore_waits_for_resize':True})
    else:
        before=json.loads((OUT/'before.json').read_text());value=current()
        assert value['buffer']==before['buffer'] and 'Completed resizing buffer pool' in value['resize_status']
        previous.update_limits(NAME,before['memory'],before['swap'])
        save('limits-restored.json',{'utc':dt.datetime.now(dt.timezone.utc).isoformat()})
    print(json.dumps({'action':action,'evidence':str(OUT)}))

if __name__=='__main__':run()
