#!/usr/bin/env python3
"""Restore only the stopped, network-isolated test definition; retain its data."""
import datetime as dt
import http.client
import json
import os
from pathlib import Path
import socket

import docker_control as control

ROOT=Path('/home/ubuntu/crypto-market-info')
BASE=ROOT/'var/storage/write-backlog-improvement'
NAME='cmi-backlog-validation-mysql'
DATA=BASE/'bench-mysql/data'
RUN=BASE/'bench-mysql/run'
CONFIG=BASE/'bench-mysql/validation.cnf'
SECRET=Path('/home/ubuntu/.config/crypto-market-info-storage/mysql-root-password')

def run():
    assert DATA.is_dir() and DATA.stat().st_uid==1000
    assert 'a93e84b2-c7c3-11f1-bf43-7ab521822a4d' in (DATA/'auto.cnf').read_text()
    assert not Path('/proc/1144722').exists(), 'old isolated process still exists'
    prod=control.inspect('crypto-market-info-mysql')
    assert prod['State']['Running'] and prod['Config']['User']=='1000:1000'
    image=prod['Image'];assert image.startswith('sha256:')
    try:
        existing=control.inspect(NAME)
    except RuntimeError as error:
        if '(404)' not in str(error):raise
    else:
        assert not existing['State']['Running']
        print(json.dumps({'existing_stopped_definition':True}));return
    config=(ROOT/'deploy/storage/mysql.cnf').read_text()
    assert config.count('innodb-buffer-pool-size=2048M')==1
    config=config.replace('innodb-buffer-pool-size=2048M','innodb-buffer-pool-size=512M')
    CONFIG.write_text(config);CONFIG.chmod(0o600)
    with CONFIG.open('rb')as stream:os.fsync(stream.fileno())
    RUN.mkdir(mode=0o700,exist_ok=True)
    body={
        'Image':image,'User':'1000:1000',
        'Env':['MYSQL_ROOT_PASSWORD_FILE=/run/secrets/mysql-root-password'],
        'HostConfig':{'NetworkMode':'none','AutoRemove':False,
          'Memory':1536<<20,'MemorySwap':2048<<20,'NanoCpus':2000000000,
          'RestartPolicy':{'Name':'no'},
          'Binds':[str(DATA)+':/var/lib/mysql',str(RUN)+':/run/cmi',
                   str(CONFIG)+':/etc/mysql/conf.d/cmi.cnf:ro',
                   str(SECRET)+':/run/secrets/mysql-root-password:ro']}}
    report={'utc':dt.datetime.now(dt.timezone.utc).isoformat(),'image':image,
            'data_directory':str(DATA),'config':str(CONFIG),'buffer_bytes':512<<20,
            'memory_bytes':1536<<20,'swap_total_bytes':2048<<20,'network':'none',
            'auto_remove':False,'started':False,'data_reinitialized':False}
    out=BASE/'isolated-mysql-definition-restored.json';out.write_text(json.dumps(report,indent=2)+'\n')
    conn=control.UnixHTTP('localhost',timeout=15)
    try:
        conn.request('POST','/v1.45/containers/create?name='+NAME,body=json.dumps(body).encode(),headers={'Content-Type':'application/json','Connection':'close'})
        response=conn.getresponse();raw=response.read(1<<20)
        if response.status!=201:raise RuntimeError(f'isolated definition create failed ({response.status})')
        created=json.loads(raw);report['container_id']=created['Id']
    finally:conn.close()
    existing=control.inspect(NAME)
    assert not existing['State']['Running'] and existing['HostConfig']['AutoRemove'] is False
    assert existing['HostConfig']['Memory']==1536<<20
    report['verified_at']=dt.datetime.now(dt.timezone.utc).isoformat();out.write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps(report))

if __name__=='__main__':run()
