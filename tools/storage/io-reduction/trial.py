#!/usr/bin/env python3
"""Finite production A/B/A flush trial. Identity/durability pinned; always restore.
No SQL contents, credentials, data deletion, or service restart are needed.
"""
import argparse, datetime as dt, json, os, signal, sys, time
from pathlib import Path
ROOT=Path(__file__).resolve().parents[3]
sys.path.insert(0,str(ROOT/'tools/storage/backlog-validation'))
import mysql_client as mysql
sys.path.insert(0,str(ROOT/'tools/storage/io-reduction'))
import observe

def utc():return dt.datetime.now(dt.timezone.utc).isoformat()
def sql(q):return mysql.sql('production','SET SESSION max_execution_time=2500;'+q,limit=8)
def save(path,obj):
 raw=json.dumps(obj,indent=2)+'\n'
 with path.with_suffix('.tmp').open('w') as f:f.write(raw);f.flush();os.fsync(f.fileno())
 os.replace(path.with_suffix('.tmp'),path)
def settings():
 r=json.loads(sql("SELECT JSON_OBJECT('uuid',@@server_uuid,'capacity',@@innodb_io_capacity,'maximum',@@innodb_io_capacity_max,'redo_capacity',@@innodb_redo_log_capacity,'commit_fsync',@@innodb_flush_log_at_trx_commit,'doublewrite',@@innodb_doublewrite,'flush_sync',@@innodb_flush_sync,'log_bin',@@log_bin);"))
 assert r['uuid']==mysql.SERVERS['production'][1] and r['commit_fsync']==1 and r['doublewrite']=='ON' and r['flush_sync']==1 and r['log_bin']==0,'identity/durability changed'
 return r

def logs(path,limit=2<<20):
 with path.open('rb') as f:
  size=f.seek(0,2);f.seek(max(0,size-limit))
  if size>limit:f.readline()
  out=[]
  for line in f:
   try:out.append(json.loads(line))
   except ValueError:pass
  return out

def capture():
 r={'utc':utc(),'monotonic':time.monotonic(),'disk':[int(x)for x in Path('/sys/block/sda/stat').read_text().split()],'processes':{}}
 for p in Path('/proc').iterdir():
  if not p.name.isdigit():continue
  try:
   name=(p/'comm').read_text().strip()
   if name not in ('mysqld','redis-server','collector','storage-writer','across-data','reserve-data','lst-data','justlend-keeper'):continue
   r['processes'][p.name]={'name':name,**{k:int(v)for k,v in (line.split(':',1)for line in (p/'io').read_text().splitlines())}}
  except (OSError,ValueError):pass
 r['mysql']={x['name']:int(x['value'])for x in map(json.loads,sql("SELECT JSON_OBJECT('name',VARIABLE_NAME,'value',VARIABLE_VALUE) FROM performance_schema.global_status WHERE VARIABLE_NAME IN ('Innodb_os_log_written','Innodb_log_waits','Innodb_buffer_pool_wait_free','Innodb_redo_log_current_lsn','Innodb_redo_log_checkpoint_lsn','Innodb_buffer_pool_pages_dirty','Innodb_buffer_pool_pages_total','Innodb_data_written','Innodb_data_fsyncs','Innodb_dblwr_pages_written');").splitlines())}
 r['queue']=observe.queues()
 wl=logs(ROOT/'var/storage/writer.log');r['writer']=next((v for v in reversed(wl)if v.get('msg')=='writer metrics'),{})
 cl=logs(Path('/home/ubuntu/.local/share/crypto-market-info-collector/logs/collector.log'))
 r['options']=next((v for v in reversed(cl)if v.get('msg')=='options catalog runtime health'),{})
 r['paired']=next((v for v in reversed(cl)if v.get('msg')=='OKX paired runtime health'),{})
 return r

def full(r):
 return (r['options'].get('entries',0)>=2900 and r['options'].get('unknown_entries',1)==0 and r['options'].get('stale_rule_entries',1)==0 and r['options'].get('stale_state_entries',1)==0 and r['paired'].get('books',{}).get('ReadyInstruments',0)>=330 and r['writer'].get('old_pending',1)==0 and r['writer'].get('blocked',1)==0 and all((dt.datetime.now(dt.timezone.utc)-dt.datetime.fromisoformat(r[k]['time'])).total_seconds()<100 for k in ('options','paired','writer')))

def change(capacity,maximum):
 state=settings();assert (state['capacity'],state['maximum'])in [(10000,20000),(1000,2000),(1000,20000)],'outside change detected'
 if capacity==10000:sql('SET GLOBAL innodb_io_capacity_max=20000;SET GLOBAL innodb_io_capacity=10000;')
 else:sql('SET GLOBAL innodb_io_capacity=1000;SET GLOBAL innodb_io_capacity_max=2000;')
 r=settings();assert (r['capacity'],r['maximum'])==(capacity,maximum)
 return r

def main():
 def interrupted(signum,frame):raise InterruptedError('trial interrupted; restoring parameters')
 for sig in (signal.SIGTERM,signal.SIGHUP,signal.SIGINT):signal.signal(sig,interrupted)
 p=argparse.ArgumentParser();p.add_argument('--output',type=Path,required=True);p.add_argument('--candidate-seconds',type=int,default=1800);a=p.parse_args()
 assert 300<=a.candidate_seconds<=1800
 out=a.output.resolve();out.mkdir(mode=0o700,parents=True,exist_ok=False)
 initial=settings();assert (initial['capacity'],initial['maximum'])==(10000,20000)
 save(out/'initial.json',initial)
 # Startup and cache reclamation are excluded from acceptance windows.
 ready=0;deadline=time.monotonic()+1800
 while ready<6:
  r=capture();save(out/'waiting.json',r);ready=ready+1 if full(r)else 0
  print(json.dumps({'utc':utc(),'phase':'waiting_full_scope','consecutive':ready,'entries':r['options'].get('entries'),'old_pending':r['writer'].get('old_pending')}),flush=True)
  if time.monotonic()>deadline:raise RuntimeError('full source scope did not recover in 30 minutes')
  time.sleep(10)
 applied=False
 def phase(name,seconds):
  rows=[];start=time.monotonic();pressure=0
  while True:
   r=capture();r['phase']=name;rows.append(r);save(out/(name+'.json'),rows)
   m=r['mysql'];age=m['Innodb_redo_log_current_lsn']-m['Innodb_redo_log_checkpoint_lsn']
   if name=='candidate':
    bad=age>initial['redo_capacity']*3//4 or r['writer'].get('old_pending',0)>0 or r['writer'].get('blocked',0)>0
    pressure=pressure+1 if bad else 0
    assert pressure<3,'sustained checkpoint/backlog pressure'
    assert m['Innodb_log_waits']==rows[0]['mysql']['Innodb_log_waits'],'new redo waits'
   if len(rows)%6==1:print(json.dumps({'utc':utc(),'phase':name,'elapsed':round(time.monotonic()-start),'full':full(r),'dirty':m['Innodb_buffer_pool_pages_dirty'],'checkpoint_age':age,'old_pending':r['writer'].get('old_pending')}),flush=True)
   if time.monotonic()-start>=seconds:return
   time.sleep(10)
 try:
  phase('baseline',300)
  # Mark intent before first SET so partial parameter application also restores.
  applied=True;save(out/'applied.json',change(1000,2000))
  phase('candidate',a.candidate_seconds)
 finally:
  if applied:save(out/'restore-settings.json',change(10000,20000))
 phase('restored',300)
 save(out/'complete.json',{'utc':utc(),'automatically_restored':True,'accepted':False})
if __name__=='__main__':main()
