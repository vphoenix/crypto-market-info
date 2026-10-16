#!/usr/bin/env python3
"""Create independent snapshots of the 59 reviewed startup dependency tables.

Never stop a service, modify a source table, or copy hot book/minute history.
The manifest is sealed after every table snapshot has completed.
"""
import datetime
import hashlib
import fcntl
import json
from pathlib import Path
import re
import sys
import urllib.parse
import urllib.request
import urllib.error

if len(sys.argv)!=2:raise SystemExit('usage: snapshot-bootstrap.py ABSOLUTE_PREPARATION_DIR')
root=Path(sys.argv[1]);assert root.is_absolute();root.mkdir(mode=0o700,parents=True,exist_ok=True)
lock=open(root/'snapshot.lock','a')
fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
project=Path(__file__).resolve().parents[2]
families=['crypto_market_info','crypto_market_info_across','crypto_market_info_reserve','crypto_market_info_lst','crypto_market_info_justlend_keeper']
excluded={'order_book_minute','order_book_second_delta','cex_book_minute_commit','derivative_book_minute','derivative_book_second_delta','derivative_book_quality_minute','derivative_book_foundation_commit','options_live_minute_commit','options_catalog_live_minute_commit','options_catalog_quality_evidence_minute','options_index_minute'}
catalog=json.loads((project/'internal/storage/mysql/mapping.json').read_text())['tables']
tables=[t for t in catalog if t['database'] in families and (t['database']!='crypto_market_info' or t['table'] not in excluded)]
assert len(tables)==59
identity=root/'snapshot-identity.json'
if not identity.exists():
 import uuid
 identity.write_text(json.dumps({'snapshot_database':'cmi_bootstrap_'+uuid.uuid4().hex[:16],'runs':{f:str(uuid.uuid4()) for f in families}},indent=2)+'\n')
ids=json.loads(identity.read_text());db=ids['snapshot_database'];assert re.fullmatch(r'cmi_bootstrap_[0-9a-f]{16}',db)
progress=root/'snapshot-progress.json';done=json.loads(progress.read_text()) if progress.exists() else []
events_file=root/'snapshot-operations.json';events=json.loads(events_file.read_text()) if events_file.exists() else []
small_blocks=False
wide_blocks=False

def query(sql,fmt=None,readonly=False):
 params={'query':sql+(' FORMAT '+fmt if fmt else ''),'max_threads':'1','max_memory_usage':str(512<<20),
         'max_execution_time':'1800','max_block_size':'64' if small_blocks else ('512' if wide_blocks else '4096'),'max_insert_threads':'1',
         'min_insert_block_size_rows':'64' if small_blocks else ('1024' if wide_blocks else '65536'),
         'min_insert_block_size_bytes':str((4 if small_blocks else (2 if wide_blocks else 16))<<20),
         'max_read_buffer_size':str(256<<10),
         'max_local_read_bandwidth':str(16<<20),'log_comment':'cmi-bootstrap-snapshot-only'}
 if readonly:params['readonly']='2'
 request=urllib.request.Request('http://127.0.0.1:8123/?'+urllib.parse.urlencode(params),data=b'')
 try:
  with urllib.request.urlopen(request,timeout=1860) as response:body=response.read()
 except urllib.error.HTTPError as error:
  failure=error.read(8192).decode(errors='replace')
  print('snapshot SQL failed:',failure,flush=True)
  raise
 events.append({'at_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'sql':sql,'read_only':readonly,'bytes':len(body),'sha256':hashlib.sha256(body).hexdigest()})
 events_file.write_text(json.dumps(events,indent=2)+'\n')
 return [json.loads(line) for line in body.splitlines()] if fmt=='JSONEachRow' else body

query('CREATE DATABASE IF NOT EXISTS `'+db+'`')
names={(t['database'],t['table']):'b'+str(i).zfill(3) for i,t in enumerate(tables)}
markers={'dex_block','across_capture','reserve_capture','lst_capture','jl_keeper_capture','options_live_run','options_collection_plan','perpetual_universe_run'}
ordered=sorted(tables,key=lambda t:(t['table'] not in markers,t['database'],t['table']))
for table in ordered:
 family=table['database'];name=table['table'];snapshot=names[family,name]
 if any(v['family']==family and v['table']==name for v in done):continue
 small_blocks=name=='options_collection_plan'
 wide_blocks=len(table['columns'])>=50
 original=f'`{family}`.`{name}`';target=f'`{db}`.`{snapshot}`'
 meta=query(f"SELECT create_table_query FROM system.tables WHERE database='{family}' AND name='{name}'",'JSONEachRow',True)
 assert len(meta)==1 and hashlib.sha256(meta[0]['create_table_query'].encode()).hexdigest()==table['source_ddl_sha256'],('source DDL changed',family,name)
 exists=query(f"SELECT count() AS n FROM system.tables WHERE database='{db}' AND name='{snapshot}'",'JSONEachRow',True)[0]['n']
 if exists:
  # Only an incomplete table in this independently owned snapshot is rebuilt.
  query('DROP TABLE '+target)
 query('CREATE TABLE '+target+' AS '+original)
 cols=','.join('`'+c['name']+'`' for c in table['columns']);final=' FINAL' if table['source_engine']=='ReplacingMergeTree' else ''
 started=datetime.datetime.now(datetime.timezone.utc).isoformat()
 print('snapshot begin',family,name,flush=True)
 # Freeze physical rows first. Applying FINAL across all live source parts can
 # exceed the memory budget; the independent clone keeps the original engine
 # and version column, so readers apply the same FINAL semantics to the clone.
 query(f'INSERT INTO {target} ({cols}) SELECT {cols} FROM {original}')
 rows=query('SELECT count() AS n FROM '+target+final,'JSONEachRow',True)[0]['n']
 ddl=query(f"SELECT create_table_query FROM system.tables WHERE database='{db}' AND name='{snapshot}'",'JSONEachRow',True)[0]['create_table_query']
 done.append({'family':family,'table':name,'snapshot_database':db,'snapshot_table':snapshot,'source_ddl_sha256':table['source_ddl_sha256'],
              'snapshot_ddl_sha256':hashlib.sha256(ddl.encode()).hexdigest(),'rows':rows,'started_at_utc':started,
              'completed_at_utc':datetime.datetime.now(datetime.timezone.utc).isoformat()})
 progress.write_text(json.dumps(done,indent=2)+'\n');print('snapshot finished',family,name,rows,len(done),'/59',flush=True)
assert len(done)==59
manifest={'version':1,'tables':sorted(done,key=lambda v:(v['family'],v['table']))}
path=root/'snapshot-manifest.json';encoded=json.dumps(manifest,indent=2)+'\n'
if path.exists():assert path.read_text()==encoded,'sealed manifest changed'
else:path.write_text(encoded)
print('sealed snapshot',path,'rows',sum(v['rows'] for v in done),flush=True)
