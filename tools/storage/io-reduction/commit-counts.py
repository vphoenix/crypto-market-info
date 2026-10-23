#!/usr/bin/env python3
"""Read bounded commit counts after trial phases, independent of log cadence."""
import argparse,datetime as dt,json,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[3]
sys.path.insert(0,str(ROOT/'tools/storage/backlog-validation'));import mysql_client as mysql
p=argparse.ArgumentParser();p.add_argument('directory',type=Path);a=p.parse_args();out={}
for phase in ('baseline','candidate','restored'):
 path=a.directory/(phase+'.json')
 if not path.exists():continue
 rows=json.loads(path.read_text());start=dt.datetime.fromisoformat(rows[0]['utc']);end=dt.datetime.fromisoformat(rows[-1]['utc'])
 if end<=start:continue
 begin=(start-dt.timedelta(minutes=3)).strftime('%Y-%m-%d %H:%M:00');last=end.strftime('%Y-%m-%d %H:%M:00')
 q="SELECT JSON_OBJECT('kind',CAST(kind AS CHAR),'batches',COUNT(*),'members',SUM(member_count)) FROM cmi_storage.ingest_batch FORCE INDEX(logical_time) WHERE database_name='crypto_market_info' AND logical_time>='"+begin+"' AND logical_time<='"+last+"' AND committed_at>='"+start.strftime('%Y-%m-%d %H:%M:%S.%f')+"' AND committed_at<'"+end.strftime('%Y-%m-%d %H:%M:%S.%f')+"' GROUP BY kind;"
 result=mysql.sql('production','SET SESSION max_execution_time=2500;'+q,limit=8)
 out[phase]={'start_utc':start.isoformat(),'end_utc':end.isoformat(),'logical_lower_bound_utc':begin,'rows':[json.loads(x)for x in result.splitlines()]}
(a.directory/'commit-counts.json').write_text(json.dumps(out,indent=2)+'\n');print(json.dumps(out,indent=2))
