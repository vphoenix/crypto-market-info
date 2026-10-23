#!/usr/bin/env python3
"""Summarize observed process counters; never infer physical attribution sums."""
import argparse,datetime as dt,json
from pathlib import Path

def summarize(rows):
 a,b=rows[0],rows[-1];seconds=b['monotonic']-a['monotonic']
 if seconds<=0:return {'seconds':seconds}
 procs={}
 for pid,p in b['processes'].items():
  if pid not in a['processes']:continue
  old=a['processes'][pid]
  procs[p['name']]={'pid':pid,'write_MiB_s':(p['write_bytes']-old['write_bytes'])/seconds/2**20,'read_MiB_s':(p['read_bytes']-old['read_bytes'])/seconds/2**20}
 members=b['writer'].get('committed_members',0)-a['writer'].get('committed_members',0)
 payload=b['writer'].get('committed_bytes',0)-a['writer'].get('committed_bytes',0)
 writes=sum(p['write_MiB_s']for p in procs.values())
 m=b['mysql'];old=a['mysql']
 d=[y-x for x,y in zip(a['disk'],b['disk'])]
 result={'start_utc':a['utc'],'end_utc':b['utc'],'seconds':seconds,'processes':procs,'project_process_write_MiB_s':writes,'whole_disk_busy_pct':d[9]/seconds/10,'whole_disk_write_MiB_s':d[6]*512/seconds/2**20,'committed_members':members,'committed_batches':b['writer'].get('committed_batches',0)-a['writer'].get('committed_batches',0),'committed_payload_bytes':payload,'project_write_bytes_per_member':writes*seconds*2**20/members if members else None,'mysql_write_bytes_per_member':procs.get('mysqld',{}).get('write_MiB_s',0)*seconds*2**20/members if members else None,'project_write_per_payload_byte':writes*seconds*2**20/payload if payload else None,'dirty_pages_start_end':[old['Innodb_buffer_pool_pages_dirty'],m['Innodb_buffer_pool_pages_dirty']],'max_dirty_pages':max(v['mysql']['Innodb_buffer_pool_pages_dirty']for v in rows),'checkpoint_age_start_end':[v['mysql']['Innodb_redo_log_current_lsn']-v['mysql']['Innodb_redo_log_checkpoint_lsn']for v in [a,b]],'max_checkpoint_age':max(v['mysql']['Innodb_redo_log_current_lsn']-v['mysql']['Innodb_redo_log_checkpoint_lsn']for v in rows),'log_waits_delta':m['Innodb_log_waits']-old['Innodb_log_waits'],'free_page_waits_delta':m['Innodb_buffer_pool_wait_free']-old['Innodb_buffer_pool_wait_free'],'max_old_pending':max(v['writer'].get('old_pending',0)for v in rows),'max_pending':max(v['writer'].get('unique_pending',0)for v in rows),'max_blocked':max(v['writer'].get('blocked',0)for v in rows),'min_paired_ready':min(v['paired'].get('books',{}).get('ReadyInstruments',0)for v in rows),'min_options_entries':min(v['options'].get('entries',0)for v in rows),'max_options_unknown':max(v['options'].get('unknown_entries',0)for v in rows),'redis_memory_start_end':[v['queue']['redis_used_bytes']for v in [a,b]],'mysql_data_written_MiB_s':(m['Innodb_data_written']-old['Innodb_data_written'])/seconds/2**20,'redo_MiB_s':(m['Innodb_os_log_written']-old['Innodb_os_log_written'])/seconds/2**20}
 incomplete=[]
 for row in rows:
  o=row['options'];p=row['paired'].get('books',{})
  # Logs are read after the sample timestamp and its bounded SQL read.
  fresh=all(k in row and 'time' in row[k] and -15<=(dt.datetime.fromisoformat(row['utc'])-dt.datetime.fromisoformat(row[k]['time'])).total_seconds()<100 for k in ('options','paired','writer'))
  if not (fresh and o.get('entries',0)>=2900 and all(o.get(k,1)==0 for k in ('unknown_entries','stale_rule_entries','stale_state_entries')) and p.get('ReadyInstruments',0)>=330):incomplete.append(row['utc'])
 result.update({'samples':len(rows),'incomplete_source_samples':len(incomplete),'first_incomplete_source_utc':incomplete[0] if incomplete else None,'whole_disk_read_MiB_s':d[2]*512/seconds/2**20,'mysql_fsyncs_per_s':(m['Innodb_data_fsyncs']-old['Innodb_data_fsyncs'])/seconds})
 return result

def main():
 p=argparse.ArgumentParser();p.add_argument('directory',type=Path);args=p.parse_args();result={}
 for phase in ('baseline','candidate','restored'):
  f=args.directory/(phase+'.json')
  if not f.exists():continue
  rows=json.loads(f.read_text());result[phase]=summarize(rows);blocks=[];start=0
  for i in range(1,len(rows)):
   if rows[i]['monotonic']-rows[start]['monotonic']>=300:
    blocks.append(summarize(rows[start:i+1]));start=i
  result[phase]['five_minute_blocks']=blocks
 (args.directory/'summary.json').write_text(json.dumps(result,indent=2)+'\n')
 for k,v in result.items():print(json.dumps({'phase':k,**{n:v.get(n)for n in ['seconds','project_process_write_MiB_s','whole_disk_busy_pct','committed_members','mysql_write_bytes_per_member','max_old_pending','max_checkpoint_age','min_paired_ready','min_options_entries','max_options_unknown']}}))
if __name__=='__main__':main()
