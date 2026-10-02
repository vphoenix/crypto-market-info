"""Capture five independent committed minutes and all sixty replay seconds."""
from pathlib import Path
import json
from capture import query,ROOT

windows=[('2026-09-28 18:00:00','2026-09-28 18:01:00'),('2026-09-29 12:00:00','2026-09-29 12:01:00'),('2026-09-30 14:00:00','2026-09-30 14:01:00'),('2026-10-01 08:00:00','2026-10-01 08:01:00')]
manifest=[]
for n,(start,end) in enumerate(windows):
    out=ROOT/'windows'/str(n)
    commits=query('latest_commit',f"SELECT * FROM options_live_minute_commit FINAL WHERE minute_time>='{start}' AND minute_time<'{end}' ORDER BY minute_time DESC LIMIT 1",out)
    if not commits:
        manifest.append({'start':start,'end':end,'status':'missing'});continue
    c=commits[0];ids=','.join(map(str,c['instrument_ids']));minute,batch=c['minute_time'],c['batch_id']
    query('instruments',f'SELECT * FROM instrument FINAL WHERE instrument_id IN ({ids}) ORDER BY instrument_id',out)
    query('specs',f'SELECT * FROM derivative_contract_spec FINAL WHERE instrument_id IN ({ids}) ORDER BY instrument_id',out)
    query('latest_books',f"SELECT * FROM derivative_book_minute FINAL WHERE instrument_id IN ({ids}) AND minute_time='{minute}' AND batch_id='{batch}' ORDER BY instrument_id",out)
    query('latest_quality',f"SELECT * FROM derivative_book_quality_minute FINAL WHERE instrument_id IN ({ids}) AND minute_time='{minute}' AND batch_id='{batch}' ORDER BY instrument_id",out)
    query('latest_deltas',f"SELECT * FROM derivative_book_second_delta FINAL WHERE minute_id IN (SELECT id FROM derivative_book_minute WHERE instrument_id IN ({ids}) AND minute_time='{minute}' AND batch_id='{batch}') AND batch_id='{batch}' ORDER BY minute_id,second_offset",out)
    query('latest_indexes',f"SELECT * FROM options_index_minute FINAL WHERE minute_time='{minute}' AND batch_id='{batch}' ORDER BY index_id",out)
    manifest.append({'start':start,'end':end,'status':'captured','run':c['run_id'],'minute':minute,'batch':batch})
(ROOT/'window_manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
