"""Capture bounded, read-only production evidence with exact JSON numbers."""
from pathlib import Path
from datetime import datetime, timezone
from decimal import Decimal
import json, urllib.request, hashlib

ROOT = Path(__file__).resolve().parent
URL = 'http://127.0.0.1:8123/?database=crypto_market_info&readonly=1&max_execution_time=60&max_threads=2'

def query(name, sql, root=ROOT):
    sql += ' FORMAT JSONEachRow'
    root.mkdir(parents=True, exist_ok=True)
    (root/(name+'.sql')).write_text(sql+'\n')
    at = datetime.now(timezone.utc).isoformat()
    with urllib.request.urlopen(urllib.request.Request(URL, data=sql.encode()), timeout=65) as r:
        raw = r.read()
    (root/(name+'.jsonl')).write_bytes(raw)
    with (root/'capture.jsonl').open('a') as f:
        f.write(json.dumps({'name':name,'captured_at_utc':at,'sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw),'url':URL})+'\n')
    print(name, len(raw), flush=True)
    return [json.loads(x, parse_float=Decimal) for x in raw.splitlines()]

if __name__ == '__main__':
    query('settings', "SELECT name,value FROM system.settings WHERE name IN ('readonly','max_threads','max_execution_time')")
    query('now', "SELECT now64(6,'UTC') AS now_utc, timezone() AS server_timezone")
    query('runs', 'SELECT * FROM options_live_run FINAL ORDER BY started_at DESC LIMIT 12')
    commit = query('latest_commit', "SELECT run_id,minute_time,batch_id,run_hash,prepared_at,instrument_ids,member_hashes,anchor_count,delta_count,index_ids,index_hashes FROM options_live_minute_commit FINAL WHERE minute_time >= '2026-10-01 00:00:00' ORDER BY minute_time DESC LIMIT 1")
    if not commit:
        commit = query('latest_commit', "SELECT run_id,minute_time,batch_id,run_hash,prepared_at,instrument_ids,member_hashes,anchor_count,delta_count,index_ids,index_hashes FROM options_live_minute_commit FINAL WHERE minute_time >= '2026-09-25 00:00:00' ORDER BY minute_time DESC LIMIT 1")
    c = commit[0]
    ids = ','.join(map(str,c['instrument_ids']))
    query('instruments', f'SELECT * FROM instrument FINAL WHERE instrument_id IN ({ids}) ORDER BY instrument_id')
    query('specs', f'SELECT * FROM derivative_contract_spec FINAL WHERE instrument_id IN ({ids}) ORDER BY instrument_id')
    query('latest_rules', f'SELECT * FROM derivative_trading_rule FINAL WHERE instrument_id IN ({ids}) ORDER BY instrument_id,known_from')
    minute, batch = c['minute_time'], c['batch_id']
    query('latest_books', f"SELECT * FROM derivative_book_minute FINAL WHERE instrument_id IN ({ids}) AND minute_time='{minute}' AND batch_id='{batch}' ORDER BY instrument_id")
    query('latest_quality', f"SELECT instrument_id,minute_time,batch_id,signed,row_hash,sampled_bitmap,stream_valid_bitmap,replay_valid_bitmap,market_known_bitmap,market_open_bitmap,source_times,received_times,captured_times,last_snapshot_times,connection_confirmed_times,rule_published_times,market_state_times,connection_epochs,change_ids,trading_rule_ids,market_state_bases,reasons,bid_level_counts,ask_level_counts FROM derivative_book_quality_minute FINAL WHERE instrument_id IN ({ids}) AND minute_time='{minute}' AND batch_id='{batch}' ORDER BY instrument_id")
    query('latest_deltas', f"SELECT * FROM derivative_book_second_delta FINAL WHERE minute_id IN (SELECT id FROM derivative_book_minute WHERE instrument_id IN ({ids}) AND minute_time='{minute}' AND batch_id='{batch}') AND batch_id='{batch}' ORDER BY minute_id,second_offset")
    query('latest_indexes', f"SELECT * FROM options_index_minute FINAL WHERE minute_time='{minute}' AND batch_id='{batch}' ORDER BY index_id")
