import json, pathlib, urllib.request
ROOT=pathlib.Path(__file__).resolve().parent
commit=json.loads((ROOT/'latest_commit.jsonl').read_text())
batch=commit['batch_id']
minute=commit['minute_time']
member_ids=','.join(str(i) for i in commit['instrument_ids'])
queries={
'latest_books':f"SELECT * FROM derivative_book_minute FINAL WHERE minute_time='{minute}' AND batch_id='{batch}' ORDER BY instrument_id FORMAT JSONEachRow",
'latest_quality':f"SELECT instrument_id,minute_time,batch_id,signed,row_hash,sampled_bitmap,stream_valid_bitmap,replay_valid_bitmap,market_known_bitmap,market_open_bitmap,source_times,received_times,captured_times,last_snapshot_times,connection_confirmed_times,rule_published_times,market_state_times,connection_epochs,change_ids,trading_rule_ids,market_state_bases,reasons,bid_level_counts,ask_level_counts FROM derivative_book_quality_minute FINAL WHERE minute_time='{minute}' AND batch_id='{batch}' ORDER BY instrument_id FORMAT JSONEachRow",
'latest_deltas':f"SELECT * FROM derivative_book_second_delta FINAL WHERE minute_id IN (SELECT id FROM derivative_book_minute WHERE minute_time='{minute}' AND instrument_id IN ({member_ids})) AND batch_id='{batch}' ORDER BY minute_id,second_offset FORMAT JSONEachRow",
'latest_indexes':f"SELECT * FROM options_index_minute FINAL WHERE minute_time='{minute}' AND batch_id='{batch}' ORDER BY index_id FORMAT JSONEachRow",
'latest_rules':f"SELECT * FROM derivative_trading_rule FINAL WHERE instrument_id IN ({','.join(str(i) for i in commit['instrument_ids'])}) ORDER BY instrument_id,known_from FORMAT JSONEachRow",
}
for name,query in queries.items():
 (ROOT/(name+'.sql')).write_text(query+'\n')
 with urllib.request.urlopen(urllib.request.Request('http://127.0.0.1:8123/?database=crypto_market_info',data=query.encode()),timeout=60) as resp: raw=resp.read()
 (ROOT/(name+'.jsonl')).write_bytes(raw)
 print(name,len(raw),len(raw.splitlines()))
