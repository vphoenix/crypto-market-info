"""Read-only localhost ClickHouse exports; no private APIs or database writes."""
import json,hashlib
from pathlib import Path
from datetime import datetime, timezone
from urllib.request import Request, urlopen
from urllib.error import HTTPError
ROOT=Path(__file__).resolve().parent
END='2026-09-26 16:00:00'
START='2026-09-19 16:00:00'
def query(name,sql):
 (ROOT/(name+'.sql')).write_text(sql+'\n')
 req=Request('http://127.0.0.1:8123/?readonly=1&max_execution_time=60&max_threads=2&output_format_json_quote_decimals=1',data=(sql+' FORMAT JSONEachRow').encode())
 try:
  with urlopen(req,timeout=90) as r:raw=r.read()
 except HTTPError as e:raise RuntimeError(e.read().decode()) from e
 (ROOT/(name+'.jsonl')).write_bytes(raw)
 rows=[json.loads(x,parse_float=str) for x in raw.splitlines()]
 (ROOT/(name+'.meta.json')).write_text(json.dumps({'queried_at_utc':datetime.now(timezone.utc).isoformat(),'sha256':hashlib.sha256(raw).hexdigest(),'rows':len(rows)},indent=2)+'\n')
 print(name,len(rows),flush=True)
 return rows
if __name__=='__main__':
 run=query('current-run',"SELECT * FROM crypto_market_info_perp_soak.perpetual_universe_run FINAL ORDER BY started_at DESC LIMIT 1")[0]
 print(run,flush=True)
 query('members',f"SELECT * FROM crypto_market_info_perp_soak.perpetual_universe_member FINAL WHERE run_id='{run['run_id']}'")
 query('mapping',f"SELECT * FROM crypto_market_info_perp_soak.instrument_canonical_mapping FINAL WHERE mapping_revision='{run['mapping_revision']}'")
 for db,label in [('crypto_market_info_perp_soak','soak'),('crypto_market_info','production')]:
  query(label+'-instruments',f'SELECT * FROM {db}.instrument FINAL ORDER BY instrument_id')
  query(label+'-funding',f"SELECT * FROM {db}.funding_rate_hourly FINAL WHERE hour_time>=toDateTime('{START}','UTC') ORDER BY instrument_id,hour_time")
  query(label+'-coverage',f"SELECT instrument_id,count() AS minutes,min(minute_time) AS first_minute,max(minute_time) AS last_minute,sum(bitCount(valid_bitmap)) AS valid_seconds,countIf(bitTest(valid_bitmap,0)) AS valid_anchors FROM {db}.order_book_minute FINAL WHERE minute_time>=toDateTime('{START}','UTC') GROUP BY instrument_id ORDER BY instrument_id")
  cols=','.join(f'{side}_{kind}_{i:02}' for side in ['bid','ask'] for kind in ['price','qty'] for i in range(1,11))
  query(label+'-latest-books',f"SELECT id,instrument_id,minute_time,valid_bitmap,stored_depth,{cols} FROM {db}.order_book_minute FINAL WHERE minute_time>=now()-INTERVAL 10 MINUTE AND bitTest(valid_bitmap,0) ORDER BY minute_time DESC LIMIT 1 BY instrument_id")
 query('spot-bbo',f"SELECT instrument_id,minute_time,valid_bitmap,bid_price_01,bid_qty_01,ask_price_01,ask_qty_01 FROM crypto_market_info.order_book_minute FINAL WHERE instrument_id IN (1,3) AND minute_time>=toDateTime('{START}','UTC') AND bitTest(valid_bitmap,0) ORDER BY minute_time,instrument_id")
