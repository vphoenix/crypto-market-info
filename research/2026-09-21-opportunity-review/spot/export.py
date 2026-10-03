import urllib.request,json,hashlib
from pathlib import Path
from datetime import datetime,timezone
P=Path(__file__).resolve().parent
queries={
'coverage':"SELECT instrument_id,min(minute_time) first,max(minute_time) last,count() minutes,sum(bitCount(valid_bitmap)) valid_seconds FROM crypto_market_info.order_book_minute FINAL GROUP BY instrument_id ORDER BY instrument_id",
'instruments':"SELECT * FROM crypto_market_info.instrument FINAL ORDER BY instrument_id",
'funding':"SELECT * FROM crypto_market_info.funding_rate_hourly FINAL ORDER BY instrument_id,hour_time",
'books':"SELECT instrument_id,minute_time,valid_bitmap,bid_price_01,bid_qty_01,ask_price_01,ask_qty_01 FROM crypto_market_info.order_book_minute FINAL WHERE instrument_id IN (1,3,5,6,7) AND minute_time >= '2026-08-22 00:00:00' AND bitTest(valid_bitmap,0) ORDER BY minute_time,instrument_id",
'funding_schema':"DESCRIBE TABLE crypto_market_info.funding_rate_hourly",
}
for name,q in queries.items():
 (P/(name+'.sql')).write_text(q+'\n')
 req=urllib.request.Request('http://127.0.0.1:8123/?readonly=1&max_execution_time=45&max_threads=2',data=(q+' FORMAT JSONEachRow').encode())
 raw=urllib.request.urlopen(req,timeout=60).read()
 rows=[json.loads(x,parse_float=str) for x in raw.splitlines()]
 (P/(name+'.json')).write_text(json.dumps(rows,ensure_ascii=False,indent=2)+'\n')
 (P/(name+'.meta.json')).write_text(json.dumps({'retrieved_at_utc':datetime.now(timezone.utc).isoformat(),'sha256_http_response':hashlib.sha256(raw).hexdigest(),'rows':len(rows)},indent=2)+'\n')
 print(name,len(rows))
 if name in ['coverage','funding_schema']:print(json.dumps(rows))
