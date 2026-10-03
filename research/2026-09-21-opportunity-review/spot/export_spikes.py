import json,urllib.request,hashlib
from pathlib import Path
from datetime import datetime,timedelta,timezone
P=Path(__file__).resolve().parent
stamps=set()
for r in json.loads((P/'spikes.json').read_text()):
 t=datetime.fromisoformat(r['time'])
 for m in range(-2,3):stamps.add(str(t+timedelta(minutes=m)))
where='instrument_id IN (1,3) AND minute_time IN ('+','.join("'"+t+"'" for t in sorted(stamps))+')'
queries={'spike_minutes':'SELECT * FROM crypto_market_info.order_book_minute FINAL WHERE '+where+' ORDER BY minute_time,instrument_id', 'spike_deltas':'SELECT * FROM crypto_market_info.order_book_second_delta FINAL WHERE minute_id IN (SELECT id FROM crypto_market_info.order_book_minute FINAL WHERE '+where+') ORDER BY minute_id,second_offset'}
for n,q in queries.items():
 (P/(n+'.sql')).write_text(q+'\n')
 raw=urllib.request.urlopen(urllib.request.Request('http://127.0.0.1:8123/?readonly=1&max_threads=2&max_execution_time=45',data=(q+' FORMAT JSONEachRow').encode()),timeout=60).read()
 rows=[json.loads(x,parse_float=str) for x in raw.splitlines()]
 (P/(n+'.json')).write_text(json.dumps(rows,indent=2)+'\n')
 (P/(n+'.meta.json')).write_text(json.dumps({'retrieved_at_utc':datetime.now(timezone.utc).isoformat(),'rows':len(rows),'response_sha256':hashlib.sha256(raw).hexdigest()},indent=2)+'\n')
 print(n,len(rows))
