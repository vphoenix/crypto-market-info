from pathlib import Path
from urllib.request import Request,urlopen
from datetime import datetime,timezone
from decimal import Decimal
import json
OUT=Path(__file__).resolve().parent
queries={
 'clock':"SELECT now64(3,'UTC') AS query_time, version() AS version",
 'schema':"DESCRIBE TABLE crypto_market_info.yield_observation",
 'routes':"SELECT * FROM crypto_market_info.yield_route FINAL ORDER BY yield_route_id",
 'latest':'''SELECT * FROM crypto_market_info.yield_observation FINAL WHERE (yield_route_id, observation_time) IN (SELECT yield_route_id,max(observation_time) FROM crypto_market_info.yield_observation GROUP BY yield_route_id) ORDER BY yield_route_id,tier_no''',
 'history':'''SELECT yield_route_id,observation_time,collected_at,tier_no,rate,rate_kind,rate_mode,exposure_ratio,availability,rule_eligibility,remaining_capacity,pool_cash,tvl,source_payload_hash,block_height,block_hash,finality FROM crypto_market_info.yield_observation FINAL WHERE observation_time >= now('UTC')-INTERVAL 30 DAY ORDER BY yield_route_id,tier_no,observation_time''',
 'all-history-summary':'''SELECT yield_route_id,count() AS n,min(observation_time) AS first_observation,max(observation_time) AS last_observation,max(collected_at) AS last_collection FROM crypto_market_info.yield_observation FINAL GROUP BY yield_route_id ORDER BY yield_route_id'''
}
meta={'query_started_utc':datetime.now(timezone.utc).isoformat(),'database':'crypto_market_info','endpoint':'http://127.0.0.1:8123/','read_only':True,'max_threads':2,'max_execution_time':60}
for name,sql in queries.items():
 full=sql+'\nSETTINGS readonly=1,max_threads=2,max_execution_time=60,output_format_json_quote_decimals=1\nFORMAT JSON\n'
 (OUT/(name+'.sql')).write_text(full)
 try:
  data=urlopen(Request(meta['endpoint'],data=full.encode()),timeout=70).read()
  (OUT/(name+'.json')).write_bytes(data)
  obj=json.loads(data,parse_float=Decimal)
  print(name,obj.get('rows'),'bytes',len(data),'statistics',obj.get('statistics'))
 except Exception as exc:
  (OUT/(name+'.error')).write_text(str(exc))
  print(name,type(exc).__name__,str(exc))
meta['query_finished_utc']=datetime.now(timezone.utc).isoformat()
(OUT/'query-meta.json').write_text(json.dumps(meta,indent=2)+'\n')
