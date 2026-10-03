from pathlib import Path
from urllib.request import Request, urlopen
from datetime import datetime,timezone
import json
out=Path(__file__).resolve().parent
queries={
'routes':'SELECT * FROM crypto_market_info.yield_route FINAL ORDER BY yield_route_id',
'latest':'''SELECT * FROM crypto_market_info.yield_observation FINAL WHERE (yield_route_id, observation_time) IN (SELECT yield_route_id,max(observation_time) FROM crypto_market_info.yield_observation GROUP BY yield_route_id) ORDER BY yield_route_id,tier_no''',
'history-summary':'''SELECT yield_route_id,count() AS n,min(observation_time) AS first_observation,max(observation_time) AS last_observation,max(collected_at) AS last_collection,min(rate) AS min_rate,max(rate) AS max_rate,countIf(isNull(source_payload_hash) OR length(source_payload_hash)!=64) AS missing_hash_count,countIf(isNotNull(block_height)) AS anchored_count,groupUniqArray(finality) AS finality_values FROM crypto_market_info.yield_observation FINAL WHERE observation_time >= now('UTC') - INTERVAL 7 DAY GROUP BY yield_route_id ORDER BY yield_route_id''',
'coverage':'''SELECT r.provider,r.yield_type,r.price_exposure_asset,count() AS routes FROM crypto_market_info.yield_route AS r FINAL GROUP BY r.provider,r.yield_type,r.price_exposure_asset ORDER BY provider,yield_type''',
'ratio-history':'''SELECT yield_route_id,observation_time,collected_at,exposure_ratio,rate,rate_kind,availability,source_payload_hash,block_height,block_hash,finality FROM crypto_market_info.yield_observation FINAL WHERE observation_time >= now('UTC')-INTERVAL 30 DAY AND yield_route_id IN (SELECT yield_route_id FROM crypto_market_info.yield_route FINAL WHERE provider IN ('BENQI','Ankr')) ORDER BY yield_route_id,observation_time'''
}
meta={'query_time_utc':datetime.now(timezone.utc).isoformat(),'database':'crypto_market_info','endpoint':'http://127.0.0.1:8123/','read_only':True}
for name,sql in queries.items():
    full=sql+'\nSETTINGS readonly=1,output_format_json_quote_decimals=1\nFORMAT JSON\n'
    (out/(name+'.sql')).write_text(full)
    try:
        data=urlopen(Request(meta['endpoint'],data=full.encode()),timeout=90).read()
        (out/(name+'.json')).write_bytes(data)
        obj=json.loads(data)
        print(name,obj.get('rows'),obj.get('statistics'))
    except Exception as exc: print(name,type(exc).__name__,str(exc))
(out/'query-meta.json').write_text(json.dumps(meta,indent=2)+'\n')
