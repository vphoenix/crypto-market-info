"""Read-only exports of already collected public data; POST explicitly sets readonly=1."""
import hashlib,json
from pathlib import Path
from datetime import datetime,timezone,timedelta
from urllib.request import Request,urlopen
from urllib.error import HTTPError
P=Path(__file__).resolve().parent
SOAK='crypto_market_info_perp_soak_20260928'
def query(name,sql):
    (P/(name+'.sql')).write_text(sql+'\n')
    req=Request('http://127.0.0.1:8123/?readonly=1&max_execution_time=60&max_threads=2&output_format_json_quote_decimals=1',data=(sql+' FORMAT JSONEachRow').encode())
    try:
        with urlopen(req,timeout=75) as r: raw=r.read()
    except HTTPError as e: raise RuntimeError(e.read().decode()) from e
    (P/(name+'.jsonl')).write_bytes(raw)
    rows=[json.loads(x,parse_float=str) for x in raw.splitlines()]
    (P/(name+'.meta.json')).write_text(json.dumps({'queried_at_utc':datetime.now(timezone.utc).isoformat(),'sha256':hashlib.sha256(raw).hexdigest(),'rows':len(rows),'readonly':1,'max_threads':2,'max_execution_time':60},indent=2)+'\n')
    print(name,len(rows),flush=True)
    return rows
if __name__=='__main__':
    query('server',"SELECT version() AS version,now('UTC') AS now_utc,(SELECT value FROM system.settings WHERE name='readonly') AS readonly")
    run=query('current-run',f"SELECT * FROM {SOAK}.perpetual_universe_run FINAL ORDER BY started_at DESC LIMIT 1")[0]
    query('members',f"SELECT * FROM {SOAK}.perpetual_universe_member FINAL WHERE run_id='{run['run_id']}'")
    query('mapping',f"SELECT * FROM {SOAK}.instrument_canonical_mapping FINAL WHERE mapping_revision='{run['mapping_revision']}'")
    for db,label in [(SOAK,'soak'),('crypto_market_info','production')]:
        query(label+'-instruments',f'SELECT * FROM {db}.instrument FINAL ORDER BY instrument_id')
        query(label+'-funding-range',f"SELECT min(hour_time) AS first_hour,max(hour_time) AS latest_hour,minIf(funding_time,is_actual) AS first_actual,maxIf(funding_time,is_actual) AS latest_actual,count() AS rows,countIf(is_actual) AS actual_rows,countIf(NOT is_actual) AS estimates FROM {db}.funding_rate_hourly FINAL")
        query(label+'-funding',f"SELECT * FROM {db}.funding_rate_hourly FINAL WHERE hour_time>=now('UTC')-INTERVAL 32 DAY ORDER BY instrument_id,hour_time")
