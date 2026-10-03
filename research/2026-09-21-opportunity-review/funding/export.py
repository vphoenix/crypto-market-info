"""Read-only localhost ClickHouse exports; no private APIs or database writes."""
import json
from pathlib import Path
from datetime import datetime, timezone
from urllib.request import Request, urlopen
from urllib.error import HTTPError

ROOT = Path(__file__).resolve().parent
def query(name, sql):
    (ROOT / (name+'.sql')).write_text(sql+'\n')
    req=Request('http://127.0.0.1:8123/?readonly=1&max_execution_time=60&max_threads=2&output_format_json_quote_decimals=1',data=(sql+' FORMAT JSONEachRow').encode())
    try:
        with urlopen(req,timeout=90) as r: raw=r.read()
    except HTTPError as e:
        raise RuntimeError(e.read().decode()) from e
    (ROOT / (name+'.jsonl')).write_bytes(raw)
    rows=[json.loads(x,parse_float=str) for x in raw.splitlines()]
    print(name,len(rows),flush=True)
    return rows

if __name__=='__main__':
    for db,label in [('crypto_market_info_perp_soak','soak'),('crypto_market_info','production')]:
        query(label+'-actual',f"SELECT instrument_id,funding_time,toString(argMax(rate,hour_time)) AS settled_rate,count() AS duplicate_rows,uniqExact(rate) AS distinct_rates FROM {db}.funding_rate_hourly FINAL WHERE is_actual GROUP BY instrument_id,funding_time ORDER BY instrument_id,funding_time")
        query(label+'-instruments',f'SELECT * FROM {db}.instrument FINAL ORDER BY instrument_id')
        query(label+'-coverage',f"SELECT is_actual,count() AS rows,uniqExact(instrument_id) AS instruments,min(hour_time) AS first_hour,max(hour_time) AS last_hour,min(funding_time) AS first_funding,max(funding_time) AS last_funding FROM {db}.funding_rate_hourly FINAL GROUP BY is_actual")
    query('mapping','SELECT * FROM crypto_market_info_perp_soak.instrument_canonical_mapping FINAL ORDER BY recorded_at DESC LIMIT 1 BY instrument_id')
    (ROOT/'exported-at.txt').write_text(datetime.now(timezone.utc).isoformat()+'\n')
