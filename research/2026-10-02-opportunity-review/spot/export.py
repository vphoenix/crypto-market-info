"""Bounded read-only exports of collected BTC spot books; no account APIs."""
import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parent


def query(name, sql):
    (ROOT / (name + '.sql')).write_text(sql + '\n')
    endpoint = ('http://127.0.0.1:8123/?readonly=1&max_execution_time=60&max_threads=2'
                '&output_format_json_quote_decimals=1')
    request = Request(endpoint, data=(sql + ' FORMAT JSONEachRow').encode())
    started = datetime.now(timezone.utc).isoformat()
    try:
        with urlopen(request, timeout=70) as response:
            raw = response.read()
    except HTTPError as exc:
        raise RuntimeError(exc.read().decode()) from exc
    (ROOT / (name + '.jsonl')).write_bytes(raw)
    rows = [json.loads(line, parse_float=str) for line in raw.splitlines()]
    (ROOT / (name + '.meta.json')).write_text(json.dumps({
        'queried_at_utc': started, 'rows': len(rows),
        'sha256': hashlib.sha256(raw).hexdigest(),
        'endpoint': endpoint, 'read_only': True,
        'readonly_setting': 'readonly=1 requested; SELECT only.',
    }, indent=2) + '\n')
    print(name, len(rows), flush=True)
    return rows


if __name__ == '__main__':
    clock = query('clock', "SELECT now64(3, 'UTC') AS server_time, value AS readonly "
                  "FROM system.settings WHERE name='readonly'")[0]
    query('instruments', 'SELECT * FROM crypto_market_info.instrument FINAL WHERE instrument_id IN (1,3) ORDER BY instrument_id')
    head = query('heads', "SELECT instrument_id,max(minute_time) AS last_minute FROM crypto_market_info.order_book_minute FINAL WHERE instrument_id IN (1,3) AND minute_time>=now()-INTERVAL 2 DAY GROUP BY instrument_id")
    end = min(datetime.fromisoformat(row['last_minute']) for row in head)
    start = end - timedelta(days=30)
    query('bbo', f"SELECT instrument_id,minute_time,valid_bitmap,bid_price_01,bid_qty_01,ask_price_01,ask_qty_01 FROM crypto_market_info.order_book_minute FINAL WHERE instrument_id IN (1,3) AND minute_time>toDateTime('{start}','UTC') AND minute_time<=toDateTime('{end}','UTC') ORDER BY minute_time,instrument_id")
