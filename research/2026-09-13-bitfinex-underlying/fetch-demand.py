"""Bounded public GET snapshots. No credentials, accounts, or trading endpoints."""
import datetime as dt
import hashlib
import json
import sys
import time
import urllib.request
from pathlib import Path

P = Path(__file__).resolve().parent
BASE = 'https://api-pub.bitfinex.com/v2/'
NOW = dt.datetime(2026, 9, 13, 14, 7, tzinfo=dt.timezone.utc)
END = int(NOW.timestamp()) * 1000
START = END - 86400000
CANDLE_START = int(dt.datetime(2026, 6, 15, tzinfo=dt.timezone.utc).timestamp()) * 1000
last_request = 0

def fetch(name, path):
    global last_request
    wait_ns = 4_200_000_000 - (time.monotonic_ns() - last_request)
    if wait_ns > 0:
        time.sleep(wait_ns / 1_000_000_000)  # Time scheduling only, never financial arithmetic.
    last_request = time.monotonic_ns()
    url = BASE + path
    stamp = dt.datetime.now(dt.timezone.utc).isoformat()
    with urllib.request.urlopen(urllib.request.Request(url, headers={'User-Agent': 'PublicFundingResearch/1.0'}), timeout=30) as r:
        raw = r.read()
    result = {'url': url, 'requested_at_utc': stamp,
              'received_at_utc': dt.datetime.now(dt.timezone.utc).isoformat(),
              'payload_sha256': hashlib.sha256(raw).hexdigest(),
              'data': json.loads(raw, parse_float=str)}
    (P / (name + '.raw.json')).write_bytes(raw)
    (P / (name + '.json')).write_text(json.dumps(result, indent=2) + '\n')
    data = result['data']
    if not isinstance(data, list) or (data and data[0] == 'error'):
        raise ValueError(data)
    print(json.dumps({'name': name, 'rows': len(data), 'first': data[:1], 'last': data[-1:]}), flush=True)
    return result

def trades(asset):
    rows = {}
    end = END
    pages = []
    completed = False
    for page in range(1, 21):
        name = f'{asset}-trades-page{page}'
        path = f'trades/{asset}/hist?limit=10000&sort=-1&start={START}&end={end}'
        if (P / (name + '.json')).exists():
            result = json.loads((P / (name + '.json')).read_text())
            assert result['url'] == BASE + path
        else:
            result = fetch(name, path)
        data = result['data']
        assert all(len(r) == 5 and START <= r[1] <= end for r in data)
        pages.append({'file': name + '.raw.json', 'url': result['url'], 'payload_sha256': result['payload_sha256']})
        for row in data:
            if row[0] in rows:
                assert rows[row[0]] == row
            rows[row[0]] = row
        if len(data) < 10000:
            completed = True
            break
        oldest = min(r[1] for r in data)
        # Preserve inclusive timestamp overlap to avoid omitting fills sharing a millisecond.
        if oldest >= end:
            raise ValueError('Cannot paginate same-ms overflow safely')
        end = oldest
    ordered = sorted(rows.values(), key=lambda x: (x[1], x[0]))
    out = {'symbol': asset, 'window_start_ms': START, 'window_end_ms': END,
           'complete_window': completed, 'sources': pages, 'data': ordered}
    (P / f'{asset}-trades-24h.json').write_text(json.dumps(out, indent=2) + '\n')
    print(json.dumps({'symbol': asset, 'complete': completed, 'fills': len(ordered)}), flush=True)

mode = sys.argv[1]
if mode == 'trades':
    for asset in ('fUSD', 'fUST'):
        trades(asset)
elif mode == 'context':
    for asset in ('fUSD', 'fUST'):
        fetch(asset + '-ticker', f'ticker/{asset}')
        fetch(asset + '-book', f'book/{asset}/P0?len=250')
        fetch(asset + '-book-raw', f'book/{asset}/R0?len=250')
        fetch(asset + '-stats', f'funding/stats/{asset}/hist?limit=2')
        for term in (2, 30, 120):
            fetch(f'{asset}-candles-p{term}', f'candles/trade:1D:{asset}:p{term}/hist?limit=10000&sort=1&start={CANDLE_START}&end={END}')
    for pair, ccy in [('tBTCUSD', 'fUSD'), ('tBTCUST', 'fUST'), ('tETHUSD', 'fUSD'), ('tETHUST', 'fUST')]:
        fetch(pair + '-longs', f'stats1/pos.size:1m:{pair}:long/last')
        fetch(pair + '-credits', f'stats1/credits.size.sym:1m:{ccy}:{pair}/last')
elif mode == 'align':
    stamp = 1789308540000  # All four observed pair-credit timestamps, 2026-09-13 14:09 UTC.
    for asset in ('fUSD', 'fUST'):
        for key in ('funding.size', 'credits.size'):
            fetch(f'{asset}-{key}-aligned', f'stats1/{key}:1m:{asset}/hist?end={stamp}&limit=1&sort=-1')
else:
    raise ValueError(mode)
