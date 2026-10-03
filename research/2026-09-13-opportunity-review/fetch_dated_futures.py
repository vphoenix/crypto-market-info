"""Read-only public OKX dated-futures screen, with raw response hashes."""
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from decimal import Decimal, getcontext
from hashlib import sha256
from pathlib import Path
import json
import requests

getcontext().prec = 40
D = Decimal
OUT = Path(__file__).parent

def fetch(path):
    url = 'https://www.okx.com' + path
    r = requests.get(url, timeout=25)
    r.raise_for_status()
    body = r.json(parse_float=D)
    assert body['code'] == '0', body
    return {'url': url, 'collected_at_utc': datetime.now(timezone.utc).isoformat(),
            'payload_sha256': sha256(r.content).hexdigest(), 'body': body}

paths = ['/api/v5/public/instruments?instType=FUTURES',
         '/api/v5/market/tickers?instType=FUTURES',
         '/api/v5/market/ticker?instId=BTC-USDT',
         '/api/v5/market/ticker?instId=ETH-USDT']
with ThreadPoolExecutor(max_workers=4) as pool:
    raw = list(pool.map(fetch, paths))
instruments = {x['instId']:x for x in raw[0]['body']['data']}
spot = {x['body']['data'][0]['instId'].split('-')[0]:x['body']['data'][0] for x in raw[2:]}
tics = [x for x in raw[1]['body']['data'] if x['instId'].startswith(('BTC-USD-', 'ETH-USD-'))]
rows = []
for t in tics:
    i = instruments[t['instId']]
    s = spot[t['instId'].split('-')[0]]
    if not t['bidPx']:
        continue
    ask, bid = D(s['askPx']), D(t['bidPx'])
    days = (D(i['expTime'])-D(t['ts'])) / D(86400000)
    if days <= 0:
        continue
    gross = bid / ask - 1
    rows.append({'instrument':t['instId'], 'spot_ask':str(ask), 'future_bid':str(bid),
                 'days_to_maturity':str(days), 'gross_basis_pct':str(gross*100),
                 'gross_annualized_pct':str(gross*365/days*100),
                 'capital_10000_gross':str(gross*10000),
                 'future_bid_size_contracts':t['bidSz'], 'ctVal':i['ctVal'],
                 'ctValCcy':i['ctValCcy'], 'settleCcy':i['settleCcy'],
                 'future_ts':t['ts'], 'spot_ts':s['ts']})
rows.sort(key=lambda x:D(x['gross_annualized_pct']), reverse=True)
(OUT/'okx-dated-futures-raw.json').write_text(json.dumps(raw,indent=2,default=str)+'\n')
(OUT/'okx-dated-futures-screen.json').write_text(json.dumps(rows,indent=2)+'\n')
print(json.dumps(rows,indent=2))
