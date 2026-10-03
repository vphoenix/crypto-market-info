"""Public depth check for a USD 10,000 inverse-futures face value."""
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
meta = json.loads((OUT/'okx-dated-futures-raw.json').read_text())[0]['body']['data']
meta = {x['instId']:x for x in meta}
ids = ['BTC-USDT', 'ETH-USDT'] + [f'{asset}-USD-{expiry}' for asset in ('BTC','ETH')
    for expiry in ('260925','261225','270326','270924')]

def fetch(inst):
    url = f'https://www.okx.com/api/v5/market/books?instId={inst}&sz=100'
    r = requests.get(url,timeout=25)
    r.raise_for_status()
    body = r.json(parse_float=D)
    assert body['code'] == '0', body
    return {'instrument':inst, 'url':url, 'collected_at_utc':datetime.now(timezone.utc).isoformat(),
            'payload_sha256':sha256(r.content).hexdigest(), 'body':body}

with ThreadPoolExecutor(max_workers=4) as pool:
    raw = list(pool.map(fetch,ids))
books = {x['instrument']:x['body']['data'][0] for x in raw}
rows = []
for inst in ids[2:]:
    m = meta[inst]
    book = books[inst]
    face = D('10000')
    left, coin, bid_levels = face, D(0), 0
    for row in book['bids']:
        px, size = D(row[0]), D(row[1]) * D(m['ctVal'])
        used = min(size,left)
        coin += used / px
        left -= used
        bid_levels += 1
        if left == 0:
            break
    if left:
        rows.append({'instrument':inst, 'insufficient_100_level_bid_face':str(left)})
        continue
    spot_book = books[inst.split('-')[0]+'-USDT']
    left, cost, ask_levels = coin, D(0), 0
    for row in spot_book['asks']:
        px,size = D(row[0]),D(row[1])
        used = min(size,left)
        cost += used*px
        left -= used
        ask_levels += 1
        if left == 0:
            break
    assert left == 0
    days = (D(m['expTime'])-D(book['ts']))/D(86400000)
    gross = face/cost-1
    net_model = gross-D('0.003')
    rows.append({'instrument':inst,'future_face_usd':str(face),'coin_hedge':str(coin),
        'spot_cost_usdt':str(cost),'future_harmonic_fill_px':str(face/coin),
        'spot_vwap':str(cost/coin),'future_bid_levels_used':bid_levels,'spot_ask_levels_used':ask_levels,
        'days_to_maturity':str(days),'gross_basis_pct':str(gross*100),
        'gross_annualized_pct':str(gross*365/days*100),
        'gross_profit_per_10000_initial_cost':str(gross*10000),
        'net_model_profit_per_10000_initial_cost':str(net_model*10000),
        'net_model_annualized_pct':str(net_model*365/days*100),
        'model_cost_bps':'30','future_ts':book['ts'],'spot_ts':spot_book['ts']})
(OUT/'okx-dated-depth-raw.json').write_text(json.dumps(raw,indent=2,default=str)+'\n')
(OUT/'okx-dated-depth-screen.json').write_text(json.dumps(rows,indent=2)+'\n')
print(json.dumps(rows,indent=2))
