"""Cross-venue BTC top-of-book crossing on second-0 anchors (hourly, 7 days). Taker 0.10% spot / 0.05% perp (Bybit 0.055%)."""
import datetime as dt, json, itertools
from decimal import Decimal as D
from query import safe, obj
inst = {int(i['instrument_id']): i for i in map(json.loads, open('mysql-instruments.jsonl'))}
ids = [1, 3, 5, 6, 7]
minutes = [(dt.datetime(2026, 10, 21) + dt.timedelta(hours=h)).strftime('%Y-%m-%d %H:%M:%S') for h in range(24 * 7)]
rows = []
for k in range(0, len(minutes), 24):
    chunk = ','.join("'" + m + "'" for m in minutes[k:k + 24])
    rows += safe(f'cex-btc-{k:03d}', 'SELECT ' + obj({'id': 'instrument_id', 'm': 'minute_time', 'v': 'valid_bitmap & 1', 'b': 'bid_price_01', 'a': 'ask_price_01'}) +
                 ' FROM crypto_market_info.order_book_minute FORCE INDEX(minute_identity) WHERE minute_time IN (' + chunk + ') AND instrument_id IN (1,3,5,6,7)', seconds=40)
fee = {'spot': D('0.001'), 'perpetual': D('0.0005')}
by = {}
for r in rows:
    if not r['v'] or not r['b'] or not r['a']:
        continue
    i = inst[int(r['id'])]
    tick = D(i['price_tick_size'])
    by.setdefault(r['m'], {})[(i['exchange'], i['market_type'])] = (D(r['b']) * tick, D(r['a']) * tick)
best, crossings, pairs = None, 0, 0
for m, q in by.items():
    for (x, bx), (y, by_) in itertools.permutations(q.items(), 2):
        if x[1] != y[1]:
            continue  # same product type only (spot-spot, perp-perp)
        pairs += 1
        gross = bx[0] / by_[1] - 1  # sell at x bid, buy at y ask
        f = fee[x[1]] + fee[y[1]] + (D('0.00005') if 'Bybit' in (x[0], y[0]) else 0)
        net = gross - f
        if gross > 0:
            crossings += 1
        if best is None or net > best[0]:
            best = (net, gross, m, x, y)
out = {'minutes_with_data': len(by), 'same_type_pairs': pairs, 'gross_crossings': crossings,
       'best_net': best[0], 'best_gross': best[1], 'best_minute': best[2], 'sell_venue': best[3], 'buy_venue': best[4]}
json.dump(out, open('cross-btc.json', 'w'), default=str, indent=1)
print(json.dumps(out, default=str))
