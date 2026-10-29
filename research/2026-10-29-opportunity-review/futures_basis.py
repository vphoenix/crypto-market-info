"""Deribit dated-futures cash-and-carry term structure from stored minute anchors.

Second-0 anchors of committed minutes only; index from options_index_minute second 0.
Annualized basis uses the best bid (we sell the future) against the index (we buy spot).
Fee reference: spot taker 0.10% in and out, future taker 0.05%, delivery 0.025%.
"""
import collections
import datetime as dt
import json
import statistics
import sys
from decimal import Decimal as D

from query import OUT, obj, safe

FUT = {'8904': 'BTC-29OCT26', '1260': 'BTC-30OCT26', '9132': 'BTC-31OCT26', '9376': 'BTC-1NOV26', '6381': 'BTC-6NOV26',
       '8013': 'BTC-13NOV26', '1261': 'BTC-27NOV26', '1262': 'BTC-25DEC26', '1263': 'BTC-26MAR27', '1264': 'BTC-25JUN27',
       '1265': 'BTC-24SEP27', '3332': 'BTC_USDC-30OCT26', '9183': 'BTC_USDC-31OCT26', '9528': 'BTC_USDC-1NOV26',
       '8268': 'BTC_USDC-6NOV26', '3334': 'BTC_USDC-27NOV26', '3336': 'BTC_USDC-25DEC26', '2104': 'ETH-30OCT26',
       '9087': 'ETH-31OCT26', '9427': 'ETH-1NOV26', '6459': 'ETH-6NOV26', '8091': 'ETH-13NOV26', '2105': 'ETH-27NOV26',
       '2106': 'ETH-25DEC26', '2107': 'ETH-26MAR27', '2108': 'ETH-25JUN27', '2109': 'ETH-24SEP27',
       '3333': 'ETH_USDC-30OCT26', '9184': 'ETH_USDC-31OCT26', '9529': 'ETH_USDC-1NOV26', '8269': 'ETH_USDC-6NOV26',
       '3335': 'ETH_USDC-27NOV26', '3337': 'ETH_USDC-25DEC26'}
EXPIRY = {}
for i in map(json.loads, (OUT / 'mysql-instruments.jsonl').read_text().splitlines()):
    if str(i['instrument_id']) in FUT:
        EXPIRY[str(i['instrument_id'])] = dt.datetime.fromisoformat(i['expiry_time'])
COST = D('0.001') * 2 + D('0.0005') + D('0.00025')


def ints(h, signed):
    raw = bytes.fromhex(h)
    n = int.from_bytes(raw[17:21], 'big')
    return [int.from_bytes(raw[21 + 8 * j:29 + 8 * j], 'big', signed=signed) for j in range(n)]


def first_decimal(h):
    raw = bytes.fromhex(h)
    pos = 21
    if raw[pos] == 0:
        return None
    return D(int.from_bytes(raw[pos + 1:pos + 17], 'big', signed=True)) / 10 ** 18


def sample(minute):
    ids = ','.join(FUT)
    books = safe('fut-' + minute.replace(' ', 'T').replace(':', ''),
                 'SELECT ' + obj({'id': 'instrument_id', 'v': 'valid_bitmap & 1', 'bp': 'HEX(bid_prices)', 'bq': 'HEX(bid_qtys)',
                                  'ap': 'HEX(ask_prices)'}) +
                 " FROM crypto_market_info.derivative_book_minute FORCE INDEX(batch_lookup) WHERE minute_time='" + minute +
                 "' AND instrument_id IN (" + ids + ')', seconds=40)
    idx = safe('idx-' + minute.replace(' ', 'T').replace(':', ''),
               'SELECT ' + obj({'i': 'CAST(index_id AS CHAR)', 'p': 'HEX(prices)'}) +
               " FROM crypto_market_info.options_index_minute WHERE minute_time='" + minute + "'", seconds=40)
    index = collections.defaultdict(list)
    for x in idx:
        v = first_decimal(x['p'])
        if v is not None:
            index[x['i']].append(v)
    index = {k: statistics.median(v) for k, v in index.items()}
    t = dt.datetime.fromisoformat(minute)
    out = []
    for b in books:
        if not b['v']:
            continue  # second 0 invalid: never use
        sym, iid = FUT[str(b['id'])], str(b['id'])
        bids, bq, asks = ints(b['bp'], True), ints(b['bq'], False), ints(b['ap'], True)
        if not bids or not asks:
            continue
        ref = index.get(('btc' if sym.startswith('BTC') else 'eth') + ('_usdc' if '_USDC' in sym else '_usd'))
        if ref is None:
            continue
        days = D((EXPIRY[iid] - t).total_seconds()) / 86400
        if days < 2:
            continue
        bid = D(bids[0]) / 10 ** 8
        basis = bid / ref - 1
        out.append({'minute': minute, 'symbol': sym, 'days': days, 'index': ref, 'bid': bid,
                    'bid_qty_l1_to_l5': sum(D(q) for q in bq) / 10 ** 8, 'basis': basis,
                    'gross_apr': basis * 365 / days, 'net_apr_after_fees': (basis - COST) * 365 / days})
    return out


if __name__ == '__main__':
    start = dt.datetime(2026, 10, 21, 0, 0)
    minutes = [(start + dt.timedelta(hours=6 * k)).strftime('%Y-%m-%d %H:%M:%S') for k in range(31)]
    rows = []
    for m in minutes:
        rows += sample(m)
    (OUT / 'futures-basis.json').write_text(json.dumps({'method': __doc__, 'fee_reference': COST, 'rows': rows},
                                                       default=str, indent=1) + '\n')
    by = collections.defaultdict(list)
    for r in rows:
        by[r['symbol']].append(r)
    print('samples', len(minutes), 'rows', len(rows))
    for s in sorted(by, key=lambda s: (s.split('-')[0], EXPIRY[[k for k, v in FUT.items() if v == s][0]])):
        g = by[s]
        net = [float(r['net_apr_after_fees']) * 100 for r in g]
        print(f"{s:18} n={len(g):2} days~{float(g[-1]['days']):6.1f} grossAPR med={statistics.median(float(r['gross_apr'])*100 for r in g):6.2f}% "
              f"netAPR med={statistics.median(net):6.2f}% min={min(net):6.2f}% max={max(net):6.2f}% L1-5 bid qty last={float(g[-1]['bid_qty_l1_to_l5']):.2f}")
