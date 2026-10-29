"""Second-by-second rerun of the screen for selected minutes and expiry groups.

For each selected minute, reads the second-0 anchor (with id, valid and delta bitmaps) and all
second deltas of every option/future in the selected family-expiry groups, replays them exactly as
internal/replay does (price-keyed changes, qty 0 deletes), and runs screen.run_minute on each of the
60 seconds whose valid bit is set. Same writer-backlog guard as the window export.

usage: replay_dense.py OUT.json 'YYYY-MM-DD HH:MM' [...]   (all expiries of both bases are replayed;
       set ONLY_EXPIRY=YYYY-MM-DD to restrict to one expiry for validation)
"""
import collections
import gzip
import json
import os
import sys
from decimal import Decimal as D
from pathlib import Path

import dbq
import screen as S

OUT = Path(__file__).resolve().parent


def fetch(minute, ids):
    rows = {}
    for k in range(0, len(ids), 200):
        chunk = ids[k:k + 200]
        sql = ("SELECT JSON_OBJECT('id',instrument_id,'mid',id,'vb',valid_bitmap,'db',delta_bitmap,'bp',HEX(bid_prices),'bq',HEX(bid_qtys),"
               "'ap',HEX(ask_prices),'aq',HEX(ask_qtys)) FROM crypto_market_info.derivative_book_minute "
               f"WHERE instrument_id IN ({','.join(map(str, chunk))}) AND minute_time = '{minute}:00'")
        dbq.guard()
        raw, _ = dbq.run(sql, seconds=90)
        for line in raw.splitlines():
            r = json.loads(line)
            if r['id'] in rows:
                raise SystemExit(f'duplicate anchor {r["id"]} {minute}')
            rows[r['id']] = r
    deltas = collections.defaultdict(list)
    mids = sorted(r['mid'] for r in rows.values())
    for k in range(0, len(mids), 300):
        chunk = mids[k:k + 300]
        sql = ("SELECT JSON_OBJECT('mid',minute_id,'s',second_offset,'bp',HEX(bid_change_prices),'bq',HEX(bid_change_qtys),"
               "'ap',HEX(ask_change_prices),'aq',HEX(ask_change_qtys)) FROM crypto_market_info.derivative_book_second_delta "
               f"WHERE minute_id IN ({','.join(map(str, chunk))})")
        dbq.guard()
        raw, _ = dbq.run(sql, seconds=90)
        for line in raw.splitlines():
            d = json.loads(line)
            deltas[d['mid']].append(d)
    return rows, deltas


def apply(side, prices, qtys):
    for p, q in zip(prices, qtys):
        if q == 0:
            if p not in side:
                raise ValueError('delta deletes absent price')
            del side[p]
        else:
            side[p] = q


def replay(rows, deltas):
    """{second: {instrument_id: (bids, asks)}} for valid seconds; checks delta bitmap consistency."""
    out = collections.defaultdict(dict)
    stats = collections.Counter()
    for i, r in rows.items():
        bids = dict(zip(S.ints(r['bp'], True), S.ints(r['bq'], False)))
        asks = dict(zip(S.ints(r['ap'], True), S.ints(r['aq'], False)))
        ds = sorted(deltas.get(r['mid'], []), key=lambda d: d['s'])
        if sorted(d['s'] for d in ds) != [s for s in range(1, 60) if r['db'] >> s & 1]:
            stats['delta_bitmap_mismatch'] += 1
            continue
        it = iter(ds)
        nxt = next(it, None)
        for s in range(60):
            if nxt is not None and nxt['s'] == s:
                apply(bids, S.ints(nxt['bp'], True), S.ints(nxt['bq'], False))
                apply(asks, S.ints(nxt['ap'], True), S.ints(nxt['aq'], False))
                nxt = next(it, None)
            if r['vb'] >> s & 1:
                b = sorted(bids.items(), reverse=True)
                a = sorted(asks.items())
                if b and a and b[0][0] >= a[0][0]:
                    stats['crossed_or_locked'] += 1
                    continue
                out[s][i] = (b, a)
                stats['valid_instrument_seconds'] += 1
            else:
                stats['invalid_instrument_seconds'] += 1
    return out, stats


if __name__ == '__main__':
    target, minutes = sys.argv[1], sys.argv[2:]
    result = {'method': __doc__, 'minutes': {}}
    total = S.Screen()
    for minute in minutes:
        ids = []
        for i, r in S.inst.items():
            s = S.spec.get(i)
            if (r['market_type'] in ('option', 'delivery') and r['expiry_time'] and s
                    and r['expiry_time'][:16] > minute and (s['creation_time'] or '')[:16] <= minute
                    and r['expiry_time'][:10] == os.environ.get('ONLY_EXPIRY', r['expiry_time'][:10])):
                ids.append(i)
        rows, deltas = fetch(minute, sorted(ids))
        seconds, stats = replay(rows, deltas)
        sc = S.Screen()
        for s in sorted(seconds):
            S.run_minute(sc, f'{minute}:{s:02d}', seconds[s], False)
            S.run_minute(total, f'{minute}:{s:02d}', seconds[s], False)
        result['minutes'][minute] = {'instruments': len(rows), 'valid_seconds': len(seconds), 'replay': dict(stats),
                                     'comparisons': sum(sc.count.values()), 'gross_positive': dict(sc.gross_pos),
                                     'net_positive_after_entry_fees': dict(sc.net_pos)}
        print(minute, result['minutes'][minute], flush=True)
    result['comparisons'] = dict(total.count)
    result['gross_positive'] = dict(total.gross_pos)
    result['net_positive_after_entry_fees'] = dict(total.net_pos)
    result['candidates'] = total.cands
    result['gross_positive_observations'] = total.gross_list
    result['closest_to_profit'] = {k: {'best_gross_per_coin': str(D(v[0]) / v[1] / S.E8), 'at': v[2], 'family': v[3], 'legs': v[5]}
                                   for k, v in total.best.items()}
    (OUT / target).write_text(json.dumps(result, default=str, indent=1) + '\n')
