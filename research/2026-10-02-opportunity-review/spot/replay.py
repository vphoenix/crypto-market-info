"""Replay full stored depth with price-key deltas; assess bounded spot candidates."""
import json
from collections import defaultdict
from datetime import datetime, timedelta
from decimal import Decimal as D, ROUND_DOWN, getcontext
from pathlib import Path

getcontext().prec = 50
ROOT = Path(__file__).resolve().parent
FEE = D('.001')
STEP = D('.00001')  # Common multiple of the two observed quantity steps.


def read(name):
    return [json.loads(line, parse_float=str) for line in
            (ROOT / (name + '.jsonl')).read_text().splitlines()]


def consume(levels, quantity):
    cash = D(0)
    for price, size in levels:
        take = min(quantity, size)
        cash += price * take
        quantity -= take
        if quantity == 0:
            return cash
    return None


def quote(asks, bids, budget):
    upper = int((min(sum(q for p, q in asks), sum(q for p, q in bids)) / STEP)
                .to_integral_value(rounding=ROUND_DOWN))
    low, high = 0, upper
    while low < high:
        middle = (low + high + 1) // 2
        cost = consume(asks, D(middle) * STEP)
        assert cost is not None
        if cost * (1 + FEE) <= budget:
            low = middle
        else:
            high = middle - 1
    quantity = D(low) * STEP
    if not quantity:
        return None
    cost, revenue = consume(asks, quantity), consume(bids, quantity)
    return {'quantity_btc': quantity, 'buy_with_fee': cost * (1 + FEE),
            'sell_after_fee': revenue * (1 - FEE),
            'net_first_two_trades': revenue * (1 - FEE) - cost * (1 + FEE),
            'buy_budget_used_fraction': cost * (1 + FEE) / budget,
            'depth_or_budget_limit': 'budget' if low < upper else 'saved_depth'}


if __name__ == '__main__':
    metadata = {row['instrument_id']: row for row in read('instruments')}
    deltas = defaultdict(dict)
    for row in read('spike-deltas'):
        minute, second = int(row['minute_id']), int(row['second_offset'])
        assert second not in deltas[minute]
        deltas[minute][second] = row
    states = defaultdict(dict)
    for minute in read('spike-minutes'):
        instrument, depth = minute['instrument_id'], minute['stored_depth']
        assert depth in [10, 50]
        bitmap = int(minute['valid_bitmap'])
        books = {}
        for side in ['bid', 'ask']:
            books[side] = {int(minute[f'{side}_price_{n:02}']): int(minute[f'{side}_qty_{n:02}'])
                           for n in range(1, depth + 1)
                           if int(minute[f'{side}_qty_{n:02}']) > 0}
        for second in range(60):
            delta = deltas[int(minute['id'])].get(second)
            if delta:
                assert bitmap & (1 << second)
                for side in ['bid', 'ask']:
                    prices, quantities = delta[side + '_change_prices'], delta[side + '_change_qtys']
                    assert len(prices) == len(quantities)
                    for price, quantity in zip(prices, quantities):
                        price, quantity = int(price), int(quantity)
                        if quantity:
                            books[side][price] = quantity
                        else:
                            books[side].pop(price, None)
            assert all(len(book) <= depth for book in books.values())
            if not bitmap & (1 << second):
                continue
            assert books['bid'] and books['ask']
            assert max(books['bid']) < min(books['ask'])
            stamp = datetime.fromisoformat(minute['minute_time']) + timedelta(seconds=second)
            tick, lot = D(metadata[instrument]['price_tick_size']), D(metadata[instrument]['quantity_step_size'])
            states[stamp][instrument] = {
                'full_signature': tuple((side, tuple(sorted(book.items()))) for side, book in books.items()),
                'bid': [(D(p) * tick, D(q) * lot) for p, q in sorted(books['bid'].items(), reverse=True)],
                'ask': [(D(p) * tick, D(q) * lot) for p, q in sorted(books['ask'].items())],
                'depth': depth,
            }
    freezes = []
    for instrument in [1, 3]:
        last_stamp = last_signature = segment = None
        for stamp, pair in sorted(states.items()):
            if instrument not in pair:
                continue
            signature = pair[instrument]['full_signature']
            if last_stamp is not None and stamp == last_stamp + timedelta(seconds=1) and signature == last_signature:
                segment['end'] = stamp
                segment['seconds'] += 1
            else:
                segment = {'instrument': instrument, 'start': stamp, 'end': stamp, 'seconds': 1}
                freezes.append(segment)
            last_stamp, last_signature = stamp, signature
    long_freezes = [row for row in freezes if row['seconds'] >= 30]
    candidates = []
    for stamp, pair in sorted(states.items()):
        if len(pair) != 2:
            continue
        for buy, sell in [(1, 3), (3, 1)]:
            asks, bids = pair[buy]['ask'], pair[sell]['bid']
            if bids[0][0] * (1 - FEE) <= asks[0][0] * (1 + FEE):
                continue
            value = quote(asks, bids, D(500000))
            if value and value['net_first_two_trades'] > 0:
                frozen = {instrument: max((row['seconds'] for row in long_freezes
                          if row['instrument'] == instrument and row['start'] <= stamp <= row['end']), default=1)
                          for instrument in [1, 3]}
                candidates.append({'time': stamp, 'buy': buy, 'sell': sell,
                                   'identical_saved_book_segment_seconds': frozen, **value})
    daily_best = {}
    for row in candidates:
        day = str(row['time'].date())
        if day not in daily_best or row['net_first_two_trades'] > daily_best[day]['net_first_two_trades']:
            daily_best[day] = row
    out = {'scope': 'Only +/-2 minutes around 52 minute-anchor spikes; existing '
           'stored_depth=10/50 replayed in full; 500000 USDT buy-leg budget '
           'including 10bp fee, two initial trades only; all dollars Decimal.',
           'paired_valid_seconds': sum(len(pair) == 2 for pair in states.values()),
           'positive_initial_pair_seconds': len(candidates),
           'daily_best': daily_best,
           'largest_10': sorted(candidates, key=lambda row: row['net_first_two_trades'], reverse=True)[:10],
           'identical_full_book_segments_ge30_seconds': sorted(long_freezes, key=lambda row: row['seconds'], reverse=True),
           'limitations': 'No source event time in standard stored books; valid bitmap '
           'does not prove synchronized fresh quotes. Repeated seconds are not '
           'independent fills. Inventory restoration, hedging, latency and future '
           'capacity are absent; no return annualization.'}
    (ROOT / 'replay-analysis.json').write_text(json.dumps(out, default=str, indent=2) + '\n')
    print(json.dumps({key: value for key, value in out.items() if key != 'identical_full_book_segments_ge30_seconds'}, default=str, indent=2))
    print('Longest identical saved books:', json.dumps(out['identical_full_book_segments_ge30_seconds'][:5], default=str))
