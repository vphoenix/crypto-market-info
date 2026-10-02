"""Decimal-only minute-anchor screening; not an executable arbitrage backtest."""
import json
from collections import defaultdict
from datetime import datetime, timedelta
from decimal import Decimal as D, getcontext
from pathlib import Path

getcontext().prec = 50
ROOT = Path(__file__).resolve().parent


def read(name):
    return [json.loads(line, parse_float=str) for line in
            (ROOT / (name + '.jsonl')).read_text().splitlines()]


if __name__ == '__main__':
    instruments = {row['instrument_id']: row for row in read('instruments')}
    states = defaultdict(dict)
    counts = defaultdict(int)
    for row in read('bbo'):
        instrument = row['instrument_id']
        counts[instrument] += 1
        if not (int(row['valid_bitmap']) & 1):
            continue
        tick = D(instruments[instrument]['price_tick_size'])
        bid, ask = (D(row[k]) * tick for k in ['bid_price_01', 'ask_price_01'])
        assert 0 < bid < ask
        states[datetime.fromisoformat(row['minute_time'])][instrument] = (bid, ask)
    end = min(datetime.fromisoformat(row['last_minute']) for row in read('heads'))
    rows = []
    for stamp, books in sorted(states.items()):
        if len(books) != 2:
            continue
        bid_a, ask_a = books[1]
        bid_b, ask_b = books[3]
        choices = [(bid_b / ask_a - 1, 1, 3), (bid_a / ask_b - 1, 3, 1)]
        edge, buy, sell = max(choices)
        rows.append({'time': stamp, 'gross_bps': edge * 10000,
                     'buy_instrument': buy, 'sell_instrument': sell})
    windows = {}
    for days in [1, 7, 30]:
        chosen = [row for row in rows if end - timedelta(days=days) < row['time'] <= end]
        values = sorted(row['gross_bps'] for row in chosen)
        windows[str(days)] = {
            'start_exclusive_utc': str(end - timedelta(days=days)),
            'end_inclusive_utc': str(end), 'paired_valid_minute_anchors': len(chosen),
            'expected_anchors': days * 1440,
            'max_gross_bps': max(values) if values else None,
            'median_gross_bps': values[len(values) // 2] if values else None,
            'p95_gross_bps': values[(len(values) - 1) * 95 // 100] if values else None,
            'above_20_bps': sum(value > D(20) for value in values),
            'above_10_bps': sum(value > D(10) for value in values),
            'days_with_above20': sorted({str(row['time'].date()) for row in chosen
                                       if row['gross_bps'] > D(20)}),
        }
    spikes = [row for row in rows if row['gross_bps'] > D(20)]
    out = {'method': 'Stored minute second 0 only; valid bitmap checked; Decimal '
           'tick conversion; 10bp per venue illustrative taker fee; no missing '
           'seconds filled; no repeated liquidity summed or annualized.',
           'server_clock': read('clock')[0], 'window_statistics': windows,
           'spike_minute_count': len(spikes),
           'largest_10_anchors': sorted(rows, key=lambda row: row['gross_bps'], reverse=True)[:10]}
    (ROOT / 'analysis.json').write_text(json.dumps(out, default=str, indent=2) + '\n')
    (ROOT / 'spikes.json').write_text(json.dumps(spikes, default=str, indent=2) + '\n')
    print(json.dumps(out, default=str, indent=2))
