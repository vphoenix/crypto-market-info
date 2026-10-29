"""Summaries for the report from screen.json (and optional dense replay output)."""
import collections
import datetime as dt
import json
import statistics
import sys
from decimal import Decimal as D
from pathlib import Path

OUT = Path(__file__).resolve().parent
STEP = dt.timedelta(minutes=1)  # consecutive observations of the same leg set (1 s for dense replay output)


def usd(x):
    return f'{D(x):,.2f}'


def main(path):
    global STEP
    if 'dense' in path:
        STEP = dt.timedelta(seconds=1)
    r = json.loads((OUT / path).read_text(), parse_float=D)
    obs = r['gross_positive_observations']
    out = {}
    print('== checks')
    for k in sorted(r['comparisons']):
        best = r['closest_to_profit'].get(k, {})
        print(f"{k:32} n={r['comparisons'][k]:>10,} gross+={r['gross_positive'].get(k, 0):>6} net+={r['net_positive_after_entry_fees'].get(k, 0):>4}"
              f"  best gross/coin={best.get('best_gross_per_coin', '')[:12]} {best.get('family')} {str(best.get('expiry'))[:10]}")
    print('== gross-positive observations (zero-fee view, USD(C) per coin of underlying)')
    by = collections.defaultdict(list)
    for o in obs:
        by[o['kind']].append(o)
    out['gross'] = {}
    for k, v in sorted(by.items()):
        g = [D(o['gross_usd_per_coin']) for o in v]
        f = [D(o['entry_fees_usd_per_coin']) for o in v]
        cap = [D(o['top_level_capacity_coin']) for o in v]
        # Zero-fee bound counting each distinct leg set once, at its best top-of-book observation.
        best_by_legs = collections.defaultdict(D)
        for o in v:
            key = tuple(o['legs'])
            best_by_legs[key] = max(best_by_legs[key], D(o['gross_usd_per_coin']) * D(o['top_level_capacity_coin']))
        bound = sum(best_by_legs.values(), D(0))
        ratio = [a / b for a, b in zip(g, f) if b > 0]
        keyed = collections.defaultdict(list)
        for o in v:
            keyed[tuple(o['legs'])].append(o['minute'])
        runs = []
        for legs, ms in keyed.items():
            ts = sorted({dt.datetime.fromisoformat(m) for m in ms})
            run = 1
            for a, b in zip(ts, ts[1:]):
                run = run + 1 if (b - a) <= STEP else 1
                runs.append(run)
            runs.append(run)
        out['gross'][k] = {'observations': len(v), 'distinct_leg_sets': len(keyed), 'median_gross': statistics.median(g), 'max_gross': max(g),
                           'median_fees': statistics.median(f), 'max_gross_to_fee': max(ratio) if ratio else None,
                           'zero_fee_bound_usd_each_leg_set_once': bound, 'longest_consecutive_run': max(runs), 'run_unit_seconds': STEP.total_seconds()}
        print(f"{k:32} obs={len(v):>5} legsets={len(keyed):>4} gross med={usd(statistics.median(g))} max={usd(max(g))}"
              f" fees med={usd(statistics.median(f))} max gross/fee={max(ratio):.2f} zero-fee bound=${usd(bound)} longest run={max(runs)}{'s' if STEP.total_seconds() == 1 else 'min'}")
    if 'quality' in r:
        q = r['quality']
        print('== windows', len(q), 'minutes', sum(x['minutes'] for x in q.values()),
              'mean valid books/min', round(statistics.mean(x['mean_valid_books_per_minute'] for x in q.values()), 1),
              'second0 invalid', sum(x.get('second0_invalid', 0) for x in q.values()),
              'crossed', sum(x.get('crossed_or_locked', 0) for x in q.values()),
              'dup conflicts', sum(x.get('conflicting_duplicates', 0) for x in q.values()))
    if 'box_rates' in r:
        print('== box: executable rates after entry fees, boxes meeting minimum size (simple annual)')
        buckets = collections.defaultdict(lambda: {'lend': [], 'borrow': [], 'bcap': []})
        for key, v in r['box_rates'].items():
            fam = key.split(' ')[0]
            for x in v:
                days = D(x['days'])
                b = '<7d' if days < 7 else '7-30d' if days < 30 else '30-90d' if days < 90 else '>90d'
                if x['best_lend_rate'] is not None:
                    buckets[(fam, b)]['lend'].append(D(x['best_lend_rate']))
                if x['best_borrow_rate'] is not None:
                    buckets[(fam, b)]['borrow'].append(D(x['best_borrow_rate']))
                    buckets[(fam, b)]['bcap'].append(D(x['borrow_capacity_coin']))
        out['box'] = {}
        for (fam, b), v in sorted(buckets.items()):
            row = {'lend_best': max(v['lend']) if v['lend'] else None, 'lend_median': statistics.median(v['lend']) if v['lend'] else None,
                   'borrow_best': min(v['borrow']) if v['borrow'] else None, 'borrow_median': statistics.median(v['borrow']) if v['borrow'] else None,
                   'borrow_capacity_median': statistics.median(v['bcap']) if v['bcap'] else None, 'samples': len(v['borrow'])}
            out['box'][f'{fam} {b}'] = row

            def pct(x):
                return '   n/a' if x is None else f'{x * 100:6.2f}%'
            print(f"{fam:12} {b:7} lend best {pct(row['lend_best'])} med {pct(row['lend_median'])} | borrow best {pct(row['borrow_best'])}"
                  f" med {pct(row['borrow_median'])} cap med {row['borrow_capacity_median']} n={row['samples']}")
    (OUT / ('summary-' + Path(path).stem + '.json')).write_text(json.dumps(out, default=str, indent=1) + '\n')


if __name__ == '__main__':
    main(sys.argv[1] if len(sys.argv) > 1 else 'screen.json')
