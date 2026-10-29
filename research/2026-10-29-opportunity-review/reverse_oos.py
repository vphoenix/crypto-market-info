"""Reverse carry out-of-sample: long OKX perp + borrow-and-sell OKX spot.
Selection on funding before the cut only; realized funding after the cut on observed settlements;
borrow cost from the 2026-10-16 public loan policy snapshot (rates float; not account-specific)."""
import datetime as dt, importlib.util, json, sys
from decimal import Decimal as D
from query import array
spec = importlib.util.spec_from_file_location('c', 'carry_oos.py'); c = importlib.util.module_from_spec(spec); spec.loader.exec_module(c)
r = json.loads(open('../2026-10-16-profitability/mysql-latest-loan.jsonl').read().splitlines()[0])
loan = dict(zip(array(r['currencies'], 'str'), array(r['daily_rates'], 'decimal')))
ev = c.load(c.OUT / 'mysql-instruments.jsonl', c.OUT / 'mysql-funding.jsonl')
okx = {a: {t: v for t, v in s.items() if t >= dt.datetime(2026, 10, 8)} for (ex, a), s in ev.items() if ex == 'OKX'}
out = {}
for sel_days in (3, 5, 7):
    cut = dt.datetime(2026, 10, 8) + dt.timedelta(days=sel_days)
    end = max(t for s in okx.values() for t in s)
    hold_days = D((end - cut).total_seconds()) / 86400
    rows = []
    for a, s in okx.items():
        h = c.modal_hours(s)
        if a not in loan or not h:
            continue
        pre = [v for t, v in s.items() if t < cut]
        post = [v for t, v in s.items() if t >= cut]
        if not pre or not post:
            continue
        pre_apr = -sum(pre, D(0)) / D(len(pre) * h) * 24 * 365
        borrow = loan[a] * 365
        if pre_apr - borrow <= 0:
            continue  # not selected: trailing funding does not cover borrow
        post_apr = -sum(post, D(0)) / D(len(post) * h) * 24 * 365
        net = post_apr - borrow - D('0.0027') * 365 / hold_days
        rows.append({'asset': a, 'trailing_long_funding_apr': pre_apr, 'borrow_apr': borrow,
                     'realized_long_funding_apr': post_apr, 'net_apr_one_roundtrip': net,
                     'settlements_after': len(post)})
    rows.sort(key=lambda x: x['trailing_long_funding_apr'] - x['borrow_apr'], reverse=True)
    out[sel_days] = {'cut': str(cut), 'hold_days': hold_days, 'selected': rows}
    for k in (1, 3, 5, 10, len(rows)):
        sub = rows[:k]
        if sub:
            avg = sum((x['net_apr_one_roundtrip'] for x in sub), D(0)) / len(sub)
            pos = sum(1 for x in sub if x['net_apr_one_roundtrip'] > 0)
            print(f"select {sel_days}d top{k:>3}: hold {float(hold_days):.1f}d  avg net {float(avg)*100:7.2f}%/yr  positive {pos}/{len(sub)}  "
                  + ' '.join(f"{x['asset']}({float(x['trailing_long_funding_apr'])*100:.0f}%->{float(x['realized_long_funding_apr'])*100:.0f}%)" for x in sub[:5]))
json.dump(out, open('reverse-carry-oos.json', 'w'), default=str, indent=1)
