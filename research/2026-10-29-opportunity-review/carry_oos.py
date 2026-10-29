"""Out-of-sample funding carry backtests. Selection uses only data before each block.

Two strategies, both on actual settled funding only (forecasts are never used):
  spot_perp: long OKX spot + short OKX perp on the paired universe (no borrowing).
  perp_perp: long the lower-funding venue, short the higher-funding venue (soak data).
Missing settlements are never imputed: an asset-block whose observed settlement count
is below the expected count for its modal interval is excluded, and exclusions are reported.
"""
import collections
import datetime as dt
import itertools
import json
from decimal import Decimal as D, getcontext
from pathlib import Path

getcontext().prec = 50
OUT = Path(__file__).resolve().parent
OLD = OUT.parent / '2026-10-16-profitability'


def read(path):
    return [json.loads(s) for s in Path(path).read_text().splitlines() if s.strip()]


def utc(s):
    return dt.datetime.fromisoformat(str(s).replace('Z', '')).replace(microsecond=0)


def load(instfile, fundfile):
    inst = {int(i['instrument_id']): i for i in read(instfile)}
    ev = collections.defaultdict(dict)
    for r in read(fundfile):
        if r['is_actual'] not in (1, True, '1', 'true'):
            continue
        i = inst[int(r['instrument_id'])]
        if i['market_type'] != 'perpetual' or i['quote_asset'] != 'USDT':
            continue
        # Same (venue, base) can have several contract versions; keep the newest id.
        ev[(i['exchange'], i['base_asset'])][utc(r['funding_time'])] = D(r['rate'])
    return ev


def modal_hours(times):
    ts = sorted(times)
    gaps = collections.Counter(int((b - a).total_seconds() // 3600) for a, b in zip(ts, ts[1:]))
    gaps = {h: n for h, n in gaps.items() if h in (1, 2, 4, 8)}
    return max(gaps, key=gaps.get) if gaps else None


def window_sum(series, start, end, hours):
    """Sum over [start, end); None when the observed count is short of the modal schedule."""
    vals = [v for t, v in series.items() if start <= t < end]
    expected = int((end - start).total_seconds() // 3600) // hours
    if hours is None or len(vals) < expected:
        return None
    return sum(vals, D(0))


def blocks(first, last, hold):
    start = dt.datetime.combine(first.date(), dt.time()) + dt.timedelta(days=hold)  # first lookback complete
    while start + dt.timedelta(days=hold) <= last:
        yield start
        start += dt.timedelta(days=hold)


def backtest(candidates, hold, k, roundtrip, lookback=None):
    """candidates: name -> (series_score_fn, series_realized_fn). Equal weight, turnover-aware fees."""
    lookback = lookback or hold
    first = min(t for c in candidates.values() for t in c['times'])
    last = max(t for c in candidates.values() for t in c['times'])
    held, rows, excluded = set(), [], 0
    for start in blocks(first, last, max(hold, lookback)):
        lb0, end = start - dt.timedelta(days=lookback), start + dt.timedelta(days=hold)
        scores = {}
        for name, c in candidates.items():
            s = c['score'](lb0, start)
            if s is not None and s > 0:
                scores[name] = s
        pick = sorted(scores, key=scores.get, reverse=True)[:k]
        realized = {}
        for name in pick:
            r = candidates[name]['realized'](start, end)
            if r is None:
                excluded += 1
            else:
                realized[name] = r
        if not realized:
            rows.append({'start': str(start), 'picked': pick, 'net': None})
            held = set()
            continue
        names = set(realized)
        turnover = D(len(names - held)) / len(names)  # share of capital newly entered
        exits = D(len(held - names)) / max(len(held), 1) if held else D(0)
        gross = sum(realized.values(), D(0)) / len(names)
        fee = (turnover + exits) * roundtrip / 2
        rows.append({'start': str(start), 'picked': sorted(names), 'gross': gross, 'fee': fee, 'net': gross - fee,
                     'trailing_scores': {n: scores[n] for n in names}})
        held = names
    done = [r for r in rows if r['net'] is not None]
    if held:  # close the final book
        done[-1]['fee'] += roundtrip / 2
        done[-1]['net'] -= roundtrip / 2
    days = D(len(done) * hold)
    total = sum((r['net'] for r in done), D(0))
    return {'hold_days': hold, 'lookback_days': lookback, 'top_k': k, 'roundtrip_fee': roundtrip,
            'blocks': len(done), 'blocks_without_data': len(rows) - len(done), 'excluded_asset_blocks': excluded,
            'days_evaluated': days, 'net_return_sum': total,
            'net_simple_apr_on_notional': total / days * 365 if days else None,
            'negative_blocks': sum(1 for r in done if r['net'] < 0), 'detail': done}


def spot_perp(ev, venue):
    cands = {}
    for (ex, base), series in ev.items():
        if ex != venue:
            continue
        h = modal_hours(series)
        if h is None:
            continue
        # Short perp receives +rate each settlement.
        f = (lambda s, hh: lambda a, b: window_sum(s, a, b, hh))(series, h)
        cands[base] = {'times': list(series), 'score': f, 'realized': f}
    return cands


def perp_perp(ev):
    bybase = collections.defaultdict(dict)
    for (ex, base), series in ev.items():
        bybase[base][ex] = series
    cands = {}
    for base, venues in bybase.items():
        for lo, hi in itertools.permutations(venues, 2):
            sl, sh = venues[lo], venues[hi]
            hl, hh = modal_hours(sl), modal_hours(sh)
            if hl is None or hh is None:
                continue

            def f(a, b, sl=sl, sh=sh, hl=hl, hh=hh):
                x, y = window_sum(sh, a, b, hh), window_sum(sl, a, b, hl)
                return None if x is None or y is None else x - y  # short hi receives, long lo pays
            cands[base + ':long-' + lo + ':short-' + hi] = {'times': list(sl) + list(sh), 'score': f, 'realized': f}
    return cands


def summary(r):
    return {k: v for k, v in r.items() if k != 'detail'}


if __name__ == '__main__':
    result = {'generated_utc': dt.datetime.now(dt.timezone.utc).isoformat(), 'method': __doc__}
    okx = load(OUT / 'mysql-instruments.jsonl', OUT / 'mysql-funding.jsonl')
    # Paired universe only exists from 2026-10-08; restrict the window to it.
    okx = {k: {t: v for t, v in s.items() if t >= dt.datetime(2026, 10, 8)} for k, s in okx.items()}
    sp = spot_perp(okx, 'OKX')
    # OKX regular tier: spot taker 0.10%, perp maker 0.02% / taker 0.05%; +0.03% execution buffer.
    fees = {'spot_taker_perp_maker': D('0.0027'), 'all_taker': D('0.0033'), 'vip_maker': D('0.0023')}
    grid = []
    for hold, k, (fname, fee) in itertools.product([1, 3, 7], [1, 3, 5, 10, 20], fees.items()):
        r = backtest(sp, hold, k, fee)
        r['fee_scenario'] = fname
        grid.append(r)
    result['okx_spot_perp_window'] = ['2026-10-08', '2026-10-27']
    result['okx_spot_perp_assets'] = len(sp)
    result['okx_spot_perp_grid'] = [summary(r) for r in grid]
    best = max((r for r in grid if r['fee_scenario'] == 'spot_taker_perp_maker'),
               key=lambda r: r['net_simple_apr_on_notional'] or D(-1))
    result['okx_spot_perp_best_detail'] = best
    # Unconditional BTC carry on each venue across all available data.
    allv = load(OUT / 'mysql-instruments.jsonl', OUT / 'mysql-funding.jsonl')
    btc = {}
    for (ex, base), s in allv.items():
        if base == 'BTC' and s:
            ts = sorted(s)
            days = D((ts[-1] - ts[0]).total_seconds()) / 86400
            btc[ex] = {'first': str(ts[0]), 'last': str(ts[-1]), 'settlements': len(ts), 'sum': sum(s.values(), D(0)),
                       'simple_apr': sum(s.values(), D(0)) / days * 365 if days else None,
                       'negative_settlement_share': D(sum(1 for v in s.values() if v < 0)) / len(s)}
    result['btc_unconditional_short_perp_funding'] = btc
    # Cross-venue perp/perp on the September soak universe (Binance, Bybit, OKX).
    soak = load(OLD / 'clickhouse-soak-old-instruments.jsonl', OLD / 'clickhouse-soak-old-funding.jsonl')
    pp = perp_perp(soak)
    pgrid = []
    for hold, k in itertools.product([1, 3, 7], [1, 3, 5, 10, 20]):
        # Four taker legs ~0.05% each (Bybit 0.055%) plus 0.03% buffer.
        r = backtest(pp, hold, k, D('0.0024'))
        pgrid.append(r)
    result['soak_perp_perp_window'] = [str(min(t for s in soak.values() for t in s)), str(max(t for s in soak.values() for t in s))]
    result['soak_perp_perp_pairs'] = len(pp)
    result['soak_perp_perp_grid'] = [summary(r) for r in pgrid]
    result['soak_perp_perp_best_detail'] = max(pgrid, key=lambda r: r['net_simple_apr_on_notional'] or D(-1))
    (OUT / 'carry-oos.json').write_text(json.dumps(result, default=str, ensure_ascii=False, indent=1) + '\n')
    def pct(x):
        return 'n/a' if x is None else f'{float(x) * 100:.2f}%'
    print('OKX spot/perp assets', len(sp))
    for r in grid:
        if r['fee_scenario'] == 'spot_taker_perp_maker':
            print(f"  hold={r['hold_days']}d k={r['top_k']:>2} blocks={r['blocks']} neg={r['negative_blocks']} excl={r['excluded_asset_blocks']} APR={pct(r['net_simple_apr_on_notional'])}")
    print('BTC unconditional', json.dumps({k: {'apr': pct(v['simple_apr']), 'n': v['settlements'], 'neg': pct(v['negative_settlement_share'])} for k, v in btc.items()}))
    print('soak perp/perp pairs', len(pp), result['soak_perp_perp_window'])
    for r in pgrid:
        print(f"  hold={r['hold_days']}d k={r['top_k']:>2} blocks={r['blocks']} neg={r['negative_blocks']} excl={r['excluded_asset_blocks']} APR={pct(r['net_simple_apr_on_notional'])}")


def hold_forever(ev, venue, select_days, roundtrip, k):
    """Pick top-k by funding in the first `select_days`, then hold to the end of data.
    Realized income uses observed settlements only; annualized over observed settlement hours."""
    series = {b: s for (ex, b), s in ev.items() if ex == venue and len(s) > 10}
    t0 = min(t for s in series.values() for t in s)
    cut = dt.datetime.combine(t0.date(), dt.time()) + dt.timedelta(days=select_days)
    score = {b: sum((v for t, v in s.items() if t < cut), D(0)) for b, s in series.items()}
    pick = [b for b in sorted(score, key=score.get, reverse=True)[:k] if score[b] > 0]
    out = []
    for b in pick:
        s = {t: v for t, v in series[b].items() if t >= cut}
        h = modal_hours(series[b])
        if not s or h is None:
            continue
        span_h = D((max(s) - cut).total_seconds()) / 3600 + h
        observed_h, total = D(len(s) * h), sum(s.values(), D(0))
        out.append({'base': b, 'settlements': len(s), 'coverage': min(observed_h / span_h, D(1)),
                    'gross_sum': total, 'gross_apr_observed': total / observed_h * 24 * 365})
    if not out:
        return None
    days = (max(t for s in series.values() for t in s) - cut).total_seconds() / 86400
    gross_apr = sum((o['gross_apr_observed'] for o in out), D(0)) / len(out)
    net_apr = gross_apr - roundtrip / D(days) * 365
    return {'venue': venue, 'select_days': select_days, 'top_k': k, 'hold_days': days, 'roundtrip_fee': roundtrip,
            'basket': out, 'basket_gross_apr': gross_apr, 'basket_net_apr_one_roundtrip': net_apr,
            'negative_settlement_share': D(sum(1 for o in out for v in series[o['base']].values() if v < 0)) /
            sum(len(series[o['base']]) for o in out)}
