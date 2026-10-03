"""Public market observations, not realised account performance. Decimal throughout."""
import datetime as dt
import hashlib
import json
from collections import defaultdict
from decimal import Decimal as D, getcontext
from pathlib import Path

getcontext().prec = 50
P = Path(__file__).resolve().parent
OLD = P.parent / '2026-09-13-structural-yield'
YEAR = D(365)
FEE = D('0.85')

def read(path):
    return json.loads(path.read_text(), parse_float=str)

def val(x):
    assert not isinstance(x, float)
    return D(str(x))

def utc(ms):
    return dt.datetime.fromtimestamp(ms // 1000, dt.timezone.utc).isoformat()

def quantile(xs, p):
    ys = sorted(xs)
    return ys[int(D(len(ys)-1) * D(p))]

def fill_stats(rows):
    if not rows:
        return {'fills': 0, 'volume': D(0)}
    amounts = [abs(val(r[2])) for r in rows]
    total = sum(amounts)
    rate = sum(abs(val(r[2])) * val(r[3]) for r in rows) / total
    buckets = {ms: defaultdict(lambda: D(0)) for ms in [60000, 3600000]}
    for r in rows:
        for ms, b in buckets.items():
            b[r[1] // ms * ms] += abs(val(r[2]))
    return {'fills': len(rows), 'volume': total, 'vw_daily_rate': rate,
            'vw_gross_apr': rate * YEAR, 'vw_after_15pct_fee_apr': rate * YEAR * FEE,
            'vw_contract_period_days': sum(abs(val(r[2])) * val(r[4]) for r in rows) / total,
            'min_gross_apr': min(val(r[3]) for r in rows) * YEAR,
            'max_gross_apr': max(val(r[3]) for r in rows) * YEAR,
            'top1_fill_share': max(amounts) / total,
            'top10_fills_share': sum(sorted(amounts, reverse=True)[:10]) / total,
            'top100_fills_share': sum(sorted(amounts, reverse=True)[:100]) / total,
            'active_utc_hours': len(buckets[3600000]),
            'largest_minute_share': max(buckets[60000].values()) / total,
            'largest_hour_share': max(buckets[3600000].values()) / total,
            'largest_fills': sorted(rows, key=lambda r: abs(val(r[2])), reverse=True)[:10],
            'top_minutes': sorted(buckets[60000].items(), key=lambda x:x[1], reverse=True)[:5]}

out = {}
for asset in ('fUSD', 'fUST'):
    d = read(P / f'{asset}-trades-24h.json')
    assert d['complete_window'] and len({r[0] for r in d['data']}) == len(d['data'])
    pages = []
    for source in d['sources']:
        raw = P / source['file']
        assert hashlib.sha256(raw.read_bytes()).hexdigest() == source['payload_sha256']
        pages.append(json.loads(raw.read_bytes(), parse_float=str))
    boundaries = []
    for a,b in zip(pages,pages[1:]):
        stamp = min(r[1] for r in a)
        x = {r[0] for r in a if r[1] == stamp}
        y = {r[0] for r in b if r[1] == stamp}
        assert x <= y and len(y) < 10000
        boundaries.append({'timestamp_ms': stamp, 'prev_boundary_fills': len(x), 'next_boundary_fills': len(y), 'prev_is_subset_next': True})
    rows = d['data']
    assert all(val(r[3]) > 0 and 2 <= r[4] <= 120 for r in rows)
    total = fill_stats(rows)
    total['window_start_utc'] = utc(d['window_start_ms'])
    total['window_end_utc'] = utc(d['window_end_ms'])
    total['observed_first_utc'] = utc(rows[0][1])
    total['observed_last_utc'] = utc(rows[-1][1])
    terms = {str(term): fill_stats([r for r in rows if r[4] == term]) for term in sorted({r[4] for r in rows})}
    for term in terms.values():
        term['share_of_all_volume'] = term['volume'] / total['volume']
    high = {}
    for threshold in ('0.035','0.05','0.08','0.10'):
        filtered = [r for r in rows if val(r[3]) * YEAR * FEE >= D(threshold)]
        z = fill_stats(filtered)
        z['share_all_volume'] = z['volume'] / total['volume']
        high[threshold] = z
    hourly = {}
    for h in range(24):
        lo = d['window_start_ms'] + h * 3600000
        hourly[utc(lo)] = fill_stats([r for r in rows if lo <= r[1] < lo+3600000])
    archived = read(OLD / f'bitfinex-{asset}-90d.json')
    for source in archived['sources']:
        path = OLD / source.get('raw_file',f'bitfinex-{asset}-history.raw.json')
        assert hashlib.sha256(path.read_bytes()).hexdigest() == source['payload_sha256']
    history = archived['data']
    assert len(history) == 2161 and all(b[0]-a[0] == 3600000 for a,b in zip(history,history[1:]))
    history_stats = {}
    for days in (7,30,90):
        z = history[-(days*24+1):-1]
        assert len(z) == days*24
        history_stats[str(days)] = {'hours':len(z),
          'first_utc':utc(z[0][0]), 'last_utc':utc(z[-1][0]),
          'gross_frr_apr_mean':sum(val(r[3])*YEAR*YEAR for r in z)/len(z),
          'after_fee_frr_apr_mean':sum(val(r[3])*YEAR*YEAR*FEE for r in z)/len(z),
          'provided_mean':sum(val(r[7]) for r in z)/len(z),
          'used_mean':sum(val(r[8]) for r in z)/len(z),
          'usage_share_mean':sum(val(r[8])/val(r[7]) for r in z)/len(z),
          'usage_share_min':min(val(r[8])/val(r[7]) for r in z),
          'usage_share_max':max(val(r[8])/val(r[7]) for r in z),
          'provided_start':val(z[0][7]),'provided_end':val(z[-1][7]),
          'used_start':val(z[0][8]),'used_end':val(z[-1][8]),
          'avg_contract_period_mean':sum(val(r[4]) for r in z)/len(z)}
    current = read(P / f'{asset}-stats.json')['data'][0]
    merged_history = {r[0]:r for r in history}
    merged_history[current[0]] = current
    same_window = [r for t,r in merged_history.items() if d['window_start_ms'] <= t < d['window_end_ms']]
    assert len(same_window) == 24
    frr_same_window = sum(val(r[3])*YEAR*YEAR for r in same_window)/24
    ticker = read(P / f'{asset}-ticker.json')['data']
    oldbook = read(OLD / f'bitfinex-{asset}-book.json')['data']
    book = read(P / f'{asset}-book.json')['data']
    rawbook = read(P / f'{asset}-book-raw.json')['data']
    bids = [r for r in book if val(r[3]) < 0]
    rawbids = [r for r in rawbook if val(r[3]) < 0]
    btotal = sum(-val(r[3]) for r in bids)
    book_by_term = {}
    for term in (2,30,60,120):
        z = [r for r in bids if r[1] == term]
        book_by_term[str(term)] = {'visible_total_bid':sum(-val(r[3]) for r in z),
          'best_gross_apr':max((val(r[0])*YEAR for r in z),default=None),
          'bids':z}
    high_bids = [r for r in rawbids if val(r[2])*YEAR*FEE >= D('0.08')]
    candles = {}
    for term in (2,30,120):
        z = read(P / f'{asset}-candles-p{term}.json')['data']
        complete = [r for r in z if r[0]+86400000 <= d['window_end_ms']]
        assert len(complete) == 90 and all(b[0]-a[0] == 86400000 for a,b in zip(complete,complete[1:]))
        candles[str(term)] = {'complete_days':len(complete), 'first_day_utc':utc(complete[0][0]),
          'last_day_utc':utc(complete[-1][0]), 'total_volume':sum(val(r[5]) for r in complete),
          'daily_volume_median':quantile([val(r[5]) for r in complete],'0.5'),
          'daily_volume_min':min(val(r[5]) for r in complete),
          'daily_volume_max':max(val(r[5]) for r in complete),
          'mean_close_gross_apr_not_vwap':sum(val(r[2])*YEAR for r in complete)/len(complete),
          'median_close_gross_apr_not_vwap':quantile([val(r[2])*YEAR for r in complete],'0.5'),
          'days_any_trade_net_above_8pct':sum(val(r[3])*YEAR*FEE >= D('0.08') for r in complete),
          'days_all_trades_net_above_8pct':sum(val(r[4])*YEAR*FEE >= D('0.08') for r in complete),
          'last_complete_day':complete[-1]}
    out[asset] = {'trades':total,'pagination_boundary_checks':boundaries,'by_period':terms,'high_rate':high,
      'same_24h_frr_gross_apr_mean':frr_same_window,
      'same_24h_frr_after_fee_apr_mean':frr_same_window*FEE,
      'hours':hourly,'history':history_stats,
      'current':{'stats_utc':utc(current[0]),'total_provided':val(current[7]),'used_in_positions':val(current[8]),
        'reserved_unused':val(current[7])-val(current[8]),'usage_share':val(current[8])/val(current[7]),
        'avg_period_days':val(current[4]),'ticker_frr_gross_apr':val(ticker[0])*YEAR,
        'stats_frr_gross_apr_rounded':val(current[3])*YEAR*YEAR,
        'ticker_frr_available':val(ticker[15]),'below_threshold_open_offers':val(current[11]),
        'visible_borrow_bids':btotal, 'largest_raw_borrow_bid':max(rawbids,key=lambda r:-val(r[3])),
        'largest_raw_bid_share':max(-val(r[3]) for r in rawbids)/sum(-val(r[3]) for r in rawbids),
        'raw_bids_at_least_8pct_after_fee':high_bids,
        'by_period':book_by_term,'prior_top_bid':oldbook[0],'new_top_bid':book[0]},'candles':candles}

out['positions'] = {}
for pair in ('tBTCUSD','tBTCUST','tETHUSD','tETHUST'):
    out['positions'][pair] = {'longs':read(P / (pair+'-longs.json'))['data'],
                              'credits':read(P / (pair+'-credits.json'))['data']}
out['aligned_credit_use'] = {}
for asset, pairs in [('fUSD', ('tBTCUSD','tETHUSD')), ('fUST', ('tBTCUST','tETHUST'))]:
    provided = read(P / f'{asset}-funding.size-aligned.json')['data'][0]
    used = read(P / f'{asset}-credits.size-aligned.json')['data'][0]
    assert provided[0] == used[0] == 1789308540000
    shares = {}
    for pair in pairs:
        credit = out['positions'][pair]['credits']
        assert credit[0] == used[0]
        shares[pair] = {'amount':val(credit[1]),'share_all_used':val(credit[1])/val(used[1])}
    out['aligned_credit_use'][asset] = {'utc':utc(used[0]),'provided':val(provided[1]),
        'used':val(used[1]),'usage_share':val(used[1])/val(provided[1]),'pairs':shares}
(P / 'demand-analysis.json').write_text(json.dumps(out,indent=2,default=str)+'\n')
print(json.dumps({a:{'all':out[a]['trades'],'2d':out[a]['by_period']['2'],
                    '30d':out[a]['by_period']['30'],'120d':out[a]['by_period']['120'],
                    'current':out[a]['current'],'candles':out[a]['candles']} for a in ('fUSD','fUST')},default=str))
