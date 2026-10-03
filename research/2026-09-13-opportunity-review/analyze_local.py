"""Historical screening, exact decimal amounts; no claim of executable strategy PnL."""
import json
from collections import defaultdict
from decimal import Decimal as D, getcontext
from pathlib import Path
from datetime import datetime

getcontext().prec = 40
P = Path(__file__).resolve().parent
load = lambda n: json.loads((P / (n + '.json')).read_text(), parse_float=str)
def dec(x): return D(str(x))
def summary(a):
    a=sorted(a)
    return dict(n=len(a), min=a[0], median=a[len(a)//2], max=a[-1], mean=sum(a)/len(a)) if a else {}

out={}
f=load('funding')
by={i:{r['funding_time']:dec(r['rate']) for r in f if r['instrument_id']==i and r['is_actual']} for i in (5,6,7)}
out['btc_actual_8h_rate_summary']={i:summary(list(v.values())) for i,v in by.items()}
out['btc_matched_actual_funding']={}
for short,long in [(6,7),(5,6),(5,7)]:
    times=sorted(by[short].keys() & by[long].keys())
    diffs=[by[short][t]-by[long][t] for t in times]
    out['btc_matched_actual_funding'][f'short_{short}_long_{long}']={
        'first':times[0], 'last':times[-1], 'diff':summary(diffs),
        'positive_count':sum(x>0 for x in diffs),
        'sum_per_leg_notional':sum(diffs),
        'sample_rate_simple_annualized_per_leg':sum(diffs)/len(diffs)*D(3)*365,
        'sample_rate_simple_annualized_2x_capital':sum(diffs)/len(diffs)*D(3)*365/2,
        'caution':'Matched observed settlements only; missing settlements excluded, not zero. Not a full-period realized return.'}

ins={r['instrument_id']:r for r in load('instruments')}
books=defaultdict(dict)
for r in load('btc-book'):
    i=ins[r['instrument_id']]
    r['bid']=dec(r['bid_price_01'])*dec(i['price_tick_size'])
    r['ask']=dec(r['ask_price_01'])*dec(i['price_tick_size'])
    books[r['minute_time']][r['instrument_id']]=r
out['btc_minute_gross_spread_bps']={}
for sell,buy in [(3,1),(1,3),(5,1),(6,3),(6,7),(7,6)]:
    rows=[(t,(v[sell]['bid']/v[buy]['ask']-1)*10000) for t,v in sorted(books.items()) if sell in v and buy in v and v[buy]['ask']>0]
    out['btc_minute_gross_spread_bps'][f'sell_{sell}_buy_{buy}']={
        **summary([x for _,x in rows]),'first':rows[0], 'last':rows[-1],
        'count_over_20bps':sum(x>D(20) for _,x in rows),
        'caution':'Synchronized valid second-0 best quotes only, gross upper bound; no depth, fees or fill-latency deduction.'}

y=load('yield-history')
yg=defaultdict(list)
for r in y: yg[(r['provider'],r['product_code'])].append(r)
out['yield_history']={}
for (provider,code),rows in yg.items():
    if provider=='TRON': continue
    rows.sort(key=lambda r:r['observation_time'])
    rates=[dec(r['rate']) for r in rows if r['rate'] is not None]
    result={'n':len(rows),'first':rows[0]['observation_time'],'latest_source_time':rows[-1]['observation_time'],
        'latest_collected_at':rows[-1]['collected_at'],'last_rate':rows[-1]['rate'], 'rate_kind':rows[-1]['rate_kind'],
        'rate_range':summary(rates),'latest_source_url':rows[-1]['source_url']}
    if code in ('avalanche-savax-staking','avalanche-ankravax-staking'):
        valid=[r for r in rows if r['exposure_ratio'] is not None]
        a,b=valid[0],valid[-1]
        seconds=D(str((datetime.fromisoformat(b['observation_time'])-datetime.fromisoformat(a['observation_time'])).total_seconds()))
        growth=dec(b['exposure_ratio'])/dec(a['exposure_ratio'])-1
        result['exchange_ratio_realized']={'first_ratio':a['exposure_ratio'],'last_ratio':b['exposure_ratio'],
          'elapsed_seconds':seconds,'growth':growth,'simple_annualized':growth*31536000/seconds,
          'caution':'Underlying coin-denominated historical exchange-ratio growth; not USD profit or forward rate.'}
    out['yield_history'][provider+'/'+code]=result
out['tron_latest_rate_summary']=summary([dec(r['rate']) for r in load('yield-latest') if r['provider']=='TRON' and r['rate'] is not None and r['observation_time']>='2026-09-12 00:00:00'])
(P/'local-analysis.json').write_text(json.dumps(out,indent=2,ensure_ascii=False,default=str)+'\n')
print(json.dumps(out,ensure_ascii=False,default=str))
