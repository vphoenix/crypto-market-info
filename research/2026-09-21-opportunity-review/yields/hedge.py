#!/usr/bin/env python3
"""Check known 8-hour SOL/AVAX/TRX actual funding against complete UTC windows.

This is a conditional carry screen, not a trading backtest. Latest yield stays
constant only in the labelled scenario. All amounts and annualization use Decimal.
"""
from collections import Counter
from datetime import datetime, timedelta, timezone
from decimal import Decimal as D, getcontext
import json
from pathlib import Path

getcontext().prec=50
ROOT=Path(__file__).resolve().parent
FUND=ROOT.parent/'funding'
def read(name): return [json.loads(l) for l in (FUND/name).read_text().splitlines()]
def dt(value):return datetime.fromisoformat(value).replace(tzinfo=timezone.utc)
end=dt('2026-09-21 08:00:00')
instruments={x['instrument_id']:x for x in read('soak-instruments.jsonl') if x['base_asset'] in ['SOL','AVAX','TRX']}
actual=read('soak-actual.jsonl')
outputs=[]
for iid,ins in instruments.items():
    rows={dt(x['funding_time']):D(x['settled_rate']) for x in actual if x['instrument_id']==iid}
    s={'instrument_id':iid,'exchange':ins['exchange'],'asset':ins['base_asset'],'end_utc':str(end)}
    for days in [1,3,7]:
        start=end-timedelta(days=days)
        expected=[start+timedelta(hours=8*i) for i in range(1,days*3+1)]
        rates=[rows[t] for t in expected if t in rows]
        complete=len(rates)==len(expected)
        daily=[]
        for n in range(days):
            times=[start+timedelta(days=n,hours=8*k) for k in [1,2,3]]
            if all(t in rows for t in times):
                daily.append(sum(rows[t] for t in times)*365)
        s[str(days)+'d']={'complete':complete,'actual_n':len(rates),'expected_n':len(expected),'missing':[str(t) for t in expected if t not in rows],
                        'short_funding_apr':sum(rates)*D(365)/D(days) if complete else None,'complete_days':len(daily),'positive_days':sum(x>0 for x in daily),'daily_aprs':daily}
    outputs.append(s)
scenarios=[]
yields=json.loads((ROOT/'summary.json').read_text())['routes']
for routeid,asset in [(1,'TRX'),(136,'SOL'),(138,'SOL'),(139,'SOL'),(149,'AVAX')]:
    route=next(x for x in yields if x['yield_route_id']==routeid)
    latest=route['latest']
    # sAVAX has no quoted APR; use realized ratio history as an explicitly
    # different sensitivity assumption, never as a newly promised APY.
    yr=D(latest['rate']) if latest['rate'] is not None else D(route['30d']['ratio_return']['simple_apr'])
    kind=latest['rate_kind'] if latest['rate'] is not None else 'historical_ratio_apr'
    for funding in outputs:
        if funding['asset']!=asset: continue
        for days in [3,7]:
            fw=funding[str(days)+'d']
            if not fw['complete']:continue
            ya=((D(1)+yr)**(D(days)/D(365))-D(1))*D(365)/D(days) if kind=='apy' else yr
            combined=ya+fw['short_funding_apr']
            scenarios.append({'route':route['product_code'],'routeid':routeid,'exchange':funding['exchange'],'days':days,'yield_source_kind':kind,'yield_apr_equivalent':ya,
                              'funding_apr':fw['short_funding_apr'],'gross_on_spot_notional_apr':combined,'gross_if_100pct_separate_margin_apr':combined/2,
                              'gross_if_50pct_separate_margin_apr':combined/D('1.5'),
                              'max_margin_fraction_to_reach_3pct_before_costs':combined/D('.03')-1})
(ROOT/'hedge-summary.json').write_text(json.dumps({'end_utc':str(end),'funding':outputs,'scenarios':scenarios},ensure_ascii=False,indent=2,default=str)+'\n')
for s in outputs:
    print(s['asset'],s['exchange'],[(d,s[d]['actual_n'],s[d]['short_funding_apr']) for d in ['1d','3d','7d']])
for s in scenarios:
    if s['days']==7: print('scenario',s)
