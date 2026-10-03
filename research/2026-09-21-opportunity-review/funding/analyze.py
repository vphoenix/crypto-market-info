"""Decimal-only fixed-direction funding screen; missing settlements never filled."""
import json, itertools, collections
from pathlib import Path
from datetime import datetime, timezone, timedelta
from decimal import Decimal as D, getcontext
getcontext().prec=38
ROOT=Path(__file__).resolve().parent
def read(n): return [json.loads(x,parse_float=str) for x in (ROOT/(n+'.jsonl')).read_text().splitlines()]
def stamp(s): return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)
END=stamp('2026-09-21 08:00:00')
META={int(x['instrument_id']):x for x in read('soak-instruments')}
MAP={int(x['instrument_id']):x for x in read('mapping')}
HIST=collections.defaultdict(dict)
for r in read('soak-actual'):
    assert int(r['distinct_rates'])==1, r
    HIST[int(r['instrument_id'])][stamp(r['funding_time'])]=D(r['settled_rate'])
GROUP=collections.defaultdict(list)
for iid,m in META.items():
    if iid in HIST and m['settle_asset']=='USDT': GROUP[MAP[iid]['canonical_market_key']].append(iid)
FEES={'Binance':D('.0005'),'Bybit':D('.00055'),'OKX':D('.0005')}
def stat(iid,start,end):
    h=HIST[iid];events=sorted((t,r) for t,r in h.items() if start<t<=end)
    return {'count':len(events),'sum':sum((r for t,r in events),D(0)),
      'first':events[0][0].isoformat() if events else None,'last':events[-1][0].isoformat() if events else None,
      'delta_hours':dict(collections.Counter(str(D(int((b[0]-a[0]).total_seconds()))/3600) for a,b in zip(events,events[1:]))) }
def span(iid,start,end):
    # Infer a constant cadence from the whole observed history, never confuse this with authoritative schedule proof.
    ts=sorted(HIST[iid]);diffs=[int((b-a).total_seconds()) for a,b in zip(ts,ts[1:])]
    step=collections.Counter(diffs).most_common(1)[0][0] if diffs else 0
    if step not in [3600,7200,14400,28800]: return None
    expected=[start+timedelta(seconds=x) for x in range(step,int((end-start).total_seconds())+1,step)]
    return {'inferred_hours':D(step)/3600,'expected':len(expected),'missing':sum(t not in HIST[iid] for t in expected),'unexpected':sum(start<t<=end and t not in expected for t in ts)}
out=[]
for key,ids in GROUP.items():
  for a,b in itertools.combinations(ids,2):
    if META[a]['exchange']==META[b]['exchange']:continue
    sa,sb=stat(a,END-timedelta(days=7),END),stat(b,END-timedelta(days=7),END)
    if min(sa['count'],sb['count'])<3:continue
    l,s=(a,b) if sa['sum']<=sb['sum'] else (b,a)
    fee=2*(FEES[META[l]['exchange']]+FEES[META[s]['exchange']])
    windows={}
    for days in [1,3,7,11]:
      start=END-timedelta(days=days);ls,ss=stat(l,start,END),stat(s,start,END)
      spread=ss['sum']-ls['sum']
      windows[str(days)]={'long':ls,'short':ss,'long_completeness':span(l,start,END),'short_completeness':span(s,start,END),'spread':spread,'gross_total_capital_apr':spread/2*365/days,'after_taker_fees_apr':(spread-fee)/2*365/days}
    daily=[]
    for d in range(7):
      end=END-timedelta(days=d);start=end-timedelta(days=1)
      ls,ss=stat(l,start,end),stat(s,start,end)
      daily.append({'end':end.isoformat(),'spread':ss['sum']-ls['sum'],'long_count':ls['count'],'short_count':ss['count']})
    total=windows['7']['spread'];largest=max(x['spread'] for x in daily)
    w=windows['7'];complete=all(w[k] is not None and w[k]['missing']==0 and w[k]['unexpected']==0 for k in ['long_completeness','short_completeness'])
    out.append({'market':key,'long_id':l,'short_id':s,'long_venue':META[l]['exchange'],'short_venue':META[s]['exchange'], 'roundtrip_fee_on_one_leg_notional':fee,'windows':windows,'daily':daily,'positive_days':sum(x['spread']>0 for x in daily),'largest_day_share':largest/total if total>0 else None,'schedule_consistent_7d':complete})
out.sort(key=lambda x:x['windows']['7']['after_taker_fees_apr'],reverse=True)
(ROOT/'screen.json').write_text(json.dumps({'end_utc':END.isoformat(),'pairs':out},indent=2,default=str)+'\n')
for x in out:
    w=x['windows'];
    if w['7']['after_taker_fees_apr']>D('.03'):
      print(x['market'],x['long_venue'],x['short_venue'],x['long_id'],x['short_id'],'7dnet',round(w['7']['after_taker_fees_apr']*100,2),'3dnet',round(w['3']['after_taker_fees_apr']*100,2),'11dnet',round(w['11']['after_taker_fees_apr']*100,2),'positive_days',x['positive_days'],'complete',x['schedule_consistent_7d'],'counts',w['7']['long']['count'],w['7']['short']['count'])
