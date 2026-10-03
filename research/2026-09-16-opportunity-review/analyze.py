"""Decimal-only financial calculations on frozen public snapshots."""
import json
from pathlib import Path
from decimal import Decimal as D, getcontext
from datetime import datetime, timezone, timedelta
getcontext().prec=40
P=Path(__file__).resolve().parent
def read(n):return json.loads((P/(n+'.json')).read_text(),parse_float=str)
def stamp(s):return int(datetime.fromisoformat(s).timestamp())*1000
END=stamp('2026-09-16T08:01:00+00:00')
def hist(s,v):
    r=read('history-'+v+'-'+s)
    if v=='binance':return {int(x['fundingTime']):D(x['fundingRate']) for x in r}
    assert r['retCode']==0
    return {int(x['fundingRateTimestamp']):D(x['fundingRate']) for x in r['result']['list']}
def window(h,days):
    a=sorted((t,r) for t,r in h.items() if END-days*86400000<t<=END)
    return {'count':len(a),'sum':sum((r for t,r in a),D(0)), 'first_ms':a[0][0] if a else None,'last_ms':a[-1][0] if a else None,'max_gap_hours':max((D(b[0]-a[0])/3600000 for a,b in zip(a,a[1:])),default=D(0)), 'history_reaches_start':min(h)<=END-days*86400000}
def book(s,v):
    r=read('depth-'+v+'-'+s)
    if v=='bybit':
        assert r['retCode']==0;r=r['result'];r={'asks':r['a'],'bids':r['b']}
    out={k:[(D(x[0]),D(x[1])) for x in r[k]] for k in ['asks','bids']}
    assert all(p>0 and q>0 for a in out.values() for p,q in a)
    assert all(a[0]<=b[0] for a,b in zip(out['asks'],out['asks'][1:]))
    assert all(a[0]>=b[0] for a,b in zip(out['bids'],out['bids'][1:]))
    return out
def consume(levels,q):
    left=q;val=D(0)
    for p,n in levels:
        take=min(n,left);val+=p*take;left-=take
        if not left:break
    return None if left else val

out={'end_utc':'2026-09-16T08:01:00Z','majors':{},'candidates':{},'dated':[]}
for s in ['BTC','ETH','SOL','TRX','AVAX']:
    h=hist(s,'binance');out['majors'][s]={}
    for days in [7,30]:
        r=window(h,days);assert r['count']==days*3 and r['history_reaches_start']
        r['notional_simple_apr']=r['sum']*365/days
        r['two_equal_capital_gross_simple_apr']=r['notional_simple_apr']/2
        out['majors'][s][days]=r
for s in ['CVC','MTL','LSK','IOST','STEEM','HIVE','ONG']:
    hs={v:hist(s,v) for v in ['binance','bybit']};bs={v:book(s,v) for v in hs}
    r={'windows':{},'capacity':{},'roundtrip_friction':{}}
    for d in [1,7]:
        ws={v:window(h,d) for v,h in hs.items()};r['windows'][d]=ws
        ws['bybit_short_minus_binance_long']=ws['bybit']['sum']-ws['binance']['sum']
    for v,b in bs.items():
        r['capacity'][v]={}
        for side,l in b.items():
            r['capacity'][v][side]={'all_visible_usdt':sum(p*q for p,q in l),'within_50bp_usdt':sum(p*q for p,q in l if abs(p/l[0][0]-1)<=D('.005'))}
    mid=sum((bs[v]['asks'][0][0]+bs[v]['bids'][0][0])/2 for v in bs)/2
    for capital in [100000,1000000]:
        q=(D(capital)/2/mid).to_integral_value(rounding='ROUND_DOWN')
        vals={v:{side:consume(levels,q) for side,levels in b.items()} for v,b in bs.items()}
        if any(x is None for vv in vals.values() for x in vv.values()):
            r['roundtrip_friction'][capital]={'qty':q,'insufficient_visible_depth':True,'leg_values':vals};continue
        cost=sum(vv['asks']-vv['bids'] for vv in vals.values())
        fees=sum(sum(vv.values())*D('.0005' if v=='binance' else '.00055') for v,vv in vals.items())
        r['roundtrip_friction'][capital]={'qty':q,'spread_slippage':cost,'fees':fees,'total':cost+fees,'leg_values':vals}
    out['candidates'][s]=r

meta={x['instId']:x for x in read('okx-futures-info')['data']}
spot=read('depth-okx-BTC-USDT')['data'][0];asks=[(D(x[0]),D(x[1])) for x in spot['asks']]
for s in ['BTC-USD-261225','BTC-USD-270326','BTC-USD-270924']:
    m=meta[s];b=read('depth-okx-'+s)['data'][0];lot=D(m['lotSz']);face_per_lot=lot*D(m['ctVal'])*D(m['ctMult'])
    levels=[(D(x[0]),D(x[1])*D(m['ctVal'])*D(m['ctMult'])) for x in b['bids']]
    def cost(lots):
        left=lots*face_per_lot;q=D(0)
        for px,cap in levels:
            take=min(left,cap);q+=take/px;left-=take
            if not left:break
        if left:return None
        c=consume(asks,q)
        return (c,q) if c is not None else None
    for capital in [100000,1000000]:
        budget=D(capital);lo=0;hi=int(sum(x[1] for x in levels)/face_per_lot)+1
        while hi-lo>1:
            k=(lo+hi)//2;c=cost(k)
            if c and c[0]*D('1.003')<=budget:lo=k
            else:hi=k
        c,q=cost(lo);face=lo*face_per_lot;expense=c*D('.003');profit=face-c-expense
        days=(D(m['expTime'])-D(b['ts']))/86400000
        out['dated'].append({'instrument':s,'total_capital':capital,'face_usd':face,'btc_hedge':q,'spot_cost':c,'modeled_total_cost':expense,'capital_used':c+expense,'idle_capital':budget-c-expense,'profit':profit,'net_simple_apr_total_capital':profit/budget*365/days,'days':days,'spot_ts':spot['ts'],'future_ts':b['ts'],'cost_assumption_bps':30})
(P/'analysis.json').write_text(json.dumps(out,indent=2,default=str)+'\n')
print(json.dumps(out,default=str))
