import json
from decimal import Decimal as D,getcontext
from pathlib import Path
from collections import defaultdict
from datetime import datetime,timedelta
getcontext().prec=36
P=Path(__file__).resolve().parent
load=lambda n:json.loads((P/(n+'.json')).read_text(),parse_float=str)
ins={x['instrument_id']:x for x in load('instruments')}
ds=defaultdict(dict)
for r in load('spike_deltas'):ds[int(r['minute_id'])][r['second_offset']]=r
states=defaultdict(dict)
for m in load('spike_minutes'):
 i=m['instrument_id'];dep=m['stored_depth'];v=int(m['valid_bitmap']);bid={};ask={}
 for side,book in [('bid',bid),('ask',ask)]:
  for n in range(1,dep+1):
   p=int(m[f'{side}_price_{n:02}']);q=int(m[f'{side}_qty_{n:02}'])
   if p and q:book[p]=q
 for sec in range(60):
  r=ds[int(m['id'])].get(sec)
  if r:
   assert (v>>sec)&1
   for side,b in [('bid',bid),('ask',ask)]:
    assert len(r[side+'_change_prices'])==len(r[side+'_change_qtys'])
    for p,q in zip(r[side+'_change_prices'],r[side+'_change_qtys']):
     p=int(p);q=int(q)
     if q:b[p]=q
     else:b.pop(p,None)
  assert len(bid)<=dep and len(ask)<=dep
  if not (v>>sec)&1 or not bid or not ask:continue
  assert max(bid)<min(ask)
  tick=D(str(ins[i]['price_tick_size']));lot=D(str(ins[i]['quantity_step_size']))
  t=datetime.fromisoformat(m['minute_time'])+timedelta(seconds=sec)
  states[t][i]={'bid':[(D(p)*tick,D(q)*lot) for p,q in sorted(bid.items(),reverse=True)],'ask':[(D(p)*tick,D(q)*lot) for p,q in sorted(ask.items())]}
def consume(levels,q):
 val=D(0)
 for p,s in levels:
  take=min(q,s);val+=p*take;q-=take
  if not q:return val
 return None
rows=[]
for t,s in sorted(states.items()):
 if len(s)<2:continue
 for buy,sell in [(1,3),(3,1)]:
  a=s[buy]['ask'][0][0];b=s[sell]['bid'][0][0]
  net=(b*(1-D('.001'))-a*(1+D('.001')))/a*10000
  if net<=0:continue
  q=min(s[buy]['ask'][0][1],s[sell]['bid'][0][1]);caps={}
  for target in [1000,10000,50000,500000]:
   qt=(D(target)/a/D('.00001')).to_integral_value(rounding='ROUND_DOWN')*D('.00001')
   cost=consume(s[buy]['ask'],qt);rev=consume(s[sell]['bid'],qt)
   caps[target]=None if cost is None or rev is None else {'quantity_btc':qt,'cost':cost,'net_cash':rev*(1-D('.001'))-cost*(1+D('.001'))}
  rows.append({'time':str(t),'buy':ins[buy]['exchange'],'sell':ins[sell]['exchange'],'best_net_bps':net,'top_qty_btc':q,'top_notional':q*a,'scales_per_leg':caps})
segments=[]
for r in rows:
 if segments and datetime.fromisoformat(r['time'])==datetime.fromisoformat(segments[-1]['end'])+timedelta(seconds=1) and r['buy']==segments[-1]['buy']:
  z=segments[-1];z['end']=r['time'];z['seconds']+=1;z['max_top_notional']=max(z['max_top_notional'],r['top_notional']);z['max_net_bps']=max(z['max_net_bps'],r['best_net_bps'])
 else:segments.append({'start':r['time'],'end':r['time'],'seconds':1,'buy':r['buy'],'sell':r['sell'],'max_top_notional':r['top_notional'],'max_net_bps':r['best_net_bps']})
summary={'method':'Replay full stored depth by price; apply deltas including deletion and skip invalid bitmap seconds. Fee assumption 10bp per trade per venue; no fill probability, latency or rebalancing. Samples only around four minute spikes, not exhaustive.','paired_valid_seconds':sum(len(s)==2 for s in states.values()),'positive_seconds':len(rows),'segments':segments,'capacities':{}}
for target in [1000,10000,50000,500000]:
 full=[r for r in rows if r['scales_per_leg'][target] is not None];positive=[r for r in full if r['scales_per_leg'][target]['net_cash']>0]
 summary['capacities'][target]={'depth_sufficient_seconds':len(full),'positive_seconds':len(positive),'best_profit':max((r['scales_per_leg'][target]['net_cash'] for r in full),default=None),'best_time':max(full,key=lambda r:r['scales_per_leg'][target]['net_cash'])['time'] if full else None}
(P/'spike_replay.json').write_text(json.dumps({'summary':summary,'positive_samples':rows},default=str,indent=2)+'\n')
print(json.dumps(summary,default=str,indent=2))
