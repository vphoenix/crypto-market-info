import json,collections
from decimal import Decimal as D,getcontext
from pathlib import Path
getcontext().prec=40
P=Path(__file__).resolve().parent
read=lambda name:[json.loads(x,parse_float=str) for x in (P/(name+'.jsonl')).read_text().splitlines()]
meta={x['instrument_id']:x for x in read('production-instruments')}
def levels(row,side,metadata=meta,mapping=None):
 i=row['instrument_id'];m=metadata[i];factor=D(mapping[i]['canonical_base_units_per_venue_base_unit']) if mapping else D(1)
 tick=D(m['price_tick_size'])/factor;step=D(m['quantity_step_size'])*D(m['contract_multiplier'])*factor
 return [(D(row[f'{side}_price_{n:02}'])*tick,D(row[f'{side}_qty_{n:02}'])*step) for n in range(1,min(row['stored_depth'],10)+1) if int(row[f'{side}_qty_{n:02}'])>0]
def walk(asks,bids,buy_fee=D('.001'),sell_fee=D('.001'),budget=None,positive_only=False):
 a=b=0;qa=asks[0][1];qb=bids[0][1];cost=rev=qty=D(0)
 while a<len(asks) and b<len(bids):
  ap,bp=asks[a][0],bids[b][0]
  if positive_only and bp*(1-sell_fee)<=ap*(1+buy_fee):break
  q=min(qa,qb)
  if budget is not None:q=min(q,max(D(0),(budget-cost)/ap))
  if q<=0:break
  cost+=q*ap;rev+=q*bp;qty+=q;qa-=q;qb-=q
  if qa==0:
   a+=1
   if a<len(asks):qa=asks[a][1]
  if qb==0:
   b+=1
   if b<len(bids):qb=bids[b][1]
 return {'base_qty':qty,'buy_notional':cost,'sell_notional':rev,'gross_profit':rev-cost,'fee_only_profit':rev*(1-sell_fee)-cost*(1+buy_fee),'fee_only_net_bps':(rev*(1-sell_fee)/cost/(1+buy_fee)-1)*10000 if cost else None,'filled_budget':budget is not None and cost==budget}
def capacity(asks,bids):
 q=min(sum(x[1] for x in asks),sum(x[1] for x in bids))
 def amount(book):
  rem=q;n=D(0)
  for p,s in book:
   use=min(rem,s);n+=p*use;rem-=use
  return n
 return {'matched_base_qty':q,'buy_notional':amount(asks),'sell_notional':amount(bids)}
latest={r['instrument_id']:r for r in read('production-latest-books')};out={'latest_books':[{k:r[k] for k in ['instrument_id','minute_time','valid_bitmap','stored_depth']} for r in latest.values()],'latest_spot':[],'latest_spot_perp':[],'historical_spot_spikes':[]}
for buy,sell in [(1,3),(3,1)]:
 asks,bids=levels(latest[buy],'ask'),levels(latest[sell],'bid')
 out['latest_spot'].append({'buy':meta[buy]['exchange'],'sell':meta[sell]['exchange'],'capacity':capacity(asks,bids),'at_100k':walk(asks,bids,budget=D(100000)),'positive_capacity':walk(asks,bids,positive_only=True)})
for spot,perp in [(s,p) for s in [1,3] for p in [5,6,7]]:
 asks,bids=levels(latest[spot],'ask'),levels(latest[perp],'bid')
 out['latest_spot_perp'].append({'spot':meta[spot]['exchange'],'perp':meta[perp]['exchange'],'capacity':capacity(asks,bids),'at_100k':walk(asks,bids,buy_fee=D('.001'),sell_fee=D('.00055') if perp==7 else D('.0005'),budget=D(100000))})
deltas=collections.defaultdict(dict)
for r in read('spot-spike-deltas'):deltas[int(r['minute_id'])][r['second_offset']]=r
minutes=collections.defaultdict(dict)
for r in read('spot-spike-minutes'):minutes[r['minute_time']][r['instrument_id']]=r
for minute,rows in minutes.items():
 states={i:{side:{int(r[f'{side}_price_{n:02}']):int(r[f'{side}_qty_{n:02}']) for n in range(1,r['stored_depth']+1) if int(r[f'{side}_qty_{n:02}'])} for side in ['ask','bid']} for i,r in rows.items()}
 results=[]
 for sec in range(60):
  for i,r in rows.items():
   delta=deltas[int(r['id'])].get(sec)
   if delta:
    for side in ['bid','ask']:
     for p,q in zip(delta[side+'_change_prices'],delta[side+'_change_qtys']):
      if int(q):states[i][side][int(p)]=int(q)
      else:states[i][side].pop(int(p),None)
  if not all((int(r['valid_bitmap'])>>sec)&1 for r in rows.values()):continue
  for buy,sell in [(1,3),(3,1)]:
   books={}
   for i,side in [(buy,'ask'),(sell,'bid')]:
    m=meta[i];books[side]=[(D(p)*D(m['price_tick_size']),D(q)*D(m['quantity_step_size'])) for p,q in sorted(states[i][side].items(),reverse=side=='bid')[:10]]
   val=walk(books['ask'],books['bid'],positive_only=True)
   if val['base_qty']:
    results.append({'second':sec,'buy':meta[buy]['exchange'],'sell':meta[sell]['exchange'],'positive_capacity':val,'at_100k':walk(books['ask'],books['bid'],budget=D(100000))})
 out['historical_spot_spikes'].append({'minute_time':minute,'positive_seconds':len(results),'results':results})
sm={x['instrument_id']:x for x in read('soak-instruments')};mapping={x['instrument_id']:x for x in read('mapping')};sb={x['instrument_id']:x for x in read('soak-stale-candidate-books')}
pairs=json.loads((P/'analysis.json').read_text())['pairs'][:20];stale=[]
for x in pairs:
 l,s=x['long_id'],x['short_id']
 if l not in sb or s not in sb:continue
 asks,bids=levels(sb[l],'ask',sm,mapping),levels(sb[s],'bid',sm,mapping)
 stale.append({'market':x['market'],'long':x['long_venue'],'short':x['short_venue'],'long_time':sb[l]['minute_time'],'short_time':sb[s]['minute_time'],'capacity':capacity(asks,bids)})
out['stale_perp_pair_capacity']=stale
(P/'books-analysis.json').write_text(json.dumps(out,default=str,indent=2)+'\n')
print(json.dumps({k:v for k,v in out.items() if k not in ['historical_spot_spikes','stale_perp_pair_capacity']},default=str,indent=2))
for s in out['historical_spot_spikes']:
 print('spike',s['minute_time'],s['positive_seconds'])
 for r in sorted(s['results'],key=lambda x:x['positive_capacity']['buy_notional'],reverse=True)[:3]:print(json.dumps(r,default=str))
for s in stale[:10]:print('stale',json.dumps(s,default=str))
