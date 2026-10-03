"""SOON equal-base-quantity depth and fixed-quantity price-PnL sensitivity."""
import json,math
from decimal import Decimal as D,ROUND_FLOOR
from pathlib import Path
from datetime import datetime,timezone,timedelta
ROOT=Path(__file__).resolve().parent
def read(n):return [json.loads(x,parse_float=str) for x in (ROOT/(n+'.jsonl')).read_text().splitlines()]
META={int(x['instrument_id']):x for x in read('soak-instruments')}
MAP={int(x['instrument_id']):x for x in read('mapping')}
FEE={1218:D('.0005'),1219:D('.00055'),1220:D('.0005')}
EPOCH=datetime(1970,1,1,tzinfo=timezone.utc)
funding={}
for i in FEE:
 v=META[i]['exchange'];p=json.loads((ROOT/f'public-{v}-SOON.json').read_text())['data']
 if v=='Binance':ev=[(r['fundingTime'],r['fundingRate']) for r in p]
 elif v=='Bybit':ev=[(r['fundingRateTimestamp'],r['fundingRate']) for r in p['result']['list']]
 else:ev=[(r['fundingTime'],r['realizedRate']) for r in p['data']]
 funding[i]=[(EPOCH+timedelta(milliseconds=int(t)),D(r)) for t,r in ev]
rows=read('soon-books');books={(r['instrument_id'],r['minute_time']):r for r in rows if int(r['valid_bitmap'])&1}
def depth(r,side):
 m=META[r['instrument_id']];a=MAP[r['instrument_id']]
 tick=D(m['price_tick_size'])/D(a['canonical_base_units_per_venue_base_unit'])
 unit=D(m['quantity_step_size'])*D(m['contract_multiplier'])*D(a['canonical_base_units_per_venue_base_unit'])
 return [(D(r[f'{side}_price_{i:02}'])*tick,D(r[f'{side}_qty_{i:02}'])*unit) for i in range(1,11) if int(r[f'{side}_qty_{i:02}'])>0]
def mid(r):return (depth(r,'ask')[0][0]+depth(r,'bid')[0][0])/2
def consume(r,side,q):
 left=q;cost=D(0)
 for p,qty in depth(r,side):
  take=min(qty,left);cost+=take*p;left-=take
  if not left:return cost
 return None
def qty_step(ids):
 steps=[D(META[i]['quantity_step_size'])*D(META[i]['contract_multiplier'])*D(MAP[i]['canonical_base_units_per_venue_base_unit']) for i in ids]
 scale=10**max(max(0,-x.as_tuple().exponent) for x in steps)
 return D(math.lcm(*[int(x*scale) for x in steps]))/D(scale)
out=[]
for li,si in [(1220,1219),(1218,1219)]:
 times=sorted(t for i,t in books if i==li and (si,t) in books);latest=times[-1];lb,sb=books[(li,latest)],books[(si,latest)]
 bases=[(t,(mid(books[(si,t)])-mid(books[(li,t)]))/mid(books[(li,t)])) for t in times]
 hourly=[x for x in bases if x[0][14:16]=='01'];latestmid=(mid(lb)+mid(sb))/2
 r={'long':META[li]['exchange'],'short':META[si]['exchange'],'first':times[0],'latest':latest,'hourly_common_points':len(hourly),'mid_basis_short_minus_long':bases[-1][1],'basis_min':min(b for t,b in hourly),'basis_max':max(b for t,b in hourly),'first_basis':bases[0][1],'top10':{},'sizes':{},'fixed_quantity_price_pnl':[]}
 for i,b in [(li,lb),(si,sb)]:r['top10'][META[i]['exchange']]={s:sum(p*q for p,q in depth(b,s)) for s in ['bid','ask']}
 step=qty_step([li,si])
 for notional in [1000,10000,50000]:
  q=(D(notional)/latestmid/step).to_integral_value(rounding=ROUND_FLOOR)*step
  vals=[consume(lb,'ask',q),consume(sb,'bid',q),consume(lb,'bid',q),consume(sb,'ask',q)]
  a={'target_leg_notional':notional,'equal_base_quantity':q,'leg_values_long_buy_short_sell_long_sell_short_buy':vals,'enough_top10':None not in vals}
  if None not in vals:
   fee=(vals[0]+vals[2])*FEE[li]+(vals[1]+vals[3])*FEE[si]
   capital=vals[0]+vals[1];spreadcost=vals[0]+vals[3]-vals[1]-vals[2]
   a.update(capital=capital,entry_basis_fraction=(vals[1]-vals[0])/((vals[0]+vals[1])/2),four_trade_fee=fee,roundtrip_spread_slippage_cost=spreadcost,total_roundtrip_friction_fraction_total_capital=(spreadcost+fee)/capital)
  r['sizes'][notional]=a
 # Show price PnL alone from available historical first anchor to latest, at fixed coin quantity.
 for start in [times[0],next((t for t in times if t>='2026-09-20 08:01:00'),times[0]),next((t for t in times if t>='2026-09-21 08:01:00'),times[0])]:
  el,es=books[(li,start)],books[(si,start)]
  q=(D(1000)/((mid(el)+mid(es))/2)/step).to_integral_value(rounding=ROUND_FLOOR)*step
  vals=[consume(el,'ask',q),consume(es,'bid',q),consume(lb,'bid',q),consume(sb,'ask',q)]
  if None in vals:continue
  capital=vals[0]+vals[1];fee=(vals[0]+vals[2])*FEE[li]+(vals[1]+vals[3])*FEE[si];pnl=vals[2]-vals[0]+vals[1]-vals[3]
  funding_cash=D(0);missing=[];counts={}
  startdt=datetime.fromisoformat(start).replace(tzinfo=timezone.utc);enddt=datetime.fromisoformat(latest).replace(tzinfo=timezone.utc)
  for i,sign in [(li,-1),(si,1)]:
   events=[(t,rate) for t,rate in funding[i] if startdt<t<=enddt];counts[META[i]['exchange']]=len(events)
   for t,rate in events:
    anchor=t.replace(minute=1,second=0,microsecond=0).strftime('%Y-%m-%d %H:%M:%S')
    book=books.get((i,anchor))
    if book is None:missing.append([i,t.isoformat()]);continue
    funding_cash+=D(sign)*q*mid(book)*rate
  r['fixed_quantity_price_pnl'].append({'entry':start,'exit':latest,'equal_base_quantity':q,'capital':capital,'price_pnl_before_funding':pnl,'price_pnl_after_fee_before_funding':pnl-fee,'return_before_funding':(pnl-fee)/capital,'settlements_count':counts,'missing_settlement_price_proxies':missing,'estimated_funding_cash_using_minute_after_settlement_mid':funding_cash if not missing else None,'estimated_pnl_after_fee_and_funding':pnl-fee+funding_cash if not missing else None,'funding_valuation_is_estimate_not_official_mark':True})
 out.append(r)
(ROOT/'books-analysis.json').write_text(json.dumps(out,indent=2,default=str)+'\n')
print(json.dumps(out,indent=2,default=str))
