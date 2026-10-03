from pathlib import Path
from decimal import Decimal as D,ROUND_FLOOR
import json,collections,itertools,math
ROOT=Path(__file__).resolve().parent
rows=json.loads((ROOT/'soak-candidate-books.json').read_text())
def depth(r,side):
 return [(D(str(r[f'{side}_price_{i:02}']))*D(str(r['price_tick_size']))/D(str(r['canonical_base_units_per_venue_base_unit'])),D(str(r[f'{side}_qty_{i:02}']))*D(str(r['quantity_step_size']))*D(str(r['contract_multiplier']))*D(str(r['canonical_base_units_per_venue_base_unit']))) for i in range(1,51) if r[f'{side}_qty_{i:02}']]
def quote(r,side,qty):
 left=qty;amount=D(0)
 for p,q in depth(r,side):
  take=min(q,left);amount+=p*take;left-=take
  if not left:break
 return amount if left==0 else None

def pair(base,lv,sv,entry=None,exit=None):
 books={}
 for r in rows:
  if r['canonical_market_key']==base+'-USDT-PERP' and r['exchange'] in (lv,sv) and int(r['valid_bitmap'])&1:books[(r['exchange'],r['minute_time'])]=r
 times=sorted(set(t for v,t in books if (lv,t) in books and (sv,t) in books));t=entry or times[-1];te=exit or t
 l=books[(lv,t)];s=books[(sv,t)];le=books[(lv,te)];se=books[(sv,te)]
 mid=(depth(l,'ask')[0][0]+depth(l,'bid')[0][0]+depth(s,'ask')[0][0]+depth(s,'bid')[0][0])/4
 print('PAIR',base,'L',lv,'S',sv,'entry',t,'exit',te,'common minutes',len(times),'ticks',l['price_tick_size'],s['price_tick_size'],'qtySteps',l['quantity_step_size'],s['quantity_step_size'],'multipliers',l['contract_multiplier'],s['contract_multiplier'])
 print('BEST',[(v,depth(r,'bid')[0],depth(r,'ask')[0]) for v,r in [(lv,l),(sv,s)]])
 for dollars in [D(1000),D(5000),D(10000)]:
  steps=[D(str(r['quantity_step_size']))*D(str(r['contract_multiplier']))*D(str(r['canonical_base_units_per_venue_base_unit'])) for r in [l,s]]
  scale=10**max(-x.as_tuple().exponent for x in steps);step=D(math.lcm(*[int(x*scale) for x in steps]))/scale
  qty=(dollars/mid/step).to_integral_value(rounding=ROUND_FLOOR)*step
  vals=[quote(l,'ask',qty),quote(s,'bid',qty),quote(le,'bid',qty),quote(se,'ask',qty)]
  if None in vals:print('NOTIONAL',dollars,'depth insufficient',vals);continue
  fees={'Binance':D('0.0005'),'Bybit':D('0.00055'),'OKX':D('0.0005')}
  fee=(vals[0]+vals[2])*fees[lv]+(vals[1]+vals[3])*fees[sv]
  pricepnl=vals[2]-vals[0]+vals[1]-vals[3];capital=vals[0]+vals[1]
  print('NOTIONAL',dollars,'Q',qty,'VWAPS',*[str(x/qty) for x in vals],'entry_basis_pct',(vals[1]-vals[0])/dollars*100,'price_PNL',pricepnl,'four_fees',fee,'capital',capital,'ROI_ex_funding_pct',(pricepnl-fee)/capital*100)
 return books,times

if __name__=='__main__':
 for x in [('IOST','Bybit','Binance'),('ONG','Binance','Bybit'),('LSK','Binance','Bybit'),('BLUR','Binance','Bybit'),('NEWT','Binance','Bybit')]:
  pair(*x)
 for x in [('IOST','Bybit','Binance'),('ONG','Binance','Bybit')]:
  pair(*x,entry='2026-09-12 18:59:00',exit='2026-09-12 19:01:00')
