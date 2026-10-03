import json
from pathlib import Path
from decimal import Decimal as D, getcontext, ROUND_DOWN
getcontext().prec=36
P=Path(__file__).resolve().parent
ins={x['instrument_id']:x for x in json.loads((P/'instruments.json').read_text(),parse_float=str)}
r={x['instrument_id']:x for x in json.loads((P/'btc-cash-perp-depth.json').read_text(),parse_float=str)}
def side(i,s):
 m=r[i];assert int(m['valid_bitmap'])&1
 return [(D(m[f'{s}_price_{j:02}'])*D(str(ins[i]['price_tick_size'])),D(m[f'{s}_qty_{j:02}'])*D(str(ins[i]['quantity_step_size']))*D(str(ins[i]['contract_multiplier']))) for j in range(1,m['stored_depth']+1) if int(m[f'{s}_price_{j:02}'])]
def value(i,s,q):
 out=D(0)
 for p,a in side(i,s):
  n=min(q,a);out+=n*p;q-=n
  if not q:return out
 return None
q=(D(100000)/(D('2.0031')*side(1,'ask')[0][0])/D('.001')).to_integral_value(rounding=ROUND_DOWN)*D('.001')
a=value(1,'ask',q);b=value(1,'bid',q);c=value(5,'ask',q);d=value(5,'bid',q)
out={'timestamp_utc':r[1]['minute_time'],'quantity_btc':q,'spot_buy':a,'spot_sell':b,'perp_buy':c,'perp_sell':d,'enough_saved_depth':None not in [a,b,c,d]}
if out['enough_saved_depth']:
 fee=(a+b)*D('.001')+(c+d)*D('.0005');friction=a-b+c-d+fee;cap=D(100000);assert 2*a+fee<=cap
 fdata=json.loads((P.parent/'funding/public-Binance-BTC.json').read_text(),parse_float=str)
 from datetime import datetime,timezone,timedelta
 end=datetime(2026,9,21,8,1,tzinfo=timezone.utc);endms=int(end.timestamp())*1000;startms=endms-30*86400000
 rates=[D(x['fundingRate']) for x in fdata['data'] if startms<int(x['fundingTime'])<=endms]
 assert len(rates)==90
 annual=sum(rates)*365/30
 out.update({'total_capital':cap,'spot_and_margin_plus_fee_reserve':2*a+fee,'unused_capital':cap-2*a-fee,'four_trade_fee':fee,'full_roundtrip_friction':friction,'current_entry_perp_discount':1-d/a,'funding_30d_apr_notional':annual,'net_scenarios':{h:(a*annual*D(h)/365-friction)/cap*365/D(h) for h in [30,90,365]},'method':'Future funding equals past30d fixed-notional average; basis and executable depth unchanged; no transfers/tax. This is not a fixed-quantity historical return backtest.'})
(P/'btc-carry.json').write_text(json.dumps(out,default=str,indent=2)+'\n');print(json.dumps(out,default=str,indent=2))
