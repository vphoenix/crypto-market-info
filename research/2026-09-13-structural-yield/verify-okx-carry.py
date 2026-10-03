import json
from decimal import Decimal as D,ROUND_CEILING
from pathlib import Path
R=Path(__file__).resolve().parent

def raw(n):return json.loads((R/(n+'.json')).read_text())['data']['data']
meta={x['instId']:x for x in raw('okx-future-instruments')}
def quote(rows,amount,inverse=False,mult=D(1)):
 left=amount;value=D(0);used=[]
 for row in rows:
  p,q=D(row[0]),D(row[1])*mult;take=min(left,q)
  value+=take/p if inverse else take*p;left-=take;used.append([p,take])
  if not left:return value,used
 raise ValueError('insufficient visible depth')
out=[]
for name in ['BTC-USD-270625','BTC-USD-270924','BTC-USD-261225','ETH-USD-270625']:
 b=raw('okx-depth-'+name)[0];m=meta[name];base=m['settleCcy']
 for stable in (['USDT','USDC'] if base=='BTC' else ['USDT']):
  sname=base+'-'+stable;s=raw('okx-depth-'+sname)[0];sm=raw('okx-instrument-'+sname)[0]
  for face in [D(10000),D(100000)]:
   r={'instrument':name,'spot':sname,'face_USD':face,'total_initial_capital_stable':face,'future_ts':b['ts'],'spot_ts':s['ts']}
   try:
    q,flevels=quote(b['bids'],face,True,D(m['ctVal'])*D(m.get('ctMult') or '1'))
    ffee=q*D('.0005');lot=D(sm['lotSz']);buy=((q+ffee)/D('.999')/lot).to_integral_value(rounding=ROUND_CEILING)*lot
    cost,slevels=quote(s['asks'],buy);residual=buy*D('.999')-q-ffee
    payout=face*D('.9999')*D('.999');net=payout-cost
    years=(D(m['expTime'])-max(D(b['ts']),D(s['ts'])))/D(1000*86400*365)
    r.update({'future_effective_entry':face/q,'future_base':q,'future_entry_fee_base':ffee,'spot_buy_qty':buy,'spot_vwap':cost/buy,'initial_cost':cost,'cash_remaining':face-cost,'residual_base_nonnegative':residual,'expiry_value_after_delivery_and_sale_cost_scenario':payout,'net_scenario':net,'days':years*365,'annualized_total_capital':net/face/years,'annualized_deployed':net/cost/years,'future_levels':flevels,'spot_levels':slevels})
   except ValueError as e:r['error']=str(e)
   out.append(r)
(R/'okx-carry-verification.json').write_text(json.dumps(out,default=str,indent=2)+'\n')
for r in out:print(json.dumps({k:v for k,v in r.items() if k not in ['future_levels','spot_levels']},default=str))
