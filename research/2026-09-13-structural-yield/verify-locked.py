"""Decimal calculations using visible public book snapshots; not fills."""
import json
from decimal import Decimal as D,ROUND_CEILING
from pathlib import Path
R=Path(__file__).resolve().parent

def load(n):return json.loads((R/(n+'.json')).read_text())['data']['result']

def quote(book,side,amount,inverse=False):
 remaining=amount;value=D(0);used=[]
 for p,q in book[side]:
  p,q=D(p),D(q);take=min(remaining,q)
  value+=take/p if inverse else take*p;remaining-=take;used.append([p,take])
  if remaining==0:return value,used
 raise ValueError('insufficient visible depth')

meta={}
for c in ['BTC','ETH','USDC']:meta.update({x['instrument_name']:x for x in load(f'deribit-{c}-instruments')})
out=[]
for name in ['BTC-25JUN27','BTC-25DEC26','ETH-25JUN27','ETH-25DEC26']:
 future=load('depth-'+name);spot=load('depth-'+name.split('-')[0]+'_USDC');m=meta[name]
 for face in [D(10000),D(100000)]:
  r={'instrument':name,'face_USD':face,'initial_total_capital_USDC':face,'future_book_timestamp':future['timestamp'],'spot_book_timestamp':spot['timestamp']}
  try:
   base,used=quote(future,'bids',face,inverse=True)
   future_fee_rate=D(str(m['taker_commission']));future_fee_base=base*future_fee_rate
   # Purchase ceiling leaves non-negative residual base after futures fee.
   buy=((base+future_fee_base)/D('.0001')).to_integral_value(rounding=ROUND_CEILING)*D('.0001')
   spotcost,spotused=quote(spot,'asks',buy)
   # Reserve conservative five bp entry and exit spot costs despite current
   # BTC/ETH spot instrument API declaring zero commissions.
   spotfee=spotcost*D('.0005');invest=spotcost+spotfee
   remainder=face-invest
   delivery_fee=face*D('.00025');exit_spot_fee=(face-delivery_fee)*D('.0005')
   locked_value_assuming_exit_at_settlement_index=face-delivery_fee-exit_spot_fee
   net=locked_value_assuming_exit_at_settlement_index-invest
   years=(D(m['expiration_timestamp'])-D(max(future['timestamp'],spot['timestamp'])))/D(1000*86400*365)
   r.update({'future_effective_entry':face/base,'future_base_equivalent':base,'future_fees_base':future_fee_base,'spot_buy_base':buy,'spot_VWAP':spotcost/buy,'spot_cost':spotcost,'entry_spot_cost_reserve':spotfee,'initial_deployed':invest,'unallocated_cash':remainder,'nonnegative_residual_base':buy-base-future_fee_base,'delivery_fee_USD':delivery_fee,'exit_spot_cost_reserve':exit_spot_fee,'terminal_index_value_floor_after_modeled_costs':locked_value_assuming_exit_at_settlement_index,'net_USD_scenario':net,'days':years*365,'budget_feasible':remainder>=0,'annualized_on_total_capital':net/face/years if remainder>=0 else None,'annualized_on_deployed':net/invest/years,'future_levels':used,'spot_levels':spotused})
  except ValueError as e:r['error']=str(e)
  out.append(r)
(R/'cash-carry-verification.json').write_text(json.dumps(out,default=str,indent=2)+'\n')
for x in out:
 print(json.dumps({k:v for k,v in x.items() if k not in ['future_levels','spot_levels']},default=str))

boxlegs=[('ETH_USDC-25DEC26-2500-C','asks',1),('ETH_USDC-25DEC26-10000-C','bids',-1),('ETH_USDC-25DEC26-10000-P','asks',1),('ETH_USDC-25DEC26-2500-P','bids',-1)]
box={};debit=D(0);fee=D(0)
for n,side,sign in boxlegs:
 b=load('depth-'+n);v,used=quote(b,side,D(1));debit+=v*sign;f=min(D('.0003')*D(str(b['index_price'])),v*D('.125'));fee+=f;box[n]={'side':side,'value':v,'used':used,'fee':f,'timestamp':b['timestamp']}
box['total']={'debit':debit,'fixed_pre_fee_payoff':D(7500),'entry_fees':fee,'pre_settlement_loss':D(7500)-debit-fee}
(R/'box-depth-verification.json').write_text(json.dumps(box,default=str,indent=2)+'\n')
print('BOX',json.dumps(box,default=str))
