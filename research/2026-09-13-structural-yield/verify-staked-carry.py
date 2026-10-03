import json
from decimal import Decimal as D,ROUND_CEILING
from pathlib import Path
R=Path(__file__).resolve().parent

def read(n):return json.loads((R/(n+'.json')).read_text())['data']['result']
def quote(rows,amount,inverse=False):
 left=amount;total=D(0)
 for p,q in rows:
  p,q=D(p),D(q);take=min(left,q);left-=take;total+=take/p if inverse else take*p
  if not left:return total
 raise ValueError('insufficient visible depth')
meta={x['instrument_name']:x for x in read('deribit-ETH-instruments')};stake=read('refresh-STETH_USDC');eth=read('refresh-ETH_USDC')
apr=D(json.loads((R/'lido-current-apr.json').read_text())['data']['data']['smaApr'])/100*D('.99');out=[]
for name in ['ETH-25JUN27','ETH-25DEC26']:
 f=read('refresh-'+name);face=D(10000);q=quote(f['bids'],face,True);feecoin=q*D('.00035');stqty=(q/D('.0001')).to_integral_value(rounding=ROUND_CEILING)*D('.0001');ethqty=(feecoin/D('.0001')).to_integral_value(rounding=ROUND_CEILING)*D('.0001')
 stcost=quote(stake['asks'],stqty);ethcost=quote(eth['asks'],ethqty);invest=(stcost+ethcost)*D('1.0005');payout=face*D('.99975')*D('.9995')
 years=(D(meta[name]['expiration_timestamp'])-max(D(f['timestamp']),D(stake['timestamp']),D(eth['timestamp'])))/D(1000*86400*365)
 # Purely conditional: stake/ETH redemption ratio=1, ETH terminal price
 # equal to current ETH index; flat price path avoids negative-ETH fees.
 eth_terminal=D(str(eth['index_price']));rewards=stqty*apr*years*eth_terminal*D('.9995');profit=payout-invest+rewards
 out.append({'instrument':name,'future_ts':f['timestamp'],'stake_ts':stake['timestamp'],'capital_total_USDC':face,'face_USD':face,'effective_future_entry':face/q,'steth_buy_qty':stqty,'steth_VWAP':stcost/stqty,'eth_fee_purchase':ethqty,'initial_cost_including_entry_fee_reserve':invest,'unallocated_cash':face-invest,'days':years*365,'assumed_Lido_APR_after_Deribit_cut':apr,'terminal_ETH_price_assumption':eth_terminal,'nonnegative_unhedged_ETH_dust':ethqty-feecoin,'nonnegative_unhedged_stETH_dust':stqty-q,'base_profit_if_steth_redeems_1_to_1_no_rewards':payout-invest,'variable_staking_reward_scenario':rewards,'total_profit_scenario':profit,'annualized_on_all_capital_scenario':profit/face/years,'excluded_path_costs':'negative ETH equity collateral fees, dynamic hedging and conversion costs; rewards and stETH/ETH ratio are not guaranteed'})
(R/'staked-carry-scenario.json').write_text(json.dumps(out,default=str,indent=2)+'\n');print(json.dumps(out,default=str,indent=2))
