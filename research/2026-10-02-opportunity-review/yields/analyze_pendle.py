from pathlib import Path
from decimal import Decimal,getcontext
from datetime import datetime,timezone
import json
getcontext().prec=60
D=Decimal;p=Path(__file__).resolve().parent
C=D('1000000');YEAR=D('31536000');EXPIRY=datetime(2026,11,26,tzinfo=timezone.utc)
names=['pendle-susds-million-quote','pendle-susds-999k-quote','pendle-susds-999k-slippage1bp','pendle-susds-999k-slippage3bp']
results=[]
for name in names:
 meta=json.loads((p/(name+'.meta.json')).read_text(),parse_float=D)
 quote=json.loads((p/(name+'.raw')).read_text(),parse_float=D)
 stamp=datetime.fromisoformat(meta['collected_at_utc'])
 td=EXPIRY-stamp;seconds=D(td.days*86400+td.seconds)+D(td.microseconds)/1000000
 days=seconds/86400
 A=D(quote['inputs'][0]['amount'])/(10**6); reserve=C-A
 for n,r in enumerate(quote['routes']):
  Q=D(r['outputs'][0]['amount'])/(10**18)
  call=r['contractParamInfo'];params=dict(zip(call['contractCallParamsName'],call['contractCallParams']))
  assert params['market'].lower()=='0x9c560ebaf78e596cbcc27411d633a74d628dd7dc'
  assert r['outputs'][0]['token'].lower()=='0xdc169abe56461a2e0c034da431ac2a3ebf596094'
  M=D(params['minPtOut'])/(10**18)
  required=C*D('.03')*seconds/YEAR
  row={'quote_name':name,'route_index':n,'collected_at_utc':stamp.isoformat(),'expiry_utc':EXPIRY.isoformat(),'hold_days':str(days),'capital_usdt':str(C),'amount_in_usdt':str(A),'reserve_usdt':str(reserve),'pt_expected':str(Q),'pt_min':str(M),'expected_profit_at_usds_usdt_1_before_extra_costs':str(Q-A),'min_profit_at_usds_usdt_1_before_extra_costs':str(M-A),'expected_simple_annualized_full_capital_rate':str((Q-A)/C*YEAR/seconds),'min_simple_annualized_full_capital_rate':str((M-A)/C*YEAR/seconds),'profit_required_for_3pct':str(required),'expected_additional_cost_budget_for_3pct':str(Q-A-required),'min_additional_cost_budget_for_3pct':str(M-A-required),'reported_effective_apy':r['data'].get('effectiveApy'),'gas_used_estimate':r['data'].get('gasUsed'),'entry_fee_in_quote':r['data'].get('fee'),'scenarios':[]}
  # Costs include all gas, bridging/offramp/onramp, and other extra fees.
  # Future USD asset loss is applied to all mature claim units as Q*(1-h).
  # Entry swap/market fees are already reflected in Q, never deducted twice.
  for gas in [D('500'),D('1000')]:
   for haircut in [D('0'),D('.0005'),D('.001')]:
    for label,q in [('expected',Q),('min',M)]:
     total_profit=q*(1-haircut)+reserve-C-gas
     row['scenarios'].append({'bound':label,'extra_gas_and_fixed_costs_usdt':str(gas),'future_usds_to_usdt_haircut_rate':str(haircut),'full_capital_hold_profit_usdt':str(total_profit),'simple_annualized_full_capital_rate':str(total_profit/C*YEAR/seconds),'max_future_exit_haircut_bps_for_3pct':str((q-A-gas-required)/q*10000),'status':'conditional: assumes successful redemption and future exit within budget; quote is read-only, unfilled'})
  results.append(row)
(p/'pendle-calculations.json').write_text(json.dumps({'definition':'Profit=PT_out*(1-future_exit_loss)+cash_reserve-total_capital-gas_extra; simple APR=profit/total_capital*365/actual_days; minOut protects entry only, not redemption or exit. PT units are 1USDS claim at maturity, not 1sUSDS.','results':results},indent=2,default=str)+'\n')
for name in names:
 arr=[x for x in results if x['quote_name']==name]
 best=max(arr,key=lambda x:D(x['pt_min']))
 print(name,{k:best[k] for k in ['route_index','collected_at_utc','hold_days','pt_expected','pt_min','expected_simple_annualized_full_capital_rate','min_simple_annualized_full_capital_rate','min_additional_cost_budget_for_3pct','gas_used_estimate']})
 for s in best['scenarios']:
  if s['bound']=='min' and s['future_usds_to_usdt_haircut_rate']=='0.001':print('  MIN EXIT-10BPS',s)
