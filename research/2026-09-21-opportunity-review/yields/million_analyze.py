#!/usr/bin/env python3
"""Decimal depth consumption for 1m USDT AVAX spot + perpetual hedge.

Future exit books are not known. Same-book round trips quantify displayed
capacity and friction, not a claim of future realized PnL. A missing book tail
is never silently filled; only a separately labelled optimistic price bound
is calculated.
"""
from decimal import Decimal as D, getcontext, ROUND_CEILING
from datetime import datetime,timezone
from pathlib import Path
import json
getcontext().prec=50
ROOT=Path(__file__).resolve().parent
def read(name):return json.loads((ROOT/name).read_text())
def book(kind):return read('million-'+kind+'-book.raw.json')['data'][0]
def spec(kind):return read('million-'+kind+'-spec.raw.json')['data'][0]
sb,pb=book('spot'),book('swap')
ss,ps=spec('spot'),spec('swap')
assert ss['instId']=='AVAX-USDT' and ss['baseCcy']=='AVAX' and ss['quoteCcy']=='USDT'
assert ps['instId']=='AVAX-USDT-SWAP' and ps['ctValCcy']=='AVAX' and ps['settleCcy']=='USDT'
assert ss['state']==ps['state']=='live'
contract_base=D(ps['ctVal'])*D(ps['ctMult'] or '1')
step=D(ps['lotSz'])*contract_base
spotstep=D(ss['lotSz'])
spotfee=D('.001');swapfee=D('.0005')
for b in [sb,pb]:
    for side in ['asks','bids']:
        px=[D(r[0]) for r in b[side]]
        assert px==sorted(px,reverse=side=='bids')
        assert all(D(r[1])>0 for r in b[side])
    assert D(b['asks'][0][0])>D(b['bids'][0][0])

def consume(b,side,base_qty,multiplier=D(1)):
    remaining=base_qty;quote=D(0);used=0;filled=D(0)
    for row in b[side]:
        price=D(row[0]);available=D(row[1])*multiplier
        take=min(remaining,available)
        quote+=take*price;filled+=take;remaining-=take;used+=1
        if remaining==0:break
    return {'requested_base':base_qty,'filled_base':filled,'missing_base':remaining,'complete':remaining==0,
            'quote':quote,'vwap':quote/filled if filled else None,'levels':used}

capital=D('1000000')
budget=capital/D('2.004')
lo=0;hi=int(budget/D(sb['asks'][0][0])/step)
def buyqty(q):return (q/(1-spotfee)/spotstep).to_integral_value(rounding=ROUND_CEILING)*spotstep
while lo<hi:
    n=(lo+hi+1)//2
    candidate=consume(sb,'asks',buyqty(n*step))
    if candidate['complete'] and candidate['quote']<=budget:lo=n
    else:hi=n-1
q=D(lo)*step
entry=consume(sb,'asks',buyqty(q))
exit=consume(sb,'bids',q)
short=consume(pb,'bids',q,contract_base)
cover=consume(pb,'asks',q,contract_base)
assert entry['complete'] and short['complete'] and cover['complete']
assert q/D(ps['ctVal'])/D(ps['lotSz']) % 1 == 0
base_fee=entry['requested_base']*spotfee
dust=entry['requested_base']-base_fee-q
assert D(0)<=dust<spotstep
entry['fee_in_base']=base_fee
entry['fee_quote_equivalent_already_in_gross_cash']=entry['quote']*spotfee
entry['net_base_after_fee']=entry['requested_base']-base_fee
entry['unhedged_dust_ignored_in_profit']=dust
exit['fee_on_filled_quote']=exit['quote']*spotfee
short['fee_quote']=short['quote']*swapfee
cover['fee_quote']=cover['quote']*swapfee

# An unobserved bid beyond the 400th level cannot exceed the last displayed
# bid. This is an optimistic mathematical upper bound, not a filled quote.
last_bid=D(sb['bids'][-1][0])
exit_upper=exit['quote']+exit['missing_base']*last_bid
exit_upper_net=exit_upper*(1-spotfee)
cost_lower=entry['quote']-exit_upper_net+cover['quote']-short['quote']+short['fee_quote']+cover['fee_quote']
other=entry['quote']*D('.001')
existing=read('public-analysis.json')
conservative=next(x for x in existing['savax_conservative'] if x['holding_days_assumed']==90)
staking_apr=D(conservative['yield_apr_input'])
funding_apr=D(conservative['funding_apr_input'])
yield_income=entry['quote']*staking_apr*88/365
funding_income=short['quote']*funding_apr*90/365
income=yield_income+funding_income
net_upper=income-cost_lower-other
threepctprofit=capital*D('.03')*90/365
allow_cost=income-other-threepctprofit
summary=read('summary.json')
savax=next(x for x in summary['routes'] if x['yield_route_id']==149)['latest']
result={'capital_usdt':capital,'initial_purchase_budget':budget,'spot_cash_spent':entry['quote'],
  'separate_margin_cash':entry['quote'],'remaining_cash_reserve':capital-entry['quote']*2,
  'matched_base_quantity':q,'contract_base':contract_base,'contract_lot_size':ps['lotSz'],'hedge_contracts':q/contract_base,
  'book_ts_utc':{'spot':datetime.fromtimestamp(int(sb['ts'])//1000,timezone.utc).isoformat(),
                 'swap':datetime.fromtimestamp(int(pb['ts'])//1000,timezone.utc).isoformat()},
  'book_ts_difference_ms':abs(int(sb['ts'])-int(pb['ts'])),
  'fee_assumptions':{'spot_taker':spotfee,'swap_taker':swapfee,'other_cost_budget_per_spot_cash':D('.001'),
                    'account_fee_tier_verified':False},
  'legs':{'spot_buy_gross':entry,'spot_sell_matched':exit,'perp_open_short':short,'perp_close_short':cover},
  'visible_spot_exit_missing_fraction':exit['missing_base']/q,
  'spot_entry_price_impact_vs_best_ask':entry['vwap']/D(sb['asks'][0][0])-1,
  'optimistic_bound_not_execution':{'missing_bid_price_upper_bound':last_bid,'full_spot_exit_quote_upper_bound':exit_upper,
    'full_spot_exit_net_quote_upper_bound':exit_upper_net,'four_leg_friction_lower_bound':cost_lower,
    'other_cost_budget':other,'staking_income_assumed':yield_income,'funding_income_assumed':funding_income,
    'gross_income_assumed':income,'profit_90d_upper_bound':net_upper,'annualized_90d_upper_bound':net_upper*365/90/capital,
    'max_four_leg_friction_for_3pct':allow_cost,'target_3pct_90d_profit':threepctprofit},
  'savax_state':{'source_time':savax['observation_time'],'availability':savax['availability'],'ratio':savax['exposure_ratio'],
    'savax_received_if_stake_matched_base':q/D(savax['exposure_ratio']),
    'pool_cash_avax':savax['pool_cash'],'matched_base_divided_by_pool_cash':q/D(savax['pool_cash']),
    'unbonding_seconds':savax['unbonding_seconds'],'redemption_window_seconds':savax['redemption_window_seconds'],
    'finality':savax['finality'],'block_height':savax['block_height']}}
(ROOT/'million-analysis.json').write_text(json.dumps(result,indent=2,default=str)+'\n')
print(json.dumps({k:result[k] for k in ['matched_base_quantity','visible_spot_exit_missing_fraction','spot_entry_price_impact_vs_best_ask','optimistic_bound_not_execution','savax_state']},indent=2,default=str))
