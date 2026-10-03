#!/usr/bin/env python3
"""Official-history funding comparison and fully labelled holding scenarios."""
from datetime import datetime, timedelta, timezone
from decimal import Decimal as D, getcontext
import json
from pathlib import Path
getcontext().prec=50
ROOT=Path(__file__).resolve().parent
end=datetime(2026,9,21,8,tzinfo=timezone.utc)
def ts(value): return int(value.timestamp())*1000
def dump(name,obj): (ROOT/name).write_text(json.dumps(obj,ensure_ascii=False,indent=2,default=str)+'\n')
stats={}
for symbol in ['SOL','AVAX','TRX']:
    raw=json.loads((ROOT/f'okx-{symbol.lower()}-funding.raw.json').read_text())['data']
    rates={int(x['fundingTime']):D(x['realizedRate']) for x in raw}
    assert len(rates)==len(raw)
    s={}
    for days in [1,3,7,30]:
        start=end-timedelta(days=days)
        expected=[ts(start+timedelta(hours=8*i)) for i in range(1,days*3+1)]
        assert all(x in rates for x in expected),(symbol,days)
        daily=[sum(rates[ts(start+timedelta(days=n,hours=8*k))] for k in [1,2,3])*365 for n in range(days)]
        s[str(days)+'d']={'n':len(expected),'short_funding_apr':sum(rates[x] for x in expected)*D(365)/days,
                         'positive_days':sum(r>0 for r in daily),'daily_aprs':daily}
    s['rolling_7d_apr_30d']=[sum(s['30d']['daily_aprs'][i:i+7])/7 for i in range(24)]
    s['rolling_7d_positive_n']=sum(x>0 for x in s['rolling_7d_apr_30d'])
    stats[symbol]=s
    print(symbol,'7d',s['7d']['short_funding_apr'],'30d',s['30d']['short_funding_apr'],'positive',s['30d']['positive_days'],'rolling',min(s['rolling_7d_apr_30d']),max(s['rolling_7d_apr_30d']))
local=[json.loads(l) for l in (ROOT.parent/'funding/soak-actual.jsonl').read_text().splitlines()]
reconciliations=[]
for sym,iid in [('SOL',1211),('AVAX',154),('TRX',1338)]:
    raw=json.loads((ROOT/f'okx-{sym.lower()}-funding.raw.json').read_text())['data']
    source={int(x['fundingTime']):D(x['realizedRate']) for x in raw}
    matched=0
    for row in local:
        if row['instrument_id']!=iid:continue
        stamp=ts(datetime.fromisoformat(row['funding_time']).replace(tzinfo=timezone.utc))
        if stamp in source:
            assert D(row['settled_rate'])==source[stamp],(sym,row)
            matched+=1
    reconciliations.append({'symbol':sym,'matched_local_actual_n':matched,'mismatch_n':0})
scenarios=[]
yields=json.loads((ROOT/'summary.json').read_text())['routes']
for iid,symbol,exitfee in [(136,'SOL',D('.002')),(138,'SOL',D('.001')),(139,'SOL',D('0')),(149,'AVAX',D('0'))]:
    route=next(x for x in yields if x['yield_route_id']==iid)
    rate=D(route['latest']['rate']) if route['latest']['rate'] is not None else D(route['30d']['ratio_return']['simple_apr'])
    for historydays in [7,30]:
        f=stats[symbol][str(historydays)+'d']['short_funding_apr']
        for holddays in [30,90]:
            # 50% of total starting capital buys spot, 50% is separate USDT
            # margin; retained reserve covers explicitly budgeted cash costs.
            # Holding-period interest is coin-neutral under constant reference
            # prices and regular delta rebalancing. Future prices, funding,
            # availability and rebalancing costs are not asserted constant.
            yr=(D(1)+rate)**(D(holddays)/365)-1 if route['latest']['rate_kind']=='apy' else rate*holddays/365
            trading=D('.001')*2+D('.0005')*2
            external=D('.001') # Explicit scenario budget, NOT observed quote.
            cost=trading+exitfee+external
            interest=yr+f*holddays/365
            # Reserve all assumed cash fees in addition to the 1:1 spot and
            # margin legs; do not silently introduce external capital.
            capital=D(2)+cost
            netapr=(interest-cost)*D(365)/holddays/capital
            scenarios.append({'route':route['product_code'],'routeid':iid,'funding_history_days':historydays,'holding_days_assumed':holddays,
                              'yield_rate_input':rate,'yield_kind':route['latest']['rate_kind'] if route['latest']['rate'] is not None else 'historical_ratio_apr',
                              'funding_apr_input':f,'yield_period_return':yr,'funding_period_return':f*holddays/365,
                              'spot_round_trip_fee_notional':D('.002'),'hedge_round_trip_fee_notional':D('.001'),'protocol_exit_fee_notional':exitfee,
                              'other_cost_budget_notional':external,'total_cost_notional':cost,
                              'capital_multiple_spot_notional':capital,
                              'gross_total_capital_apr':interest*365/holddays/capital,'scenario_net_total_capital_apr':netapr,
                              'net_total_capital_return':(interest-cost)/capital,
                              'extra_loss_notional_before_falling_below_3pct':interest-cost-D('.03')*capital*holddays/365,
                              'missing': ['executable spot/LST depth', 'actual exchange withdrawal and gas cost', 'basis change', 'delta rebalancing cost', 'yield/funding future path', 'account fee tier'],
                              'kamino_zero_exit_fee_is_unverified_assumption':iid==139})
avax=next(x for x in yields if x['yield_route_id']==149)
conservative=[]
for holddays in [30,90]:
    yr=D(avax['7d']['ratio_return']['simple_apr'])
    f=stats['AVAX']['30d']['short_funding_apr']
    cost=D('.004')
    capital=D(2)+cost
    earningdays=holddays-2 # final claim window conservatively earns no staking
    gain=yr*D(earningdays)/365+f*D(holddays)/365-cost
    amount=D('100000')
    conservative.append({'holding_days_assumed':holddays,'staking_earning_days_assumed':earningdays,'yield_apr_input':yr,
      'yield_observed_period':avax['7d']['ratio_return'],'funding_apr_input':f,'total_cost_notional':cost,
      'initial_capital':amount,'spot_notional':amount/capital,'margin_notional':amount/capital,'cost_reserve':amount*cost/capital,
      'conditional_profit_usdt':amount*gain/capital,'conditional_net_apr':gain*365/holddays/capital,
      'extra_loss_notional_before_falling_below_3pct':gain-D('.03')*capital*holddays/365,
      'extra_loss_usdt_before_falling_below_3pct':amount*(gain-D('.03')*capital*holddays/365)/capital})
dump('public-analysis.json',{'cutoff_utc':str(end),'funding':stats,'local_reconciliation':reconciliations,'scenarios':scenarios,'savax_conservative':conservative})
for s in scenarios:
    if s['funding_history_days']==30:print('scenario',s['route'],s['holding_days_assumed'],s['scenario_net_total_capital_apr'])
