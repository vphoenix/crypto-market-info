"""One-year principal-floor allocations and explicit lending cash-flow scenarios.

These are conditional calculations, not guaranteed investment returns. The floor
requires the protected leg to pay in the benchmark currency at the target date,
bounded costs, no leverage, and no creditor recourse to that leg.
"""
from decimal import Decimal as D, ROUND_CEILING
from pathlib import Path
import json

P=Path(__file__).resolve().parent
out={'principal_floor':[], 'usd_wire_lending':[], 'assumptions':{
    'safe_one_year_rate':'0.04', 'risky_one_year_rate_after_platform_interest_fee':'0.085',
    'safe_rate_is_conditional_on_eligibility_and_contract':'true',
    'wire_deposit_fee':'max(0.1%, USD60)', 'wire_withdraw_fee':'max(0.1%, USD100)',
    'minimum_usd_wire_deposit_and_withdrawal':'10000; full verification and supported banking route required',
    'excluded_costs':'external/intermediary bank fees, FX, tax; no asserted zero principal-loss probability'}}
for capital in (D('10000'),D('100000')):
    for target_return in (D('0'),D('0.035')):
        for upfront_cost in (D('0'),D('30')):
            # Costs are explicitly prepaid before allocation; no further costs
            # are assumed for this illustrative bound.
            floor=capital*(1+target_return)
            safe=(floor/D('1.04')).quantize(D('0.01'),rounding=ROUND_CEILING)
            risky=capital-upfront_cost-safe
            assert risky>=0
            safe_terminal=safe*D('1.04')
            assert safe_terminal>=floor
            success_terminal=safe_terminal+risky*D('1.085')
            out['principal_floor'].append({'capital':capital,'target_floor_return':target_return,'upfront_cost_budget':upfront_cost,
                'safe_allocation':safe,'risky_allocation':risky,'risky_weight':risky/capital,
                'risky_total_loss_terminal':safe_terminal,'risky_pays_8_5pct_terminal':success_terminal,
                'conditional_success_return':success_terminal/capital-1,
                'risky_sleeve_meets_standalone_usd_wire_minimum':risky>=D('10000'),
                'note':'Allocation illustration, not an executable bank-plus-Bitfinex USD wire package. Actual costs must be reserved and account eligibility verified.'})
    deposit_fee=max(capital*D('.001'),D('60'))
    deploy=capital-deposit_fee
    for days in (D('90'),D('120'),D('365')):
        for occupancy in (D('1'),D('.9')):
            income=deploy*D('.085')*days/365*occupancy
            withdrawal_fee=max((deploy+income)*D('.001'),D('100'))
            profit=deploy+income-withdrawal_fee-capital
            out['usd_wire_lending'].append({'initial_total_capital':capital,'deposit_fee':deposit_fee,'lendable':deploy,'days':days,
                'occupancy':occupancy,'interest_after_15pct_platform_fee':income,'withdrawal_fee':withdrawal_fee,
                'profit_after_these_fees':profit,'period_return':profit/capital,'simple_annualized_scenario':profit/capital*365/days,
                'note':'Assumes future available rate remains 8.5% after platform fee, no credit losses and specified occupancy. Not fixed for the year. Days measure lending horizon and exclude bank transfer time.'})
out['one_year_with_transfer_idle_days']=[]
for capital in (D('10000'),D('100000')):
    deposit_fee=max(capital*D('.001'),D('60'))
    deploy=capital-deposit_fee
    for occupancy in (D('1'),D('.9')):
        income=deploy*D('.085')*D(345)/365*occupancy
        withdrawal_fee=max((deploy+income)*D('.001'),D('100'))
        profit=deploy+income-withdrawal_fee-capital
        out['one_year_with_transfer_idle_days'].append({'total_capital':capital,
            'calendar_days':D(365),'transfer_idle_days_assumed':D(20),'lending_window_days':D(345),
            'occupancy_within_lending_window':occupancy,'profit_after_platform_and_wire_fees':profit,
            'one_year_return_scenario':profit/capital,
            'note':'20 days is a sensitivity assumption, not a guaranteed transfer SLA. External bank costs and tax remain excluded.'})
(P/'portfolio-math.json').write_text(json.dumps(out,indent=2,default=str)+'\n')
print(json.dumps(out,default=str))
