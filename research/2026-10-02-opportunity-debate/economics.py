"""Decimal research targets, not forecasts or observed trading returns."""
import json
from decimal import Decimal as D, getcontext
from pathlib import Path

getcontext().prec = 40
ROOT = Path(__file__).resolve().parent
capital, daily_target = D('1000000'), D('200')
turnover_targets = [
    {'net_edge_bps': edge, 'required_daily_completed_cycle_notional_usd':
     daily_target / (edge / D('10000'))}
    for edge in map(D, ['0.5', '1', '3', '5', '10'])
]
scenarios = [
    {'business': 'cross_chain_inventory', 'reserved_capital_usd': D('250000'),
     'daily_completed_notional_usd': D('1000000'), 'net_variable_edge_bps': D('0.8'),
     'daily_contribution_usd': D('1000000') * D('0.8') / D('10000')},
    {'business': 'inventory_market_making', 'reserved_capital_usd': D('200000'),
     'daily_completed_notional_usd': D('200000'), 'net_variable_edge_bps': D('3'),
     'daily_contribution_usd': D('200000') * D('3') / D('10000')},
    {'business': 'discount_redemption', 'reserved_capital_usd': D('200000'),
     'asset_purchase_usd': D('100000'), 'hedge_and_other_capital_usd': D('100000'),
     'cycle_days': D('7'), 'net_cycle_edge_bps_on_purchase': D('35'),
     'daily_contribution_usd': D('100000') * D('35') / D('10000') / D('7')},
    {'business': 'keepers_and_auctions', 'reserved_capital_usd': D('50000'),
     'required_30day_net_event_profit_usd': D('900'),
     'daily_contribution_usd': D('900') / D('30')},
]
reserved = sum(row['reserved_capital_usd'] for row in scenarios)
operating_cost = D('20')
contribution = sum(row['daily_contribution_usd'] for row in scenarios)
assert reserved <= capital
assert contribution - operating_cost == daily_target
out = {
    'status': 'Research acceptance targets ONLY; none of these assumed margins, '
              'volumes, cycle lengths or market shares has been validated.',
    'all_money_and_rates': 'Decimal',
    'total_capital_usd': capital,
    'daily_target_usd': daily_target,
    'full_capital_simple_annual_target_pct': daily_target * D('365') / capital * D('100'),
    '30day_target_usd': daily_target * D('30'),
    'turnover_requirements': turnover_targets,
    'illustrative_portfolio_acceptance_targets': scenarios,
    'reserved_business_capital_usd': reserved,
    'unallocated_liquidity_buffer_usd': capital - reserved,
    'assumed_daily_common_operating_cost_usd': operating_cost,
    'conditional_daily_net_usd': contribution - operating_cost,
    'cash_accounting': 'Capital is segregated by chain/venue and not double counted. '
                       'Variable margins include execution, hedging and inventory '
                       'restoration; common operating costs are deducted once. '
                       'Unallocated reserves earn zero. Daily contributions are '
                       'calendar averages, not guaranteed daily realized cash.',
    'stress_cases': {
        'no_cross_chain_access_daily_net_usd': contribution - scenarios[0]['daily_contribution_usd'] - operating_cost,
        'all_business_contributions_halved_daily_net_usd': contribution / D('2') - operating_cost,
        'one_10000_usd_loss_erases_target_days': D('10000') / daily_target,
    },
}
(ROOT / 'economics.json').write_text(json.dumps(out, default=str, indent=2) + '\n')
print(json.dumps(out, default=str, indent=2))
