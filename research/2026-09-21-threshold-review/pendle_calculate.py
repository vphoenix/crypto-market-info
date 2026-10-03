"""Recompute indicative and size-specific PT cash flows using Decimal only."""
from datetime import datetime
from decimal import Decimal, getcontext
import json
from pathlib import Path

getcontext().prec = 50
P = Path(__file__).resolve().parent
D = Decimal

def read(name):
    return json.loads((P / name).read_text(), parse_float=D)

def days_between(start, end):
    delta = end - start
    return D(delta.days) + D(delta.seconds) / D(86400) + D(delta.microseconds) / D(86400000000)

markets = read('pendle_active_ethereum.raw.json')['markets']
quote_meta = read('pendle_susds_million_usdt_quote.meta.json')
quote = read('pendle_susds_million_usdt_quote.raw.json')
quote_time = datetime.fromisoformat(quote_meta['observed_at_utc'])
capital = D(1000000)
target_simple_apr = D('0.045')
summary = {'quote_observed_at_utc': quote_meta['observed_at_utc'], 'capital_usdt': capital, 'target_simple_apr': target_simple_apr, 'assumptions': ['Normal PT redemption in accounting asset', 'Accounting asset converts 1:1 into USDT at maturity', 'No gas, post-quote slippage, redemption fee, or exit cost yet deducted', 'API quote only; no independent eth_call or guaranteed inclusion'], 'markets': [], 'susds_routes': []}
for m in markets:
    if m['name'] not in ['sUSDS', 'sUSDe']:
        continue
    days = days_between(quote_time, datetime.fromisoformat(m['expiry'].replace('Z', '+00:00')))
    apy = D(m['details']['impliedApy'])
    growth = ((D(1) + apy).ln() * days / D(365)).exp()
    summary['markets'].append({'name': m['name'], 'address': m['address'], 'pt': m['pt'], 'expiry': m['expiry'], 'days_to_expiry': days, 'indicative_apy': apy, 'api_liquidity_usd': m['details']['liquidity'], 'indicative_simple_apr': (growth-D(1))*D(365)/days, 'hypothetical_million_cost_headroom_usdt_before_size_quote': capital*(growth-D(1)-target_simple_apr*days/D(365))})
susds = next(m for m in summary['markets'] if m['name'] == 'sUSDS')
days = susds['days_to_expiry']
for i, route in enumerate(quote['routes']):
    output = D(route['outputs'][0]['amount']) / D(10)**18
    minimum = D(route['contractParamInfo']['contractCallParams'][2]) / D(10)**18
    summary['susds_routes'].append({'route_index': i, 'aggregator': route['data']['aggregatorType'], 'quoted_pt_out': output, 'min_pt_out_for_requested_0_1_percent_slippage': minimum, 'api_effective_apy_in_underlying_terms': route['data']['effectiveApy'], 'api_fee_usd_included_in_quote_not_subtracted_twice': route['data']['fee']['usd'], 'cashflow_simple_apr_before_remaining_costs': (output/capital-D(1))*D(365)/days, 'cashflow_apy_before_remaining_costs': (((output/capital).ln()*D(365)/days).exp()-D(1)), 'remaining_cost_budget_usdt_to_target_simple_apr': output-capital-capital*target_simple_apr*days/D(365), 'min_out_cashflow_simple_apr': (minimum/capital-D(1))*D(365)/days})
(P / 'pendle_summary.json').write_text(json.dumps(summary, default=str, indent=2) + '\n')
print(json.dumps(summary, default=str, indent=2))
