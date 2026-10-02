"""Independent offline checks of the main candidate against raw official responses."""
import hashlib
import json
from datetime import datetime, timezone
from decimal import Decimal as D, getcontext
from pathlib import Path

getcontext().prec = 60
ROOT = Path(__file__).resolve().parent
YIELDS = ROOT / 'yields'


def load(name):
    return json.loads((YIELDS / name).read_text(), parse_float=str)


name = 'pendle-susds-999k-slippage1bp'
raw = (YIELDS / (name + '.raw')).read_bytes()
meta = load(name + '.meta.json')
assert hashlib.sha256(raw).hexdigest() == meta['sha256']
quote = json.loads(raw, parse_float=str)
route = quote['routes'][0]
info = route['contractParamInfo']
parameters = dict(zip(info['contractCallParamsName'], info['contractCallParams']))
assert parameters['market'].lower() == '0x9c560ebaf78e596cbcc27411d633a74d628dd7dc'
assert info['method'] == 'swapExactTokenForPt'
assert quote['inputs'][0]['token'].lower() == '0xdac17f958d2ee523a2206206994597c13d831ec7'
assert D(quote['inputs'][0]['amount']) / D(10) ** 6 == D(999000)
assert route['outputs'][0]['token'].lower() == '0xdc169abe56461a2e0c034da431ac2a3ebf596094'
assert parameters['input']['tokenMintSy'].lower() == '0xdc035d45d973e3ec169d2276ddab16f1e407384f'
minimum_pt = D(parameters['minPtOut']) / D(10) ** 18
expected_pt = D(route['outputs'][0]['amount']) / D(10) ** 18
assert minimum_pt < expected_pt
market_raw = (YIELDS / 'pendle-current-metadata.raw').read_bytes()
market_receipt = load('pendle-current-metadata.meta.json')
assert hashlib.sha256(market_raw).hexdigest() == market_receipt['sha256']
market_response = json.loads(market_raw, parse_float=str)
markets = [row for row in market_response['results']
           if row['address'].lower() == parameters['market'].lower()]
assert len(markets) == 1
market = markets[0]
assert market['accountingAsset'].lower() == '1-0xdc035d45d973e3ec169d2276ddab16f1e407384f'
assert market['pt'].lower() == '1-' + route['outputs'][0]['token'].lower()
collected = datetime.fromisoformat(meta['collected_at_utc'])
expiry = datetime.fromisoformat(market['expiry'].replace('Z', '+00:00'))
assert expiry == datetime(2026, 11, 26, tzinfo=timezone.utc)
span = expiry - collected
days = (D(span.days) * 86400 + span.seconds + D(span.microseconds) / 1000000) / 86400
capital, reserve, extra_fixed, exit_loss = D(1000000), D(1000), D(500), D('.001')
profit = minimum_pt * (1 - exit_loss) + reserve - capital - extra_fixed
annualized = profit / capital * 365 / days
required_profit = capital * D('.03') * days / 365
maximum_loss_bps = ((minimum_pt + reserve - capital - extra_fixed - required_profit)
                    / minimum_pt * 10000)
result = next(row for row in load('pendle-calculations.json')['results']
              if row['quote_name'] == name and row['route_index'] == 0)
scenario = next(row for row in result['scenarios'] if row['bound'] == 'min'
                and row['extra_gas_and_fixed_costs_usdt'] == '500'
                and row['future_usds_to_usdt_haircut_rate'] == '0.001')
assert abs(days - D(result['hold_days'])) < D('1e-45')
assert profit == D(scenario['full_capital_hold_profit_usdt'])
assert abs(annualized - D(scenario['simple_annualized_full_capital_rate'])) < D('1e-45')
assert annualized > D('.03')

exit_raw = (YIELDS / 'pendle-usds-usdt-exit.raw').read_bytes()
assert hashlib.sha256(exit_raw).hexdigest() == load('pendle-usds-usdt-exit.meta.json')['sha256']
exit_quote = json.loads(exit_raw, parse_float=str)
assert exit_quote['inputs'][0]['token'].lower() == '0xdc035d45d973e3ec169d2276ddab16f1e407384f'
assert exit_quote['routes'][0]['outputs'][0]['token'].lower() == '0xdac17f958d2ee523a2206206994597c13d831ec7'
exit_input = D(exit_quote['inputs'][0]['amount']) / D(10) ** 18
exit_output = D(exit_quote['routes'][0]['outputs'][0]['amount']) / D(10) ** 6
exit_info = exit_quote['routes'][0]['contractParamInfo']
exit_params = dict(zip(exit_info['contractCallParamsName'], exit_info['contractCallParams']))
assert exit_params['swaps'][0]['tokenOut'].lower() == '0xdac17f958d2ee523a2206206994597c13d831ec7'
exit_minimum = D(exit_params['swaps'][0]['minOut']) / D(10) ** 6
assert exit_minimum == D('1005805.480650')
out = {'verified_offline': True, 'source_sha256': meta['sha256'],
       'minimum_pt_from_contract_parameter': minimum_pt,
       'holding_days': days, 'conditional_net_profit_usdt': profit,
       'conditional_simple_annualized_pct': annualized * 100,
       'max_future_exit_loss_bps_with_500_fixed_cost': maximum_loss_bps,
       'current_exit_reference_input_usds': exit_input,
       'current_exit_reference_output_usdt': exit_output,
       'current_exit_reference_minimum_output_usdt': exit_minimum,
       'scope': 'Raw API byte hash, market and currency identities, contract minPtOut '
       'and independent Decimal arithmetic checked. No chain simulation, transaction '
       'or future exit guarantee; fixed cost and haircut are assumptions.'}
(ROOT / 'independent-validation.json').write_text(json.dumps(out, default=str, indent=2) + '\n')
print(json.dumps(out, default=str, indent=2))
