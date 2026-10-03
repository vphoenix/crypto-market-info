#!/usr/bin/env python3
"""Read-only U offer arithmetic; source files were fetched from public endpoints."""
from decimal import Decimal as D, ROUND_FLOOR
from pathlib import Path
from datetime import datetime, timezone
import hashlib
import json

ROOT = Path(__file__).resolve().parent

def read(name):
    return json.loads((ROOT / name).read_text(), parse_float=D)

book = read('u-depth-usdt.json')
rule = read('u-rule-usdt.json')['symbols'][0]
step = D(next(f['stepSize'] for f in rule['filters'] if f['filterType'] == 'LOT_SIZE'))
assert step == D('1')
asks = [(D(p), D(q)) for p, q in book['asks']]
bids = [(D(p), D(q)) for p, q in book['bids']]

def buy(capital):
    cash, qty, cost = capital, D(0), D(0)
    for p, available in asks:
        filled = min(available, (cash / p / step).to_integral_value(rounding=ROUND_FLOOR) * step)
        qty += filled
        cost += filled * p
        cash -= filled * p
        if cash < p * step:
            break
    assert qty and cash < asks[-1][0] * step
    return qty, cost, cash

def sell(qty):
    sold, proceeds = D(0), D(0)
    for p, available in bids:
        filled = min(available, qty - sold)
        sold += filled
        proceeds += filled * p
        if sold == qty:
            break
    assert sold == qty
    return proceeds

days = D(28)  # 2026-09-17 00:00 through 2026-10-15 00:00 UTC, bonus accrual.
offers = [('regular', D(8000), D('.06')), ('vip1_3', D(200000), D('.06')), ('vip4_9', D(500000), D('.07'))]
scenarios = []
for capital in map(D, ('100000', '200000', '500000', '1000000')):
    qty, cost, cash = buy(capital)
    immediate_exit = sell(qty)
    for tier, cap, bonus in offers:
        # Fixed public benchmark 0.7% and no compounding; not a committed rate.
        interest_u = (qty * D('.007') + min(qty, cap) * bonus) * days / D(365)
        # Token reward dust remains valued at the same public exit price;
        # this is pre-tax/transfer costs and not an actual completed withdrawal.
        future_same_book_exit = sell(qty + interest_u)
        future_par_exit = qty + interest_u
        scenarios.append(dict(capital_usdt=capital, tier=tier, eligible_cap_u=cap,
            acquired_u=qty, cash_dust_usdt=cash, entry_cost_usdt=cost,
            entry_vwap=cost/qty, immediate_roundtrip_cost_usdt=cost-immediate_exit,
            bonus_accrual_days=days, gross_rewards_u=interest_u,
            whole_capital_gross_run_rate_pct=(qty*D('.007')+min(qty,cap)*bonus)/capital*D(100),
            net_if_current_exit_book_repeats_usdt=future_same_book_exit+cash-capital,
            net_if_exit_at_one_usdt=future_par_exit+cash-capital))

assets = D('1109077959.78')
circulation = D('1103009107.80')
reserves = {k: dict(usd=D(v), share_pct=D(v)/assets*100) for k,v in {
    'binance_total':'724302276.47', 'binance_rwusd':'303376167.55',
    'binance_usdc':'340331263.83', 'binance_usd1':'80219237.73',
    'institutional_custody':'23500558.99','bank_cash':'56096563.42',
    'tbill_fund_token':'201178560.90','smart_wallet_usdc':'50000000',
    'money_market_fund':'54000000'}.items()}
reserves.update(total_assets_usd=assets, circulation_u=circulation,
    excess_assets_usd=assets-circulation,
    reserve_ratio_pct=assets/circulation*100,
    binance_loss_haircut_exhausting_buffer_pct=(assets-circulation)/D('724302276.47')*100)

sources = [
    ('u-depth-usdt.json', 'https://api.binance.com/api/v3/depth?symbol=UUSDT&limit=100'),
    ('u-rule-usdt.json', 'https://api.binance.com/api/v3/exchangeInfo?symbol=UUSDT'),
    ('u-reserves-jul-2026.pdf', 'https://u.tech/assets/audit-report/United%20Stables%20-%20Assurance%20%26%20Reserves%20Report%20Jul%202026.pdf')]
manifest = [dict(file=name, url=url, sha256=hashlib.sha256((ROOT/name).read_bytes()).hexdigest(),
    downloaded_file_mtime_utc=datetime.fromtimestamp((ROOT/name).stat().st_mtime,timezone.utc).isoformat()) for name,url in sources]
output = dict(generated_utc=datetime.now(timezone.utc).isoformat(), book_update_id=book['lastUpdateId'],
    execution_assumptions='Public depth only; zero spot fee if eligible; entry size rounded to lot step; no tax/transfer costs; source book has no wall-clock exchange timestamp; all capital counted; no private account data.',
    scenario_assumptions='All calculations are 28 full bonus days after 2026-09-16 subscription; subscription eligibility and pool availability unverified; both 0.7% real-time and offer persistence assumed; exit price not guaranteed; quoted rewards are U, not dollars.',
    u_offer_url='https://www.binance.com/en/support/announcement/detail/47934ad1629f4236a80c3709e2cd116a',
    sources=manifest, scenarios=scenarios, reserve_report_timestamp_utc='2026-07-31T23:59:00Z', reserves=reserves)
(ROOT/'u-verification-calculations.json').write_text(json.dumps(output,ensure_ascii=False,indent=2,default=str)+'\n')
for row in scenarios:
    if row['tier'] != 'regular':
        print(row)
print('reserves', reserves)
