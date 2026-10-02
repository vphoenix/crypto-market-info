"""Independently reproduce the seven-day keeper evidence using Decimal.

Read-only analysis of saved public responses; no network or transaction execution.
"""
import hashlib
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from decimal import Decimal as D, getcontext
from pathlib import Path
from urllib.parse import parse_qs, urlparse

getcontext().prec = 40
ROOT = Path(__file__).resolve().parent
P = ROOT / 'proposer'
SUN = D('1000000')


def read(name):
    return json.loads((P / name).read_text(), parse_float=D)


def utc_ms(value):
    return datetime.fromtimestamp(value // 1000, timezone.utc).isoformat()


def address_hex(address):
    alphabet = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'
    number = 0
    for char in address:
        number = number * 58 + alphabet.index(char)
    raw = number.to_bytes(25, 'big')
    assert hashlib.sha256(hashlib.sha256(raw[:-4]).digest()).digest()[:4] == raw[-4:]
    return raw[:-4].hex()


hashes = {}
for stem in ['tron_energy_7d', 'trx_bookticker', 'tron_chainparameters',
             'tron_receipt_0', 'tron_receipt_1', 'tron_receipt_2']:
    digest = hashlib.sha256((P / f'{stem}.raw.json').read_bytes()).hexdigest()
    assert digest == read(f'{stem}.meta.json')['sha256'], stem
    hashes[stem] = digest

meta = read('tron_energy_7d.meta.json')
query = parse_qs(urlparse(meta['url']).query)
start, end = (int(query[key][0]) for key in ['min_block_timestamp', 'max_block_timestamp'])
assert end - start == 7 * 86400000
response = read('tron_energy_7d.raw.json')
assert response['success'] is True
assert not response.get('meta', {}).get('links', {}).get('next')
events = response['data']
event_keys = {(row['transaction_id'], row['event_index']) for row in events}
assert len(event_keys) == len(events)
assert all(start <= row['block_timestamp'] <= end for row in events)
assert all(row['event_name'] == 'Liquidate' for row in events)
assert all(row['result']['resourceType'] == '1' for row in events)
bid = D(read('trx_bookticker.raw.json')['bidPrice'])
counts, rewards = Counter(), defaultdict(lambda: D('0'))
for row in events:
    who = row['result']['liquidator']
    counts[who] += 1
    rewards[who] += D(row['result']['liquidateFee']) / SUN
gross = sum(rewards.values())
assert gross == D('3256.784436')
amount_sorted = sorted(rewards, key=rewards.get, reverse=True)
fees = {row['key']: D(row.get('value', 0)) for row in read('tron_chainparameters.raw.json')['chainParameter']}
contract_hex = address_hex('TU2MJ5Veik1LRAgjeSzEdvmDYx7mefJZvd')
receipts = []
for index in range(3):
    receipt = read(f'tron_receipt_{index}.raw.json')
    assert receipt['receipt']['result'] == 'SUCCESS'
    matching_events = [row for row in events if row['transaction_id'] == receipt['id']]
    assert len(matching_events) == 1
    event = matching_events[0]
    assert event['block_number'] == receipt['blockNumber']
    assert event['block_timestamp'] == receipt['blockTimeStamp']
    recipient = '41' + event['result']['liquidator'][2:]
    payment = int(event['result']['liquidateFee'])
    matched = [tx for tx in receipt.get('internal_transactions', [])
               if tx['caller_address'] == contract_hex
               and tx['transferTo_address'] == recipient
               and not tx.get('rejected', False)
               and any(not amount.get('tokenId') and amount.get('callValue') == payment
                       for amount in tx.get('callValueInfo', []))]
    assert len(matched) == 1
    energy = D(receipt['receipt']['energy_usage_total'])
    receipts.append({
        'transaction_id': receipt['id'],
        'block_number': receipt['blockNumber'],
        'event_reward_trx': D(payment) / SUN,
        'exact_native_transfer_from_rental_contract_to_liquidator': True,
        'matched_internal_transfer_hash': matched[0]['hash'],
        'total_transaction_energy': energy,
        'energy_all_burn_equivalent_trx_at_current_parameter': energy * fees['getEnergyFee'] / SUN,
        'receipt_burned_fee_field': receipt.get('fee', 'omitted'),
        'cost_limitation': 'Whole transaction may include other actions. Energy burn-equivalent is not actual marginal liquidation cost; bandwidth, resource opportunity cost and failed attempts are additional.',
    })
out = {
    'window_start_utc_seconds': utc_ms(start),
    'window_end_utc_seconds': utc_ms(end),
    'window_start_ms': start, 'window_end_ms': end,
    'unique_events': len(events),
    'unique_transactions': len({row['transaction_id'] for row in events}),
    'gross_trx': gross, 'valuation_bid_usdt_per_trx': bid,
    'gross_current_bid_valuation_usdt': gross * bid,
    'seven_day_mean_gross_usdt': gross * bid / D('7'),
    'liquidator_addresses': [
        {'address': who, 'events': counts[who], 'reward_trx': rewards[who],
         'reward_fraction': rewards[who] / gross} for who in amount_sorted],
    'top_two_by_reward_amount_fraction': sum(rewards[who] for who in amount_sorted[:2]) / gross,
    'receipts': receipts, 'verified_payload_sha256': hashes,
    'limitations': 'Observed selected-contract rewards, not our income or marketwide capacity. Current bid valuation excludes trading fees. confirmed=true is provider evidence; block hashes and independent finality checks are not yet complete.',
}
(ROOT / 'keeper-verified.json').write_text(json.dumps(out, default=str, indent=2) + '\n')

# The additional 30-day query is independently checked against its two raw pages.
pages = sorted(P.glob('tron_energy_30d_page_*.raw.json'))
if pages:
    month_events, month_hashes = [], {}
    for index, file in enumerate(pages):
        page_meta = read(file.name.replace('.raw.', '.meta.'))
        digest = hashlib.sha256(file.read_bytes()).hexdigest()
        assert digest == page_meta['sha256']
        month_hashes[file.name] = digest
        page = read(file.name)
        assert page['success'] is True
        if index == len(pages) - 1:
            assert not page.get('meta', {}).get('links', {}).get('next')
        else:
            next_url = page['meta']['links']['next']
            assert next_url == read(pages[index + 1].name.replace('.raw.', '.meta.'))['url']
        month_events.extend(page['data'])
    month_start = end - 30 * 86400000
    assert all(month_start <= row['block_timestamp'] <= end for row in month_events)
    assert len({(row['transaction_id'], row['event_index']) for row in month_events}) == len(month_events)
    recent_ids = {(row['transaction_id'], row['event_index']) for row in month_events if row['block_timestamp'] >= start}
    assert recent_ids == event_keys
    day_rewards, address_rewards = defaultdict(lambda: D('0')), defaultdict(lambda: D('0'))
    for row in month_events:
        reward = D(row['result']['liquidateFee']) / SUN
        day_rewards[row['block_timestamp'] // 86400000] += reward
        address_rewards[row['result']['liquidator']] += reward
    month_gross = sum(address_rewards.values())
    assert month_gross == D('50064.663099')
    complete_days = list(range(month_start // 86400000 + 1, end // 86400000))
    full_day_values = sorted(day_rewards[day] * bid for day in complete_days)
    amount_sorted = sorted(address_rewards.values(), reverse=True)
    extra = {
        'events': len(month_events),
        'unique_transactions': len({row['transaction_id'] for row in month_events}),
        'gross_trx': month_gross,
        'gross_current_bid_usdt': month_gross * bid,
        'mean_gross_daily_usdt': month_gross * bid / D('30'),
        'full_utc_days': len(complete_days),
        'zero_event_full_utc_days': sum(day_rewards[day] == 0 for day in complete_days),
        'median_full_utc_day_gross_usdt': full_day_values[len(full_day_values) // 2],
        'full_utc_days_gross_below_200_usdt': sum(value < D('200') for value in full_day_values),
        'largest_day_reward_fraction': max(day_rewards.values()) / month_gross,
        'top_five_days_reward_fraction': sum(sorted(day_rewards.values(), reverse=True)[:5]) / month_gross,
        'top_two_addresses_reward_fraction': sum(amount_sorted[:2]) / month_gross,
        'other_addresses_mean_gross_daily_usdt': sum(amount_sorted[2:]) * bid / D('30'),
        'gross_share_needed_for_30_usdt_daily_at_month_mean': D('30') / (month_gross * bid / D('30')),
        'gross_share_needed_for_30_usdt_daily_at_week_mean': D('30') / (gross * bid / D('7')),
        'raw_page_sha256': month_hashes,
        'limitations': out['limitations'],
    }
    (ROOT / 'keeper-30d-verified.json').write_text(json.dumps(extra, default=str, indent=2) + '\n')
    print(json.dumps(extra, default=str, indent=2))
else:
    print(json.dumps(out, default=str, indent=2))
