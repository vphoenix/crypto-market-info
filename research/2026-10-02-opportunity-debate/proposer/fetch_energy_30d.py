"""Read-only, bounded public-event capture and Decimal-only reward statistics."""
import hashlib
import json
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from statistics import median

OUT = Path(__file__).parent
DAY_MS = 86400000
meta7 = json.loads((OUT / 'tron_energy_7d.meta.json').read_text())
end_ms = meta7['window_end_ms']
start_ms = end_ms - 30 * DAY_MS
base = 'https://api.trongrid.io/v1/contracts/TU2MJ5Veik1LRAgjeSzEdvmDYx7mefJZvd/events'
url = base + '?' + urllib.parse.urlencode({
    'event_name': 'Liquidate', 'only_confirmed': 'true', 'limit': 200,
    'order_by': 'block_timestamp,desc', 'min_block_timestamp': start_ms,
    'max_block_timestamp': end_ms,
})
rows = []
seen_pages = set()
pages = []
complete = False
for page in range(1, 11):
    parsed = urllib.parse.urlsplit(url)
    if parsed.scheme != 'https' or parsed.netloc != 'api.trongrid.io':
        raise ValueError('Unexpected pagination destination')
    if url in seen_pages:
        raise ValueError('Pagination loop')
    seen_pages.add(url)
    with urllib.request.urlopen(url, timeout=25) as response:
        raw = response.read()
    data = json.loads(raw, parse_float=Decimal)
    if data.get('success') is False or not isinstance(data.get('data'), list):
        raise ValueError('Invalid source envelope')
    name = f'tron_energy_30d_page_{page:02d}'
    (OUT / f'{name}.raw.json').write_bytes(raw)
    meta = {
        'url': url, 'collected_at': datetime.now(timezone.utc).isoformat(),
        'window_start_ms': start_ms, 'window_end_ms': end_ms,
        'sha256': hashlib.sha256(raw).hexdigest(),
    }
    (OUT / f'{name}.meta.json').write_text(json.dumps(meta, indent=2))
    rows.extend(data['data'])
    next_url = data.get('meta', {}).get('links', {}).get('next')
    pages.append({'page': page, 'count': len(data['data']), 'has_next': bool(next_url)})
    if not next_url:
        complete = True
        break
    url = urllib.parse.urljoin(url, next_url)
if not complete:
    raise RuntimeError('Reached ten-page cap; raw capture retained but incomplete')

epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
as_utc = lambda ms: epoch + timedelta(milliseconds=ms)
start_dt, end_dt = as_utc(start_ms), as_utc(end_ms)
assert all(start_ms <= row['block_timestamp'] <= end_ms for row in rows)
event_keys = {(row['transaction_id'], row['event_index']) for row in rows}
assert len(rows) == len(event_keys), 'Duplicate source event'
bid = Decimal(json.loads((OUT / 'trx_bookticker.raw.json').read_text())['bidPrice'])
daily = defaultdict(lambda: {'count': 0, 'TRX': Decimal(0)})
addresses = defaultdict(lambda: {'count': 0, 'TRX': Decimal(0)})
for row in rows:
    if row['event_name'] != 'Liquidate' or row['contract_address'] != 'TU2MJ5Veik1LRAgjeSzEdvmDYx7mefJZvd':
        raise ValueError('Unexpected event identity')
    fee = Decimal(row['result']['liquidateFee']) / Decimal(1000000)
    day = as_utc(row['block_timestamp']).date().isoformat()
    address = row['result']['liquidator']
    daily[day]['count'] += 1
    daily[day]['TRX'] += fee
    addresses[address]['count'] += 1
    addresses[address]['TRX'] += fee
total = sum((v['TRX'] for v in daily.values()), Decimal(0))
calendar = []
dt = start_dt.replace(hour=0, minute=0, second=0, microsecond=0)
while dt <= end_dt:
    day_end = dt + timedelta(days=1)
    lo, hi = max(dt, start_dt), min(day_end, end_dt)
    value = daily[dt.date().isoformat()]
    coverage_ms = (hi - lo) // timedelta(milliseconds=1)
    calendar.append({
        'UTC_date': dt.date().isoformat(), 'coverage_ms': coverage_ms,
        'complete_UTC_day': coverage_ms == DAY_MS,
        'event_count': value['count'], 'gross_TRX': str(value['TRX']),
        'gross_USDT_at_reference_bid': str(value['TRX'] * bid),
        'share_of_total_rewards': str(value['TRX'] / total if total else Decimal(0)),
    })
    dt = day_end
ranking = sorted(addresses.items(), key=lambda kv: kv[1]['TRX'], reverse=True)
full_days = [v for v in calendar if v['complete_UTC_day']]
full_day_values = [Decimal(v['gross_USDT_at_reference_bid']) for v in full_days]
zero_full_days = [v['UTC_date'] for v in full_days if v['event_count'] == 0]
largest = max(calendar, key=lambda v: Decimal(v['gross_TRX']))
top_two_amount = sum((v['TRX'] for _, v in ranking[:2]), Decimal(0))
top_two_count = sum(v['count'] for _, v in ranking[:2])
seven = [v for v in rows if v['block_timestamp'] >= end_ms - 7 * DAY_MS]
seven_total = sum((Decimal(v['result']['liquidateFee']) / Decimal(1000000) for v in seven), Decimal(0))
result = {
    'complete_endpoint_pagination': complete, 'pages': pages,
    'window_start_UTC': start_dt.isoformat(), 'window_end_UTC': end_dt.isoformat(),
    'fixed_duration_days': '30', 'event_count': len(rows),
    'unique_transaction_count': len({v['transaction_id'] for v in rows}),
    'gross_rewards_TRX': str(total), 'reference_TRXUSDT_bid': str(bid),
    'gross_rewards_USDT_at_reference_bid': str(total * bid),
    'gross_USDT_per_fixed_day': str(total * bid / Decimal(30)),
    'full_UTC_day_count': len(full_days), 'zero_event_full_UTC_days': zero_full_days,
    'full_UTC_day_gross_USDT_min': str(min(full_day_values)),
    'full_UTC_day_gross_USDT_median': str(median(full_day_values)),
    'full_UTC_days_gross_below_200_USDT': sum(v < Decimal(200) for v in full_day_values),
    'top_five_full_UTC_days_share_of_all_rewards': str(sum(sorted(full_day_values, reverse=True)[:5]) / (total * bid)),
    'largest_UTC_day': largest,
    'top_two_addresses_by_reward_amount_share': str(top_two_amount / total),
    'top_two_addresses_by_reward_event_share': str(Decimal(top_two_count) / Decimal(len(rows))),
    'address_rewards': [{
        'liquidator_hex': a, 'events': v['count'], 'gross_TRX': str(v['TRX']),
        'gross_USDT_at_reference_bid': str(v['TRX'] * bid),
        'share_of_reward_amount': str(v['TRX'] / total),
    } for a, v in ranking],
    'nested_last_7d_events': len(seven), 'nested_last_7d_gross_TRX': str(seven_total),
    'other_three_addresses_gross_USDT_daily': str((total - top_two_amount) * bid / Decimal(30)),
    'required_gross_pool_share_for_30_USDT_daily_30d': str(Decimal(30) / (total * bid / Decimal(30))),
    'required_gross_pool_share_for_30_USDT_daily_7d': str(Decimal(30) / (seven_total * bid / Decimal(7))),
    'calendar_UTC': calendar,
    'limitations': [
        'Public confirmed endpoint, not independent block-hash finality validation.',
        'Current reference-bid revaluation, not historical realized USD proceeds.',
        'Gross rewards only; no resource, competition, failure, conversion, or operating costs deducted.',
        'First and last UTC calendar dates are partial; only 29 full UTC days in the rolling window.',
    ],
}
(OUT / 'tron_energy_30d_summary.json').write_text(json.dumps(result, indent=2))
compact = {k: v for k, v in result.items() if k not in ('calendar_UTC', 'limitations')}
print(json.dumps(compact, indent=2))
