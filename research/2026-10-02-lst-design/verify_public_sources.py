"""Design-time public source checks; not a collector or a trading program."""
import hashlib
import json
import time
import urllib.request
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parent
SOURCES = {
    'lido-withdrawal-queue': 'https://raw.githubusercontent.com/lidofinance/core/v4.0.1/contracts/0.8.9/WithdrawalQueue.sol',
    'lido-withdrawal-base': 'https://raw.githubusercontent.com/lidofinance/core/v4.0.1/contracts/0.8.9/WithdrawalQueueBase.sol',
    'lido-withdrawal-erc721': 'https://raw.githubusercontent.com/lidofinance/core/v4.0.1/contracts/0.8.9/WithdrawalQueueERC721.sol',
    'uniswap-quoter-v2': 'https://raw.githubusercontent.com/Uniswap/v3-periphery/0682387198a24c7cd63566a2c58398533860a5d1/contracts/lens/QuoterV2.sol',
    'binance-eth-depth': 'https://fapi.binance.com/fapi/v1/depth?symbol=ETHUSDT&limit=10',
    'binance-eth-mark': 'https://fapi.binance.com/fapi/v1/premiumIndex?symbol=ETHUSDT',
    'binance-eth-funding': 'https://fapi.binance.com/fapi/v1/fundingRate?symbol=ETHUSDT&limit=5',
    'binance-exchange-info': 'https://fapi.binance.com/fapi/v1/exchangeInfo',
}
manifest = []
last_started_by_host = {}
for name, url in SOURCES.items():
    host = urlsplit(url).hostname
    wait = last_started_by_host.get(host, 0) + 5 - time.monotonic()
    if wait > 0:
        time.sleep(wait)
    last_started_by_host[host] = time.monotonic()
    item = {'name': name, 'url': url, 'requested_at': datetime.now(timezone.utc).isoformat()}
    abort = False
    try:
        request = urllib.request.Request(url, headers={'User-Agent': 'crypto-market-info-design-check/1.0'})
        with urllib.request.urlopen(request, timeout=15) as response:
            raw = response.read(4 * 1024 * 1024 + 1)
            assert len(raw) <= 4 * 1024 * 1024
            item['http_status'] = response.status
        item['received_at'] = datetime.now(timezone.utc).isoformat()
        item['sha256'] = hashlib.sha256(raw).hexdigest()
        suffix = 'sol' if name.startswith(('lido-', 'uniswap-')) else 'json'
        filename = name + '.raw.' + suffix
        (ROOT / filename).write_bytes(raw)
        item['file'] = filename
        if suffix == 'json':
            value = json.loads(raw, parse_float=Decimal)
            if name.endswith('depth'):
                item['fields_checked'] = {key: value[key] for key in ['E', 'T', 'lastUpdateId']}
                item['bid_count'], item['ask_count'] = len(value['bids']), len(value['asks'])
                assert all(isinstance(v, str) for sides in ['bids', 'asks'] for row in value[sides] for v in row)
            elif name.endswith('mark'):
                item['fields_checked'] = {key: value[key] for key in ['symbol', 'markPrice', 'indexPrice', 'lastFundingRate', 'nextFundingTime', 'time']}
            elif name.endswith('funding'):
                assert value and all(row['symbol'] == 'ETHUSDT' for row in value)
                item['rows'], item['row_fields'] = len(value), sorted(value[0])
                item['mark_present_all_rows'] = all(row.get('markPrice') for row in value)
            elif name.endswith('exchange-info'):
                eth = [row for row in value['symbols'] if row['symbol'] == 'ETHUSDT']
                assert len(eth) == 1
                item['fields_checked'] = {key: eth[0][key] for key in ['symbol', 'contractType', 'status', 'onboardDate', 'baseAsset', 'quoteAsset', 'marginAsset']}
                item['filters'] = [row for row in eth[0]['filters'] if row['filterType'] in ['PRICE_FILTER', 'LOT_SIZE', 'MARKET_LOT_SIZE', 'MIN_NOTIONAL']]
        item['status'] = 'ok'
    except Exception as error:
        item['status'], item['error_type'], item['error'] = 'failed', type(error).__name__, str(error)
        if isinstance(error, HTTPError):
            item['http_status'] = error.code
            abort = error.code in (403, 418, 429)
    manifest.append(item)
    print(json.dumps({'name': name, 'status': item['status'], 'sha256': item.get('sha256'), 'error': item.get('error')}, ensure_ascii=False))
    (ROOT / 'source-manifest.json').write_text(json.dumps(manifest, indent=2, default=str) + '\n')
    if abort:
        raise SystemExit('Source refused or throttled requests; stopping without retries.')
