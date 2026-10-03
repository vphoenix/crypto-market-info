"""Read-only official Pendle market/quote probe. Never signs or broadcasts."""
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import sys
import urllib.error
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parent

def fetch(path, name):
    url = 'https://api-v2.pendle.finance/core' + path
    now = datetime.now(timezone.utc).isoformat()
    request = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json'})
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            raw = response.read()
            status = response.status
            headers = {key: response.headers.get(key) for key in ['Date', 'Content-Type', 'Last-Modified', 'Age']}
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        status = exc.code
        headers = {}
    (ROOT / (name + '.raw.json')).write_bytes(raw)
    meta = {'url': url, 'observed_at_utc': now, 'http_status': status, 'sha256': hashlib.sha256(raw).hexdigest(), 'headers': headers}
    (ROOT / (name + '.meta.json')).write_text(json.dumps(meta, indent=2) + '\n')
    print(json.dumps(meta))
    try:
        return json.loads(raw, parse_float=Decimal)
    except json.JSONDecodeError:
        return None

if __name__ == '__main__':
    path, name = sys.argv[1:]
    data = fetch(path, name)
    if data and ('markets' in data or 'results' in data):
        markets = data.get('markets', data.get('results', []))
        print('market_count', len(markets))
        for market in markets:
            label = json.dumps({k: v for k, v in market.items() if k in ['name', 'pt', 'underlyingAsset', 'proSymbol', 'simpleName']}, default=str)
            if market.get('name') in ['sUSDS', 'sUSDe', 'aUSDC', 'aUSDT'] and market.get('expiry', '') > datetime.now(timezone.utc).isoformat():
                print(json.dumps(market, default=str))
    else:
        print(json.dumps(data, default=str)[:6000])
