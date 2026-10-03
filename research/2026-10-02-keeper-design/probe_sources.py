"""Bounded public, read-only endpoint checks for the keeper DATA design."""
import hashlib
import json
import time
import urllib.request
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parent
BASE = 'https://api.trongrid.io'
CONTRACT = 'TU2MJ5Veik1LRAgjeSzEdvmDYx7mefJZvd'


def fetch(name, url, body=None):
    start = datetime.now(timezone.utc).isoformat()
    request = urllib.request.Request(url, data=None if body is None else json.dumps(body).encode(),
                                     headers={'Content-Type': 'application/json'})
    try:
        with urllib.request.urlopen(request, timeout=25) as response:
            raw = response.read()
            status = response.status
    except urllib.error.HTTPError as error:
        raw, status = error.read(), error.code
    ROOT.joinpath(name + '.raw').write_bytes(raw)
    meta = {'url': url, 'request': body, 'started_at': start,
            'received_at': datetime.now(timezone.utc).isoformat(), 'http_status': status,
            'sha256': hashlib.sha256(raw).hexdigest()}
    ROOT.joinpath(name + '.meta.json').write_text(json.dumps(meta, indent=2) + '\n')
    print(name, status, len(raw), meta['sha256'])
    time.sleep(1)
    if status != 200:
        return {}
    return json.loads(raw, parse_float=Decimal)


if __name__ == '__main__':
    abi = fetch('docs-abi', 'https://docs.justlend.org/developers/abis/energy-market.json')
    contract = fetch('deployed-contract', BASE + '/wallet/getcontract', {'value': CONTRACT, 'visible': True})
    fetch('head-before', BASE + '/wallet/getnowblock', {})
    fetch('solid-head', BASE + '/walletsolidity/getnowblock', {})
    sample = json.loads((ROOT.parent / '2026-10-02-opportunity-debate/proposer/tron_energy_7d.raw.json').read_text())['data'][0]
    args = sample['result']
    owner = '41' + args['receiver'][2:]
    # addresses inside TVM ABI are 20-byte payloads; HTTP hex addresses have 41 prefix.
    parameter = args['renter'][2:].rjust(64, '0') + args['receiver'][2:].rjust(64, '0') + '1'.rjust(64, '0')
    # getcontract currently returns the 21-byte hex form even with visible=true.
    address = contract.get('contract_address', '')
    if address.startswith('T'):
        alphabet = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'
        number = 0
        for char in address:
            number = number * 58 + alphabet.index(char)
        decoded = number.to_bytes(25, 'big')
        assert hashlib.sha256(hashlib.sha256(decoded[:-4]).digest()).digest()[:4] == decoded[-4:]
        address = decoded[:-4].hex()
    if address.startswith('41') and len(address) == 42:
        for name, selector in [('rental', 'rentals(address,address,uint256)'),
                               ('rent-info', 'getRentInfo(address,address,uint256)'),
                               ('simulation', 'liquidate(address,address,uint256)')]:
            fetch(name, BASE + '/wallet/triggerconstantcontract',
                  {'owner_address': owner, 'contract_address': address,
                   'function_selector': selector, 'parameter': parameter, 'visible': False})
        fetch('implementation', BASE + '/wallet/triggerconstantcontract',
              {'owner_address': owner, 'contract_address': address,
               'function_selector': 'implementation()', 'parameter': '', 'visible': False})
    fetch('head-after', BASE + '/wallet/getnowblock', {})
    functions = [entry for entry in contract.get('abi', {}).get('entrys', [])
                 if entry.get('name') in ['rentals', 'getRentInfo', 'liquidate', '_liquidateRate',
                                           'RentResource', 'ReturnResource', 'Liquidate']]
    ROOT.joinpath('abi-comparison.json').write_text(json.dumps({
        'docs_selected': [entry for entry in abi if entry.get('name') in ['rentals', 'getRentInfo', 'liquidate']],
        'deployed_selected': functions,
        'contract_address': address,
        'bytecode_sha256': hashlib.sha256(bytes.fromhex(contract.get('bytecode', ''))).hexdigest(),
    }, indent=2) + '\n')
