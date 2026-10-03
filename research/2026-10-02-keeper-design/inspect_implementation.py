"""Additional public getters and a non-owner simulation; no signing/broadcast."""
import json
from probe_sources import ROOT, BASE, CONTRACT, fetch

if __name__ == '__main__':
    events = json.loads((ROOT.parent / '2026-10-02-opportunity-debate/proposer/tron_energy_7d.raw.json').read_text())['data']
    args = events[0]['result']
    others = [row['result']['receiver'] for row in events
              if row['result']['receiver'] not in [args['renter'], args['receiver']]]
    owner = '41' + others[0][2:]
    implementation = json.loads((ROOT / 'implementation.raw').read_text())['constant_result'][0]
    impl = '41' + implementation[-40:]
    fetch('implementation-contract', BASE + '/wallet/getcontract', {'value': impl, 'visible': False})
    fetch('simulation-caller-account', BASE + '/wallet/getaccount', {'address': owner, 'visible': False})
    fetch('light-head-before', BASE + '/wallet/getblock', {'detail': False})
    parameter = args['renter'][2:].rjust(64, '0') + args['receiver'][2:].rjust(64, '0') + '1'.rjust(64, '0')
    fetch('non-owner-simulation', BASE + '/wallet/triggerconstantcontract', {
        'owner_address': owner, 'contract_address': '41c60a6f5c81431c97ed01b61698b6853557f3afd4',
        'function_selector': 'liquidate(address,address,uint256)', 'parameter': parameter, 'visible': False})
    fetch('light-head-after', BASE + '/wallet/getblock', {'detail': False})
    fetch('light-solid-head', BASE + '/walletsolidity/getblock', {'detail': False})
