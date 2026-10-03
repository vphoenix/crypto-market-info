"""Public read-only Ethereum deployment/parameter verification; sends no transactions."""
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
import requests

ROOT = Path(__file__).resolve().parent / 'contract-sources'
RPC = 'https://ethereum-rpc.publicnode.com'
SESSION = requests.Session()

def batch(items):
    payload = [dict(jsonrpc='2.0', id=i, method=m, params=p) for i, (m,p) in enumerate(items)]
    response = SESSION.post(RPC, json=payload, timeout=30)
    response.raise_for_status()
    results = {r['id']:r for r in response.json()}
    return [results[i] for i in range(len(items))]

chainlog = json.loads((ROOT / 'chainlog-active.json').read_text())
addresses = {
    'converter':chainlog['DAI_USDS'], 'psm':chainlog['MCD_LITE_PSM_USDC_A'],
    'wrapper':chainlog['WRAPPER_USDS_LITE_PSM_USDC_A'],
    'pocket':chainlog['MCD_LITE_PSM_USDC_A_POCKET'],
    'dai':chainlog['MCD_DAI'], 'usds':chainlog['USDS'],
    'usdc':'0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48',
    'daiJoin':chainlog['MCD_JOIN_DAI'], 'usdsJoin':chainlog['USDS_JOIN'],
    'vat':chainlog['MCD_VAT'],
}
signatures = ['daiJoin()','usdsJoin()','dai()','usds()','gem()','vat()','ilk()','pocket()',
 'to18ConversionFactor()','tin()','tout()','buf()','rush()','gush()','cut()','HALTED()',
 'psm()','dec()','live()','decimals()','balanceOf(address)','allowance(address,address)',
 'wards(address)','bud(address)','daiToUsds(address,uint256)','usdsToDai(address,uint256)',
 'sellGem(address,uint256)','buyGem(address,uint256)','fill()','trim()','chug()',
 'getPool(address,address,uint24)','slot0()','liquidity()','tickBitmap(int16)','ticks(int24)',
 'token0()','token1()','fee()','tickSpacing()',
 'DaiToUsds(address,address,uint256)','UsdsToDai(address,address,uint256)',
 'SellGem(address,uint256,uint256)','BuyGem(address,uint256,uint256)',
 'Fill(uint256)','Trim(uint256)','Chug(uint256)','File(bytes32,uint256)','File(bytes32,address)',
 'Kiss(address)','Diss(address)','Rely(address)','Deny(address)',
 'PoolCreated(address,address,uint24,int24,address)',
 'Initialize(uint160,int24)','Swap(address,address,int256,int256,uint160,uint128,int24)',
 'Mint(address,address,int24,int24,uint128,uint256,uint256)',
 'Burn(address,int24,int24,uint128,uint256,uint256)',
 'Collect(address,address,int24,int24,uint128,uint128)',
 'Flash(address,address,uint256,uint256,uint256,uint256)',
 'SetFeeProtocol(uint8,uint8,uint8,uint8)',
 'CollectProtocol(address,address,uint128,uint128)',
 'IncreaseObservationCardinalityNext(uint16,uint16)',
]
hashes = batch([('web3_sha3',['0x'+s.encode().hex()]) for s in signatures])
hash_map = {s:r['result'] for s,r in zip(signatures,hashes)}
(ROOT/'signatures.json').write_text(json.dumps(hash_map,indent=2)+'\n')
header = batch([('eth_chainId',[]),('eth_getBlockByNumber',['finalized',False])])
assert header[0]['result'] == '0x1'
block = header[1]['result']
anchor = {'blockHash':block['hash'],'requireCanonical':True}
calls = []
labels = []
for key in ['converter','psm','wrapper','daiJoin','usdsJoin','usds','usdc']:
    labels.append(key+'.code')
    calls.append(('eth_getCode',[addresses[key],anchor]))
groups = {
 'converter':['daiJoin()','usdsJoin()','dai()','usds()'],
 'psm':['daiJoin()','dai()','gem()','vat()','ilk()','pocket()','to18ConversionFactor()',
        'tin()','tout()','buf()','rush()','gush()','cut()','HALTED()'],
 'wrapper':['psm()','usdsJoin()','usds()','dai()','gem()','vat()','pocket()','dec()',
            'to18ConversionFactor()','tin()','tout()','buf()','live()'],
 'daiJoin':['live()'], 'usdsJoin':['live()'], 'vat':['live()'],
 'dai':['decimals()'], 'usds':['decimals()'], 'usdc':['decimals()'],
}
for key,sigs in groups.items():
    for sig in sigs:
        labels.append(key+'.'+sig)
        calls.append(('eth_call',[{'to':addresses[key],'data':hash_map[sig][:10]},anchor]))
for key,sig,args in [
 ('dai','balanceOf(address)',['psm']), ('usdc','balanceOf(address)',['pocket']),
 ('usdc','allowance(address,address)',['pocket','psm']),
 ('psm','bud(address)',['wrapper']), ('usds','wards(address)',['usdsJoin']),
 ('dai','wards(address)',['daiJoin'])]:
    data=hash_map[sig][:10]+''.join(addresses[a][2:].lower().rjust(64,'0') for a in args)
    labels.append(key+'.'+sig+':'+','.join(args))
    calls.append(('eth_call',[{'to':addresses[key],'data':data},anchor]))
results=batch(calls)
record={'observed_at_utc':datetime.now(timezone.utc).isoformat(),'rpc':RPC,
 'block':{k:block[k] for k in ['number','hash','timestamp']},'finality':'finalized',
 'addresses':addresses,'calls':[dict(label=l,method=c[0],params=c[1],response=r)
                             for l,c,r in zip(labels,calls,results)]}
(ROOT/'sky-snapshot.json').write_text(json.dumps(record,indent=2)+'\n')
print('block',int(block['number'],16),block['hash'])
for label,r in zip(labels,results):
    val=r.get('result')
    if label.endswith('.code'):
        print(label,'bytes',len(val[2:])//2 if val else 'ERROR')
    elif not val:
        print(label,r.get('error'))
    elif label.startswith(('psm.tin','psm.tout','psm.buf','psm.rush','dai.balance','usdc.balance',
                             'usdc.allowance','psm.bud','usds.wards','dai.wards')) or label.endswith(('live()','decimals()')):
        print(label,int(val,16))
manifest={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in ROOT.iterdir() if p.is_file() and p.name!='sha256.json'}
(ROOT/'sha256.json').write_text(json.dumps(manifest,indent=2)+'\n')
