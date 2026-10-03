#!/usr/bin/env python3
"""Read-only public OKX books/specs for the 1m USDT AVAX scenario."""
from datetime import datetime, timezone
from pathlib import Path
import hashlib,json,urllib.request
ROOT=Path(__file__).resolve().parent
jobs=[('spot-book','https://www.okx.com/api/v5/market/books?instId=AVAX-USDT&sz=400'),
      ('swap-book','https://www.okx.com/api/v5/market/books?instId=AVAX-USDT-SWAP&sz=400'),
      ('spot-spec','https://www.okx.com/api/v5/public/instruments?instType=SPOT&instId=AVAX-USDT'),
      ('swap-spec','https://www.okx.com/api/v5/public/instruments?instType=SWAP&instId=AVAX-USDT-SWAP')]
manifest=[]
for name,url in jobs:
    requested=datetime.now(timezone.utc).isoformat()
    with urllib.request.urlopen(urllib.request.Request(url,headers={'User-Agent':'crypto-market-info-public-research'}),timeout=40) as response:raw=response.read()
    obj=json.loads(raw)
    assert obj['code']=='0' and len(obj['data'])==1
    file='million-'+name+'.raw.json'
    (ROOT/file).write_bytes(raw)
    manifest.append({'url':url,'requested_utc':requested,'received_utc':datetime.now(timezone.utc).isoformat(),'file':file,'sha256':hashlib.sha256(raw).hexdigest()})
    print(name,obj['data'][0].get('ts'),len(obj['data'][0].get('asks',[])),len(obj['data'][0].get('bids',[])))
(ROOT/'million-manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
