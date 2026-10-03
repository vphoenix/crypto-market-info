#!/usr/bin/env python3
"""Fetch three official public OKX funding histories; no credentials or writes."""
from pathlib import Path
from datetime import datetime, timezone
import hashlib
import json
import urllib.request

root=Path(__file__).resolve().parent
manifest=[]
for symbol in ['SOL','AVAX','TRX']:
    url=f'https://www.okx.com/api/v5/public/funding-rate-history?instId={symbol}-USDT-SWAP&limit=100'
    now=datetime.now(timezone.utc).isoformat()
    with urllib.request.urlopen(urllib.request.Request(url,headers={'User-Agent':'crypto-market-info-public-research'}),timeout=40) as response:
        raw=response.read()
    obj=json.loads(raw)
    assert obj['code']=='0' and len(obj['data'])>0
    fn=f'okx-{symbol.lower()}-funding.raw.json'
    (root/fn).write_bytes(raw)
    manifest.append({'url':url,'requested_at_utc':now,'sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw),'file':fn})
    print(symbol,'rows',len(obj['data']))
(root/'public-manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
