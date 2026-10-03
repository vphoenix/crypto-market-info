"""Public-only Binance funding history, archival research snapshot."""
import hashlib,json,urllib.request,urllib.parse
from pathlib import Path
from datetime import datetime,timezone

P=Path(__file__).resolve().parent
manifest=[]
for symbol in ('BTCUSDT','SOLUSDT','AVAXUSDT','TRXUSDT'):
    params=urllib.parse.urlencode({'symbol':symbol,'startTime':int(datetime(2026,8,13,tzinfo=timezone.utc).timestamp())*1000,'endTime':int(datetime(2026,9,12,19,tzinfo=timezone.utc).timestamp())*1000,'limit':1000})
    url='https://fapi.binance.com/fapi/v1/fundingRate?'+params
    try:
        with urllib.request.urlopen(url,timeout=25) as res: payload=res.read()
        rows=json.loads(payload,parse_float=str)
        if not isinstance(rows,list) or any(r['symbol']!=symbol for r in rows): raise ValueError('Unexpected source identity')
        dest=P/('public-funding-'+symbol+'.json')
        dest.write_bytes(payload)
        manifest.append({'url':url,'retrieved_at_utc':datetime.now(timezone.utc).isoformat(),'sha256':hashlib.sha256(payload).hexdigest(),'file':dest.name,'rows':len(rows)})
    except Exception as exc:
        manifest.append({'url':url,'error':str(exc)})
(P/'public-funding-manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
print(json.dumps(manifest))
