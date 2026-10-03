"""Two unsigned SDK simulations using a dummy receiver; never broadcast."""
import urllib.request,urllib.error,json,hashlib,datetime
from pathlib import Path
P=Path(__file__).resolve().parent
URL='https://api-v2.pendle.finance/core/v3/sdk/143/convert'
for amount in [100000,1000000]:
    body=('''{"receiver":"0x0000000000000000000000000000000000000001","slippage":0.001,"enableAggregator":false,"inputs":[{"token":"0x00000000efe302beaa2b3e6e1b18d08d69a9012a","amount":"AMOUNT"}],"outputs":["0x9fc74f8ed616b5baf52a170caa97d6d3898602d1"],"additionalData":"impliedApy,effectiveApy","useLimitOrder":true}''').replace('AMOUNT',str(amount*1000000)).encode()
    meta={'url':URL,'amount_ausd':amount,'retrieved_at_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'request':json.loads(body,parse_float=str),'unsigned_only':True}
    try:
        req=urllib.request.Request(URL,data=body,headers={'Content-Type':'application/json'})
        with urllib.request.urlopen(req,timeout=40) as r:raw=r.read()
        (P/f'pendle-quote-{amount}.raw').write_bytes(raw)
        meta['sha256']=hashlib.sha256(raw).hexdigest()
        data=json.loads(raw,parse_float=str)
        (P/f'pendle-quote-{amount}.json').write_text(json.dumps(data,indent=2)+'\n')
        print('success',amount, list(data))
    except urllib.error.HTTPError as e:
        meta['error']=str(e);meta['error_body']=e.read().decode()[:3000];print(meta)
    except Exception as e:meta['error']=str(e);print(meta)
    (P/f'pendle-quote-{amount}.meta.json').write_text(json.dumps(meta,indent=2)+'\n')
