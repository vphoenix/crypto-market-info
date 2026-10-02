"""One-off official public verification of locally found ATOM/DOT candidates.
No accounts, credentials, trades, database mutations or collector changes.
"""
import concurrent.futures,hashlib,json
from pathlib import Path
from datetime import datetime,timezone
from urllib.request import Request,urlopen
from urllib.error import HTTPError
from urllib.parse import urlencode
P=Path(__file__).resolve().parent
def get(name,url):
    start=datetime.now(timezone.utc).isoformat();status=None;error=None
    try:
        with urlopen(Request(url,headers={'User-Agent':'crypto-market-info-read-only-research/1.0'}),timeout=25) as r:
            status=r.status;raw=r.read()
    except HTTPError as e:
        status=e.code;raw=e.read();error=str(e)
    except Exception as e:
        raw=b'';error=str(e)
    (P/(name+'.raw.json')).write_bytes(raw)
    meta={'url':url,'requested_at_utc':start,'received_at_utc':datetime.now(timezone.utc).isoformat(),'http_status':status,'error':error,'sha256':hashlib.sha256(raw).hexdigest()}
    (P/(name+'.meta.json')).write_text(json.dumps(meta,indent=2)+'\n')
    print(name,status,error,flush=True)
    return meta
if __name__=='__main__':
    urls=[]
    for asset in ('ATOM','DOT'):
        bybit='https://api.bybit.com';okx='https://www.okx.com';bs=asset+'USDT';os=asset+'-USDT-SWAP'
        urls.extend([
            ('public-bybit-'+asset+'-funding',bybit+'/v5/market/funding/history?'+urlencode({'category':'linear','symbol':bs,'limit':'200'})),
            ('public-okx-'+asset+'-funding',okx+'/api/v5/public/funding-rate-history?'+urlencode({'instId':os,'limit':'100'})),
            ('public-bybit-'+asset+'-book',bybit+'/v5/market/orderbook?'+urlencode({'category':'linear','symbol':bs,'limit':'10'})),
            ('public-okx-'+asset+'-book',okx+'/api/v5/market/books?'+urlencode({'instId':os,'sz':'10'})),
            ('public-bybit-'+asset+'-instrument',bybit+'/v5/market/instruments-info?'+urlencode({'category':'linear','symbol':bs})),
            ('public-okx-'+asset+'-instrument',okx+'/api/v5/public/instruments?'+urlencode({'instType':'SWAP','instId':os})),
        ])
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        res=list(pool.map(lambda x:get(*x),urls))
    (P/'public-request-summary.json').write_text(json.dumps(res,indent=2)+'\n')
