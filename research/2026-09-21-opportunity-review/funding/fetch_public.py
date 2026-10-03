"""Verify selected local candidates against official public settlement endpoints."""
import json, concurrent.futures, hashlib
from urllib.request import Request,urlopen
from urllib.error import HTTPError
from datetime import datetime,timezone
from pathlib import Path
ROOT=Path(__file__).resolve().parent
def fetch(v,s):
    url=(f'https://fapi.binance.com/fapi/v1/fundingRate?symbol={s}USDT&limit=1000' if v=='Binance' else
         f'https://api.bybit.com/v5/market/funding/history?category=linear&symbol={s}USDT&limit=200' if v=='Bybit' else
         f'https://www.okx.com/api/v5/public/funding-rate-history?instId={s}-USDT-SWAP&limit=100')
    out={'venue':v,'symbol':s,'url':url,'retrieved_at':datetime.now(timezone.utc).isoformat()}
    try:
        with urlopen(Request(url,headers={'User-Agent':'market-info-research/1.0'}),timeout=25) as r:
            raw=r.read();out.update(status=r.status,sha256=hashlib.sha256(raw).hexdigest(),data=json.loads(raw,parse_float=str))
    except HTTPError as e:out.update(status=e.code,error=e.read().decode()[:1000])
    except Exception as e:out['error']=str(e)
    (ROOT/f'public-{v}-{s}.json').write_text(json.dumps(out,indent=2)+'\n')
    print(v,s,out.get('status'),out.get('error','')[:100],flush=True)
    return {k:v for k,v in out.items() if k!='data'}
if __name__=='__main__':
    jobs=[(v,s) for s in ['LSK','IOST','ONG','T','SOON'] for v in ['Binance','Bybit']]+[('OKX','IOST'),('OKX','SOON'),('OKX','ONE'),('Binance','ONE')]
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool: manifest=list(pool.map(lambda x:fetch(*x),jobs))
    (ROOT/'public-manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
