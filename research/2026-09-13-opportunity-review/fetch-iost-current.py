import json,datetime,urllib.request,hashlib,concurrent.futures
from pathlib import Path
ROOT=Path(__file__).resolve().parent
urls=[('Bybit-ticker','https://api.bybit.com/v5/market/tickers?category=linear&symbol=IOSTUSDT'),('OKX-funding','https://www.okx.com/api/v5/public/funding-rate?instId=IOST-USDT-SWAP'),('OKX-mark','https://www.okx.com/api/v5/public/mark-price?instType=SWAP&instId=IOST-USDT-SWAP')]
def get(a):
 n,u=a;r={'name':n,'url':u,'fetched_at':datetime.datetime.now(datetime.timezone.utc).isoformat()}
 try:
  with urllib.request.urlopen(urllib.request.Request(u,headers={'User-Agent':'crypto-market-info/1.0'}),timeout=20) as res:b=res.read()
  r['payload_sha256']=hashlib.sha256(b).hexdigest();r['data']=json.loads(b,parse_float=str)
 except Exception as e:r['error']=str(e)
 return r
with concurrent.futures.ThreadPoolExecutor(max_workers=3) as ex:out=list(ex.map(get,urls))
(ROOT/'iost-current.json').write_text(json.dumps(out,ensure_ascii=False,indent=2)+'\n');print(json.dumps(out,ensure_ascii=False))
