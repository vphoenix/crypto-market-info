import json,datetime,urllib.request,urllib.parse,hashlib,concurrent.futures
from pathlib import Path
ROOT=Path(__file__).resolve().parent
urls=[]
for s in ['IOSTUSDT','ONGUSDT','LSKUSDT','BLURUSDT','NEWTUSDT']:
 urls.extend([('Binance',s,'https://fapi.binance.com/fapi/v1/premiumIndex?symbol='+s),('Bybit',s,'https://api.bybit.com/v5/market/tickers?category=linear&symbol='+s)])

def get(args):
 v,s,u=args;r={'venue':v,'symbol':s,'url':u,'fetched_at':datetime.datetime.now(datetime.timezone.utc).isoformat()}
 try:
  with urllib.request.urlopen(u,timeout=20) as res:b=res.read()
  r['payload_sha256']=hashlib.sha256(b).hexdigest();r['data']=json.loads(b,parse_float=str)
 except Exception as e:r['error']=str(e)
 return r
with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:out=list(ex.map(get,urls))
(ROOT/'funding-candidate-current.json').write_text(json.dumps(out,ensure_ascii=False,indent=2)+'\n')
print(json.dumps(out,ensure_ascii=False,indent=2))
