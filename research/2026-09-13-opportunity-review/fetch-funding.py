"""Fetch public, read-only market history; preserve raw body and SHA256."""
import json,hashlib,datetime,urllib.request,urllib.parse,concurrent.futures
from pathlib import Path
ROOT=Path(__file__).resolve().parent
bases=['IOST','NEWT','RVN','ONE','ANIME','BZ','ONG','LSK','DEXE','ICX','BLUR']
rows=json.loads((ROOT/'soak-funding.json').read_text()); instruments={}
for r in rows:
 if r['canonical_base_asset'] in bases: instruments[(r['exchange'],r['exchange_symbol'])]=r

def run(key):
 venue,symbol=key
 existing=ROOT/('public-funding-'+venue.lower()+'-'+symbol+'.json')
 if existing.exists() and not json.loads(existing.read_text()).get('error'):return {'venue':venue,'symbol':symbol,'cached':True}
 if venue=='Binance':url='https://fapi.binance.com/fapi/v1/fundingRate?'+urllib.parse.urlencode({'symbol':symbol,'limit':1000})
 elif venue=='Bybit':url='https://api.bybit.com/v5/market/funding/history?'+urllib.parse.urlencode({'category':'linear','symbol':symbol,'limit':200})
 elif venue=='OKX':url='https://www.okx.com/api/v5/public/funding-rate-history?'+urllib.parse.urlencode({'instId':symbol,'limit':100})
 else:return None
 out={'venue':venue,'symbol':symbol,'url':url,'fetched_at':datetime.datetime.now(datetime.timezone.utc).isoformat()}
 try:
  with urllib.request.urlopen(urllib.request.Request(url,headers={'User-Agent':'crypto-market-info/1.0'}),timeout=30) as res:body=res.read()
  out['payload_sha256']=hashlib.sha256(body).hexdigest();out['data']=json.loads(body,parse_float=str)
 except Exception as e:out['error']=str(e)
 (ROOT/('public-funding-'+venue.lower()+'-'+symbol+'.json')).write_text(json.dumps(out,ensure_ascii=False,indent=2)+'\n')
 return {'venue':venue,'symbol':symbol,'error':out.get('error'),'size':len(str(out.get('data')))}
with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
 for r in ex.map(run,instruments):print(json.dumps(r),flush=True)
