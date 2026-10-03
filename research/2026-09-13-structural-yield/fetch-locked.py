"""Public read-only structural-yield snapshots. Never uses account APIs."""
import json,datetime,urllib.request,hashlib,concurrent.futures,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parent

def fetch(name,url):
 r={'name':name,'url':url,'requested_at':datetime.datetime.now(datetime.timezone.utc).isoformat()}
 try:
  with urllib.request.urlopen(urllib.request.Request(url,headers={'User-Agent':'crypto-market-info-research/1.0'}),timeout=25) as res:b=res.read()
  r['received_at']=datetime.datetime.now(datetime.timezone.utc).isoformat();r['payload_sha256']=hashlib.sha256(b).hexdigest();r['data']=json.loads(b,parse_float=str)
 except Exception as e:r['error']=str(e)
 (ROOT/(name+'.json')).write_text(json.dumps(r,ensure_ascii=False,indent=2)+'\n')
 return {'name':name,'error':r.get('error'),'bytes':len(str(r.get('data')))}

if __name__=='__main__':
 phase=sys.argv[1] if len(sys.argv)>1 else 'discovery'
 if phase=='discovery':
  urls=[]
  for c in ['BTC','ETH','USDC']:
   for kind in ['future','option']:
    urls.append((f'deribit-{c}-{kind}-summaries',f'https://www.deribit.com/api/v2/public/get_book_summary_by_currency?currency={c}&kind={kind}'))
   urls.append((f'deribit-{c}-instruments',f'https://www.deribit.com/api/v2/public/get_instruments?currency={c}&expired=false'))
  urls.append(('okx-future-instruments','https://www.okx.com/api/v5/public/instruments?instType=FUTURES'))
  for c in ['BTC','ETH','SOL']:
   urls.append((f'binance-{c}-spot','https://api.binance.com/api/v3/depth?symbol='+c+'USDC&limit=100'))
 else:
  urls=json.loads((ROOT/(phase+'-requests.json')).read_text())
 with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
  for r in ex.map(lambda x:fetch(*x),urls):print(json.dumps(r),flush=True)
