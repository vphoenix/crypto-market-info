from pathlib import Path
from urllib.request import Request,urlopen
from datetime import datetime,timezone
from concurrent.futures import ThreadPoolExecutor
import json,hashlib
p=Path(__file__).resolve().parent
old=json.loads((p/'pendle-susds-999k-quote.meta.json').read_text())['url']
queries=[('pendle-susds-999k-slippage1bp',old.replace('slippage=0.001&','slippage=0.0001&')),('pendle-susds-999k-slippage3bp',old.replace('slippage=0.001&','slippage=0.0003&'))]
def run(a):
 name,url=a;meta={'collected_at_utc':datetime.now(timezone.utc).isoformat(),'url':url,'read_only':True}
 try:
  with urlopen(Request(url,headers={'Accept':'application/json','User-Agent':'Mozilla/5.0'}),timeout=45) as r:raw=r.read();meta['http_status']=r.status
  (p/(name+'.raw')).write_bytes(raw);meta['sha256']=hashlib.sha256(raw).hexdigest();print(name,r.status,len(raw))
 except Exception as e:meta['error']=str(e);print(name,type(e).__name__,str(e))
 (p/(name+'.meta.json')).write_text(json.dumps(meta,indent=2)+'\n')
with ThreadPoolExecutor(max_workers=2) as pool:list(pool.map(run,queries))
