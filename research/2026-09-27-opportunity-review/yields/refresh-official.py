from pathlib import Path
from urllib.request import Request,urlopen
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime,timezone
from decimal import Decimal
import json,hashlib
p=Path(__file__).resolve().parent
old=Path('research/2026-09-21-threshold-review')
aave=json.loads((old/'aave-current-meta.json').read_text())
quote=json.loads((old/'pendle_susds_million_usdt_quote.meta.json').read_text())
market=json.loads((old/'pendle_susds_metadata.meta.json').read_text())
queries=[('aave-current',aave['endpoint'],json.dumps({'query':aave['query']}).encode()),('pendle-susds-100k-quote',quote['url'].replace('amountsIn=1000000000000','amountsIn=100000000000'),None),('pendle-susds-metadata',market['url'],None)]
def run(q):
 name,url,body=q
 now=datetime.now(timezone.utc).isoformat()
 request=Request(url,data=body,headers={'Content-Type':'application/json','Accept':'application/json','User-Agent':'Mozilla/5.0'})
 try:
  with urlopen(request,timeout=45) as response:
   raw=response.read();status=response.status
  (p/(name+'.raw.json')).write_bytes(raw)
  meta={'collected_at_utc':now,'url':url,'http_status':status,'sha256':hashlib.sha256(raw).hexdigest(),'body':json.loads(body) if body else None,'read_only':'Public API query; no private account, no signing or broadcasting'}
  (p/(name+'.meta.json')).write_text(json.dumps(meta,indent=2)+'\n')
  print(name,status,len(raw))
 except Exception as e: print(name,type(e).__name__,str(e))
with ThreadPoolExecutor(max_workers=3) as pool: list(pool.map(run,queries))
