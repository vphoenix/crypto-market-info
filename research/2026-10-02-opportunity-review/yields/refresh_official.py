from pathlib import Path
from urllib.request import Request,urlopen
from urllib.error import HTTPError
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime,timezone
from decimal import Decimal
import json,hashlib
p=Path(__file__).resolve().parent
old=Path('research/2026-09-27-opportunity-review/yields')
quote=json.loads((old/'pendle-susds-100k-quote.meta.json').read_text())['url'].replace('amountsIn=100000000000&','amountsIn=1000000000000&')
queries=[('pendle-susds-million-quote',quote,None),('pendle-market', 'https://api-v2.pendle.finance/core/v2/1/markets/0x9c560ebaf78e596cbcc27411d633a74d628dd7dc',None),('justlend-strx-docs','https://docs.justlend.org/getting_started/concepts/staked_trx/',None),('benqi-staking-docs','https://docs.benqi.fi/benqi-liquid-staking/getting-started',None)]
def run(q):
 name,url,body=q
 meta={'collected_at_utc':datetime.now(timezone.utc).isoformat(),'url':url,'read_only':'Public API/docs; no signing, account access or broadcasting'}
 try:
  with urlopen(Request(url,data=body,headers={'Accept':'application/json','User-Agent':'Mozilla/5.0'}),timeout=45) as response:
   raw=response.read(); meta['http_status']=response.status
  (p/(name+'.raw')).write_bytes(raw)
  meta['sha256']=hashlib.sha256(raw).hexdigest()
  print(name,meta['http_status'],len(raw))
 except HTTPError as e:
  raw=e.read(); (p/(name+'.error-response')).write_bytes(raw); meta.update({'http_status':e.code,'error':str(e),'sha256':hashlib.sha256(raw).hexdigest()}); print(name,e.code)
 except Exception as e:meta['error']=str(e); print(name,type(e).__name__,str(e))
 (p/(name+'.meta.json')).write_text(json.dumps(meta,indent=2)+'\n')
with ThreadPoolExecutor(max_workers=4) as pool:list(pool.map(run,queries))
