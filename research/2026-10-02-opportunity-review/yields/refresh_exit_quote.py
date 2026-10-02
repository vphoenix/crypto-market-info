from pathlib import Path
from urllib.request import Request,urlopen
from urllib.error import HTTPError
from datetime import datetime,timezone
import json,hashlib
p=Path(__file__).resolve().parent
url='https://api-v2.pendle.finance/core/v2/sdk/1/convert?receiver=0x000000000000000000000000000000000000dEaD&slippage=0.0001&tokensIn=0xdc035d45d973e3ec169d2276ddab16f1e407384f&tokensOut=0xdac17f958d2ee523a2206206994597c13d831ec7&amountsIn=1005500000000000000000000&enableAggregator=true'
meta={'collected_at_utc':datetime.now(timezone.utc).isoformat(),'url':url,'read_only':True,'role':'current_exit_reference_only_not_future_exit_guarantee'}
try:
 with urlopen(Request(url,headers={'Accept':'application/json','User-Agent':'Mozilla/5.0'}),timeout=45) as r:raw=r.read();meta['http_status']=r.status
 (p/'pendle-usds-usdt-exit.raw').write_bytes(raw);meta['sha256']=hashlib.sha256(raw).hexdigest();print(r.status,len(raw));print(raw.decode()[:120])
except HTTPError as e:
 raw=e.read();(p/'pendle-usds-usdt-exit.error').write_bytes(raw);meta.update({'http_status':e.code,'sha256':hashlib.sha256(raw).hexdigest(),'error':str(e)});print(e.code,raw.decode()[:500])
except Exception as e:meta['error']=str(e);print(type(e).__name__,str(e))
(p/'pendle-usds-usdt-exit.meta.json').write_text(json.dumps(meta,indent=2)+'\n')
