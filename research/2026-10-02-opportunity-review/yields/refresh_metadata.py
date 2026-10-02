from urllib.request import Request,urlopen
from pathlib import Path
from decimal import Decimal
from datetime import datetime,timezone
import hashlib,json
p=Path(__file__).resolve().parent
url='https://api-v2.pendle.finance/core/v2/markets/all?limit=25&skip=300'
meta={'collected_at_utc':datetime.now(timezone.utc).isoformat(),'url':url,'read_only':True}
with urlopen(Request(url,headers={'Accept':'application/json','User-Agent':'Mozilla/5.0'}),timeout=40) as r:
 raw=r.read();meta['http_status']=r.status
(p/'pendle-current-metadata.raw').write_bytes(raw);meta['sha256']=hashlib.sha256(raw).hexdigest()
(p/'pendle-current-metadata.meta.json').write_text(json.dumps(meta,indent=2)+'\n')
x=json.loads(raw,parse_float=Decimal)
target=[a for a in x['results'] if a['address'].lower()=='0x9c560ebaf78e596cbcc27411d633a74d628dd7dc']
assert len(target)==1,'Target not found: pagination changed'
(p/'pendle-current-susds-market.json').write_text(json.dumps(target[0],default=str,indent=2)+'\n')
print(json.dumps({k:target[0].get(k) for k in ['address','name','expiry','pt','sy','underlyingAsset','accountingAsset','details','marketInfo']},default=str))
