import json,hashlib
from pathlib import Path
from urllib.request import Request,urlopen
from datetime import datetime,timezone
ROOT=Path(__file__).resolve().parent
if __name__=='__main__':
 for venue,url in [('OKX','https://www.okx.com/api/v5/public/funding-rate?instId=SOON-USDT-SWAP'),('Bybit','https://api.bybit.com/v5/market/tickers?category=linear&symbol=SOONUSDT')]:
  with urlopen(Request(url,headers={'User-Agent':'market-info-research/1.0'}),timeout=20) as r:raw=r.read()
  out={'venue':venue,'retrieved_at':datetime.now(timezone.utc).isoformat(),'url':url,'sha256':hashlib.sha256(raw).hexdigest(),'data':json.loads(raw,parse_float=str)}
  (ROOT/f'current-{venue}-SOON.json').write_text(json.dumps(out,indent=2)+'\n')
  print(json.dumps(out))
