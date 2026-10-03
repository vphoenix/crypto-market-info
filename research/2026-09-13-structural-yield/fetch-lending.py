"""Archive public Bitfinex lending market data; no accounts or mutations."""
import concurrent.futures, datetime, hashlib, json, urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent
def fetch(item):
    name, url = item
    stamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
    result = {'url': url, 'retrieved_at_utc': stamp}
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers={'User-Agent':'PublicYieldResearch/1.0'}), timeout=25) as response:
            raw = response.read()
        result.update(payload_sha256=hashlib.sha256(raw).hexdigest(), data=json.loads(raw, parse_float=str))
        (ROOT/(name+'.raw.json')).write_bytes(raw)
    except Exception as error:
        result['error'] = str(error)
    (ROOT/(name+'.json')).write_text(json.dumps(result, indent=2)+'\n')
    return {'name':name, 'url':url, 'error':result.get('error'), 'rows':len(result.get('data',[]))}

requests=[]
for asset in ('fUST','fUSD'):
    for endpoint, path in [('ticker','ticker/'+asset),('book','book/'+asset+'/P0?len=100'),('history','funding/stats/'+asset+'/hist?limit=250')]:
        requests.append(('bitfinex-'+asset+'-'+endpoint, 'https://api-pub.bitfinex.com/v2/'+path))
with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
    print(json.dumps(list(pool.map(fetch, requests))))
