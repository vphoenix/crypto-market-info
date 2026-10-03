"""Read public hourly lending FRR history back 90 days, preserving source bytes."""
import concurrent.futures, datetime, hashlib, json, urllib.request, time
from pathlib import Path
P=Path(__file__).resolve().parent
CUTOFF=int(datetime.datetime(2026,6,15,13,tzinfo=datetime.timezone.utc).timestamp())*1000
def fetch(asset):
    first=json.loads((P/('bitfinex-'+asset+'-history.json')).read_text())
    rows={r[0]:r for r in first['data']}
    manifest=[{k:first[k] for k in ('url','retrieved_at_utc','payload_sha256')}]
    for page in range(1,12):
        end=min(rows)-1
        if end<CUTOFF:break
        url=f'https://api-pub.bitfinex.com/v2/funding/stats/{asset}/hist?limit=250&end={end}'
        with urllib.request.urlopen(urllib.request.Request(url,headers={'User-Agent':'PublicYieldResearch/1.0'}),timeout=25) as response: raw=response.read()
        data=json.loads(raw,parse_float=str)
        if not isinstance(data,list) or not data:raise ValueError('Invalid funding history page')
        if min(r[0] for r in data)>=min(rows):raise ValueError('History pagination made no progress')
        name=f'bitfinex-{asset}-history-page{page}.raw.json'
        (P/name).write_bytes(raw)
        manifest.append({'url':url,'retrieved_at_utc':datetime.datetime.now(datetime.timezone.utc).isoformat(),'payload_sha256':hashlib.sha256(raw).hexdigest(),'raw_file':name})
        for r in data:
            if r[0] in rows and rows[r[0]]!=r:raise ValueError('Conflicting funding observation')
            rows[r[0]]=r
        time.sleep(0.3)
    out={'asset':asset,'start_cutoff_utc':'2026-06-15T13:00:00+00:00','sources':manifest,'data':[rows[k] for k in sorted(rows) if k>=CUTOFF]}
    (P/('bitfinex-'+asset+'-90d.json')).write_text(json.dumps(out,indent=2)+'\n')
    return {'asset':asset,'rows':len(out['data']),'pages':len(manifest),'first':out['data'][0][0],'last':out['data'][-1][0]}
with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:print(json.dumps(list(pool.map(fetch,('fUSD','fUST')))))
