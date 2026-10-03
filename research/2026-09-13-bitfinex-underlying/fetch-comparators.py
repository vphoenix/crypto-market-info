"""Archive unauthenticated public comparison sources; never access accounts."""
import concurrent.futures, datetime, hashlib, json, urllib.request
from pathlib import Path
P=Path(__file__).resolve().parent
sources={
 'okx-usdt-summary':'https://www.okx.com/api/v5/finance/savings/lending-rate-summary?ccy=USDT',
 'okx-usdt-history':'https://www.okx.com/api/v5/finance/savings/lending-rate-history?ccy=USDT&limit=100',
 'okx-usdc-summary':'https://www.okx.com/api/v5/finance/savings/lending-rate-summary?ccy=USDC',
 'okx-usdc-history':'https://www.okx.com/api/v5/finance/savings/lending-rate-history?ccy=USDC&limit=100',
 'okx-simple-earn-rules':'https://www.okx.com/en-gb/help/introduction-to-okx-simple-earn-flexible',
 'binance-simple-earn-rules':'https://www.binance.com/en/support/faq/detail/3bd1a6eba20a445da1e94bf6cfa52e80',
 'aave-supply-rules':'https://aave.com/help/supplying/supply-tokens',
 'okx-api-docs':'https://www.okx.com/docs-v5/en/',
}
def get(item):
    key,url=item
    out={'url':url,'retrieved_at_utc':datetime.datetime.now(datetime.timezone.utc).isoformat()}
    try:
        req=urllib.request.Request(url,headers={'User-Agent':'PublicMarketResearch/1.0'})
        with urllib.request.urlopen(req,timeout=35) as r:
            body=r.read();out['http_status']=r.status
        out['sha256']=hashlib.sha256(body).hexdigest()
        suffix='.raw.json' if '/api/' in url else '.html'
        (P/(key+suffix)).write_bytes(body)
        out['raw_file']=key+suffix
        if suffix=='.raw.json':out['data']=json.loads(body,parse_float=str)
    except Exception as e:out['error']=str(e)
    (P/(key+'.json')).write_text(json.dumps(out,ensure_ascii=False,indent=2)+'\n')
    return {'key':key,**{k:v for k,v in out.items() if k!='data'}}
with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
    print(json.dumps(list(pool.map(get,sources.items())),ensure_ascii=False))
