"""Read-only public-data research snapshots; never accesses account endpoints."""
import concurrent.futures, datetime as dt, hashlib, json, sys, urllib.request
from pathlib import Path

P = Path(__file__).resolve().parent
def get(name, url, sql=None):
    meta = {'name': name, 'url': url, 'retrieved_at_utc': dt.datetime.now(dt.timezone.utc).isoformat()}
    try:
        req = urllib.request.Request(url, data=sql.encode() if sql else None, headers={'User-Agent':'public-market-research/1.0'})
        with urllib.request.urlopen(req, timeout=55) as r: raw = r.read()
        (P/(name+'.raw')).write_bytes(raw)
        meta['sha256'] = hashlib.sha256(raw).hexdigest()
        data = [json.loads(x, parse_float=str) for x in raw.splitlines() if x] if sql else json.loads(raw, parse_float=str)
        (P/(name+'.json')).write_text(json.dumps(data, ensure_ascii=False, indent=2)+'\n')
        meta['rows'] = len(data) if isinstance(data,list) else None
    except Exception as e: meta['error'] = str(e)
    if sql: (P/(name+'.sql')).write_text(sql+'\n')
    (P/(name+'.meta.json')).write_text(json.dumps(meta, indent=2)+'\n')
    print(json.dumps(meta), flush=True)

if __name__ == '__main__':
    if sys.argv[1] == 'local':
        queries = {
          'local-funding': "SELECT i.exchange,i.exchange_symbol,i.base_asset,i.quote_asset,i.settle_asset,i.contract_multiplier,f.* FROM crypto_market_info_perp_soak.funding_rate_hourly AS f FINAL INNER JOIN crypto_market_info_perp_soak.instrument AS i FINAL USING(instrument_id) WHERE hour_time>=now()-INTERVAL 8 DAY ORDER BY instrument_id,hour_time",
          'local-yields': "SELECT r.provider,r.product_code,r.yield_type,r.deposit_asset_key,r.source_url,o.* FROM crypto_market_info.yield_route AS r FINAL INNER JOIN (SELECT * FROM crypto_market_info.yield_observation FINAL ORDER BY observation_time DESC LIMIT 1 BY yield_route_id,tier_no) o USING(yield_route_id)",
          'local-coverage': "SELECT instrument_id,min(minute_time) AS first,max(minute_time) AS last,count() AS minutes,sum(bitCount(valid_bitmap)) AS valid_seconds FROM crypto_market_info.order_book_minute FINAL GROUP BY instrument_id",
          'local-instruments': "SELECT * FROM crypto_market_info.instrument FINAL",
          'local-production-funding': "SELECT * FROM crypto_market_info.funding_rate_hourly FINAL ORDER BY instrument_id,hour_time"
        }
        for name, sql in queries.items():
            get(name, 'http://127.0.0.1:8123/?readonly=1&max_execution_time=45&max_threads=2', sql+' FORMAT JSONEachRow')
    elif sys.argv[1] == 'public':
        jobs = [
          ('binance-premium','https://fapi.binance.com/fapi/v1/premiumIndex'),
          ('binance-intervals','https://fapi.binance.com/fapi/v1/fundingInfo'),
          ('binance-info','https://fapi.binance.com/fapi/v1/exchangeInfo'),
          ('bybit-tickers','https://api.bybit.com/v5/market/tickers?category=linear'),
          ('okx-futures','https://www.okx.com/api/v5/market/tickers?instType=FUTURES'),
          ('okx-futures-info','https://www.okx.com/api/v5/public/instruments?instType=FUTURES'),
          ('okx-swaps-info','https://www.okx.com/api/v5/public/instruments?instType=SWAP'),
          ('okx-spot-tickers','https://www.okx.com/api/v5/market/tickers?instType=SPOT'),
          ('pendle-markets','https://api-v2.pendle.finance/core/v1/1/markets/active')
        ]
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as ex:
            list(ex.map(lambda j:get(*j),jobs))
