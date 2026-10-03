"""Archival public histories and executable book capacity, no account access."""
from snapshot import get
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime,timezone,timedelta
import urllib.parse
now=datetime.now(timezone.utc); start=now-timedelta(days=31)
start_ms=int(start.timestamp())*1000;end_ms=int(now.timestamp())*1000
jobs=[]
for s in ['BTC','ETH','SOL','TRX','AVAX','CVC','MTL','LSK','IOST','STEEM','HIVE','ONG']:
    symbol=s+'USDT'
    jobs.append(('history-binance-'+s,'https://fapi.binance.com/fapi/v1/fundingRate?'+urllib.parse.urlencode({'symbol':symbol,'startTime':start_ms,'endTime':end_ms,'limit':1000})))
    if s in ['CVC','MTL','LSK','IOST','STEEM','HIVE','ONG']:
        jobs.append(('history-bybit-'+s,'https://api.bybit.com/v5/market/funding/history?category=linear&limit=200&symbol='+symbol))
        jobs.append(('depth-binance-'+s,'https://fapi.binance.com/fapi/v1/depth?limit=100&symbol='+symbol))
        jobs.append(('depth-bybit-'+s,'https://api.bybit.com/v5/market/orderbook?category=linear&limit=200&symbol='+symbol))
for s in ['BTC-USDT','BTC-USD-261225','BTC-USD-270326','BTC-USD-270924']:
    jobs.append(('depth-okx-'+s,'https://www.okx.com/api/v5/market/books?sz=400&instId='+s))
with ThreadPoolExecutor(max_workers=3) as ex: list(ex.map(lambda j:get(*j),jobs))
