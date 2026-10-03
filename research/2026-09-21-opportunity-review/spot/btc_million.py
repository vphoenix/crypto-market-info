import json,urllib.request,hashlib,sys
from decimal import Decimal as D,getcontext,ROUND_DOWN
from datetime import datetime,timezone,timedelta
from pathlib import Path
getcontext().prec=40
P=Path(__file__).resolve().parent
urls={'spot':'https://data-api.binance.vision/api/v3/depth?symbol=BTCUSDT&limit=1000','perp':'https://fapi.binance.com/fapi/v1/depth?symbol=BTCUSDT&limit=1000'}
if '--fetch' in sys.argv:
 for name,url in urls.items():
  raw=urllib.request.urlopen(urllib.request.Request(url,headers={'User-Agent':'public-market-research/1.0'}),timeout=30).read()
  (P/('btc-million-'+name+'.json')).write_bytes(raw)
  (P/('btc-million-'+name+'.meta.json')).write_text(json.dumps({'url':url,'retrieved_at_utc':datetime.now(timezone.utc).isoformat(),'sha256':hashlib.sha256(raw).hexdigest()},indent=2)+'\n')
books={name:{s:[(D(x[0]),D(x[1])) for x in json.loads((P/('btc-million-'+name+'.json')).read_text(),parse_float=str)[s]] for s in ['bids','asks']} for name in urls}
for b in books.values():
 assert b['asks'][0][0]>b['bids'][0][0]
 for s,ls in b.items():
  assert all(p>0 and q>0 for p,q in ls)
  assert all((a[0]>=b[0] if s=='bids' else a[0]<=b[0]) for a,b in zip(ls,ls[1:]))
def val(name,side,q):
 out=D(0)
 for p,n in books[name][side]:
  take=min(q,n);out+=p*take;q-=take
  if not q:return out
 return None
cap=D('1000000');q=(cap/(D('2.0031')*books['spot']['asks'][0][0])/D('.001')).to_integral_value(rounding=ROUND_DOWN)*D('.001')
while True:
 a=val('spot','asks',q);b=val('spot','bids',q);c=val('perp','asks',q);d=val('perp','bids',q)
 assert None not in (a,b,c,d),'insufficient saved depth'
 fees=(a+b)*D('.001')+(c+d)*D('.0005')
 if 2*a+fees<=cap:break
 q-=D('.001')
friction=a-b+c-d+fees
hist=json.loads((P.parent/'funding/public-Binance-BTC.json').read_text(),parse_float=str)['data'];end=datetime(2026,9,21,8,1,tzinfo=timezone.utc);endms=int(end.timestamp())*1000
rs=[D(x['fundingRate']) for x in hist if endms-30*86400000<int(x['fundingTime'])<=endms];assert len(rs)==90
fapr=sum(rs)*365/30
out={'capital':cap,'quantity_btc':q,'spot_buy':a,'spot_sell':b,'perp_buy':c,'perp_sell':d,'four_trade_fees':fees,'spread_and_book_impact':a-b+c-d,'total_roundtrip_friction':friction,'unused_after_equal_margin_and_all_fees':cap-2*a-fees,'entry_perp_discount':1-d/a,'funding_apr_on_notional_30d':fapr,'scenarios':{},'limitations':['Sequential public snapshots, not synchronized executable quotes.','Future funding equals historical 30-day average; basis and relative roundtrip liquidity unchanged.','Using fixed entry perpetual notional for future funding is an approximation, not mark-weighted realized PnL.','Transfer, tax, account specific fees and future basis costs unverified.']}
for h in [30,90,365]:
 net=d*fapr*D(h)/365-friction
 out['scenarios'][h]={'net_usdt':net,'net_apr':net/cap*365/D(h),'headroom_above_3pct_usdt':net-cap*D('.03')*D(h)/365,'basis_loss_notional_before_below3':(net-cap*D('.03')*D(h)/365)/a}
(P/'btc-million-analysis.json').write_text(json.dumps(out,default=str,indent=2)+'\n');print(json.dumps(out,default=str,indent=2))
