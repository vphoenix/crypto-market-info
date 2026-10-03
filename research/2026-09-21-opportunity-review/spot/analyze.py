import json
from pathlib import Path
from decimal import Decimal as D, getcontext
from datetime import datetime,timedelta
from collections import defaultdict
getcontext().prec=36
P=Path(__file__).resolve().parent
load=lambda n:json.loads((P/(n+'.json')).read_text(),parse_float=str)
ins={x['instrument_id']:x for x in load('instruments')}
books=defaultdict(dict)
for x in load('books'):
 i=x['instrument_id']; tick=D(str(ins[i]['price_tick_size']));qty=D(str(ins[i]['quantity_step_size']))*D(str(ins[i]['contract_multiplier']))
 books[x['minute_time']][i]={'bid':D(x['bid_price_01'])*tick,'ask':D(x['ask_price_01'])*tick,'bid_qty':D(x['bid_qty_01'])*qty,'ask_qty':D(x['ask_qty_01'])*qty}
end=datetime(2026,9,21,14,29)
out={'book_end_utc':str(end),'method':'minute-second-0, both valid; top-of-book only; Decimal; 20bp total taker scenario for cross-spot','spot_windows':{},'btc_cash_perp':{}}
for days in [1,3,7,14,30]:
 rows=[]
 for t,v in books.items():
  if not str(end-timedelta(days=days))<t<=str(end) or 1 not in v or 3 not in v:continue
  a,b=v[1],v[3]
  if min(a['bid'],a['ask'],b['bid'],b['ask'])<=0:raise ValueError(t)
  edges=[(b['bid']/a['ask']-1)*10000,(a['bid']/b['ask']-1)*10000]
  rows.append((t,max(edges),edges.index(max(edges))))
 if not rows:continue
 vals=sorted(x[1] for x in rows)
 out['spot_windows'][days]={'paired_minutes':len(rows),'expected_minutes':days*1440,'median_best_gross_bps':vals[len(vals)//2],'p95_best_gross_bps':vals[int(len(vals)*.95)],'max':max(rows,key=lambda x:x[1]),'gt_20bp':sum(x[1]>20 for x in rows),'gt_10bp':sum(x[1]>10 for x in rows),'gt_5bp':sum(x[1]>5 for x in rows),'days_any_gt20bp':len({t[:10] for t,r,d in rows if r>20})}
fund=defaultdict(dict)
for x in load('funding'):
 if not x['is_actual']:continue
 i=x['instrument_id'];ts=datetime.fromisoformat(x['funding_time']).replace(microsecond=0);r=D(str(x['rate']))
 if ts in fund[i]:assert fund[i][ts]==r
 fund[i][ts]=r
fund_end=datetime(2026,9,21,8)
for i in [5,6,7]:
 rs=fund[i];win={}
 for d in [1,3,7,14]:
  start=fund_end-timedelta(days=d);expect=[start+timedelta(hours=8*j) for j in range(1,d*3+1)]
  vals=[rs[t] for t in expect if t in rs];missing=[str(t) for t in expect if t not in rs]
  win[d]={'actual_count':len(vals),'expected_count':len(expect),'missing':missing,'observed_sum':sum(vals,D(0)),'complete':not missing}
  if not missing:
   fees=D('.003') if i!=7 else D('.0031')
   win[d].update({'gross_two_equal_capital_apr_pct':sum(vals,D(0))/2*365/d*100,'fee_only_net_apr_pct':(sum(vals,D(0))-fees)/2*365/d*100,'roundtrip_fee_on_leg_notional':fees})
 out['btc_cash_perp'][ins[i]['exchange']]={'first_actual':str(min(rs)),'last_actual':str(max(rs)),'windows':win}
(P/'analysis.json').write_text(json.dumps(out,default=str,indent=2)+'\n')
print(json.dumps(out,default=str,indent=2))
