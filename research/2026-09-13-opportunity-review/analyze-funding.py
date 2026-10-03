import json,datetime,collections,itertools,sys
from pathlib import Path
from decimal import Decimal as D
ROOT=Path(__file__).resolve().parent
UTC=datetime.timezone.utc; EPOCH=datetime.datetime(1970,1,1,tzinfo=UTC)
def ms(dt):return (dt-EPOCH)//datetime.timedelta(milliseconds=1)
def dt(t):return EPOCH+datetime.timedelta(milliseconds=t)
def read():
 out={}
 for f in ROOT.glob('public-funding-*.json'):
  r=json.loads(f.read_text())
  if not isinstance(r,dict) or 'venue' not in r:continue
  v=r['venue'];s=r['symbol'];rows=r.get('data')
  if not rows:continue
  if v=='Binance':arr=[(q['fundingTime'],D(str(q['fundingRate']))) for q in rows]
  elif v=='Bybit':arr=[(int(q['fundingRateTimestamp']),D(q['fundingRate'])) for q in rows['result']['list']]
  elif v=='OKX':arr=[(int(q['fundingTime']),D(q['realizedRate'])) for q in rows['data']]
  else:continue
  out[(s.replace('-USDT-SWAP','USDT'),v)]=dict(arr)
 return out

def totals(rows,start,end):
 events=sorted((t,r) for t,r in rows.items() if ms(start)<t<=ms(end))
 return {'sum':sum((r for t,r in events),D(0)),'count':len(events),'first':dt(events[0][0]).isoformat() if events else None,'last':dt(events[-1][0]).isoformat() if events else None,'max_gap_hours':max(((D(b[0]-a[0])/3600000) for a,b in zip(events,events[1:])),default=None)}

if __name__=='__main__':
 data=read();end=datetime.datetime(2026,9,12,int(sys.argv[1]) if len(sys.argv)>1 else 19,1,tzinfo=UTC)
 for h in [24,48,72,168]:
  start=end-datetime.timedelta(hours=h);out=[]
  for sym in sorted(set(k[0] for k in data)):
   legs=[(v,totals(rows,start,end)) for (s,v),rows in data.items() if s==sym]
   if len(legs)<2:continue
   legs.sort(key=lambda x:x[1]['sum']);l,s=legs[0],legs[-1]
   out.append({'symbol':sym,'long':l,'short':s,'difference':s[1]['sum']-l[1]['sum']})
  print('HOURS',h,'END',end.isoformat())
  for x in sorted(out,key=lambda x:x['difference'],reverse=True):print(json.dumps(x,default=str))
 for sym in ['IOSTUSDT','ONGUSDT']:
  print('Daily constant direction',sym)
  lv,sv=('Bybit','Binance') if sym=='IOSTUSDT' else ('Binance','Bybit')
  for day in range(7):
   e=end-datetime.timedelta(days=day);s=e-datetime.timedelta(days=1)
   tl=totals(data[(sym,lv)],s,e);ts=totals(data[(sym,sv)],s,e)
   print(s.isoformat(),ts['sum']-tl['sum'],tl['count'],ts['count'],tl['first'],ts['first'])
  print('LAST HOURS')
  for key,events in data.items():
   if key[0]==sym:print(key,[(dt(t).isoformat(),str(r)) for t,r in sorted(events.items()) if t>ms(end-datetime.timedelta(hours=8))])
