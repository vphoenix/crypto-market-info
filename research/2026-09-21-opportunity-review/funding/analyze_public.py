"""Official historical checks, with Decimal financial arithmetic."""
from analyze import D,ROOT,END,META,MAP,HIST,FEES,stat,stamp
from datetime import datetime,timezone,timedelta
import json,itertools,collections
END=END+timedelta(minutes=1) # Binance publishes some settlements a few milliseconds after the exact hour.
EPOCH=datetime(1970,1,1,tzinfo=timezone.utc)
def dt(ms):return EPOCH+timedelta(milliseconds=int(ms))
hist={};audit=[]
for f in ROOT.glob('public-*.json'):
    r=json.loads(f.read_text(),parse_float=str)
    if not isinstance(r,dict) or r.get('status')!=200:continue
    v,s,rows=r['venue'],r['symbol'],r['data']
    if v=='Binance':h={dt(x['fundingTime']):D(x['fundingRate']) for x in rows}
    elif v=='Bybit':
      assert rows['retCode']==0
      h={dt(x['fundingRateTimestamp']):D(x['fundingRate']) for x in rows['result']['list']}
    else:
      assert rows['code']=='0'
      h={dt(x['fundingTime']):D(x['realizedRate']) for x in rows['data'] if x['realizedRate']!=''}
    hist[(s,v)]=h
    ids=[iid for iid,m in META.items() if m['exchange']==v and MAP[iid]['canonical_base_asset']==s]
    local={t:r for iid in ids for t,r in HIST[iid].items()}
    start=END-timedelta(days=7);public_win={t:r for t,r in h.items() if start<t<=END};local_win={t:r for t,r in local.items() if start<t<=END}
    audit.append({'symbol':s,'venue':v,'ids':ids,'public_count_7d':len(public_win),'local_count_7d':len(local_win),'missing_local_7d':len(public_win.keys()-local_win.keys()),'unexplained_local_7d':len(local_win.keys()-public_win.keys()),'conflicting_values_7d':sum(local_win[t]!=r for t,r in public_win.items() if t in local_win),'official_first':min(h).isoformat(),'official_last':max(h).isoformat(),'official_gap_hours_7d':dict(collections.Counter(str(D(int((b-a).total_seconds()))/3600) for a,b in zip(sorted(public_win),sorted(public_win)[1:])))})
def window(h,d,symbol=None):
    start=END-timedelta(days=d);ev=sorted((t,r) for t,r in h.items() if start<t<=END)
    interval={'SOON':4,'BTC':8}.get(symbol)
    cadence=None
    if interval:
      # Each known scheduled slot must occur exactly once; source timestamps remain unmodified in the evidence.
      expected={start.replace(minute=0)+timedelta(hours=i*interval) for i in range(1,d*24//interval+1)}
      slots=[t.replace(minute=0,second=0,microsecond=0) for t,r in ev]
      near_hour=all(t.minute==0 and t.second==0 and t.microsecond<1000000 for t,r in ev)
      cadence=near_hour and len(slots)==len(expected) and set(slots)==expected
    return {'count':len(ev),'sum':sum((r for t,r in ev),D(0)),'reaches_start':min(h)<=start,'reaches_end':max(h)>=END-timedelta(minutes=1),'constant_schedule_verified':cadence,'schedule_hours':interval}
out=[]
for s in sorted(set(s for s,v in hist)):
  for lv,sv in itertools.combinations(sorted(v for sym,v in hist if sym==s),2):
    if window(hist[(s,lv)],7)['sum']>window(hist[(s,sv)],7)['sum']:lv,sv=sv,lv
    hl,hs=hist[(s,lv)],hist[(s,sv)];windows={}
    fee=2*(FEES[lv]+FEES[sv])
    for d in [1,3,7,11,30]:
      l,z=window(hl,d,s),window(hs,d,s);spread=z['sum']-l['sum']
      boundary=all(x['reaches_start'] and x['reaches_end'] for x in [l,z])
      windows[str(d)]={'long':l,'short':z,'complete':boundary and all(x['constant_schedule_verified'] is True for x in [l,z]),'history_covers_boundaries':boundary,'spread_on_single_leg_notional':spread,'gross_total_capital_apr':spread/2*365/d,'after_taker_fees_apr':(spread-fee)/2*365/d}
    daily=[]
    for d in range(7):
      end=END-timedelta(days=d);start=end-timedelta(days=1)
      sums=[sum((r for t,r in h.items() if start<t<=end),D(0)) for h in [hl,hs]]
      daily.append({'end':end.isoformat(),'spread':sums[1]-sums[0]})
    positive=sum(x['spread']>0 for x in daily);total=windows['7']['spread_on_single_leg_notional']
    out.append({'symbol':s,'long':lv,'short':sv,'windows':windows,'daily':daily,'positive_days':positive,'largest_day_share':max(x['spread'] for x in daily)/total if total>0 else None,'fee_on_leg_notional':fee})
out.sort(key=lambda x:x['windows']['7']['after_taker_fees_apr'],reverse=True)
btc={}
if ('BTC','Binance') in hist:
  for d in [3,7,30]:
    w=window(hist[('BTC','Binance')],d,'BTC')
    w.update(gross_two_capital_apr=w['sum']/2*365/d,after_spot_10bp_perp_5bp_roundtrip_apr=(w['sum']-D('.003'))/2*365/d)
    btc[str(d)]=w
(ROOT/'public-analysis.json').write_text(json.dumps({'end_utc':END.isoformat(),'audit':audit,'pairs':out,'btc_spot_short_perp':btc},indent=2,default=str)+'\n')
for x in out:
  w=x['windows'];print('VERIFIED',x['symbol'],x['long'],x['short'],'7dnet',round(w['7']['after_taker_fees_apr']*100,2),'3dnet',round(w['3']['after_taker_fees_apr']*100,2),'11dnet',round(w['11']['after_taker_fees_apr']*100,2),'positive',x['positive_days'],'largest',round(x['largest_day_share']*100,2),'complete7',w['7']['complete'])
print('AUDIT',json.dumps(audit,default=str))
