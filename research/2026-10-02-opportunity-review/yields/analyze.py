from pathlib import Path
from decimal import Decimal,getcontext
from datetime import datetime,timezone,timedelta
from collections import defaultdict
import json
getcontext().prec=60
D=Decimal; p=Path(__file__).resolve().parent
read=lambda name:json.loads((p/name).read_text(),parse_float=D)
routes={r['yield_route_id']:r for r in read('routes.json')['data']}
latest=read('latest.json')['data']; history=read('history.json')['data']
def dt(s):return datetime.fromisoformat(s.replace('Z','+00:00')).replace(tzinfo=timezone.utc)
def secs(td):return D(td.days*86400+td.seconds)+D(td.microseconds)/1000000
now=dt(read('clock.json')['data'][0]['query_time'])
byroute=defaultdict(list)
for o in history:byroute[o['yield_route_id']].append(o)
summaries=[];ratios=[]
for o in latest:
 r=routes[o['yield_route_id']]; obs=byroute[o['yield_route_id']]
 s={**r,**o,'source_age_hours':str(secs(now-dt(o['observation_time']))/3600),'collection_age_hours':str(secs(now-dt(o['collected_at']))/3600),'windows':[]}
 for days in [7,30]:
  w=[a for a in obs if dt(a['observation_time'])>=now-timedelta(days=days)]
  rates=sorted(D(a['rate']) for a in w if a['rate'] is not None)
  gaps=[secs(dt(b['observation_time'])-dt(a['observation_time'])) for a,b in zip(w,w[1:])]
  s['windows'].append({'requested_days':days,'n':len(w),'first_time':w[0]['observation_time'] if w else None,'last_time':w[-1]['observation_time'] if w else None,'actual_span_days':str(secs(dt(w[-1]['observation_time'])-dt(w[0]['observation_time']))/86400) if len(w)>1 else None,'min_rate':str(min(rates)) if rates else None,'median_rate':str(rates[len(rates)//2]) if rates else None,'max_rate':str(max(rates)) if rates else None,'reported_rate_ge_3pct_samples':sum(v>=D('.03') for v in rates),'rate_samples':len(rates),'max_gap_hours':str(max(gaps)/3600) if gaps else None,'missing_hash_count':sum(a['source_payload_hash'] is None or len(a['source_payload_hash'])!=64 for a in w),'anchored_count':sum(a['block_height'] is not None and a['block_hash'] is not None and a['finality'] is not None for a in w),'finality_values':sorted(set(a['finality'] or 'NULL' for a in w))})
 if r['yield_type']=='liquid_staking':
  obs=[a for a in obs if a['exposure_ratio'] is not None]
  if len(obs)>1:
   end=obs[-1]
   for days in [7,30]:
    target=dt(end['observation_time'])-timedelta(days=days)
    before=[a for a in obs if dt(a['observation_time'])<=target]
    start=before[-1] if before else obs[0]
    elapsed=secs(dt(end['observation_time'])-dt(start['observation_time']))
    if elapsed<=0:continue
    ret=D(end['exposure_ratio'])/D(start['exposure_ratio'])-1
    selected=[a for a in obs if start['observation_time']<=a['observation_time']<=end['observation_time']]
    ratios.append({'yield_route_id':o['yield_route_id'],'provider':r['provider'],'product':r['product_code'],'requested_days':days,'actual_days':str(elapsed/86400),'start_time':start['observation_time'],'end_time':end['observation_time'],'ratio_start':start['exposure_ratio'],'ratio_end':end['exposure_ratio'],'holding_return':str(ret),'simple_annualized_rate':str(ret*31536000/elapsed),'n':len(selected),'decreasing_intervals':sum(D(b['exposure_ratio'])<D(a['exposure_ratio']) for a,b in zip(selected,selected[1:]))})
 summaries.append(s)
fresh=[o for o in summaries if D(o['source_age_hours'])<=24]
tron=[o for o in fresh if o['provider']=='TRON']
summary={'query_time_utc':now.isoformat(),'capital_usdt':'1000000','counts':{'registered_routes':len(routes),'latest_rows':len(latest),'30d_rows':len(history),'fresh_source_24h':len(fresh),'stale_source_24h':len(latest)-len(fresh),'fresh_tron':len(tron),'fresh_tron_max_apr':str(max(D(o['rate']) for o in tron)),'fresh_tron_above_3pct':sum(D(o['rate'])>=D('.03') for o in tron),'latest_missing_payload_hash':sum(o['source_payload_hash'] is None or len(o['source_payload_hash'])!=64 for o in latest),'30d_missing_payload_hash':sum(o['source_payload_hash'] is None or len(o['source_payload_hash'])!=64 for o in history)},'highest_fresh_tron':sorted(tron,key=lambda o:D(o['rate']),reverse=True)[:3],'ratio_returns':ratios,'routes':summaries}
# Illustrations only: zero observed asset funding, 80% spot/stake, 20% idle margin/cash;
# 3000 USDT total one-year expense budget is a scenario, not a fee quotation.
ys=[('sTRX_realized_12_8d_ratio',D(next(o for o in ratios if o['yield_route_id']==1 and o['requested_days']==30)['simple_annualized_rate']),14),('sAVAX_realized_12_8d_ratio',D(next(o for o in ratios if o['yield_route_id']==149 and o['requested_days']==30)['simple_annualized_rate']),2),('Kamino_SOL_30d_reported_median',D(next(o for o in summaries if o['yield_route_id']==139)['windows'][1]['median_rate']),0),('Kamino_SOL_30d_reported_min',D(next(o for o in summaries if o['yield_route_id']==139)['windows'][1]['min_rate']),0)]
summary['conditional_hedge_scenarios']=[]
for label,y,idledays in ys:
 conservative_y=y*D(365-idledays)/365
 alpha=D('.8');cost=D('.003');net=alpha*conservative_y-cost
 summary['conditional_hedge_scenarios'].append({'label':label,'asset_yield_rate':str(y),'non_earning_exit_days_assumed':idledays,'yield_after_idle_days':str(conservative_y),'invested_fraction':str(alpha),'expense_fraction_of_capital':str(cost),'full_capital_rate_before_funding':str(net),'minimum_short_received_funding_annualized':str((D('.03')+cost)/alpha-conservative_y),'status':'conditional_only: asset funding, execution depth and actual costs unavailable; no certified >=3% result'})
(p/'calculations.json').write_text(json.dumps(summary,indent=2,ensure_ascii=False,default=str)+'\n')
print(json.dumps({k:v for k,v in summary.items() if k not in ['routes','highest_fresh_tron']},indent=2,ensure_ascii=False,default=str))
print('Highest TRON',[(o['yield_route_id'],o['product_name'],o['rate'],o['windows']) for o in summary['highest_fresh_tron']])
for o in summaries:
 if o['provider']!='TRON':print('WINDOW',o['yield_route_id'],o['provider'],o['product_code'],json.dumps(o['windows']))
