from pathlib import Path
from decimal import Decimal, getcontext
from datetime import datetime,timezone,timedelta
import json
getcontext().prec=60
p=Path(__file__).resolve().parent
D=Decimal
routes={v['yield_route_id']:v for v in json.loads((p/'routes.json').read_text(),parse_float=D)['data']}
latest=json.loads((p/'latest.json').read_text(),parse_float=D)['data']
history=json.loads((p/'history-summary.json').read_text(),parse_float=D)['data']
ratios=json.loads((p/'ratio-history.json').read_text(),parse_float=D)['data']
meta=json.loads((p/'query-meta.json').read_text())
def dt(s):return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)
def secs(td):return D(td.days*86400+td.seconds)+D(td.microseconds)/D(1000000)
now=dt(meta['query_time_utc'])
summary=[]
for o in latest:
 r=routes[o['yield_route_id']]
 summary.append({**r,**o,'rate_pct':str(D(o['rate'])*100) if o['rate'] is not None else None,'source_age_hours':str(secs(now-dt(o['observation_time']))/3600),'collection_age_hours':str(secs(now-dt(o['collected_at']))/3600)})
ratio_returns=[]
for rid in sorted({o['yield_route_id'] for o in ratios}):
 obs=[o for o in ratios if o['yield_route_id']==rid and o['exposure_ratio'] is not None]
 last=obs[-1]
 for days in [7,30]:
  target=dt(last['observation_time'])-timedelta(days=days)
  before=[o for o in obs if dt(o['observation_time'])<=target]
  first=before[-1] if before else obs[0]
  seconds=secs(dt(last['observation_time'])-dt(first['observation_time']))
  ret=D(last['exposure_ratio'])/D(first['exposure_ratio'])-1
  result={'yield_route_id':rid,'product_code':routes[rid]['product_code'],'requested_days':days,'actual_days':str(seconds/86400),'start_time':first['observation_time'],'end_time':last['observation_time'],'first_exposure_ratio':first['exposure_ratio'],'last_exposure_ratio':last['exposure_ratio'],'holding_return':str(ret),'simple_annualized_rate':str(ret*31536000/seconds),'simple_annualized_pct':str(ret*31536000/seconds*100),'observations':len([o for o in obs if first['observation_time']<=o['observation_time']<=last['observation_time']]),'definition':'(last / first - 1) * 365d / elapsed; backward-looking, asset denominated, no promise of future rate'}
  ratio_returns.append(result)
cut=now-timedelta(days=1)
fresh=[o for o in summary if dt(o['observation_time'])>=cut]
t=[o for o in fresh if o['provider']=='TRON']
counts={'registered_routes':len(routes),'latest_rows':len(latest),'fresh_within_24h':len(fresh),'fresh_tron':len(t),'stablecoin_yield_routes':sum(r['price_exposure_asset'] is None for r in routes.values()),'latest_missing_payload_hash':sum(o['source_payload_hash'] is None or len(o['source_payload_hash'])!=64 for o in latest),'recent_7d_logical_observations':sum(int(h['n']) for h in history),'recent_7d_missing_payload_hash':sum(int(h['missing_hash_count']) for h in history),'fresh_rate_gt_3pct':sum(o['rate'] is not None and D(o['rate'])>D('.03') for o in fresh),'fresh_candidate_rate_gt_3pct':sum(o['rate'] is not None and D(o['rate'])>D('.03') and o['rule_eligibility']=='candidate' for o in fresh),'fresh_tron_rate_gt_3pct':sum(D(o['rate'])>D('.03') for o in t),'fresh_tron_max_apr_pct':str(max(D(o['rate']) for o in t)*100),'capacity_known_routes':[o['yield_route_id'] for o in latest if o['remaining_capacity'] is not None]}
# Conditional one-year examples: applies source rate without forecasting it and excludes hedge/gas/fees not explicitly modelled.
illustrations=[]
for rid in [137,140,141,142]:
 o=next(o for o in latest if o['yield_route_id']==rid)
 net=(1-D(o['entry_fee_rate']))*(1+D(o['rate']))*(1-D(o['exit_fee_rate']))-1
 illustrations.append({'route_id':rid,'product_code':routes[rid]['product_code'],'conditional_one_year_after_entry_exit_pct':str(net*100),'assumption':'Current approximate asset APR constant for 365d, entry/exit fees constant; excludes unstaking delay, gas, hedge; epoch performance fee already reflected in ratio APR.'})
kamino=next(o for o in latest if o['yield_route_id']==139)
counts['kamino_capacity_USD100k_SOL_price_threshold']=str(D(100000)/D(kamino['remaining_capacity']))
output={'meta':meta,'counts':counts,'ratio_returns':ratio_returns,'illustrations':illustrations,'routes':summary}
(p/'calculations.json').write_text(json.dumps(output,indent=2,ensure_ascii=False,default=str)+'\n')
print(json.dumps({k:v for k,v in output.items() if k!='routes'},indent=2,ensure_ascii=False,default=str))
print('TRON highest',json.dumps(sorted(t,key=lambda o:D(o['rate']),reverse=True)[:2],ensure_ascii=False))
