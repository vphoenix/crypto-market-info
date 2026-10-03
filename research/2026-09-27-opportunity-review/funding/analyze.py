"""Decimal-only analysis. Actual settlements deduplicated; no missing values filled."""
import json,itertools,collections
from pathlib import Path
from datetime import datetime,timedelta
from decimal import Decimal as D,getcontext
getcontext().prec=40
P=Path(__file__).resolve().parent
read=lambda name:[json.loads(x,parse_float=str) for x in (P/(name+'.jsonl')).read_text().splitlines()]
dt=datetime.fromisoformat
END=dt('2026-09-26 16:00:00');START=END-timedelta(days=7)
FEES={'Binance':D('.0005'),'OKX':D('.0005'),'Bybit':D('.00055')}
def funding(label):
 h=collections.defaultdict(dict);targets=collections.defaultdict(set)
 for r in read(label+'-funding'):
  i=r['instrument_id'];ft=dt(r['funding_time']);slot=ft.replace(microsecond=0)
  targets[i].add(slot)
  if r['is_actual']:
   v=D(r['rate'])
   if slot in h[i]:assert h[i][slot]==v
   h[i][slot]=v
 return h,targets
def stats(i,h,targets):
 vals={t:r for t,r in h[i].items() if START<t<=END}
 hours=collections.Counter(int((b-a).total_seconds()) for a,b in zip(sorted(h[i]),sorted(h[i])[1:]))
 step=hours.most_common(1)[0][0] if hours else 0
 expected={START+timedelta(seconds=s) for s in range(step,int((END-START).total_seconds())+1,step)} if step in (3600,7200,14400,28800) else set()
 observed_target={t for t in targets[i] if START<t<=END}
 missing=sorted(expected-set(vals));unexpected=sorted(set(vals)-expected)
 return {'actual_count':len(vals),'sum':sum(vals.values(),D(0)),'cadence_hours_inferred':D(step)/3600 if step else None,'expected_count_inferred':len(expected),'missing_inferred':[str(t) for t in missing],'unexpected_actual':[str(t) for t in unexpected],'missing_observed_targets':[str(t) for t in sorted(observed_target-set(vals))],'schedule_consistent':bool(expected) and not missing and not unexpected,'first':str(min(vals)) if vals else None,'last':str(max(vals)) if vals else None}
h,targets=funding('soak');meta={x['instrument_id']:x for x in read('soak-instruments')};mapping={x['instrument_id']:x for x in read('mapping')};members={x['instrument_id'] for x in read('members')}
groups=collections.defaultdict(list)
for i in sorted(members):groups[mapping[i]['canonical_market_key']].append(i)
st={i:stats(i,h,targets) for i in members};pairs=[]
for key,ids in groups.items():
 for a,b in itertools.combinations(ids,2):
  if meta[a]['exchange']==meta[b]['exchange'] or min(st[a]['actual_count'],st[b]['actual_count'])<3:continue
  l,s=(a,b) if st[a]['sum']<=st[b]['sum'] else (b,a)
  spread=st[s]['sum']-st[l]['sum'];fees=2*(FEES[meta[l]['exchange']]+FEES[meta[s]['exchange']])
  complete=st[l]['schedule_consistent'] and st[s]['schedule_consistent']
  pairs.append({'market':key,'long_id':l,'short_id':s,'long_venue':meta[l]['exchange'],'short_venue':meta[s]['exchange'],'long':st[l],'short':st[s],'observed_spread':spread,'complete_inferred_schedule':complete,'roundtrip_fee':fees,'gross_APR_pct_only_if_complete':spread/2*D(365)/7*100 if complete else None,'fee_only_net_APR_pct_only_if_complete':(spread-fees)/2*D(365)/7*100 if complete else None})
pairs.sort(key=lambda x:x['observed_spread'],reverse=True)
ph,pt=funding('production');pm={x['instrument_id']:x for x in read('production-instruments')}
btc={}
for i in [5,6,7]:
 z=stats(i,ph,pt);fees=2*(D('.001')+FEES[pm[i]['exchange']]);z['roundtrip_fee']=fees
 z['gross_APR_pct_two_equal_capital']=z['sum']/2*D(365)/7*100 if z['schedule_consistent'] else None
 z['fee_only_net_APR_pct_two_equal_capital']=(z['sum']-fees)/2*D(365)/7*100 if z['schedule_consistent'] else None
 btc[pm[i]['exchange']]=z
spot=collections.defaultdict(dict)
for x in read('spot-bbo'):
 i=x['instrument_id'];tick=D(pm[i]['price_tick_size']);qty=D(pm[i]['quantity_step_size'])
 spot[x['minute_time']][i]={k:D(x[k])*tick for k in ['bid_price_01','ask_price_01']}
paired=[]
for t,v in spot.items():
 if 1 not in v or 3 not in v:continue
 a,b=v[1],v[3];ba=(b['bid_price_01']/a['ask_price_01']-1)*10000;ab=(a['bid_price_01']/b['ask_price_01']-1)*10000
 paired.append({'minute_time':t,'best_bps':max(ba,ab),'buy_venue':'Binance' if ba>=ab else 'OKX','sell_venue':'OKX' if ba>=ab else 'Binance'})
paired.sort(key=lambda x:x['best_bps'],reverse=True)
spot_out={'paired_minute_anchors':len(paired),'max':paired[0],'above_20bps':sum(x['best_bps']>20 for x in paired),'above_10bps':sum(x['best_bps']>10 for x in paired),'above_5bps':sum(x['best_bps']>5 for x in paired),'top10':paired[:10]}
out={'window_start_exclusive_utc':str(START),'window_end_inclusive_utc':str(END),'rate_basis':'APR on 2N capital, each leg N notional; fees scenario; not future locked yield; incomplete rows never annualized','current_member_count':len(members),'market_group_count':len(groups),'complete_inferred_schedule_instruments':sum(x['schedule_consistent'] for x in st.values()),'pairs':pairs,'btc_carry':btc,'spot':spot_out}
(P/'analysis.json').write_text(json.dumps(out,default=str,indent=2)+'\n')
print('complete instruments',out['complete_inferred_schedule_instruments'],'pairs',len(pairs),'complete pairs',sum(x['complete_inferred_schedule'] for x in pairs))
print('BTC',json.dumps(btc,default=str,indent=2));print('SPOT',json.dumps(spot_out,default=str,indent=2))
for x in pairs[:20]:print(x['market'],x['long_venue'],x['short_venue'],x['long_id'],x['short_id'],'counts',x['long']['actual_count'],x['short']['actual_count'],'complete',x['complete_inferred_schedule'],'spread',x['observed_spread'],'net APR%',x['fee_only_net_APR_pct_only_if_complete'])
