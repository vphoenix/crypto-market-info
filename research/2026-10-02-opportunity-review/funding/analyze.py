"""Decimal-only funding research. Missing settlements are never replaced with estimates."""
import collections,itertools,json
from pathlib import Path
from datetime import datetime,timedelta
from decimal import Decimal as D,getcontext
getcontext().prec=45
P=Path(__file__).resolve().parent
read=lambda name:[json.loads(x,parse_float=str) for x in (P/(name+'.jsonl')).read_text().splitlines()]
dt=datetime.fromisoformat
ZERO=D(0)
FEES={'Binance':D('.0005'),'OKX':D('.0005'),'Bybit':D('.00055')}
CAPITAL=D('1000000')
def settlements(name):
    actual=collections.defaultdict(dict);targets=collections.defaultdict(set)
    for row in read(name):
        i=row['instrument_id'];t=dt(row['funding_time']).replace(microsecond=0)
        targets[i].add(t)
        if row['is_actual']:
            rate=D(row['rate'])
            if t in actual[i]: assert actual[i][t]==rate,(i,t)
            actual[i][t]=rate
    return actual,targets
def scenario(daily,f,hold_days):
    # Two N-sized legs plus cash reserve for all opening/closing fees, within 1m USDT.
    denom=D(2)+f;n=CAPITAL/denom
    return {'days':hold_days,'notional_each_leg_USDT':n,'total_fee_reserve_USDT':n*f,
        'fee_only_profit_USDT':n*(daily*hold_days-f),
        'fee_only_net_APR_pct':(daily*D(365)-f*D(365)/hold_days)/denom*100,
        'additional_exit_cost_budget_USDT_to_keep_3pct':n*(daily*hold_days-f)-CAPITAL*D('.03')*hold_days/D(365)}
def summary(values,start,end,step=28800):
    expected={start+timedelta(seconds=i) for i in range(step,int((end-start).total_seconds())+1,step)}
    have={t:r for t,r in values.items() if start<t<=end}
    complete=set(have)==expected
    count=collections.Counter(t.strftime('%Y-%m-%d') for t in have)
    total=sum(have.values(),ZERO);days=D(str((end-start).total_seconds()))/D(86400)
    return {'start_exclusive_utc':str(start),'end_inclusive_utc':str(end),'expected_count_inferred':len(expected),'actual_count':len(have),'missing':[str(t) for t in sorted(expected-set(have))],'complete_inferred_schedule':complete,'observed_rate_sum':total,'negative_settlement_count':sum(r<0 for r in have.values()),'gross_APR_pct_2N_only_if_complete':total*365/days/2*100 if complete else None}
prod,pt=settlements('production-funding');pm={r['instrument_id']:r for r in read('production-instruments')}
END=dt('2026-10-01 16:00:00')
btc=[]
for i in (5,6,7):
    # Previous contract IDs are included only for requested 30d availability check.
    vals=dict(prod.get({5:2,6:4}.get(i,-1),{}));vals.update(prod[i])
    fee=2*(D('.001')+FEES[pm[i]['exchange']])
    w7=summary(vals,END-timedelta(days=7),END);w30=summary(vals,END-timedelta(days=30),END)
    w12=summary(vals,END-timedelta(days=12),END)
    daily=w7['observed_rate_sum']/7
    daily_rates=collections.defaultdict(lambda:ZERO)
    for t,r in prod[i].items():
        if END-timedelta(days=7)<t<=END:daily_rates[str(t.date())]+=r
    btc.append({'instrument_id':i,'venue':pm[i]['exchange'],'last_actual_source_time':max(r['funding_time'] for r in read('production-funding') if r['instrument_id']==i and r['is_actual']),
        '7d':w7,'12d':w12,'30d':w30,'roundtrip_fee_per_N':fee,
        'scenarios_7d_mean':[scenario(daily,fee,D(h)) for h in (10,30,90)],
        'daily_settlement_sums':dict(daily_rates),'gross_APR_latest_full_day_pct_2N':daily_rates.get('2026-10-01',ZERO)*365/2*100,
        'daily_extra_roundtrip_trading_fee_APR_drag_pct':fee*365/(2+fee)*100})
btc_pairs=[]
for a,b in itertools.combinations((5,6,7),2):
    av=summary(prod[a],END-timedelta(days=7),END);bv=summary(prod[b],END-timedelta(days=7),END)
    l,s=(a,b) if av['observed_rate_sum']<=bv['observed_rate_sum'] else (b,a)
    spread=abs(av['observed_rate_sum']-bv['observed_rate_sum'])
    f=2*(FEES[pm[a]['exchange']]+FEES[pm[b]['exchange']])
    btc_pairs.append({'market':'BTC-USDT-PERP','long':pm[l]['exchange'],'short':pm[s]['exchange'],'actual_counts':[av['actual_count'],bv['actual_count']],'complete':av['complete_inferred_schedule'] and bv['complete_inferred_schedule'],'spread_sum':spread,'gross_APR_pct_2N':spread/7*365/2*100,'scenarios':[scenario(spread/7,f,D(h)) for h in (10,30,90)]})
old,targets=settlements('old-soak-funding');om={r['instrument_id']:r for r in read('old-soak-instruments')};mp={r['instrument_id']:r for r in read('old-soak-mapping')};members=read('old-soak-members')
groups=collections.defaultdict(list)
for r in members:groups[r['canonical_market_key']].append(r['instrument_id'])
# Rates metadata lacks authoritative historical cadence. Infer common target spacing, and label it.
cadence={}
days={}
DAY_START=dt('2026-09-10 00:00:00');DAY_END=dt('2026-09-28 00:00:00')
for i in om:
    ts=sorted(targets[i]);gaps=collections.Counter(int((b-a).total_seconds()) for a,b in zip(ts,ts[1:]) if int((b-a).total_seconds()) in (3600,7200,14400,28800))
    step=gaps.most_common(1)[0][0] if gaps else 0
    cadence[i]=step;daily={}
    for offset in range((DAY_END-DAY_START).days):
        day=DAY_START+timedelta(days=offset)
        expected={day+timedelta(seconds=s) for s in range(0,86400,step)} if step else set()
        vals={t:r for t,r in old[i].items() if day<=t<day+timedelta(days=1)}
        observed_targets={t for t in targets[i] if day<=t<day+timedelta(days=1)}
        complete=bool(expected) and expected==set(vals) and observed_targets<=set(vals)
        daily[str(day.date())]={'complete_inferred_schedule':complete,'actual_count':len(vals),'expected_count':len(expected),'rate_sum':sum(vals.values(),ZERO),'missing_observed_targets':len(observed_targets-set(vals)),'missing_grid_targets':len(expected-set(vals))}
    days[i]=daily
pairs=[];week_complete_pairs=0
for market,ids in groups.items():
    for a,b in itertools.combinations(ids,2):
        if om[a]['exchange']==om[b]['exchange']:continue
        fixed_dates=[str((dt('2026-09-21')+timedelta(days=k)).date()) for k in range(7)]
        week_complete=all(days[a][d]['complete_inferred_schedule'] and days[b][d]['complete_inferred_schedule'] for d in fixed_dates)
        week_complete_pairs+=week_complete
        for l,s in ((a,b),(b,a)):
            streaks=[];running=[]
            for d in sorted(days[l]):
                ll,ss=days[l][d],days[s][d]
                spread=ss['rate_sum']-ll['rate_sum']
                if ll['complete_inferred_schedule'] and ss['complete_inferred_schedule'] and spread>0:
                    running.append({'date_utc':d,'long_sum':ll['rate_sum'],'short_sum':ss['rate_sum'],'spread':spread,'long_actual_count':ll['actual_count'],'short_actual_count':ss['actual_count']})
                else:
                    if len(running)>=3:streaks.append(running)
                    running=[]
            if len(running)>=3:streaks.append(running)
            if not streaks:continue
            best=max(streaks,key=lambda z:(z[-1]['date_utc'],len(z)))
            daily=sum((x['spread'] for x in best),ZERO)/D(len(best));f=2*(FEES[om[l]['exchange']]+FEES[om[s]['exchange']])
            pairs.append({'market':market,'long_id':l,'short_id':s,'long_venue':om[l]['exchange'],'short_venue':om[s]['exchange'],
                'sample_status':'STALE historical reference; retrospectively chosen direction/streak, no current funding verification',
                'sample_days':len(best),'first_day_utc':best[0]['date_utc'],'last_day_utc':best[-1]['date_utc'],'daily':best,
                'long_cadence_hours_inferred':D(cadence[l])/3600,'short_cadence_hours_inferred':D(cadence[s])/3600,
                'mean_daily_spread_per_N':daily,'gross_APR_pct_2N':daily*365/2*100,'roundtrip_fee_per_N':f,
                'scenarios':[scenario(daily,f,D(h)) for h in (10,30,90)],
                'latest_long_actual_source_time':max((str(t) for t in old[l]),default=None),'latest_short_actual_source_time':max((str(t) for t in old[s]),default=None),
                'negative_spread_complete_days_outside_selected_streak':sum(days[l][d]['complete_inferred_schedule'] and days[s][d]['complete_inferred_schedule'] and days[s][d]['rate_sum']-days[l][d]['rate_sum']<0 for d in sorted(days[l])),
                'daily_extra_roundtrip_APR_drag_pct':f*365/(2+f)*100})
pairs.sort(key=lambda z:z['scenarios'][2]['fee_only_net_APR_pct'],reverse=True)
hedge=[]
for i,m in om.items():
    if m['base_asset'] not in ('SOL','TRX','AVAX'):continue
    good=[{'date_utc':d,**v} for d,v in days[i].items() if v['complete_inferred_schedule']]
    if good:
        mean=sum((v['rate_sum'] for v in good),ZERO)/len(good)
        hedge.append({'asset':m['base_asset'],'venue':m['exchange'],'instrument_id':i,'complete_day_count':len(good),'first_complete_day':good[0]['date_utc'],'last_complete_day':good[-1]['date_utc'],'observed_mean_short_funding_APR_pct_on_N':mean*365*100,'last_actual':max(str(t) for t in old[i]),'current_verified':False,'daily':good})
out={'observed_at_utc':read('server')[0]['now_utc'],'capital_USDT':CAPITAL,'basis':'APR, two equal N-sized unlevered allocations and all roundtrip fee reserve within 1m; no compounding; hypothetical future rate continuation, not locked yield; all values Decimal','new_soak':{'run':read('current-run')[0],'funding_range':read('soak-funding-range')[0],'book_range':read('new-soak-book-range')[0]},'btc_carry':btc,'btc_perp_pairs':btc_pairs,'old_soak_reference':{'latest_actual':read('old-soak-funding-range')[0]['latest_actual'],'fixed_7d_complete_pair_count':week_complete_pairs,'pairs_with_three_consecutive_complete_positive_days':len(pairs),'pairs':pairs,'hedge_reference':hedge}}
(P/'analysis.json').write_text(json.dumps(out,default=str,ensure_ascii=False,indent=2)+'\n')
print('fixed7dcompletepairs',week_complete_pairs,'historical streak candidates',len(pairs))
for b in btc:print('BTC',b['venue'],'gross7',b['7d']['gross_APR_pct_2N_only_if_complete'],'scenarios',[(r['days'],r['fee_only_net_APR_pct']) for r in b['scenarios_7d_mean']])
for r in pairs[:25]:print(r['market'],r['long_venue'],r['short_venue'],r['sample_days'],r['first_day_utc'],r['last_day_utc'],r['gross_APR_pct_2N'],r['scenarios'][2]['fee_only_net_APR_pct'])
