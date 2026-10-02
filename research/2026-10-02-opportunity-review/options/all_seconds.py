"""Exact tick/lot replay and executable BBO upper bounds for five independent minutes.

These are screening bounds: deeper fills cannot improve a buy ask or sell bid.
Negative gross bounds rule out every positive size, even with zero fees. Positive
bounds are not investment returns: margin, settlement, currency hedge and entry
conversion still have to be established against the full 1,000,000 USDT capital.
"""
from pathlib import Path
from decimal import Decimal as D,getcontext
import json,itertools,datetime
getcontext().prec=50
ROOT=Path(__file__).resolve().parent
def rows(p,n):return [json.loads(x,parse_float=D) for x in (p/(n+'.jsonl')).read_text().splitlines()]
def dt(x):return datetime.datetime.fromisoformat(x).replace(tzinfo=datetime.timezone.utc)
def seconds(delta):return D(delta.days*86400+delta.seconds)+D(delta.microseconds)/D(1000000)

summaries=[];positives=[]
for directory in [ROOT]+[ROOT/'windows'/str(n) for n in range(4)]:
    I={r['instrument_id']:r for r in rows(directory,'instruments')};S={r['instrument_id']:r for r in rows(directory,'specs')}
    B={r['instrument_id']:r for r in rows(directory,'latest_books')};Q={r['instrument_id']:r for r in rows(directory,'latest_quality')};E=rows(directory,'latest_commit')[0]
    indexes={r['index_id']:r for r in rows(directory,'latest_indexes')};deltas={}
    for r in rows(directory,'latest_deltas'):deltas.setdefault(int(r['minute_id']),{})[int(r['second_offset'])]=r
    sides={i:{s:dict(zip(map(int,b[s+'_prices']),map(int,b[s+'_qtys']))) for s in ['bid','ask']} for i,b in B.items()}
    for i,b in B.items():
        assert int(b['valid_bitmap'])==int(Q[i]['replay_valid_bitmap'])
        assert sum(1<<s for s in deltas.get(int(b['id']),{}))==int(b['delta_bitmap'])
    families={}
    for i in I:
        key=(I[i]['base_asset'],I[i]['settle_asset'],I[i]['expiry_time']);f=families.setdefault(key,{'strikes':{}})
        if I[i]['market_type']=='delivery':f['future']=i
        else:f['strikes'].setdefault(S[i]['strike'],{})[S[i]['option_type']]=i
    candidates=[];valid_seconds=0;all_valid_seconds=0;max_age=D(0)
    for sec in range(60):
        for i,b in B.items():
            change=deltas.get(int(b['id']),{}).get(sec)
            if change:
                for s in ['bid','ask']:
                    for price,qty in zip(change[s+'_change_prices'],change[s+'_change_qtys']):
                        price,qty=int(price),int(qty)
                        if qty:sides[i][s][price]=qty
                        else:sides[i][s].pop(price,None)
        books={};valid=[]
        for i,b in B.items():
            index=indexes[S[i]['index_id']]
            if not (int(b['valid_bitmap'])&(1<<sec)) or Q[i]['reasons'][sec]!=0 or index['states'][sec] not in [1,2]:continue
            sample=dt(E['minute_time'])+datetime.timedelta(seconds=sec)
            age=seconds(sample-dt(Q[i]['source_times'][sec]));assert age>=0;max_age=max(age,max_age)
            books[i]={s:[(D(p)/D(100000000),D(q)/D(100000000)) for p,q in sorted(sides[i][s].items(),reverse=(s=='bid'))] for s in ['bid','ask']}
            valid.append(i)
        valid_seconds+=len(valid)
        if len(valid)==len(I):all_valid_seconds+=1
        def has(legs):return all(i in books and books[i][s] for i,s in legs)
        def prices(legs):return [books[i][s][0][0] for i,s in legs]
        for key,f in families.items():
            future=f['future'];linear=key[1]=='USDC';index=indexes[S[future]['index_id']]['prices'][sec]
            if index is None:continue
            for strike,cp in f['strikes'].items():
                c,p=cp['call'],cp['put']
                for long in [True,False]:
                    legs=[(c,'ask'),(p,'bid'),(future,'bid')] if long else [(c,'bid'),(p,'ask'),(future,'ask')]
                    if not has(legs):continue
                    C,P,F=prices(legs)
                    gross=(F-strike-C+P if long else strike-F+C-P) if linear else (1-strike/F-C+P if long else strike/F-1+C-P)
                    fee=min(D('.0003')*(index if linear else D(1)),D('.125')*C)+min(D('.0003')*(index if linear else D(1)),D('.125')*P)+D('.00035')*(F if linear else strike/F)
                    candidates.append({'second':sec,'type':'conversion','family':key,'strikes':[strike],'direction':'long_synthetic_short_future' if long else 'short_synthetic_long_future','gross_native_per_base':gross,'entry_fee_native_per_base':fee,'net_before_delivery_native_per_base':gross-fee,'native_currency':key[1],'gross_usd_reference_per_base':gross*(D(1) if linear else index),'net_usd_reference_per_base':(gross-fee)*(D(1) if linear else index)})
            for lo,hi in itertools.combinations(sorted(f['strikes']),2):
                cl,pl=f['strikes'][lo]['call'],f['strikes'][lo]['put'];ch,ph=f['strikes'][hi]['call'],f['strikes'][hi]['put'];width=hi-lo
                for long in [True,False]:
                    legs=[(cl,'ask'),(pl,'bid'),(ch,'bid'),(ph,'ask')] if long else [(cl,'bid'),(pl,'ask'),(ch,'ask'),(ph,'bid')]
                    all_legs=legs+([] if linear else [(future,'ask' if long else 'bid')])
                    if not has(all_legs):continue
                    pp=prices(legs);box=pp[0]-pp[1]-pp[2]+pp[3]
                    F=books[future]['ask' if long else 'bid'][0][0] if not linear else D(1)
                    payoff=width if linear else width/F
                    gross=payoff-box if long else box-payoff
                    fee=sum(min(D('.0003')*(index if linear else D(1)),D('.125')*p) for p in pp)+(D('.00035')*width/F if not linear else D(0))
                    candidates.append({'second':sec,'type':'box','family':key,'strikes':[lo,hi],'direction':'long_box' if long else 'short_box','gross_native_per_base':gross,'entry_fee_native_per_base':fee,'net_before_delivery_native_per_base':gross-fee,'native_currency':key[1],'gross_usd_reference_per_base':gross*(D(1) if linear else index),'net_usd_reference_per_base':(gross-fee)*(D(1) if linear else index)})
    for c in candidates:
        if c['gross_native_per_base']>0:positives.append({'minute':E['minute_time'],**c})
    summary={'directory':str(directory.relative_to(ROOT)),'minute_utc':E['minute_time'],'run':E['run_id'],'instruments':len(I),'valid_instrument_seconds':valid_seconds,'all_instruments_valid_seconds':all_valid_seconds,'candidates':len(candidates),'gross_positive':sum(c['gross_native_per_base']>0 for c in candidates),'net_after_standard_entry_positive':sum(c['net_before_delivery_native_per_base']>0 for c in candidates),'max_source_age_seconds':max_age,'closest_gross_reference':max(candidates,key=lambda c:c['gross_usd_reference_per_base']) if candidates else None,'closest_net_reference':max(candidates,key=lambda c:c['net_usd_reference_per_base']) if candidates else None}
    summaries.append(summary)
    (directory/'all_second_bounds.json').write_text(json.dumps(candidates,default=str,indent=2)+'\n')
    print(E['minute_time'],len(candidates),summary['gross_positive'],summary['net_after_standard_entry_positive'],flush=True)
(ROOT/'all_second_summary.json').write_text(json.dumps(summaries,default=str,indent=2)+'\n')
(ROOT/'positive_bounds.json').write_text(json.dumps(positives,default=str,indent=2)+'\n')
