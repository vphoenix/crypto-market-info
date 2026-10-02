"""Strictly parsed, Decimal-only official follow-up; separate from local history."""
import collections,json,re
from datetime import datetime,timedelta,timezone
from decimal import Decimal as D,getcontext
from pathlib import Path
getcontext().prec=45
P=Path(__file__).resolve().parent
PAT=re.compile(r'-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?\Z')
EPOCH=datetime(1970,1,1,tzinfo=timezone.utc)
END=datetime(2026,10,1,16,tzinfo=timezone.utc)
CAPITAL=D('1000000');ZERO=D(0)
def dec(s):
    assert isinstance(s,str) and PAT.fullmatch(s),repr(s)
    result=D(s);assert result.is_finite();return result
def millis(v):
    assert isinstance(v,int) or (isinstance(v,str) and re.fullmatch(r'[0-9]+',v)),repr(v)
    return int(v)
def at(v):return EPOCH+timedelta(milliseconds=millis(v))
def load(name):
    meta=json.loads((P/(name+'.meta.json')).read_text());assert meta['http_status']==200 and meta['error'] is None,meta
    body=json.loads((P/(name+'.raw.json')).read_text(),parse_float=str)
    return body,meta
def scenario(daily,fee,h):
    n=CAPITAL/(2+fee)
    return {'days':h,'N_USDT':n,'fee_reserve_USDT':n*fee,'net_APR_pct':(daily*365-fee*D(365)/h)/(2+fee)*100,'period_profit_USDT':n*(daily*h-fee)}
def value(book,qty):
    amount=ZERO;rem=qty
    for price,size in book:
        use=min(rem,size);amount+=price*use;rem-=use
        if rem==0:break
    return amount,rem
out=[]
for asset in ('ATOM','DOT'):
    rates={};books={};book_ts={};metadata={};proof={}
    for venue in ('bybit','okx'):
        prefix=f'public-{venue}-{asset}-'
        history,hmeta=load(prefix+'funding');book,bmeta=load(prefix+'book');instrument,imeta=load(prefix+'instrument')
        if venue=='bybit':
            assert history['retCode']==book['retCode']==instrument['retCode']==0
            rows=history['result']['list'];b=book['result'];ir=instrument['result']['list'];assert len(ir)==1
            im=ir[0];assert im['symbol']==asset+'USDT' and im['baseCoin']==asset and im['quoteCoin']==im['settleCoin']=='USDT' and im['contractType']=='LinearPerpetual' and im['status']=='Trading'
            assert im['fundingInterval']==480
            ct=dec('1');tick=dec(im['priceFilter']['tickSize']);step=dec(im['lotSizeFilter']['qtyStep']);funding_ts='fundingRateTimestamp';rate_key='fundingRate';bb=b['b'];aa=b['a'];ts=at(b['cts']);metadata[venue]=im
            for r in rows:assert r['symbol']==asset+'USDT'
        else:
            assert history['code']==book['code']==instrument['code']=='0'
            rows=history['data'];assert len(book['data'])==1 and len(instrument['data'])==1;b=book['data'][0];im=instrument['data'][0]
            assert im['instId']==asset+'-USDT-SWAP' and im['ctType']=='linear' and im['ctValCcy']==asset and im['settleCcy']=='USDT' and im['state']=='live'
            ct=dec(im['ctVal'])*dec(im['ctMult']);tick=dec(im['tickSz']);step=dec(im['lotSz']);funding_ts='fundingTime';rate_key='realizedRate';bb=b['bids'];aa=b['asks'];ts=at(b['ts']);metadata[venue]=im
            for r in rows:assert r['instId']==asset+'-USDT-SWAP' and r['realizedRate']!=''
        v={}
        for r in rows:
            t=at(r[funding_ts]);rate=dec(r[rate_key]);assert t<=datetime.fromisoformat(hmeta['received_at_utc'])
            if t in v:assert v[t]==rate
            v[t]=rate
        rates[venue]=v
        bs={}
        for side,raw in (('bid',bb),('ask',aa)):
            vals=[]
            for row in raw:
                price=dec(row[0]);quantity=dec(row[1]);assert price>0 and quantity>0 and price%tick==0 and quantity%step==0
                vals.append((price,quantity*ct))
            assert len(vals)==10
            assert [p for p,q in vals]==sorted((p for p,q in vals),reverse=side=='bid')
            bs[side]=vals
        assert bs['bid'][0][0]<bs['ask'][0][0]
        books[venue]=bs;book_ts[venue]=ts
        age=datetime.fromisoformat(bmeta['received_at_utc'])-ts
        assert abs(age.total_seconds())<15,(venue,age)
        proof[venue]={'received_at_utc':bmeta['received_at_utc'],'book_source_utc':str(ts),'book_source_age_ms_at_receipt':int(age.total_seconds()*1000),'latest_actual_source_utc':str(max(v)),'funding_rows_returned':len(rows),'funding_payload_sha256':hmeta['sha256'],'book_payload_sha256':bmeta['sha256'],'metadata_payload_sha256':imeta['sha256'],'contract_multiplier':ct,'tick':tick,'quantity_step':step}
    long,short=('bybit','okx') if asset=='ATOM' else ('okx','bybit')
    windows=[]
    for numdays in (7,30):
        start=END-timedelta(days=numdays)
        expected={start+timedelta(hours=k*8) for k in range(1,numdays*3+1)}
        totals={};counts={}
        for venue,v in rates.items():
            have={t:r for t,r in v.items() if start<t<=END};assert set(have)==expected,(asset,venue,numdays,len(have),len(expected))
            totals[venue]=sum(have.values(),ZERO);counts[venue]=len(have)
        signed=totals[short]-totals[long];best=abs(signed)/numdays
        daily=[]
        for k in range(numdays):
            day=(start+timedelta(days=k+1)).date();dvals={venue:sum((r for t,r in v.items() if t.date()==day),ZERO) for venue,v in rates.items()}
            daily.append({'date_utc':str(day),'long_sum':dvals[long],'short_sum':dvals[short],'spread':dvals[short]-dvals[long]})
        windows.append({'days':numdays,'start_exclusive_utc':str(start),'end_inclusive_utc':str(END),'expected_each':numdays*3,'actual_counts':counts,'rate_sums':totals,'signed_original_direction_spread':signed,'gross_original_APR_pct_2N':signed/numdays*365/2*100,'best_static_orientation':{'long':long if signed>=0 else short,'short':short if signed>=0 else long,'gross_APR_pct_2N':best*365/2*100,'regular_nonVIP_fee_scenarios':[scenario(best,D('.00210'),D(h)) for h in (10,30,90)],'conditional_Bybit_VIP2_fee_scenarios':[scenario(best,D('.00175'),D(h)) for h in (10,30,90)]},'original_direction_positive_daily_count':sum(r['spread']>0 for r in daily),'original_direction_negative_daily_count':sum(r['spread']<0 for r in daily),'original_direction_daily':daily})
    # Capacity at original historical direction: same base quantity on both legs.
    la,sb=books[long]['ask'],books[short]['bid'];lb,sa=books[long]['bid'],books[short]['ask']
    qty=min(sum(q for p,q in la),sum(q for p,q in sb));ln,_=value(la,qty);sn,_=value(sb,qty)
    target=CAPITAL/D('2.00210');tq=target/la[0][0];_,lr=value(la,tq);_,sr=value(sb,tq)
    rq=min(qty,sum(q for p,q in lb),sum(q for p,q in sa));lbuy,_=value(la,rq);lsell,_=value(lb,rq);ssell,_=value(sb,rq);sbuy,_=value(sa,rq)
    cap={'long':long,'short':short,'matched_base_qty':qty,'long_notional_USDT':ln,'short_notional_USDT':sn,'target_N_USDT':target,'target_fits_10_levels':lr==0 and sr==0,'BBO_entry_short_minus_long_bps':(sb[0][0]/la[0][0]-1)*10000,'four_side_visible_qty':rq,'static_roundtrip_spread_loss_USDT_at_four_side_qty':lbuy-lsell+sbuy-ssell,'static_roundtrip_spread_loss_bps_per_N':(lbuy-lsell+sbuy-ssell)/lbuy*10000,'source_time_difference_ms':abs(int((book_ts[long]-book_ts[short]).total_seconds()*1000))}
    out.append({'asset':asset,'locally_found_original_direction':{'long':long,'short':short},'proof':proof,'windows':windows,'current_10_level_capacity_original_direction':cap})
(P/'public-analysis.json').write_text(json.dumps({'scope':'only ATOM/DOT official follow-up, separate evidence from existing local data','observed_utc':max(r['proof']['bybit']['received_at_utc'] for r in out),'capital_USDT':CAPITAL,'results':out},default=str,ensure_ascii=False,indent=2)+'\n')
for r in out:
    print(r['asset'],'capacity',r['current_10_level_capacity_original_direction'])
    for w in r['windows']:print('window',w['days'],'gross-original',w['gross_original_APR_pct_2N'],'best',w['best_static_orientation']['gross_APR_pct_2N'],'nonVIP90net',w['best_static_orientation']['regular_nonVIP_fee_scenarios'][2]['net_APR_pct'])
