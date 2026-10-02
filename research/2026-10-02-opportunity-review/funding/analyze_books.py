import json
from decimal import Decimal as D,getcontext
from pathlib import Path
getcontext().prec=45
P=Path(__file__).resolve().parent
read=lambda n:[json.loads(x,parse_float=str) for x in (P/(n+'.jsonl')).read_text().splitlines()]
meta={r['instrument_id']:r for r in read('old-soak-instruments')};mapping={r['instrument_id']:r for r in read('old-soak-mapping')}
def levels(row,side,metadata=meta,mp=mapping):
    i=row['instrument_id'];m=metadata[i];factor=D(mp[i]['canonical_base_units_per_venue_base_unit']) if mp else D(1)
    tick=D(m['price_tick_size'])/factor;step=D(m['quantity_step_size'])*D(m['contract_multiplier'])*factor
    return [(D(row[f'{side}_price_{n:02}'])*tick,D(row[f'{side}_qty_{n:02}'])*step) for n in range(1,min(row['stored_depth'],10)+1) if int(row[f'{side}_qty_{n:02}'])>0]
def cost(book,q):
    rem=q;amount=D(0)
    for p,size in book:
        use=min(rem,size);amount+=use*p;rem-=use
        if rem==0:break
    return amount,rem
def pair(la,sb,lb,sa,target=D('499475.550671794615653563758054043254582688177')):
    if not all((la,sb,lb,sa)):return {'error':'empty side'}
    q=min(sum(s for p,s in la),sum(s for p,s in sb));long,_=cost(la,q);short,_=cost(sb,q)
    qrt=min(q,sum(s for p,s in lb),sum(s for p,s in sa));longrt,_=cost(la,qrt);shortrt,_=cost(sb,qrt);lexit,_=cost(lb,qrt);sexit,_=cost(sa,qrt)
    loss=longrt-lexit+sexit-shortrt
    qt=target/la[0][0];bl,rl=cost(la,qt);bs,rs=cost(sb,qt)
    return {'matched_entry_base_qty':q,'long_ask_notional_USDT':long,'short_bid_notional_USDT':short,'target_each_leg_notional_USDT':target,'target_qty_at_long_BBO':qt,'target_fits_visible_10_levels':rl==0 and rs==0,'long_unfilled_base':rl,'short_unfilled_base':rs,'entry_BBO_short_minus_long_bps':(sb[0][0]/la[0][0]-1)*10000,'all_four_sides_visible_matched_qty':qrt,'static_roundtrip_bidask_loss_USDT_for_that_qty':loss,'static_bidask_loss_bps_of_each_long_notional':loss/longrt*10000 if longrt else None}
latest={r['instrument_id']:r for r in read('old-candidate-latest-books')};sync={(r['instrument_id'],r['minute_time']):r for r in read('old-candidate-synchronized-books')};cands=json.loads((P/'book-candidates.json').read_text())
out=[]
for r in cands:
    l,s=r['long_id'],r['short_id']
    if l not in latest or s not in latest:continue
    t=min(latest[l]['minute_time'],latest[s]['minute_time']);lr=sync.get((l,t));sr=sync.get((s,t))
    v={'market':r['market'],'long_venue':r['long_venue'],'short_venue':r['short_venue'],'anchor_utc':t,'current':False,'both_rows_present':bool(lr and sr),'long_latest_utc':latest[l]['minute_time'],'short_latest_utc':latest[s]['minute_time']}
    if lr and sr:
        v.update(pair(levels(lr,'ask'),levels(sr,'bid'),levels(lr,'bid'),levels(sr,'ask')))
        cap=min(D(v['long_ask_notional_USDT']),D(v['short_bid_notional_USDT']))
        # Optimistic capacity bound: no slippage/transfer costs or idle-cash income.
        v['optimistic_90d_APR_pct_on_whole_1m_at_visible_capacity']=cap*(D(r['mean_daily_spread_per_N'])*365-D(r['roundtrip_fee_per_N'])*365/90)/D('1000000')*100
    out.append(v)
prodmeta={r['instrument_id']:r for r in read('production-instruments')};prod={r['instrument_id']:r for r in read('production-latest-books')};bt=[]
for spot in (1,3):
    for perp in (5,6,7):
        lr,sr=prod[spot],prod[perp]
        v={'spot_venue':prodmeta[spot]['exchange'],'short_perp_venue':prodmeta[perp]['exchange'],'spot_anchor_utc':lr['minute_time'],'perp_anchor_utc':sr['minute_time']}
        v.update(pair(levels(lr,'ask',prodmeta,None),levels(sr,'bid',prodmeta,None),levels(lr,'bid',prodmeta,None),levels(sr,'ask',prodmeta,None),D('499251.123315027458811782326510234648027958063')))
        bt.append(v)
(P/'books-analysis.json').write_text(json.dumps({'historical_candidates':out,'current_BTC_spot_perp':bt},default=str,indent=2)+'\n')
for v in out[:10]+[x for x in out if x['market'] in ('ATOM-USDT-PERP','DOT-USDT-PERP')]:print(v)
