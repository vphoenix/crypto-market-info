import json, pathlib, datetime, itertools
from decimal import Decimal as D, getcontext
getcontext().prec=40
ROOT=pathlib.Path(__file__).resolve().parent

def rows(n): return [json.loads(x,parse_float=D) for x in (ROOT/(n+'.jsonl')).read_text().splitlines()]
def dt(s): return datetime.datetime.fromisoformat(s.replace('Z','+00:00')).replace(tzinfo=datetime.timezone.utc)
def ds(d): return str(d)
I={r['instrument_id']:r for r in rows('instruments')}; S={r['instrument_id']:r for r in rows('specs')}
Q={r['instrument_id']:r for r in rows('latest_quality')}; B={r['instrument_id']:r for r in rows('latest_books')}
E=rows('latest_commit')[0]; last=dt(E['minute_time'])+datetime.timedelta(seconds=59)
idx={r['index_id']:r for r in rows('latest_indexes')}
Deltas={}
for r in rows('latest_deltas'): Deltas.setdefault(int(r['minute_id']),[]).append(r)
books={}; output=[]
for i,b in B.items():
 sides={side:dict(zip(map(int,b[side+'_prices']),map(int,b[side+'_qtys']))) for side in ['bid','ask']}
 seen=0
 for delta in Deltas.get(int(b['id']),[]):
  sec=delta['second_offset'];seen|=1<<sec
  assert int(b['delta_bitmap'])&(1<<sec)
  for side in ['bid','ask']:
   for p,q in zip(delta[side+'_change_prices'],delta[side+'_change_qtys']):
    if int(q): sides[side][int(p)]=int(q)
    else: sides[side].pop(int(p),None)
 assert seen==int(b['delta_bitmap'])
 assert int(b['valid_bitmap'])==int(Q[i]['replay_valid_bitmap'])
 assert int(Q[i]['replay_valid_bitmap'])&(1<<59)
 assert Q[i]['reasons'][59]==0
 books[i]={s:[(D(p)/D(10**8),D(q)/D(10**8)) for p,q in sorted(sides[s].items(),reverse=(s=='bid'))] for s in sides}
 age=(last-dt(Q[i]['source_times'][59])).total_seconds()
 output.append({'instrument_id':i,'symbol':I[i]['exchange_symbol'],'valid_seconds':int(b['valid_bitmap']).bit_count(),'source_time':Q[i]['source_times'][59],'source_age_seconds':str(age),'bids':books[i]['bid'],'asks':books[i]['ask'],'index':idx[S[i]['index_id']]['prices'][59]})
(ROOT/'replayed_latest.json').write_text(json.dumps({'sample_time':last.isoformat(),'books':output},default=str,indent=2))
for r in output: print(r['symbol'], 'bid',r['bids'][:1],'ask',r['asks'][:1],'age',r['source_age_seconds'])

# All prices/quantities from exact decimal/tick paths. No maker execution assumption.
def fill(i,side,amount):
 remaining=amount; value=D(0); optfees=D(0); index=idx[S[i]['index_id']]['prices'][59]
 for p,q in books[i][side]:
  t=min(remaining,q);value+=p*t
  if I[i]['market_type']=='option':
   feebase=(index if S[i]['source_instrument_type']=='linear' else D(1))
   optfees+=min(D('.0003')*feebase,D('.125')*p)*t
  remaining-=t
  if remaining==0:return value,optfees
 return None
families={}
for i in B:
 k=(I[i]['base_asset'],I[i]['settle_asset'],I[i]['expiry_time'])
 f=families.setdefault(k,{'strikes':{}})
 if I[i]['market_type']=='delivery':f['future']=i
 else:f['strikes'].setdefault(S[i]['strike'],{})[S[i]['option_type']]=i
candidates=[]
for key,f in families.items():
 future=f['future'];linear=I[future]['settle_asset']=='USDC';index=idx[S[future]['index_id']]['prices'][59]
 for strike,cp in f['strikes'].items():
  c,p=cp['call'],cp['put']
  for mode in ['long_synthetic_short_future','short_synthetic_long_future']:
   long=mode.startswith('long'); cs,ps,fs=('ask','bid','bid') if long else ('bid','ask','ask')
   # Report BBO upper bound per one base unit; no market-depth fill assumes partial top levels unlimited.
   if not all(books[i][side] for i,side in [(c,cs),(p,ps),(future,fs)]):continue
   C,P,F=[books[i][side][0][0] for i,side in [(c,cs),(p,ps),(future,fs)]]
   qcap=min(books[c][cs][0][1],books[p][ps][0][1],books[future][fs][0][1]/(D(1) if linear else strike))
   if linear:
    gross=(F-strike-C+P) if long else (strike-F+C-P)
    fees=min(D('.0003')*index,D('.125')*C)+min(D('.0003')*index,D('.125')*P)+D('.00035')*F
   else:
    gross=(1-strike/F-C+P) if long else (strike/F-1+C-P)
    fees=min(D('.0003'),D('.125')*C)+min(D('.0003'),D('.125')*P)+D('.00035')*strike/F
   candidates.append({'type':'conversion','family':key,'strike':strike,'direction':mode,'gross_per_base_native':gross,'entry_fees_per_base_native':fees,'net_before_delivery_margin_per_base_native':gross-fees,'native_currency':I[future]['settle_asset'],'gross_per_base_usd_reference':gross*(D(1) if linear else index),'bbo_base_capacity':qcap,'bbo_underlying_notional_usd':qcap*index,'symbols':[I[x]['exchange_symbol'] for x in [c,p,future]]})
 for low,high in itertools.combinations(sorted(f['strikes']),2):
  cl,pl=f['strikes'][low]['call'],f['strikes'][low]['put'];ch,ph=f['strikes'][high]['call'],f['strikes'][high]['put'];width=high-low
  for buy in [True,False]:
   # Long box +cl -pl -ch +ph. Inverse includes a long width-USD future to lock coin payoff.
   legs=[(cl,'ask'),(pl,'bid'),(ch,'bid'),(ph,'ask')] if buy else [(cl,'bid'),(pl,'ask'),(ch,'ask'),(ph,'bid')]
   if any(not books[i][side] for i,side in legs):continue
   bbo=[books[i][side][0][0] for i,side in legs]
   price=bbo[0]-bbo[1]-bbo[2]+bbo[3]
   fside='ask' if buy else 'bid'; fp=books[future][fside][0][0]
   payoff=width if linear else width/fp
   gross=payoff-price if buy else price-payoff
   qcap=min(books[i][side][0][1] for i,side in legs)
   if not linear:qcap=min(qcap,books[future][fside][0][1]/width)
   fees=sum(min(D('.0003')*(index if linear else D(1)),D('.125')*price_) for price_ in bbo)
   if not linear:fees+=D('.00035')*width/fp
   full_qcap=min(sum(q for p,q in books[i][side]) for i,side in legs)
   if not linear:full_qcap=min(full_qcap,sum(q for p,q in books[future][fside])/width)
   candidates.append({'type':'box','family':key,'strikes':[low,high],'direction':'long_box' if buy else 'short_box','gross_per_base_native':gross,'entry_fees_per_base_native':fees,'net_before_delivery_margin_per_base_native':gross-fees,'box_price_per_base_native':price,'payoff_per_base_native':payoff,'native_currency':I[future]['settle_asset'],'bbo_base_capacity':qcap,'bbo_underlying_notional_usd':qcap*index,'full_depth_max_base_capacity':full_qcap,'full_depth_payoff_usd_reference':full_qcap*payoff*(D(1) if linear else index),'symbols':[I[x]['exchange_symbol'] for x in [cl,pl,ch,ph]]})
(ROOT/'candidates.json').write_text(json.dumps(candidates,default=str,indent=2))
print('\nCANDIDATES')
for c in candidates:print(json.dumps(c,default=str))
