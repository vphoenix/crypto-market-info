import json,collections,itertools
from decimal import Decimal as D
from pathlib import Path
R=Path(__file__).resolve().parent

def read(n):return json.loads((R/(n+'.json')).read_text())['data']['result']
out=[]
for c in ['BTC','ETH']:
 meta={x['instrument_name']:x for x in read(f'deribit-{c}-instruments')};rows=read(f'deribit-{c}-option-summaries');now=max(D(x['creation_timestamp']) for x in rows);groups=collections.defaultdict(dict);spot=read('depth-'+c+'_USDC');ask=D(spot['asks'][0][0])
 for x in rows:
  m=meta[x['instrument_name']]
  if x['bid_price'] is None or x['ask_price'] is None or not m['is_active']:continue
  groups[m['expiration_timestamp']].setdefault(D(str(m['strike'])),{})[m['option_type']]=x
 for expiry,ks in groups.items():
  years=(D(expiry)-now)/D(1000*86400*365)
  if years<D(5)/365:continue
  for k1,k2 in itertools.combinations(sorted(ks),2):
   if len(ks[k1])!=2 or len(ks[k2])!=2:continue
   legs=[(ks[k1]['call'],'ask_price',1),(ks[k2]['call'],'bid_price',-1),(ks[k2]['put'],'ask_price',1),(ks[k1]['put'],'bid_price',-1)]
   debit=sum((D(x[s])*sign for x,s,sign in legs),D(0));fee=sum((min(D('.0003'),D(x[s])*D('.125')) for x,s,_ in legs),D(0));cost=(debit+fee)*ask*D('1.0005');width=k2-k1
   if cost<=0:continue
   # At-current-index reserve only; actual inverse option delivery fee USD varies.
   payout=width-D('.0003')*D(legs[0][0]['estimated_delivery_price']);apr=(payout-cost)/cost/years
   out.append({'base':c,'expiry':expiry,'days':years*365,'low_strike':k1,'high_strike':k2,'debit_base':debit,'entry_fee_base':fee,'estimated_initial_cost_USDC_before_margin':cost,'pre_fee_fixed_payoff_USD':width,'APR_before_margin_with_approx_delivery':apr,'legs':[x['instrument_name'] for x,_,_ in legs]})
out.sort(key=lambda x:x['APR_before_margin_with_approx_delivery'],reverse=True)
(R/'inverse-box-screen.json').write_text(json.dumps(out,default=str,indent=2)+'\n');print('COUNT',len(out));print(json.dumps(out[:5],default=str,indent=2))
