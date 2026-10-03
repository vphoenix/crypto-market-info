import json,datetime,itertools,collections
from decimal import Decimal as D
from pathlib import Path
R=Path(__file__).resolve().parent

def read(name):return json.loads((R/(name+'.json')).read_text())['data']['result']
meta={x['instrument_name']:x for x in read('deribit-USDC-instruments')}
rows=read('deribit-USDC-option-summaries');now=D(max(x['creation_timestamp'] for x in rows));out=[];groups=collections.defaultdict(dict)
for x in rows:
 m=meta[x['instrument_name']]
 if x['base_currency'] not in ['BTC','ETH','SOL'] or not m['is_active'] or m['expiration_timestamp']<=now:continue
 if x['bid_price'] is None or x['ask_price'] is None:continue
 groups[(x['base_currency'],m['expiration_timestamp'])].setdefault(D(str(m['strike'])),{})[m['option_type']]=x
for (base,expiry),ks in groups.items():
 years=(D(expiry)-now)/D(1000*86400*365)
 if years<D(5)/365:continue
 for k1,k2 in itertools.combinations(sorted(ks),2):
  if len(ks[k1])!=2 or len(ks[k2])!=2:continue
  c1,p1=ks[k1]['call'],ks[k1]['put'];c2,p2=ks[k2]['call'],ks[k2]['put']
  legs=[(c1,'ask_price',1),(c2,'bid_price',-1),(p2,'ask_price',1),(p1,'bid_price',-1)]
  debit=sum((D(x[side])*sign for x,side,sign in legs),D(0));spot=D(c1['estimated_delivery_price']);width=k2-k1
  fee=sum((min(spot*D('.0003'),D(x[side])*D('.125')) for x,side,sign in legs),D(0));settlement_allowance=spot*D('.0003')
  apr=(width-debit-fee-settlement_allowance)/(debit+fee)/years if debit+fee>0 else D(-999)
  out.append({'base':base,'expiry':expiry,'k1':k1,'k2':k2,'debit':debit,'width':width,'fees':fee,'assumed_settlement_fees':settlement_allowance,'APR_before_margin':apr,'days':years*365,'legs':[x['instrument_name'] for x,_,_ in legs]})
out.sort(key=lambda x:x['APR_before_margin'],reverse=True)
(R/'box-screen.json').write_text(json.dumps(out,default=str,indent=2)+'\n')
print('BOXES',len(out))
for x in out[:15]:print(json.dumps(x,default=str))
print('FUTURES')
for cur in ['BTC','ETH','USDC']:
 for x in read(f'deribit-{cur}-future-summaries'):
  if x['base_currency'] not in ['BTC','ETH','SOL'] or 'PERPETUAL' in x['instrument_name'] or not x['bid_price']:continue
  m=meta.get(x['instrument_name']) or next(m for m in read(f'deribit-{cur}-instruments') if m['instrument_name']==x['instrument_name'])
  spot=D(x['estimated_delivery_price']);years=(D(m['expiration_timestamp'])-now)/D(1000*86400*365)
  print(x['instrument_name'],x['bid_price'],spot,'days',years*365,'APR_gross',(D(x['bid_price'])/spot-1)/years)
