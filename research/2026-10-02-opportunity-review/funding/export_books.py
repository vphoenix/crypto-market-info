import json
from pathlib import Path
from export import query
P=Path(__file__).resolve().parent
a=json.loads((P/'analysis.json').read_text())
pairs=a['old_soak_reference']['pairs']
large=set('BTC ETH SOL XRP BNB DOGE AVAX TRX ADA LTC LINK ATOM SUI DOT NEAR BCH TON'.split())
chosen=pairs[:10]+[r for r in pairs if r['market'].split('-')[0] in large]
ids=sorted({r[k] for r in chosen for k in ('long_id','short_id')}|{r['instrument_id'] for r in a['old_soak_reference']['hedge_reference']})
cols=','.join(f'{side}_{kind}_{i:02}' for side in ['bid','ask'] for kind in ['price','qty'] for i in range(1,11))
latest=query('old-candidate-latest-books',f"SELECT id,instrument_id,minute_time,valid_bitmap,stored_depth,{cols} FROM crypto_market_info_perp_soak.order_book_minute FINAL WHERE instrument_id IN ({','.join(map(str,ids))}) AND minute_time>=toDateTime('2026-09-24 00:00:00','UTC') AND bitTest(valid_bitmap,0) ORDER BY minute_time DESC LIMIT 1 BY instrument_id")
last={r['instrument_id']:r['minute_time'] for r in latest}
at=[]
for r in chosen:
    l,s=r['long_id'],r['short_id']
    if l not in last or s not in last:continue
    t=min(last[l],last[s]);at.extend([(l,t),(s,t)])
pred=' OR '.join(f"(instrument_id={i} AND minute_time=toDateTime('{t}','UTC'))" for i,t in sorted(set(at)))
query('old-candidate-synchronized-books',f"SELECT id,instrument_id,minute_time,valid_bitmap,stored_depth,{cols} FROM crypto_market_info_perp_soak.order_book_minute FINAL WHERE ({pred}) AND bitTest(valid_bitmap,0) ORDER BY instrument_id,minute_time")
(P/'book-candidates.json').write_text(json.dumps(chosen,indent=2)+'\n')
