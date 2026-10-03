from export import query,ROOT
import json
analysis=json.loads((ROOT/'analysis.json').read_text())
spikes=[x for x in analysis['spot']['top10'] if float(x['best_bps'])>20]
minutes=','.join("toDateTime('%s','UTC')"%x['minute_time'] for x in spikes)
rows=query('spot-spike-minutes',f'SELECT * FROM crypto_market_info.order_book_minute FINAL WHERE instrument_id IN (1,3) AND minute_time IN ({minutes}) ORDER BY minute_time,instrument_id')
ids=','.join(str(x['id']) for x in rows)
query('spot-spike-deltas',f'SELECT * FROM crypto_market_info.order_book_second_delta FINAL WHERE minute_id IN ({ids}) ORDER BY minute_id,second_offset')
cols=','.join(f'{side}_{kind}_{i:02}' for side in ['bid','ask'] for kind in ['price','qty'] for i in range(1,11))
candidates=analysis['pairs'][:20];ids=sorted({x[f'{side}_id'] for x in candidates for side in ['long','short']})
query('soak-stale-candidate-books',f"SELECT instrument_id,minute_time,valid_bitmap,stored_depth,{cols} FROM crypto_market_info_perp_soak.order_book_minute FINAL WHERE instrument_id IN ({','.join(map(str,ids))}) AND minute_time>=toDateTime('2026-09-25 04:00:00','UTC') ORDER BY minute_time DESC LIMIT 1 BY instrument_id")
