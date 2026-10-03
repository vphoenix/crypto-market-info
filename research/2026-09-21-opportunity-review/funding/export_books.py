"""Only SOON candidates: bounded historical minute anchors and latest depth."""
from export import query
cols=','.join(f'{side}_{kind}_{i:02}' for side in ['bid','ask'] for kind in ['price','qty'] for i in range(1,11))
if __name__=='__main__':
 query('soon-books',f"SELECT instrument_id,minute_time,valid_bitmap,stored_depth,{cols} FROM crypto_market_info_perp_soak.order_book_minute FINAL WHERE instrument_id IN (1218,1219,1220) AND minute_time>=toDateTime('2026-09-14 08:01:00','UTC') AND (toMinute(minute_time)=1 OR minute_time>=now()-INTERVAL 15 MINUTE) ORDER BY minute_time,instrument_id")
