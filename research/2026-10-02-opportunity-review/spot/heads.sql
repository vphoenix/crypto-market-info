SELECT instrument_id,max(minute_time) AS last_minute FROM crypto_market_info.order_book_minute FINAL WHERE instrument_id IN (1,3) AND minute_time>=now()-INTERVAL 2 DAY GROUP BY instrument_id
