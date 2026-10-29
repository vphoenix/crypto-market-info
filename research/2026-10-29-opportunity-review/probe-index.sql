SELECT JSON_OBJECT('i',CAST(index_id AS CHAR),'m',minute_time,'p',HEX(prices)) FROM crypto_market_info.options_index_minute WHERE minute_time='2026-10-28 18:40:00'
