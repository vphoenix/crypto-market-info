SELECT JSON_OBJECT('i',CAST(index_id AS CHAR),'p',HEX(prices)) FROM crypto_market_info.options_index_minute WHERE minute_time='2026-10-27 18:00:00'
