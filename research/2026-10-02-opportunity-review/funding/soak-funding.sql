SELECT * FROM crypto_market_info_perp_soak_20260928.funding_rate_hourly FINAL WHERE hour_time>=now('UTC')-INTERVAL 32 DAY ORDER BY instrument_id,hour_time
