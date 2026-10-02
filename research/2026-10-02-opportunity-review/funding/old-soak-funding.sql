SELECT * FROM crypto_market_info_perp_soak.funding_rate_hourly FINAL WHERE hour_time>=toDateTime('2026-09-01 00:00:00','UTC') ORDER BY instrument_id,hour_time
