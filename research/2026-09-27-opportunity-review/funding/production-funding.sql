SELECT * FROM crypto_market_info.funding_rate_hourly FINAL WHERE hour_time>=toDateTime('2026-09-19 16:00:00','UTC') ORDER BY instrument_id,hour_time
