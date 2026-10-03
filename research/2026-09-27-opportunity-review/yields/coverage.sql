SELECT r.provider,r.yield_type,r.price_exposure_asset,count() AS routes FROM crypto_market_info.yield_route AS r FINAL GROUP BY r.provider,r.yield_type,r.price_exposure_asset ORDER BY provider,yield_type
SETTINGS readonly=1,output_format_json_quote_decimals=1
FORMAT JSON
