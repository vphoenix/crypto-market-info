SELECT * FROM crypto_market_info.yield_route FINAL ORDER BY yield_route_id
SETTINGS readonly=1,output_format_json_quote_decimals=1
FORMAT JSON
