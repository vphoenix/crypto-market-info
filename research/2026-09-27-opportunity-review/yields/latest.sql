SELECT * FROM crypto_market_info.yield_observation FINAL WHERE (yield_route_id, observation_time) IN (SELECT yield_route_id,max(observation_time) FROM crypto_market_info.yield_observation GROUP BY yield_route_id) ORDER BY yield_route_id,tier_no
SETTINGS readonly=1,output_format_json_quote_decimals=1
FORMAT JSON
