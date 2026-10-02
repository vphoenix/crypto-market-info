SELECT yield_route_id,count() AS n,min(observation_time) AS first_observation,max(observation_time) AS last_observation,max(collected_at) AS last_collection FROM crypto_market_info.yield_observation FINAL GROUP BY yield_route_id ORDER BY yield_route_id
SETTINGS readonly=1,max_threads=2,max_execution_time=60,output_format_json_quote_decimals=1
FORMAT JSON
