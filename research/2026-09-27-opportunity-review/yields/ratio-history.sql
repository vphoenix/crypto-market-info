SELECT yield_route_id,observation_time,collected_at,exposure_ratio,rate,rate_kind,availability,source_payload_hash,block_height,block_hash,finality FROM crypto_market_info.yield_observation FINAL WHERE observation_time >= now('UTC')-INTERVAL 30 DAY AND yield_route_id IN (SELECT yield_route_id FROM crypto_market_info.yield_route FINAL WHERE provider IN ('BENQI','Ankr')) ORDER BY yield_route_id,observation_time
SETTINGS readonly=1,output_format_json_quote_decimals=1
FORMAT JSON
