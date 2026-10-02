SELECT yield_route_id,observation_time,collected_at,tier_no,rate,rate_kind,rate_mode,exposure_ratio,availability,rule_eligibility,remaining_capacity,pool_cash,tvl,source_payload_hash,block_height,block_hash,finality FROM crypto_market_info.yield_observation FINAL WHERE observation_time >= now('UTC')-INTERVAL 30 DAY ORDER BY yield_route_id,tier_no,observation_time
SETTINGS readonly=1,max_threads=2,max_execution_time=60,output_format_json_quote_decimals=1
FORMAT JSON
