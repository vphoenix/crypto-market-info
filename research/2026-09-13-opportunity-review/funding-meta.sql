SELECT 'run' AS section, toJSONString(tuple(run_id,mapping_revision,started_at)) AS payload FROM crypto_market_info_perp_soak.perpetual_universe_run FINAL ORDER BY started_at DESC LIMIT 1
