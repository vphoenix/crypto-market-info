CREATE TABLE IF NOT EXISTS {database}.source_batch_status (
 source_kind String,source_id String,content_hash String,source_ms Int64,observed_ms Int64,known_from_ms Int64,published_ms Int64,complete UInt8,codec String,instrument_id UInt32,effective_ms Int64,source_count UInt16,provenance_hash String
) ENGINE=MergeTree ORDER BY (source_kind,source_id) SETTINGS fsync_after_insert=1,fsync_part_directory=1,min_rows_to_fsync_after_merge=1;
CREATE TABLE IF NOT EXISTS {database}.funding_forecast_observation (
 observation_id String,instrument_id UInt32,observed_ms Int64,source_ms Int64,funding_ms Int64,rate Decimal(38,18),mark Decimal(38,18),mark_ms Int64,base_index Decimal(38,18),base_index_ms Int64,base_usd_index Decimal(38,18),quote_usd_index Decimal(38,18),quote_index_ms Int64,row_hash String
) ENGINE=MergeTree ORDER BY observation_id SETTINGS fsync_after_insert=1,fsync_part_directory=1,min_rows_to_fsync_after_merge=1;
CREATE TABLE IF NOT EXISTS {database}.cex_trade_bar_1m (
 observation_id String,instrument_id UInt32,observed_ms Int64,source_ms Int64,minute_ms Int64,base_volume Decimal(38,18),is_final UInt8,conversion_hash String,row_hash String
) ENGINE=MergeTree ORDER BY observation_id SETTINGS fsync_after_insert=1,fsync_part_directory=1,min_rows_to_fsync_after_merge=1;
CREATE TABLE IF NOT EXISTS {database}.cex_fee_schedule_rule (
 observation_id String,instrument_id UInt32,observed_ms Int64,source_ms Int64,effective_ms Int64,valid_until_ms Int64,perp_maker Decimal(38,18),perp_taker Decimal(38,18),spot_taker Decimal(38,18),buy_fee_currency String,sell_fee_currency String,spot_maximum Decimal(38,18),perp_maximum Decimal(38,18),position_limit Decimal(38,18),reference_profile String,interest_rule_version String,row_hash String
) ENGINE=MergeTree ORDER BY observation_id SETTINGS fsync_after_insert=1,fsync_part_directory=1,min_rows_to_fsync_after_merge=1;
CREATE TABLE IF NOT EXISTS {database}.cex_collateral_risk_rule (
 observation_id String,instrument_id UInt32,observed_ms Int64,source_ms Int64,effective_ms Int64,valid_until_ms Int64,model_version String,reference_profile String,model_hash String,row_hash String
) ENGINE=MergeTree ORDER BY observation_id SETTINGS fsync_after_insert=1,fsync_part_directory=1,min_rows_to_fsync_after_merge=1;
CREATE TABLE IF NOT EXISTS {database}.cex_book_minute_commit (
 source_id String,instrument_id UInt32,minute_ms Int64,stored_depth UInt8,content_hash String,delta_seconds Array(UInt8),codec String
) ENGINE=MergeTree ORDER BY source_id SETTINGS fsync_after_insert=1,fsync_part_directory=1,min_rows_to_fsync_after_merge=1;
CREATE TABLE IF NOT EXISTS {database}.public_observation_provenance (
 source_kind String,source_id String,source_order UInt16,source_url String,payload_hash FixedString(64),time_basis LowCardinality(String),source_ms Int64,observed_ms Int64
) ENGINE=ReplacingMergeTree ORDER BY (source_kind,source_id,source_order) SETTINGS fsync_after_insert=1,fsync_part_directory=1,min_rows_to_fsync_after_merge=1;
CREATE TABLE IF NOT EXISTS {database}.okx_public_capital_parameters (
 observation_id String,instrument_id UInt32,observed_ms Int64,source_ms Int64,effective_ms Int64,valid_until_ms Int64,base String,borrow_leverage Decimal(38,18),model_hash String,
 quote_discount_minimum Array(Decimal(38,18)),quote_discount_maximum Array(Decimal(38,18)),quote_discount_rate Array(Decimal(38,18)),quote_discount_liquidation_penalty Array(Decimal(38,18)),
 base_discount_minimum Array(Decimal(38,18)),base_discount_maximum Array(Decimal(38,18)),base_discount_rate Array(Decimal(38,18)),base_discount_liquidation_penalty Array(Decimal(38,18)),
 perp_minimum Array(Decimal(38,18)),perp_maximum Array(Decimal(38,18)),perp_imr Array(Decimal(38,18)),perp_mmr Array(Decimal(38,18)),perp_maximum_leverage Array(Decimal(38,18)),
 borrow_minimum Array(Decimal(38,18)),borrow_maximum Array(Decimal(38,18)),borrow_imr Array(Decimal(38,18)),borrow_mmr Array(Decimal(38,18)),borrow_maximum_leverage Array(Decimal(38,18)),row_hash String
) ENGINE=MergeTree ORDER BY observation_id SETTINGS fsync_after_insert=1,fsync_part_directory=1,min_rows_to_fsync_after_merge=1;

ALTER TABLE {database}.funding_forecast_observation ADD COLUMN IF NOT EXISTS base_index Decimal(38,18),ADD COLUMN IF NOT EXISTS base_index_ms Int64;

ALTER TABLE {database}.funding_forecast_observation ADD COLUMN IF NOT EXISTS base_usd_index Decimal(38,18),ADD COLUMN IF NOT EXISTS quote_usd_index Decimal(38,18),ADD COLUMN IF NOT EXISTS quote_index_ms Int64;

CREATE TABLE IF NOT EXISTS {database}.source_publication_intents (source_kind String,source_id String,content_hash String,codec String,source_ms Int64,observed_ms Int64,known_from_ms Int64,instrument_id UInt32,effective_ms Int64,source_count UInt16,provenance_hash String) ENGINE=MergeTree ORDER BY (source_kind,source_id) SETTINGS fsync_after_insert=1,fsync_part_directory=1,min_rows_to_fsync_after_merge=1;

ALTER TABLE {database}.source_batch_status ADD COLUMN IF NOT EXISTS source_count UInt16,ADD COLUMN IF NOT EXISTS provenance_hash String;
ALTER TABLE {database}.source_publication_intents ADD COLUMN IF NOT EXISTS source_count UInt16,ADD COLUMN IF NOT EXISTS provenance_hash String;
