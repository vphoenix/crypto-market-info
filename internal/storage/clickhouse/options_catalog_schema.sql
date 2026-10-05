CREATE TABLE IF NOT EXISTS options_catalog_scope_observation (
 observation_id UUID, scope String, source_kind LowCardinality(String), source_url String,
 requested_at DateTime64(6,'UTC'), observed_at DateTime64(6,'UTC'), payload_hash String, status LowCardinality(String), raw_count UInt32,
 native_ids Array(UInt64), symbols Array(String), instrument_ids Array(UInt32), definition_hashes Array(FixedString(64)), rule_ids Array(FixedString(64)), states Array(String), active Array(Bool),
 excluded_symbols Array(String), excluded_reasons Array(String), row_hash FixedString(64)
) ENGINE=ReplacingMergeTree PARTITION BY toYYYYMM(observed_at) ORDER BY (observation_id,scope);
CREATE TABLE IF NOT EXISTS options_lifecycle_observation (
 observation_id UUID, kind LowCardinality(String), source_channel String, epoch UUID, ingress_sequence UInt64,
 source_time Nullable(DateTime64(6,'UTC')), received_at DateTime64(6,'UTC'), payload_hash FixedString(64), symbol String, state String, index_id String,
 locked Nullable(Bool), maintenance Nullable(Bool), lock_mode String, locked_indices Array(String), row_hash FixedString(64)
) ENGINE=ReplacingMergeTree PARTITION BY toYYYYMM(received_at) ORDER BY observation_id;
CREATE TABLE IF NOT EXISTS options_collection_plan (
 plan_id UUID, session_id UUID, revision UInt64, previous_plan_id UUID, previous_plan_hash String, config_hash FixedString(64),
 created_at DateTime64(6,'UTC'), effective_minute DateTime64(0,'UTC'),
 instrument_ids Array(UInt32), definition_hashes Array(FixedString(64)), run_ids Array(UUID), observation_ids Array(UUID),
 pending_symbols Array(String), pending_reasons Array(String), excluded_symbols Array(String), excluded_reasons Array(String), row_hash FixedString(64)
) ENGINE=ReplacingMergeTree ORDER BY (session_id,revision);
CREATE TABLE IF NOT EXISTS options_catalog_quality_evidence_minute (
 run_id UUID, instrument_id UInt32, minute_time DateTime64(0,'UTC'), batch_id FixedString(64),
 state_ids Array(Nullable(UUID)), state_kinds Array(UInt8), rule_observation_ids Array(Nullable(UUID)), platform_ids Array(Nullable(UUID)),
 maintenance_ids Array(Nullable(UUID)), lock_ids Array(Nullable(UUID)), lifecycle_epochs Array(UUID), lifecycle_confirmed Array(Nullable(DateTime64(6,'UTC'))), row_hash FixedString(64)
) ENGINE=ReplacingMergeTree PARTITION BY toYYYYMM(minute_time) ORDER BY (run_id,instrument_id,minute_time,batch_id);
CREATE TABLE IF NOT EXISTS options_catalog_live_minute_commit (
 run_id UUID, minute_time DateTime64(0,'UTC'), batch_id FixedString(64), plan_id UUID, plan_hash FixedString(64), run_hash FixedString(64), prepared_at DateTime64(6,'UTC'),
 instrument_ids Array(UInt32), member_hashes Array(FixedString(64)), evidence_hashes Array(FixedString(64)), anchor_count UInt32, delta_count UInt32, index_ids Array(String), index_hashes Array(FixedString(64))
) ENGINE=ReplacingMergeTree PARTITION BY toYYYYMM(minute_time) ORDER BY (run_id,minute_time);
