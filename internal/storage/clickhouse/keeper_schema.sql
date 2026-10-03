-- Applied by justlend-keeper-data init-schema; five independent research tables.
-- Use an isolated crypto_market_info_justlend_keeper database.
-- FixedString address/hash values are raw 21/32 bytes, not printable hex.
-- capture_started_at is frozen with capture_id, including across-month retries.
-- Missing values are NULL. Monetary protocol values are integer sun (1 TRX=1e6).

CREATE TABLE IF NOT EXISTS jl_keeper_capture
(
    capture_id UUID,
    capture_started_at DateTime64(6, 'UTC'),
    config_hash FixedString(32),
    network LowCardinality(String),
    contract_address FixedString(21),
    capture_kind LowCardinality(String), -- events, receipts, probes, costs
    capture_mode LowCardinality(String), -- backfill, bootstrap, live, catchup
    source_id LowCardinality(String), -- no credential-bearing URL
    requested_from Nullable(DateTime64(6, 'UTC')),
    requested_to Nullable(DateTime64(6, 'UTC')),
    event_kind LowCardinality(String), -- scoped stream; empty outside event scans
    coverage_scope LowCardinality(String), -- returned_liquidations, sampled_rental_events, selected_tasks
    solid_height Nullable(UInt64),
    solid_hash Nullable(FixedString(32)),
    available_at DateTime64(6, 'UTC'),
    status LowCardinality(String), -- complete, partial, error, skipped
    reason String,
    expected_tasks UInt32,
    completed_tasks UInt32,
    page_count UInt32,
    pagination_exhausted Bool,
    discovered_candidates UInt32,
    selected_candidates UInt32,
    skipped_candidates UInt32,
    event_rows UInt32,
    event_digest FixedString(32),
    receipt_rows UInt32,
    receipt_digest FixedString(32),
    probe_rows UInt32,
    probe_digest FixedString(32),
    cost_rows UInt32,
    cost_digest FixedString(32),
    evidence_manifest_hash FixedString(32),
    committed Bool,
    CONSTRAINT task_counts CHECK completed_tasks <= expected_tasks,
    CONSTRAINT sample_counts CHECK selected_candidates <= discovered_candidates
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(capture_started_at)
ORDER BY capture_id;

CREATE TABLE IF NOT EXISTS jl_keeper_rental_event
(
    capture_id UUID,
    capture_started_at DateTime64(6, 'UTC'),
    contract_address FixedString(21),
    block_number UInt64,
    block_hash FixedString(32),
    block_time DateTime64(6, 'UTC'),
    finality LowCardinality(String), -- solid only in MVP facts
    tx_id FixedString(32),
    transaction_index Nullable(UInt32), -- block order; never sort transactions by tx hash
    provider_event_index UInt32,
    receipt_log_index Nullable(UInt32),
    position_status LowCardinality(String), -- indexed_only, receipt_verified
    abi_revision LowCardinality(String),
    event_kind Enum8('rent' = 1, 'return' = 2, 'liquidate' = 3),
    renter FixedString(21),
    receiver FixedString(21),
    resource_type UInt8,
    liquidator Nullable(FixedString(21)),
    amount_sun UInt256, -- post-update rent/return balance; liquidated balance for liquidate
    added_amount_sun Nullable(UInt256),
    added_deposit_sun Nullable(UInt256),
    returned_amount_sun Nullable(UInt256),
    returned_deposit_sun Nullable(UInt256),
    usage_rental_sun Nullable(UInt256),
    reward_sun Nullable(UInt256),
    send_back_sun Nullable(UInt256),
    security_deposit_sun Nullable(UInt256), -- extended Rent/Return event balance; legacy event NULL
    rent_index Nullable(UInt256), -- raw protocol index, without business calculation
    request_started_at DateTime64(6, 'UTC'),
    available_at DateTime64(6, 'UTC'),
    payload_hash FixedString(32),
    CONSTRAINT energy_only CHECK resource_type = 1,
    CONSTRAINT verified_position CHECK position_status != 'receipt_verified'
        OR (isNotNull(receipt_log_index) AND isNotNull(transaction_index)),
    CONSTRAINT reward_fields CHECK event_kind != 'liquidate'
        OR (isNotNull(liquidator) AND isNotNull(reward_sun) AND isNotNull(send_back_sun))
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(capture_started_at)
ORDER BY (capture_id, block_number, tx_id, provider_event_index);

CREATE TABLE IF NOT EXISTS jl_keeper_tx_receipt
(
    capture_id UUID,
    capture_started_at DateTime64(6, 'UTC'),
    block_number UInt64,
    block_hash FixedString(32),
    block_time DateTime64(6, 'UTC'),
    finality LowCardinality(String),
    tx_id FixedString(32),
    sender Nullable(FixedString(21)),
    outer_target Nullable(FixedString(21)),
    outer_selector Nullable(FixedString(4)),
    outer_call_value_sun Nullable(UInt256),
    execution_result LowCardinality(String),
    body_complete Bool,
    receipt_complete Bool,
    fee_sun Nullable(UInt64),
    energy_usage_total Nullable(UInt64),
    energy_usage Nullable(UInt64),
    origin_energy_usage Nullable(UInt64),
    energy_fee_sun Nullable(UInt64),
    energy_penalty_total Nullable(UInt64),
    net_usage Nullable(UInt64),
    net_fee_sun Nullable(UInt64),
    multisign_fee_sun Nullable(UInt64),
    memo_fee_sun Nullable(UInt64),
    native_transfer_status LowCardinality(String), -- present, absent_unknown, invalid
    native_transfers Array(Tuple(
        internal_index UInt32, value_index UInt32,
        sender FixedString(21), recipient FixedString(21),
        amount_sun UInt256, rejected Bool)),
    rental_liquidation_log_count UInt32,
    other_log_count UInt32,
    call_class LowCardinality(String), -- direct_liquidate, wrapper_or_mixed, unknown
    request_started_at DateTime64(6, 'UTC'),
    available_at DateTime64(6, 'UTC'),
    body_payload_hash Nullable(FixedString(32)),
    receipt_payload_hash FixedString(32)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(capture_started_at)
ORDER BY (capture_id, block_hash, tx_id);

CREATE TABLE IF NOT EXISTS jl_keeper_probe
(
    capture_id UUID,
    capture_started_at DateTime64(6, 'UTC'),
    probe_index UInt32,
    contract_address FixedString(21),
    renter FixedString(21),
    receiver FixedString(21),
    resource_type UInt8,
    candidate_origin_tx Nullable(FixedString(32)),
    candidate_origin_event_index Nullable(UInt32),
    cohort_id UUID,
    caller_address FixedString(21),
    scheduled_at DateTime64(6, 'UTC'),
    request_started_at Nullable(DateTime64(6, 'UTC')),
    response_received_at Nullable(DateTime64(6, 'UTC')),
    available_at DateTime64(6, 'UTC'),
    endpoint_view LowCardinality(String), -- wallet_latest
    state_binding LowCardinality(String), -- node_latest_unpinned; never promote to solid
    head_before_number Nullable(UInt64),
    head_before_hash Nullable(FixedString(32)),
    head_before_time Nullable(DateTime64(6, 'UTC')),
    head_after_number Nullable(UInt64),
    head_after_hash Nullable(FixedString(32)),
    head_after_time Nullable(DateTime64(6, 'UTC')),
    implementation_address Nullable(FixedString(21)),
    implementation_code_hash Nullable(FixedString(32)),
    implementation_checked_at Nullable(DateTime64(6, 'UTC')),
    identity_status LowCardinality(String), -- verified_manifest, unknown, changed
    status LowCardinality(String), -- success_reward, success_zero, revert, rpc_error, timeout, skipped, unknown
    api_success Nullable(Bool),
    tvm_result LowCardinality(String),
    reward_return_sun Nullable(UInt256),
    reward_log_sun Nullable(UInt256),
    reward_transfer_sun Nullable(UInt256),
    energy_used Nullable(UInt64),
    energy_penalty Nullable(UInt64),
    reward_consistency LowCardinality(String), -- matched, incomplete, mismatch
    error_code LowCardinality(String),
    error_message String,
    request_payload_hash Nullable(FixedString(32)),
    response_payload_hash Nullable(FixedString(32)),
    CONSTRAINT probe_resource CHECK resource_type = 1,
    CONSTRAINT caller_separate CHECK caller_address != renter AND caller_address != receiver,
    CONSTRAINT live_binding CHECK state_binding = 'node_latest_unpinned'
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(capture_started_at)
ORDER BY (capture_id, probe_index);

CREATE TABLE IF NOT EXISTS jl_keeper_cost_observation
(
    capture_id UUID,
    capture_started_at DateTime64(6, 'UTC'),
    observation_index UInt32,
    observation_kind Enum8('chain_resource' = 1, 'trx_usdt_bbo' = 2),
    source_id LowCardinality(String),
    request_started_at DateTime64(6, 'UTC'),
    received_at Nullable(DateTime64(6, 'UTC')),
    available_at DateTime64(6, 'UTC'),
    source_time Nullable(DateTime64(6, 'UTC')),
    source_time_kind LowCardinality(String), -- provider, received
    head_before_number Nullable(UInt64),
    head_before_hash Nullable(FixedString(32)),
    head_after_number Nullable(UInt64),
    head_after_hash Nullable(FixedString(32)),
    state_binding LowCardinality(String), -- node_latest_unpinned, offchain
    energy_fee_sun_per_unit Nullable(UInt64),
    bandwidth_fee_sun_per_byte Nullable(UInt64),
    contract_user_resource_percent Nullable(UInt8),
    contract_origin_energy_limit Nullable(UInt64),
    implementation_address Nullable(FixedString(21)),
    implementation_code_hash Nullable(FixedString(32)),
    symbol LowCardinality(String), -- TRXUSDT only for bbo
    bid_price_usdt Nullable(Decimal(38, 18)),
    bid_qty_trx Nullable(Decimal(38, 18)),
    ask_price_usdt Nullable(Decimal(38, 18)),
    ask_qty_trx Nullable(Decimal(38, 18)),
    status LowCardinality(String), -- ok, partial, error
    reason String,
    payload_hash Nullable(FixedString(32)), -- NULL only if no response; manifest retains attempt
    CONSTRAINT price_asset CHECK observation_kind != 'trx_usdt_bbo' OR symbol = 'TRXUSDT'
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(capture_started_at)
ORDER BY (capture_id, observation_index);
