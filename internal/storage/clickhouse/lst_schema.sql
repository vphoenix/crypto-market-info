-- Reviewed-design draft only: execute in an isolated scratch database to validate.
-- Do not apply to a running collection database implicitly.
-- Reuse the existing instrument schema/model in the dedicated research database.
-- FixedString(20/32/4) are raw address/hash/selector bytes, not printable hex.
-- Quote USDT on Ethereum has 6 decimals; CEX notional USDT atoms have 8.
-- Trade depth uses instrument tick/lot. Reference mark/index ticks use 1e-8 USDT/ETH.

CREATE TABLE IF NOT EXISTS lst_capture
(
    capture_id UUID,
    manifest_hash FixedString(32),
    capture_kind LowCardinality(String), -- market, logs, funding
    capture_mode LowCardinality(String), -- live, backfill, restart
    source_id LowCardinality(String),
    started_at DateTime64(6, 'UTC'),
    received_at Nullable(DateTime64(6, 'UTC')),
    available_at DateTime64(6, 'UTC'),
    window_from_at Nullable(DateTime64(6, 'UTC')),
    window_to_at Nullable(DateTime64(6, 'UTC')),
    chain_id Nullable(UInt64),
    from_block Nullable(UInt64),
    to_block Nullable(UInt64),
    from_block_hash Nullable(FixedString(32)),
    to_block_hash Nullable(FixedString(32)),
    from_block_time Nullable(DateTime64(6, 'UTC')),
    to_block_time Nullable(DateTime64(6, 'UTC')),
    finality LowCardinality(String), -- head, safe, finalized, orphaned, not_applicable
    canonical Bool,
    revision UInt64,
    status LowCardinality(String), -- complete, partial, failed
    reason String,
    expected_protocol_rows UInt32,
    expected_quote_rows UInt32,
    protocol_rows UInt32,
    quote_rows UInt32,
    request_rows UInt32,
    finalization_rows UInt32,
    claim_rows UInt32,
    funding_rows UInt32,
    fact_digest FixedString(32),
    evidence_root_hash FixedString(32),
    committed Bool
)
ENGINE = ReplacingMergeTree(revision)
PARTITION BY toYYYYMM(started_at)
ORDER BY capture_id;

CREATE TABLE IF NOT EXISTS lst_protocol_state
(
    capture_id UUID,
    observed_at DateTime64(6, 'UTC'),
    chain_id UInt64,
    block_number Nullable(UInt64),
    block_hash Nullable(FixedString(32)),
    block_time Nullable(DateTime64(6, 'UTC')),
    queue_address FixedString(20),
    identity_ok Bool,
    state_status LowCardinality(String),
    reason String,
    steth_total_pooled_eth_wei Nullable(UInt256),
    steth_total_shares_raw Nullable(UInt256),
    one_wsteth_to_steth_wei Nullable(UInt256),
    queue_paused Nullable(Bool),
    bunker_active Nullable(Bool),
    min_request_steth_wei Nullable(UInt256),
    max_request_steth_wei Nullable(UInt256),
    last_request_id Nullable(UInt256),
    last_finalized_request_id Nullable(UInt256),
    unfinalized_steth_wei Nullable(UInt256),
    locked_eth_wei Nullable(UInt256),
    base_fee_per_gas_wei Nullable(UInt256),
    priority_fee_reference_wei Nullable(UInt256),
    requested_at Nullable(DateTime64(6, 'UTC')),
    received_at Nullable(DateTime64(6, 'UTC')),
    source_available_at Nullable(DateTime64(6, 'UTC')),
    available_at DateTime64(6, 'UTC'),
    payload_hashes Array(FixedString(32)),
    row_hash FixedString(32)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(observed_at)
ORDER BY capture_id;

CREATE TABLE IF NOT EXISTS lst_quote_observation
(
    capture_id UUID,
    quote_id FixedString(32),
    observed_at DateTime64(6, 'UTC'),
    quote_role LowCardinality(String), -- entry, followup
    reference_quote_id Nullable(FixedString(32)),
    is_followup_seed Bool,
    target_delay_seconds Nullable(UInt32),
    planned_for_at Nullable(DateTime64(6, 'UTC')),
    route_id LowCardinality(String),
    quote_asset_address FixedString(20), -- Ethereum USDT, not a CEX account balance
    lst_address FixedString(20),
    purchase_budget_usdt_raw Nullable(UInt256), -- 1e-6 USDT; NULL for followup
    buy_weth_out_wei Nullable(UInt256),
    buy_lst_out_raw Nullable(UInt256),
    request_steth_wei Nullable(UInt256),
    request_shares_raw Nullable(UInt256),
    request_parts Nullable(UInt32),
    nominal_redeem_eth_wei Nullable(UInt256), -- entry cap; identical for its followups
    eth_exit_input_wei Nullable(UInt256),
    eth_exit_usdt_out_raw Nullable(UInt256), -- 1e-6 USDT
    quoter_internal_gas Array(UInt64), -- component observations, NOT transaction gas
    buy_status LowCardinality(String),
    buy_reason String,
    conversion_status LowCardinality(String),
    conversion_reason String,
    exit_status LowCardinality(String),
    exit_reason String,
    chain_requested_at Nullable(DateTime64(6, 'UTC')),
    chain_received_at Nullable(DateTime64(6, 'UTC')),
    chain_available_at Nullable(DateTime64(6, 'UTC')),
    chain_payload_hashes Array(FixedString(32)),
    hedge_instrument_id UInt32,
    hedge_quantity_lot Nullable(Int64),
    hedge_eth_wei Nullable(UInt256),
    unhedged_eth_residual_wei Nullable(UInt256),
    hedge_sell_notional_usdt_e8 Nullable(UInt256),
    hedge_buy_notional_usdt_e8 Nullable(UInt256),
    hedge_depth_last_update_id Nullable(UInt64),
    hedge_depth_event_time Nullable(DateTime64(6, 'UTC')),
    hedge_depth_transaction_time Nullable(DateTime64(6, 'UTC')),
    hedge_depth_requested_at Nullable(DateTime64(6, 'UTC')),
    hedge_depth_received_at Nullable(DateTime64(6, 'UTC')),
    hedge_depth_available_at Nullable(DateTime64(6, 'UTC')),
    hedge_depth_payload_hash Nullable(FixedString(32)),
    hedge_status LowCardinality(String),
    hedge_reason String,
    mark_price_tick_e8 Nullable(Int64),
    index_price_tick_e8 Nullable(Int64),
    indicated_funding_rate Nullable(Decimal(38, 18)),
    next_funding_time Nullable(DateTime64(6, 'UTC')),
    mark_source_time Nullable(DateTime64(6, 'UTC')),
    mark_requested_at Nullable(DateTime64(6, 'UTC')),
    mark_received_at Nullable(DateTime64(6, 'UTC')),
    mark_available_at Nullable(DateTime64(6, 'UTC')),
    mark_payload_hash Nullable(FixedString(32)),
    timing_status LowCardinality(String), -- fresh, stale, late, missed, unknown, not_scheduled
    reason String,
    available_at DateTime64(6, 'UTC'),
    row_hash FixedString(32)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(observed_at)
ORDER BY (capture_id, quote_id);

CREATE TABLE IF NOT EXISTS lst_withdrawal_request
(
    capture_id UUID,
    chain_id UInt64,
    queue_address FixedString(20),
    block_number UInt64,
    block_hash FixedString(32),
    block_time DateTime64(6, 'UTC'),
    transaction_hash FixedString(32),
    transaction_index UInt32,
    log_index UInt32,
    abi_version LowCardinality(String),
    request_id UInt256,
    sender FixedString(20),
    initial_owner FixedString(20),
    amount_steth_wei UInt256,
    amount_shares_raw UInt256,
    event_payload_hash FixedString(32),
    available_at DateTime64(6, 'UTC'),
    receipt_status Nullable(UInt8),
    transaction_sender Nullable(FixedString(20)),
    transaction_to Nullable(FixedString(20)),
    input_selector Nullable(FixedString(4)),
    transaction_operation_count Nullable(UInt32),
    gas_sample_class LowCardinality(String), -- direct_single, direct_batch, mixed, missing
    gas_used Nullable(UInt64),
    effective_gas_price_wei Nullable(UInt256),
    receipt_payload_hash Nullable(FixedString(32)),
    receipt_available_at Nullable(DateTime64(6, 'UTC')),
    row_hash FixedString(32)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(block_time)
ORDER BY (capture_id, block_number, transaction_hash, log_index);

CREATE TABLE IF NOT EXISTS lst_withdrawal_finalization
(
    capture_id UUID,
    chain_id UInt64,
    queue_address FixedString(20),
    block_number UInt64,
    block_hash FixedString(32),
    block_time DateTime64(6, 'UTC'),
    transaction_hash FixedString(32),
    transaction_index UInt32,
    log_index UInt32,
    abi_version LowCardinality(String),
    from_request_id UInt256, -- inclusive, the first request finalized by this event
    to_request_id UInt256, -- inclusive
    eth_locked_wei UInt256,
    shares_to_burn_raw UInt256,
    event_timestamp DateTime64(6, 'UTC'),
    event_payload_hash FixedString(32),
    available_at DateTime64(6, 'UTC'),
    row_hash FixedString(32)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(block_time)
ORDER BY (capture_id, block_number, transaction_hash, log_index);

CREATE TABLE IF NOT EXISTS lst_withdrawal_claim
(
    capture_id UUID,
    chain_id UInt64,
    queue_address FixedString(20),
    block_number UInt64,
    block_hash FixedString(32),
    block_time DateTime64(6, 'UTC'),
    transaction_hash FixedString(32),
    transaction_index UInt32,
    log_index UInt32,
    abi_version LowCardinality(String),
    request_id UInt256,
    owner FixedString(20),
    receiver FixedString(20),
    amount_eth_wei UInt256,
    event_payload_hash FixedString(32),
    available_at DateTime64(6, 'UTC'),
    receipt_status Nullable(UInt8),
    transaction_sender Nullable(FixedString(20)),
    transaction_to Nullable(FixedString(20)),
    input_selector Nullable(FixedString(4)),
    transaction_operation_count Nullable(UInt32),
    gas_sample_class LowCardinality(String),
    gas_used Nullable(UInt64),
    effective_gas_price_wei Nullable(UInt256),
    receipt_payload_hash Nullable(FixedString(32)),
    receipt_available_at Nullable(DateTime64(6, 'UTC')),
    row_hash FixedString(32)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(block_time)
ORDER BY (capture_id, block_number, transaction_hash, log_index);

CREATE TABLE IF NOT EXISTS lst_funding_settlement
(
    capture_id UUID,
    instrument_id UInt32,
    funding_time DateTime64(6, 'UTC'),
    funding_rate Decimal(38, 18),
    settlement_mark_price_tick_e8 Nullable(Int64),
    source_id LowCardinality(String),
    requested_at DateTime64(6, 'UTC'),
    received_at DateTime64(6, 'UTC'),
    available_at DateTime64(6, 'UTC'),
    source_payload_hash FixedString(32),
    row_hash FixedString(32)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(funding_time)
ORDER BY (capture_id, instrument_id, funding_time);

-- All child-table membership is frozen per capture; source identity and range
-- are validated by the collector. ReplacingMergeTree does not imply uniqueness.
-- Query capture FINAL (or argMax by revision) before canonical/committed filters.
-- Child rows require their committed capture and matching counts/member digest.
-- Cross-capture event dedup compares immutable protocol fields only.
-- Late receipt enrichment is evidence, not an immutable event conflict.
-- No TTL, implicit migrations, aggregate P&L table, or JSON fact table.
