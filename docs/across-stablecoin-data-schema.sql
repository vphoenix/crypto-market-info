-- Across typed research DDL. Target: isolated crypto_market_info_across database.
-- No database creation or production migration is performed by this file.
-- Hashes/addresses are raw bytes. UInt256 money is token/wei atomic units.
-- Facts are immutable within capture_id; retries repeat identical row content.

CREATE TABLE IF NOT EXISTS across_capture
(
    manifest_hash FixedString(32),
    capture_id FixedString(32),
    chain_id UInt64,
    capture_kind LowCardinality(String), -- logs, probes, receipts
    capture_mode LowCardinality(String), -- backfill, live, catchup, research
    from_block Nullable(UInt64),
    to_block Nullable(UInt64),
    from_hash Nullable(FixedString(32)),
    to_hash Nullable(FixedString(32)),
    started_at DateTime64(6, 'UTC'),
    available_at DateTime64(6, 'UTC'),
    source_id LowCardinality(String), -- credential-free identifier
    status LowCardinality(String), -- complete, partial, error
    reason String,
    expected_tasks UInt32,
    completed_tasks UInt32,
    unknown_event_count UInt32,
    table_ids Array(UInt8), -- 1 deposit, 2 update, 3 fill, 4 refund, 5 receipt, 6 probe
    row_counts Array(UInt32),
    row_digests Array(FixedString(32)),
    evidence_hash FixedString(32), -- archived manifest of exact requests/responses/row members
    canonical Bool,
    finality LowCardinality(String), -- head, safe, finalized, unknown, orphaned
    revision UInt64,
    committed Bool,
    CONSTRAINT member_arrays_equal CHECK length(table_ids) = length(row_counts)
        AND length(row_counts) = length(row_digests),
    CONSTRAINT task_bounds CHECK completed_tasks <= expected_tasks
)
ENGINE = ReplacingMergeTree(revision)
PARTITION BY toYYYYMM(started_at)
ORDER BY (manifest_hash, chain_id, capture_kind, capture_id);

CREATE TABLE IF NOT EXISTS across_deposit
(
    capture_id FixedString(32),
    chain_id UInt64, -- origin chain
    spoke_pool FixedString(20),
    block_number UInt64,
    block_hash FixedString(32),
    block_time DateTime64(6, 'UTC'),
    tx_hash FixedString(32),
    tx_index UInt32,
    log_index UInt32,
    abi_revision LowCardinality(String),
    destination_chain_id UInt64,
    deposit_id UInt256,
    depositor FixedString(32),
    recipient FixedString(32),
    exclusive_relayer FixedString(32),
    input_token FixedString(32),
    output_token FixedString(32),
    input_amount_raw UInt256,
    output_amount_raw UInt256,
    quote_timestamp UInt32,
    fill_deadline UInt32,
    exclusivity_deadline UInt32,
    message String, -- raw bytes, never JSON
    message_hash FixedString(32), -- protocol keccak/message convention, not payload SHA256
    relay_hash FixedString(32), -- exact ABI RelayData + destination chain
    live_received_at Nullable(DateTime64(6, 'UTC')),
    available_at DateTime64(6, 'UTC'),
    payload_hash FixedString(32),
    CONSTRAINT valid_input CHECK input_amount_raw > 0
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(block_time)
ORDER BY (chain_id, spoke_pool, deposit_id, block_hash, tx_hash, log_index, capture_id);

CREATE TABLE IF NOT EXISTS across_deposit_update
(
    capture_id FixedString(32),
    chain_id UInt64,
    spoke_pool FixedString(20),
    block_number UInt64,
    block_hash FixedString(32),
    block_time DateTime64(6, 'UTC'),
    tx_hash FixedString(32),
    tx_index UInt32,
    log_index UInt32,
    abi_revision LowCardinality(String),
    deposit_id UInt256,
    depositor FixedString(32),
    updated_output_amount_raw UInt256,
    updated_recipient FixedString(32),
    updated_message String,
    updated_message_hash FixedString(32),
    depositor_signature String, -- public signature already emitted by protocol, no private key
    live_received_at Nullable(DateTime64(6, 'UTC')),
    available_at DateTime64(6, 'UTC'),
    payload_hash FixedString(32)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(block_time)
ORDER BY (chain_id, spoke_pool, deposit_id, block_hash, tx_hash, log_index, capture_id);

CREATE TABLE IF NOT EXISTS across_fill
(
    capture_id FixedString(32),
    chain_id UInt64, -- destination chain
    spoke_pool FixedString(20),
    block_number UInt64,
    block_hash FixedString(32),
    block_time DateTime64(6, 'UTC'),
    tx_hash FixedString(32),
    tx_index UInt32,
    log_index UInt32,
    abi_revision LowCardinality(String),
    origin_chain_id UInt64,
    deposit_id UInt256,
    input_token FixedString(32),
    output_token FixedString(32),
    input_amount_raw UInt256,
    output_amount_raw UInt256, -- original RelayData amount
    depositor FixedString(32),
    recipient FixedString(32),
    exclusive_relayer FixedString(32),
    fill_deadline UInt32,
    exclusivity_deadline UInt32,
    message_hash FixedString(32),
    repayment_chain_id UInt64,
    repayment_address FixedString(32), -- event relayer; not transaction sender
    updated_recipient FixedString(32),
    updated_message_hash FixedString(32),
    updated_output_amount_raw UInt256, -- actual payment amount in execution event
    fill_type UInt8, -- 0 fast, 1 replaced slow, 2 slow; unknown value fails decoder
    live_received_at Nullable(DateTime64(6, 'UTC')),
    available_at DateTime64(6, 'UTC'),
    payload_hash FixedString(32),
    CONSTRAINT known_fill_type CHECK fill_type <= 2
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(block_time)
ORDER BY (origin_chain_id, deposit_id, chain_id, block_hash, tx_hash, log_index, capture_id);

CREATE TABLE IF NOT EXISTS across_refund
(
    capture_id FixedString(32),
    chain_id UInt64,
    spoke_pool FixedString(20),
    block_number UInt64,
    block_hash FixedString(32),
    block_time DateTime64(6, 'UTC'),
    tx_hash FixedString(32),
    tx_index UInt32,
    log_index UInt32,
    abi_revision LowCardinality(String),
    event_kind LowCardinality(String), -- leaf_execution or deferred_claim
    token FixedString(20),
    root_bundle_id Nullable(UInt32), -- local to this chain/SpokePool
    leaf_id Nullable(UInt32),
    amount_to_return_raw Nullable(UInt256), -- HubPool bridging; not relayer income
    refund_addresses Array(FixedString(20)),
    refund_amounts_raw Array(UInt256),
    deferred_refunds Nullable(Bool),
    caller FixedString(20), -- claim debits caller's accrued credit, may pay another address
    available_at DateTime64(6, 'UTC'),
    payload_hash FixedString(32),
    CONSTRAINT refund_arrays_equal CHECK length(refund_addresses) = length(refund_amounts_raw)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(block_time)
ORDER BY (chain_id, spoke_pool, block_hash, tx_hash, log_index, capture_id);

CREATE TABLE IF NOT EXISTS across_tx_receipt
(
    capture_id FixedString(32),
    chain_id UInt64,
    block_number UInt64,
    block_hash FixedString(32),
    block_time DateTime64(6, 'UTC'),
    tx_hash FixedString(32),
    tx_index UInt32,
    sender FixedString(20),
    recipient Nullable(FixedString(20)),
    tx_type UInt8,
    input_selector Nullable(FixedString(4)),
    calldata_hash FixedString(32),
    tx_value_wei UInt256,
    success Bool,
    gas_used UInt64,
    effective_gas_price_wei UInt256,
    execution_fee_wei UInt256,
    l1_data_fee_wei Nullable(UInt256),
    operator_fee_wei Nullable(UInt256),
    gas_used_for_l1 Nullable(UInt64), -- Arbitrum info only; never add to gas_used again
    total_fee_wei Nullable(UInt256),
    fee_rule LowCardinality(String),
    fee_complete Bool,
    reason String,
    requested_at DateTime64(6, 'UTC'),
    available_at DateTime64(6, 'UTC'),
    transaction_payload_hash FixedString(32),
    receipt_payload_hash FixedString(32),
    CONSTRAINT complete_fee_known CHECK NOT fee_complete OR isNotNull(total_fee_wei)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(block_time)
ORDER BY (chain_id, block_hash, tx_hash, capture_id);

CREATE TABLE IF NOT EXISTS across_order_probe
(
    capture_id FixedString(32),
    probe_id FixedString(32),
    chain_id UInt64, -- destination chain
    origin_chain_id UInt64,
    origin_spoke_pool FixedString(20),
    origin_block_hash FixedString(32),
    deposit_id UInt256,
    relay_hash FixedString(32),
    block_number Nullable(UInt64),
    block_hash Nullable(FixedString(32)),
    block_time Nullable(DateTime64(6, 'UTC')),
    contract_time Nullable(UInt64),
    destination_spoke_pool FixedString(20),
    planned_for_at DateTime64(6, 'UTC'),
    target_delay_ms Nullable(UInt32), -- NULL baseline; otherwise 2000/5000/10000
    requested_at DateTime64(6, 'UTC'),
    available_at DateTime64(6, 'UTC'),
    observation_origin LowCardinality(String), -- live, restart, catchup; never historical first_seen
    probe_status LowCardinality(String), -- ok, late, stale_head, partial, rpc_error, skipped_budget, cancelled_terminal
    fill_status Nullable(UInt8),
    paused_fills Nullable(Bool),
    reason String,
    eth_usdt_ask Nullable(Decimal(38,18)),
    eth_usdt_ask_qty Nullable(Decimal(38,18)),
    eth_price_requested_at Nullable(DateTime64(6, 'UTC')),
    eth_price_available_at Nullable(DateTime64(6, 'UTC')),
    eth_price_payload_hash Nullable(FixedString(32)),
    usdc_usdt_bid Nullable(Decimal(38,18)),
    usdc_usdt_ask Nullable(Decimal(38,18)),
    usdc_usdt_bid_qty Nullable(Decimal(38,18)),
    usdc_usdt_ask_qty Nullable(Decimal(38,18)),
    usdc_price_requested_at Nullable(DateTime64(6, 'UTC')),
    usdc_price_available_at Nullable(DateTime64(6, 'UTC')),
    usdc_price_payload_hash Nullable(FixedString(32)),
    payload_hash FixedString(32)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(requested_at)
ORDER BY (origin_chain_id, deposit_id, relay_hash, probe_id, capture_id);

-- Application/query invariants:
-- 1. Query latest capture revision BEFORE canonical/finality/committed filters.
-- 2. Facts require their committed capture and exact member count/digest verification.
-- 3. Retry same capture repeats frozen rows/times; fresh request has a new capture.
-- 4. Captures can publish partial/error attempts; successful empty logs are complete.
-- 5. Facts de-duplicate across captures by immutable chain/blockHash/tx/log identity.
--    Compare normalized on-chain content only, excluding capture/time/source/hash
--    metadata. Missing optional receipt fee fields can be supplemented; two known
--    conflicting values are an error. Receipt cost counts once by chain/hash/tx.
-- 6. Reorg invalidates ALL capture kinds/attempts touching the old branch; no old
--    successful capture may reappear. Probe eligibility also needs canonical deposit.
-- 7. UInt256 decode is strict (including full-range depositId); EVM bytes32 addresses
--    require twelve zero prefix bytes before taking twenty address bytes.
-- 8. Relay match checks complete original tuple and destination; pair ID alone is weak.
-- 9. Non-exclusive submitter requires contract_time > exclusivity_deadline;
--    filling is allowed at contract_time == fill_deadline, subject to other checks.
-- 10. Speed-up records never overwrite original terms/identity/first availability.
-- 11. Leaf execution is not per-order repayment. Never infer membership by FIFO,
--     next payment time, or matching amount; unknown attribution stays unknown.
-- 12. Reported verified payout needs one-to-one exact USDC Transfer allocation
--     from the raw receipt; payment status is derived, never patched into the event.
--     Claim caller identifies credit; refundAddress identifies receiving account.
-- 13. Missing fee/price/receipt is NULL, not zero. Price is only CEX reference;
--     source timestamps, freshness, BBO quantity and actual execution scope matter.
-- 14. All modes preserve original availability. Backfill/restart/catchup cannot
--     prove historical live detection or a continuous opportunity window.
--     Use earliest canonical live available time, never min across backfill rows.
-- 15. capture evidence freezes request scope, ordered raw hashes, canonical headers,
--     implementation/ABI references and exact sorted table member identities/hashes.
-- 16. Later probes have explicit scheduled/actual times and skipped/late states.
--     Income ceiling sums per-order max(0, spread), not signed route aggregates.
