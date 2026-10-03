-- Live research schema; apply only to the isolated Reserve database.
-- Also create dex_log and dex_tx_receipt using their existing definitions in
-- internal/storage/clickhouse/dex_schema.go; no dex_block or Sky quote table.
-- Hash/address FixedString values are binary bytes, not hexadecimal text.

CREATE TABLE IF NOT EXISTS reserve_capture
(
    chain_id UInt64,
    manifest_hash FixedString(32),
    capture_id FixedString(32),
    batch_id FixedString(32),
    capture_kind Enum8('logs' = 1, 'snapshot' = 2),
    capture_mode Enum8('live' = 1, 'backfill' = 2, 'research' = 3),
    from_block UInt64,
    from_hash FixedString(32),
    to_block UInt64,
    to_hash FixedString(32),
    from_time DateTime64(6, 'UTC'),
    to_time DateTime64(6, 'UTC'),
    received_at DateTime64(6, 'UTC'),
    available_at DateTime64(6, 'UTC'),
    canonical Bool,
    finality Enum8('head' = 1, 'safe' = 2, 'finalized' = 3),
    revision UInt64,
    committed Bool,
    log_coverage LowCardinality(String),
    state_coverage LowCardinality(String),
    quote_coverage LowCardinality(String),
    receipt_coverage LowCardinality(String),
    expected_states UInt32,
    actual_states UInt32,
    expected_quotes UInt32,
    actual_quotes UInt32,
    skipped_routes UInt32,
    log_count UInt32,
    expected_receipts UInt32,
    actual_receipts UInt32,
    state_members FixedString(32),
    quote_members FixedString(32),
    log_members FixedString(32),
    receipt_members FixedString(32),
    plan_hash FixedString(32),
    payload_hash FixedString(32),
    reason LowCardinality(String),
    receipt_refs Array(Tuple(block_hash FixedString(32), tx_hash FixedString(32), receipt_hash FixedString(32), calldata_hash FixedString(32)))
)
ENGINE = ReplacingMergeTree(revision)
PARTITION BY toYYYYMM(to_time)
ORDER BY (chain_id, manifest_hash, capture_id, batch_id);

CREATE TABLE IF NOT EXISTS reserve_folio_state
(
    chain_id UInt64,
    manifest_hash FixedString(32),
    capture_id FixedString(32),
    batch_id FixedString(32),
    block_number UInt64,
    block_hash FixedString(32),
    block_time DateTime64(6, 'UTC'),
    available_at DateTime64(6, 'UTC'),
    payload_hash FixedString(32),
    folio FixedString(20),
    implementation Nullable(FixedString(20)),
    implementation_code_hash Nullable(FixedString(32)),
    protocol_version LowCardinality(String),
    identity_ok Bool,
    state_complete Bool,
    reason LowCardinality(String),
    base_fee_wei Nullable(UInt256),
    total_supply_raw Nullable(UInt256),
    pending_fee_shares_raw Nullable(UInt256),
    share_decimals Nullable(UInt8),
    mint_fee_d18 Nullable(UInt256),
    tvl_fee_per_second_d18 Nullable(UInt256),
    dao_fee_registry Nullable(FixedString(20)),
    dao_fee_numerator Nullable(UInt256),
    dao_fee_denominator Nullable(UInt256),
    dao_fee_floor_d18 Nullable(UInt256),
    minimum_mint_fee_d18 Nullable(UInt256),
    deprecated Nullable(Bool),
    sync_state_change_active Nullable(Bool),
    async_state_change_active Nullable(Bool),
    trusted_filler_registry Nullable(FixedString(20)),
    trusted_filler_enabled Nullable(Bool),
    global_bids_enabled Nullable(Bool),
    rebalance_bids_enabled Nullable(Bool),
    rebalance_nonce Nullable(UInt256),
    price_control Nullable(UInt8),
    rebalance_started_at Nullable(UInt64),
    rebalance_restricted_until Nullable(UInt64),
    rebalance_available_until Nullable(UInt64),
    rebalance_limit_low_d18 Nullable(UInt256),
    rebalance_limit_spot_d18 Nullable(UInt256),
    rebalance_limit_high_d18 Nullable(UInt256),
    next_auction_id Nullable(UInt256),
    auction_id Nullable(UInt256),
    auction_rebalance_nonce Nullable(UInt256),
    auction_start_time Nullable(UInt64),
    auction_end_time Nullable(UInt64),
    basket Array(Tuple(
        token FixedString(20),
        decimals Nullable(UInt8),
        amount_raw Nullable(UInt256),
        in_rebalance Nullable(Bool),
        weight_low_d27 Nullable(UInt256),
        weight_spot_d27 Nullable(UInt256),
        weight_high_d27 Nullable(UInt256),
        initial_price_low_d27 Nullable(UInt256),
        initial_price_high_d27 Nullable(UInt256),
        max_auction_size_raw Nullable(UInt256),
        auction_price_low_d27 Nullable(UInt256),
        auction_price_high_d27 Nullable(UInt256)
    ))
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(block_time)
ORDER BY (chain_id, manifest_hash, folio, block_number, block_hash, capture_id, batch_id);

CREATE TABLE IF NOT EXISTS reserve_route_quote
(
    chain_id UInt64,
    manifest_hash FixedString(32),
    capture_id FixedString(32),
    batch_id FixedString(32),
    block_number UInt64,
    block_hash FixedString(32),
    block_time DateTime64(6, 'UTC'),
    available_at DateTime64(6, 'UTC'),
    payload_hash FixedString(32),
    folio FixedString(20),
    route_id FixedString(32),
    quote_id FixedString(32),
    route_kind Enum8('auction' = 1, 'mint' = 2, 'redeem' = 3, 'cost_reference' = 4),
    budget_token FixedString(20),
    requested_budget_raw UInt256,
    token_in FixedString(20),
    token_out FixedString(20),
    amount_in_raw Nullable(UInt256),
    amount_out_raw Nullable(UInt256),
    auction_id Nullable(UInt256),
    sell_token Nullable(FixedString(20)),
    buy_token Nullable(FixedString(20)),
    requested_max_sell_raw Nullable(UInt256),
    auction_sell_raw Nullable(UInt256),
    auction_buy_raw Nullable(UInt256),
    auction_price_d27 Nullable(UInt256),
    gross_shares_raw Nullable(UInt256),
    fee_shares_raw Nullable(UInt256),
    net_shares_raw Nullable(UInt256),
    basket_amounts Array(Tuple(token FixedString(20), amount_raw UInt256)),
    dex_legs Array(Tuple(
        leg_index UInt16,
        side Enum8('entry' = 1, 'exit' = 2),
        quote_mode Enum8('exact_input' = 1, 'exact_output' = 2, 'identity' = 3),
        token_in FixedString(20),
        token_out FixedString(20),
        requested_raw UInt256,
        amount_in_raw Nullable(UInt256),
        amount_out_raw Nullable(UInt256),
        path_tokens Array(FixedString(20)),
        path_pools Array(FixedString(20)),
        pool_fees Array(UInt32),
        quoter_gas_estimate Nullable(UInt256),
        available_at DateTime64(6, 'UTC'),
        payload_hash FixedString(32),
        status LowCardinality(String)
    )),
    shared_pools Array(FixedString(20)),
    expected_legs UInt16,
    successful_legs UInt16,
    within_budget Nullable(Bool),
    quality Enum8('incomplete' = 1, 'indicative_overlap' = 2, 'quoted_complete' = 3),
    status LowCardinality(String),
    reason LowCardinality(String)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(block_time)
ORDER BY (chain_id, manifest_hash, folio, block_number, block_hash, capture_id, batch_id, quote_id);

-- Application invariants (not all can be expressed as ClickHouse CHECKs):
-- 1. Snapshot capture from_block/hash == to_block/hash. State and quote anchors
--    match that capture and contain only the referenced committed batch.
-- 2. Logs have their own per-event anchors; a logs capture is not a price sample.
-- 3. Native UInt256/NULL stays exact through the driver; no Float64 intermediates.
-- 4. basket retains totalAssets()/toAssets() token order and ALL nonzero assets.
-- 5. Each leg is hash-pinned; missing/failed legs cannot yield quoted_complete.
-- 6. shared_pools != [] prevents quoted_complete; protocol permission failure,
--    active state change and insufficient basket coverage also prevent it.
-- 7. Inactive rebalance deadline does not automatically close its live auction.
-- 8. capture committed is written LAST after facts, hashes and counts are saved.
--    Coverage values are one of complete/partial/missing/not_requested.
-- 9. FIRST pick latest revision per capture_id+batch_id, THEN filter canonical /
--    finalized / committed. Select a batch explicitly; never union facts from
--    separate attempts into a synthetic complete quote.
-- 10. Retries of a stored batch repeat identical row bytes. A fresh RPC attempt
--     gets a different batch_id and preserves its real availability timestamp.
-- 11. cost_reference rows use folio=20 zero bytes, no associated Folio state,
--     and are excluded from strategy counts/profit totals.
-- 12. dex_tx_receipt is a shared immutable first-validated fact keyed by
--     chain_id+block_hash+tx_hash, NOT joined through its original batch_id.
--     receipt_members hashes sorted (chain_id,block_hash,tx_hash,receipt_hash,
--     calldata_hash), excluding observation times and original capture identity.
-- 13. Reorg invalidates ALL capture/batch revisions referencing the orphaned
--     branch. Fallback to an older successful batch must never revive that branch.
-- 14. Activity counts de-duplicate dex_log across captures by chain/block/tx/log.
