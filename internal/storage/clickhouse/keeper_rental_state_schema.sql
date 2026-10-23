-- Optional typed rental-state extension; existing seven tables are unchanged.

CREATE TABLE IF NOT EXISTS jl_keeper_rental_state_observation
(
    capture_id UUID,
    capture_started_at DateTime64(6, 'UTC'),
    observation_index UInt32,
    contract_address FixedString(21),
    renter FixedString(21),
    receiver FixedString(21),
    resource_type UInt8,
    candidate_origin_tx Nullable(FixedString(32)),
    candidate_origin_event_index Nullable(UInt32),
    caller_address FixedString(21),
    scheduled_at DateTime64(6, 'UTC'),
    request_started_at Nullable(DateTime64(6, 'UTC')),
    response_received_at Nullable(DateTime64(6, 'UTC')),
    available_at DateTime64(6, 'UTC'),
    endpoint_view String,
    state_binding String,
    head_before_number Nullable(UInt64),
    head_before_hash Nullable(FixedString(32)),
    head_before_time Nullable(DateTime64(6, 'UTC')),
    head_after_number Nullable(UInt64),
    head_after_hash Nullable(FixedString(32)),
    head_after_time Nullable(DateTime64(6, 'UTC')),
    implementation_address Nullable(FixedString(21)),
    implementation_code_hash Nullable(FixedString(32)),
    implementation_checked_at Nullable(DateTime64(6, 'UTC')),
    identity_status String,
    status String,
    api_success Nullable(Bool),
    tvm_result String,
    security_deposit_sun Nullable(UInt256),
    rent_index Nullable(UInt256),
    liquidatable Nullable(Bool),
    energy_used Nullable(UInt64),
    energy_penalty Nullable(UInt64),
    error_code String,
    error_message String,
    request_payload_hash Nullable(FixedString(32)),
    response_payload_hash Nullable(FixedString(32))
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(capture_started_at)
ORDER BY (capture_id, observation_index);

CREATE TABLE IF NOT EXISTS jl_keeper_rental_state_batch
(
    capture_id UUID,
    capture_started_at DateTime64(6, 'UTC'),
    row_count UInt32,
    digest FixedString(32)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(capture_started_at)
ORDER BY capture_id;
