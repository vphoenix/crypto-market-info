package clickhouse

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/vphoenix/crypto-market-info/internal/model"
)

var identifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func SchemaStatements(database string) ([]string, error) {
	if !identifierPattern.MatchString(database) {
		return nil, fmt.Errorf("invalid ClickHouse database identifier %q", database)
	}
	db := "`" + database + "`"
	minuteColumns := make([]string, 0, model.BookDepth*4)
	for _, side := range []string{"bid", "ask"} {
		for level := 1; level <= model.BookDepth; level++ {
			minuteColumns = append(minuteColumns,
				fmt.Sprintf("    %s_price_%02d Int64", side, level),
				fmt.Sprintf("    %s_qty_%02d UInt64", side, level))
		}
	}
	return []string{
		"CREATE DATABASE IF NOT EXISTS " + db,
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.instrument
(
    instrument_id UInt32,
    exchange String,
    market_type LowCardinality(String),
    exchange_symbol String,
    base_asset LowCardinality(String),
    quote_asset LowCardinality(String),
    settle_asset Nullable(String),
    contract_multiplier Decimal(38, 18),
    price_tick_size Decimal(38, 18),
    quantity_step_size Decimal(38, 18),
    expiry_time Nullable(DateTime('UTC'))
)
ENGINE = ReplacingMergeTree
ORDER BY instrument_id`, db),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.order_book_minute
(
    id UInt64,
    instrument_id UInt32,
    minute_time DateTime('UTC'),
    valid_bitmap UInt64,
%s
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(minute_time)
ORDER BY (instrument_id, minute_time)`, db, strings.Join(minuteColumns, ",\n")),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.order_book_second_delta
(
    minute_id UInt64,
    second_offset UInt8,
    bid_change_prices Array(Int64),
    bid_change_qtys Array(UInt64),
    ask_change_prices Array(Int64),
    ask_change_qtys Array(UInt64),
    CONSTRAINT second_in_minute CHECK second_offset >= 1 AND second_offset <= 59,
    CONSTRAINT bid_array_lengths CHECK length(bid_change_prices) = length(bid_change_qtys),
    CONSTRAINT ask_array_lengths CHECK length(ask_change_prices) = length(ask_change_qtys)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(toDateTime(toUInt32(bitShiftRight(minute_id, 32)) * 60, 'UTC'))
ORDER BY (minute_id, second_offset)`, db),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.funding_rate_hourly
(
    instrument_id UInt32,
    hour_time DateTime('UTC'),
    funding_time DateTime64(3, 'UTC'),
    rate Decimal(38, 18),
    is_actual Bool
)
ENGINE = ReplacingMergeTree(is_actual)
PARTITION BY toYYYYMM(hour_time)
ORDER BY (instrument_id, hour_time)`, db),
		fmt.Sprintf(`ALTER TABLE %s.funding_rate_hourly
    MODIFY COLUMN funding_time DateTime64(3, 'UTC')`, db),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.yield_route
(
    yield_route_id UInt32,
    provider_type LowCardinality(String),
    provider LowCardinality(String),
    product_code String,
    product_name String,
    yield_type LowCardinality(String),
    deposit_asset_key String,
    position_asset_key String,
    redeem_asset_key String,
    network Nullable(String),
    contract_address Nullable(String),
    price_exposure_asset Nullable(String),
    income_source LowCardinality(String),
    source_url String,
    collection_enabled Bool
)
ENGINE = ReplacingMergeTree
ORDER BY yield_route_id`, db),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.yield_observation
(
    yield_route_id UInt32,
    observation_time DateTime64(3, 'UTC'),
    collected_at DateTime64(3, 'UTC'),
    tier_no UInt16,
    tier_min_amount Decimal(38, 18),
    tier_max_amount Nullable(Decimal(38, 18)),
    tier_mode LowCardinality(String),
    rate Nullable(Decimal(38, 18)),
    rate_kind LowCardinality(String),
    rate_origin LowCardinality(String),
    rate_mode LowCardinality(String),
    reward_asset_keys Array(String),
    reward_component_rates Array(Nullable(Decimal(38, 18))),
    entry_fee_rate Nullable(Decimal(38, 18)),
    exit_fee_rate Nullable(Decimal(38, 18)),
    fixed_penalty_rate Nullable(Decimal(38, 18)),
    performance_fee_rate Nullable(Decimal(38, 18)),
    entry_fee_amount Nullable(Decimal(38, 18)),
    exit_fee_amount Nullable(Decimal(38, 18)),
    fixed_fee_asset_key Nullable(String),
    lock_seconds UInt64,
    unbonding_seconds UInt64,
    rule_principal_loss_mode LowCardinality(String),
    fixed_principal_loss_rate Nullable(Decimal(38, 18)),
    rule_eligibility LowCardinality(String),
    eligibility_reason Nullable(String),
    exposure_ratio Nullable(Decimal(38, 18)),
    capacity Nullable(Decimal(38, 18)),
    remaining_capacity Nullable(Decimal(38, 18)),
    tvl Nullable(Decimal(38, 18)),
    availability LowCardinality(String),
    block_height Nullable(UInt64),
    block_hash Nullable(String),
    finality Nullable(String),
    source_payload_hash Nullable(String),
    CONSTRAINT reward_lengths CHECK length(reward_asset_keys) = length(reward_component_rates)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(observation_time)
ORDER BY (yield_route_id, observation_time, tier_no)`, db),
	}, nil
}

func MinuteColumns() []string {
	columns := []string{"id", "instrument_id", "minute_time", "valid_bitmap"}
	for _, side := range []string{"bid", "ask"} {
		for level := 1; level <= model.BookDepth; level++ {
			columns = append(columns, fmt.Sprintf("%s_price_%02d", side, level), fmt.Sprintf("%s_qty_%02d", side, level))
		}
	}
	return columns
}
