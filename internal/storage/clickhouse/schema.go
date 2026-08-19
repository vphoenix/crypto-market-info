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
