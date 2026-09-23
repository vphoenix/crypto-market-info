package clickhouse

import (
	"context"
	"fmt"
	"strings"
)

// DerivativeSchemaStatements is opt-in through InitDerivativeSchema or the
// enabled options task. Foundation commits remain explicitly offline/book-only.
func DerivativeSchemaStatements(database string) ([]string, error) {
	if !identifierPattern.MatchString(database) {
		return nil, fmt.Errorf("invalid database identifier")
	}
	db := "`" + database + "`."
	ddl := func(name, cols, partition, key string) string {
		p := ""
		if partition != "" {
			p = "\nPARTITION BY " + partition
		}
		return "CREATE TABLE IF NOT EXISTS " + db + name + "\n(\n" + cols + "\n)\nENGINE = ReplacingMergeTree" + p + "\nORDER BY " + key
	}
	quality := []string{
		"instrument_id UInt32", "minute_time DateTime('UTC')", "batch_id FixedString(64)", "signed Bool", "row_hash FixedString(64)",
	}
	for _, s := range []string{"sampled", "stream_valid", "replay_valid", "market_known", "market_open"} {
		quality = append(quality, s+"_bitmap UInt64")
	}
	for _, s := range []string{"source_times", "received_times", "captured_times", "last_snapshot_times", "connection_confirmed_times", "rule_published_times", "market_state_times"} {
		quality = append(quality, s+" Array(Nullable(DateTime64(6, 'UTC')))")
	}
	quality = append(quality, "connection_epochs Array(Nullable(UUID))", "change_ids Array(UInt64)", "trading_rule_ids Array(Nullable(FixedString(64)))", "market_state_bases Array(UInt8)", "reasons Array(UInt8)", "bid_level_counts Array(UInt8)", "ask_level_counts Array(UInt8)")
	for _, s := range []string{"source_times", "received_times", "captured_times", "last_snapshot_times", "connection_confirmed_times", "rule_published_times", "market_state_times", "connection_epochs", "change_ids", "trading_rule_ids", "market_state_bases", "reasons", "bid_level_counts", "ask_level_counts"} {
		quality = append(quality, "CONSTRAINT "+s+"_60 CHECK length("+s+") = 60")
	}
	return []string{
		ddl("derivative_contract_spec", `instrument_id UInt32,
native_instrument_id UInt64,
creation_time DateTime64(3, 'UTC'),
definition_hash FixedString(64),
normalization_version String,
source_instrument_type LowCardinality(String),
native_amount_kind LowCardinality(String),
native_amount_currency LowCardinality(String),
source_contract_size Decimal(38,18),
index_id String,
settlement_semantics_id String,
option_type Nullable(String),
strike Nullable(Decimal(38,18)),
strike_currency Nullable(String),
leg_instrument_ids Array(UInt32),
leg_signed_ratios Array(Int32),
leg_amount_per_combo Array(Decimal(38,18)),
definition_evidence_hash FixedString(64),
CONSTRAINT leg_lengths CHECK length(leg_instrument_ids)=length(leg_signed_ratios) AND length(leg_instrument_ids)=length(leg_amount_per_combo)`, "", "instrument_id"),
		ddl("derivative_trading_rule", `trading_rule_id FixedString(64),
instrument_id UInt32,
source_tick_size Decimal(38,18),
min_trade_amount Decimal(38,18),
amount_step Decimal(38,18),
band_above_prices Array(Decimal(38,18)),
band_tick_sizes Array(Decimal(38,18)),
observed_at DateTime64(6, 'UTC'),
known_from DateTime64(6, 'UTC'),
effective_from DateTime64(6, 'UTC'),
effective_time_basis LowCardinality(String),
source_url String,
payload_hash FixedString(64),
CONSTRAINT band_lengths CHECK length(band_above_prices)=length(band_tick_sizes)`, "", "trading_rule_id"),
		ddl("derivative_book_minute", `id UInt64,
instrument_id UInt32,
minute_time DateTime('UTC'),
batch_id FixedString(64),
encoding_version UInt8,
stored_depth UInt8,
signed Bool,
valid_bitmap UInt64,
delta_bitmap UInt64,
bid_prices Array(Int64),
bid_qtys Array(UInt64),
ask_prices Array(Int64),
ask_qtys Array(UInt64),
CONSTRAINT encoding CHECK stored_depth=10 AND encoding_version=1,
CONSTRAINT bid_lengths CHECK length(bid_prices)=length(bid_qtys) AND length(bid_prices)<=10,
CONSTRAINT ask_lengths CHECK length(ask_prices)=length(ask_qtys) AND length(ask_prices)<=10`, "toYYYYMM(minute_time)", "(instrument_id,minute_time,batch_id)"),
		ddl("derivative_book_second_delta", `minute_id UInt64,
second_offset UInt8,
batch_id FixedString(64),
bid_change_prices Array(Int64),
bid_change_qtys Array(UInt64),
ask_change_prices Array(Int64),
ask_change_qtys Array(UInt64),
CONSTRAINT second_range CHECK second_offset BETWEEN 1 AND 59,
CONSTRAINT bid_lengths CHECK length(bid_change_prices)=length(bid_change_qtys),
CONSTRAINT ask_lengths CHECK length(ask_change_prices)=length(ask_change_qtys)`, "toYYYYMM(toDateTime(toUInt32(bitShiftRight(minute_id,32))*60, 'UTC'))", "(minute_id,second_offset,batch_id)"),
		ddl("derivative_book_quality_minute", strings.Join(quality, ",\n"), "toYYYYMM(minute_time)", "(instrument_id,minute_time,batch_id)"),
		ddl("derivative_book_foundation_commit", `run_id UUID,
minute_time DateTime('UTC'),
batch_id FixedString(64),
origin LowCardinality(String),
evidence_hash FixedString(64),
prepared_at DateTime64(6, 'UTC'),
instrument_ids Array(UInt32),
member_hashes Array(FixedString(64)),
anchor_count UInt32,
delta_count UInt32,
CONSTRAINT origin_offline CHECK origin IN ('fixture','synthetic'),
CONSTRAINT member_lengths CHECK length(instrument_ids)=length(member_hashes) AND length(instrument_ids)>0`, "toYYYYMM(minute_time)", "(run_id,minute_time)"),
	}, nil
}

func (c *Client) InitDerivativeSchema(ctx context.Context) error {
	statements, err := DerivativeSchemaStatements(c.database)
	if err != nil {
		return err
	}
	for _, sql := range statements {
		if err := c.conn.Exec(ctx, sql); err != nil {
			return fmt.Errorf("initialize derivative schema: %w", err)
		}
	}
	return nil
}
