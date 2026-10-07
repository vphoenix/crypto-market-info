package clickhouse

import (
	"context"
	"fmt"
)

// InitOptionsLiveSchema is opt-in. Existing installations with options disabled
// do not create these tables. Compact tables retain exact legacy read values.
func (c *Client) InitOptionsLiveSchema(ctx context.Context) error {
	if err := c.InitDerivativeSchema(ctx); err != nil {
		return err
	}
	statements := []string{
		`CREATE TABLE IF NOT EXISTS %s (
run_id UUID, run_hash FixedString(64), started_at DateTime64(6,'UTC'), rest_url String, ws_url String, selection LowCardinality(String),
instrument_ids Array(UInt32), symbols Array(String), definition_hashes Array(FixedString(64)), member_index_ids Array(String), index_ids Array(String),
ref_symbols Array(String), ref_bids Array(Int64), ref_asks Array(Int64), ref_source_times Array(DateTime64(6,'UTC')), ref_received_times Array(DateTime64(6,'UTC')), ref_hashes Array(FixedString(64))
) ENGINE=ReplacingMergeTree ORDER BY run_id`,
		`CREATE TABLE IF NOT EXISTS %s (
run_id UUID, attempt_id UUID, instrument_id UInt32, symbol String, scope String, source_url String,
requested_at DateTime64(6,'UTC'), observed_at DateTime64(6,'UTC'), payload_hash String, status LowCardinality(String), definition_hash String, trading_rule_id String, state LowCardinality(String), active Bool, scope_complete Bool, scope_raw_count UInt32, scope_accepted_count UInt32, scope_excluded_count UInt32, row_hash FixedString(64)
) ENGINE=ReplacingMergeTree PARTITION BY toYYYYMM(observed_at) ORDER BY (run_id,attempt_id,instrument_id)`,
		`CREATE TABLE IF NOT EXISTS %s (
index_id LowCardinality(String), minute_time DateTime64(0,'UTC'), batch_id FixedString(64), row_hash FixedString(64),
prices Array(Nullable(Decimal(38,18))), source_times Array(Nullable(DateTime64(6,'UTC'))), received_times Array(Nullable(DateTime64(6,'UTC'))), epochs Array(UUID), states Array(UInt8)
) ENGINE=ReplacingMergeTree PARTITION BY toYYYYMM(minute_time) ORDER BY (index_id,minute_time,batch_id)`,
		`CREATE TABLE IF NOT EXISTS %s (
run_id UUID, minute_time DateTime64(0,'UTC'), batch_id FixedString(64), run_hash FixedString(64), prepared_at DateTime64(6,'UTC'),
instrument_ids Array(UInt32), member_hashes Array(FixedString(64)), anchor_count UInt32, delta_count UInt32, index_ids Array(String), index_hashes Array(FixedString(64))
) ENGINE=ReplacingMergeTree PARTITION BY toYYYYMM(minute_time) ORDER BY (run_id,minute_time)`,
	}
	for n, table := range []string{"options_live_run", "options_metadata_observation", "options_index_minute", "options_live_minute_commit"} {
		if err := c.conn.Exec(ctx, compactDDL(fmt.Sprintf(statements[n], c.table(table)))); err != nil {
			return err
		}
	}
	return nil
}
