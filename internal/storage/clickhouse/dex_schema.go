package clickhouse

import (
	"context"
	"fmt"
)

const dexAnchorDDL = `chain_id UInt64, block_number UInt64, block_hash FixedString(32), manifest_hash FixedString(32), batch_id FixedString(32), payload_hash FixedString(32), block_time DateTime64(6,'UTC')`

func DEXSchemaStatements(database string) ([]string, error) {
	if !identifierPattern.MatchString(database) {
		return nil, fmt.Errorf("invalid ClickHouse database identifier")
	}
	specs := []struct{ name, cols, key, engine string }{
		{"dex_block", `parent_hash FixedString(32), base_fee_wei UInt256, fee_recipient FixedString(20), received_at DateTime64(6,'UTC'), available_at DateTime64(6,'UTC'), capture_mode LowCardinality(String), canonical Bool, finality LowCardinality(String), revision UInt64, log_coverage LowCardinality(String), receipt_coverage LowCardinality(String), quote_coverage LowCardinality(String), expected_quotes UInt32, actual_quotes UInt32, log_count UInt32, receipt_count UInt32, quote_members FixedString(32), log_members FixedString(32), receipt_members FixedString(32), committed Bool`, "chain_id,manifest_hash,block_number,block_hash", "ReplacingMergeTree(revision)"},
		{"dex_sky_state", `module_id LowCardinality(String), tin Nullable(UInt256), tout Nullable(UInt256), buf Nullable(UInt256), dai_cash Nullable(UInt256), usdc_pocket_cash Nullable(UInt256), pocket_allowance Nullable(UInt256), vat_live Bool, dai_join_live Bool, dai_join_ward Bool, usds_join_ward Bool, identity_ok Bool, state_complete Bool, reason String`, "chain_id,manifest_hash,block_number,block_hash,batch_id,module_id", "ReplacingMergeTree"},
		{"dex_route_quote", `quote_id FixedString(32), quote_role LowCardinality(String), route_id LowCardinality(String), quote_mode LowCardinality(String), token_in FixedString(20), token_out FixedString(20), requested_amount_raw UInt256, amount_in_raw Nullable(UInt256), amount_out_raw Nullable(UInt256), dust_dai Nullable(UInt256), dust_usds Nullable(UInt256), v3_input_token FixedString(20), v3_output_token FixedString(20), v3_input_raw Nullable(UInt256), v3_output_raw Nullable(UInt256), sqrt_price_after_x96 Nullable(UInt256), ticks_crossed UInt32, quoter_gas_estimate Nullable(UInt256), status LowCardinality(String), reason String, available_at DateTime64(6,'UTC')`, "chain_id,manifest_hash,block_number,block_hash,batch_id,quote_id", "ReplacingMergeTree"},
		{"dex_log", `tx_hash FixedString(32), tx_index UInt32, log_index UInt32, emitter FixedString(20), topics Array(FixedString(32)), data String, event_type LowCardinality(String), removed Bool`, "chain_id,manifest_hash,block_number,block_hash,batch_id,tx_hash,log_index", "ReplacingMergeTree"},
		{"dex_tx_receipt", `tx_hash FixedString(32), tx_index UInt32, sender FixedString(20), recipient FixedString(20), has_recipient Bool, tx_type UInt8, value_raw UInt256, input_selector Nullable(FixedString(4)), calldata_hash FixedString(32), status UInt8, gas_used UInt64, effective_gas_price UInt256, receipt_log_count UInt32, receipt_hash FixedString(32), available_at DateTime64(6,'UTC')`, "chain_id,block_number,block_hash,tx_hash", "ReplacingMergeTree(available_at)"},
	}
	out := make([]string, 0, 5)
	for _, s := range specs {
		out = append(out, fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s`.`%s` (%s, %s) ENGINE = %s PARTITION BY toYYYYMM(block_time) ORDER BY (%s)", database, s.name, dexAnchorDDL, s.cols, s.engine, s.key))
	}
	return out, nil
}
func (c *Client) InitDEXSchema(ctx context.Context) error {
	ss, e := DEXSchemaStatements(c.database)
	if e != nil {
		return e
	}
	for _, s := range ss {
		if e = c.conn.Exec(ctx, s); e != nil {
			return e
		}
	}
	return nil
}
