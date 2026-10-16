package migrate

import (
	"context"
	"os"
	"testing"

	"github.com/vphoenix/crypto-market-info/internal/storage/codec"
	"github.com/vphoenix/crypto-market-info/internal/storage/mysql"
)

// Exercise the production bounds builder and real driver. Literal fixtures
// avoid creating or changing any ClickHouse table. The original review test
// deliberately retains the old direct driver binding as failure evidence.
func TestSourceBoundsRetainExactKeys(t *testing.T) {
	if os.Getenv("CLICKHOUSE_SOURCE_TEST") != "1" {
		t.Skip("read-only ClickHouse test")
	}
	ctx := context.Background()
	source, err := OpenSource(ctx, SourceConfig{Address: "127.0.0.1:9000"})
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	cases := []struct{ name, typ, literal string }{
		{"uint8_control", "UInt8", "toUInt8(255)"},
		{"uint16_control", "UInt16", "toUInt16(65535)"},
		{"uint32_control", "UInt32", "toUInt32(4294967295)"},
		{"string_control", "String", "unhex('005c273fff')"},
		{"low_cardinality_control", "LowCardinality(String)", "toLowCardinality(unhex('005c273fff'))"},
		{"fixed20_control", "FixedString(20)", "toFixedString(unhex('005c273fff'),20)"},
		{"fixed32_control", "FixedString(32)", "toFixedString(unhex('005c273fff'),32)"},
		{"fixed64_control", "FixedString(64)", "toFixedString(unhex('005c273fff'),64)"},
		{"date64_seconds_control", "DateTime64(0, 'UTC')", "toDateTime64('2026-10-01 00:00:00',0,'UTC')"},
		{"uint256_zero", "UInt256", "toUInt256(0)"},
		{"uint256_above64", "UInt256", "toUInt256('18446744073709551616')"},
		{"microseconds_trailing_zero", "DateTime64(6, 'UTC')", "toDateTime64('2026-10-01 00:00:00.123000',6,'UTC')"},
		{"microseconds_epoch", "DateTime64(6, 'UTC')", "toDateTime64('1970-01-01 00:00:00.000001',6,'UTC')"},
		{"uint64_control", "UInt64", "toUInt64('18446744073709551615')"},
		{"uuid_control", "UUID", "toUUID('00000001-0000-0000-0000-000000000001')"},
		{"seconds_control", "DateTime('UTC')", "toDateTime('2026-10-01 00:00:00', 'UTC')"},
		{"uint256", "UInt256", "toUInt256('115792089237316195423570985008687907853269984665640564039457584007913129639935')"},
		{"milliseconds", "DateTime64(3, 'UTC')", "toDateTime64('2026-10-01 00:00:00.123', 3, 'UTC')"},
		{"microseconds", "DateTime64(6, 'UTC')", "toDateTime64('2026-10-01 00:00:00.123456', 6, 'UTC')"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			typ, err := codec.Parse(tc.typ)
			if err != nil {
				t.Fatal(err)
			}
			tab := mysql.Table{Key: []string{"key"}, Columns: []mysql.Column{{Name: "key", SourceType: tc.typ, Type: typ}}}
			rows, err := source.conn.Query(ctx, "SELECT "+tc.literal+" AS key")
			if err != nil {
				t.Fatal(err)
			}
			if !rows.Next() {
				rows.Close()
				t.Fatal("missing fixture")
			}
			row, err := readSourceRow(rows, tab)
			rows.Close()
			if err != nil {
				t.Fatal(err)
			}
			key, err := mysql.MigrationKey(tab, row)
			if err != nil {
				t.Fatal(err)
			}
			bounds, values, err := sourceBounds(tab, nil, key)
			if err != nil {
				t.Fatal(err)
			}
			query := "SELECT count() FROM (SELECT " + tc.literal + " AS key) WHERE " + bounds
			var count uint64
			err = source.conn.QueryRow(ctx, query, values...).Scan(&count)
			if err != nil || count != 1 {
				t.Fatalf("frozen upper bound must retain its own source row: count=%d error=%v", count, err)
			}
			bounds, values, err = sourceBounds(tab, key, key)
			if err != nil {
				t.Fatal(err)
			}
			query = "SELECT count() FROM (SELECT " + tc.literal + " AS key) WHERE " + bounds
			err = source.conn.QueryRow(ctx, query, values...).Scan(&count)
			if err != nil || count != 0 {
				t.Fatalf("exclusive lower bound repeated its own source row: count=%d error=%v", count, err)
			}
		})
	}
}
