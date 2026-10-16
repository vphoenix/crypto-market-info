package migrate

import (
	"context"
	"github.com/vphoenix/crypto-market-info/internal/storage/codec"
	"github.com/vphoenix/crypto-market-info/internal/storage/mysql"
	"math/big"
	"os"
	"testing"
)

func TestNativeSourcePreservesTypedBinaryAndNullableArrays(t *testing.T) {
	if os.Getenv("CLICKHOUSE_SOURCE_TEST") != "1" {
		t.Skip("read-only native SELECT fixture")
	}
	ctx := context.Background()
	source, e := OpenSource(ctx, SourceConfig{Address: "127.0.0.1:9000"})
	if e != nil {
		t.Fatal(e)
	}
	defer source.Close()
	cols := []struct{ name, typ string }{{"max64", "UInt64"}, {"wide", "UInt256"}, {"rate", "Decimal(38, 18)"}, {"blob", "FixedString(32)"}, {"values", "Array(Tuple(n UInt256, absent Nullable(UInt256), flags Array(UInt8)))"}}
	table := mysql.Table{Name: "fixture"}
	for _, c := range cols {
		typ, e := codec.Parse(c.typ)
		if e != nil {
			t.Fatal(e)
		}
		table.Columns = append(table.Columns, mysql.Column{Name: c.name, SourceType: c.typ, Type: typ})
	}
	query := `SELECT toUInt64('18446744073709551615') AS max64,toUInt256('115792089237316195423570985008687907853269984665640564039457584007913129639935') AS wide,toDecimal128('0.123456789012345678',18) AS rate,toFixedString(unhex(repeat('ff',32)),32) AS blob,CAST([(toUInt256(7),CAST(NULL,'Nullable(UInt256)'),[toUInt8(0),toUInt8(255)])],'Array(Tuple(n UInt256, absent Nullable(UInt256), flags Array(UInt8)))') AS values`
	rows, e := source.conn.Query(ctx, query)
	if e != nil {
		t.Fatal(e)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal(rows.Err())
	}
	row, e := readSourceRow(rows, table)
	if e != nil {
		t.Fatal(e)
	}
	wide := row["wide"].(big.Int)
	if row["max64"].(uint64) != ^uint64(0) || wide.BitLen() != 256 || len(row["blob"].(string)) != 32 {
		t.Fatal("native numeric/binary changed")
	}
	tuple := row["values"].([]any)[0].(map[string]any)
	n := tuple["n"].(big.Int)
	if tuple["absent"] != nil || n.Cmp(big.NewInt(7)) != 0 {
		t.Fatal("native tuple/NULL changed")
	}
	raw, e := mysql.PackMigrationRow(table, row)
	if e != nil {
		t.Fatal(e)
	}
	decoded, e := mysql.UnpackMigrationRow(table, raw)
	if e != nil {
		t.Fatal(e)
	}
	decodedWide := decoded["wide"].(big.Int)
	if decodedWide.Cmp(&wide) != 0 {
		t.Fatal("source spool roundtrip", e)
	}
}
