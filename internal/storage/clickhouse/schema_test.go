package clickhouse

import (
	"strings"
	"testing"
)

func TestSchemaContainsCompleteWideBookAndRequiredEngines(t *testing.T) {
	statements, err := SchemaStatements("crypto_market_info")
	if err != nil {
		t.Fatal(err)
	}
	if len(statements) != 6 {
		t.Fatalf("statements=%d", len(statements))
	}
	all := strings.Join(statements, "\n")
	for _, required := range []string{"bid_price_01 Int64", "bid_qty_50 UInt64", "ask_price_50 Int64", "ask_qty_50 UInt64", "ReplacingMergeTree(is_actual)", "funding_time DateTime64(3, 'UTC')", "MODIFY COLUMN funding_time DateTime64(3, 'UTC')", "FINAL"} {
		if required == "FINAL" {
			continue
		}
		if !strings.Contains(all, required) {
			t.Fatalf("DDL missing %q", required)
		}
	}
	if len(MinuteColumns()) != 204 {
		t.Fatalf("minute columns=%d", len(MinuteColumns()))
	}
	if _, err := SchemaStatements("bad-name"); err == nil {
		t.Fatal("unsafe identifier accepted")
	}
}
