package mysql

import (
	"strings"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/lendingmarket"
)

func TestJustLendMarketMappingIsRuntimeOnlyAndTimeKeyed(t *testing.T) {
	tables, err := justLendMarketMappings()
	if err != nil {
		t.Fatal(err)
	}
	if len(tables) != 1 {
		t.Fatalf("tables = %d", len(tables))
	}
	table := tables[0]
	resolved, err := Mapping("crypto_market_info", lendingmarket.TableName)
	if err != nil || resolved.DDL != table.DDL {
		t.Fatalf("main family does not resolve the runtime mapping: %v", err)
	}
	if _, err = Mapping("crypto_market_info_perp_soak", lendingmarket.TableName); err == nil {
		t.Fatal("runtime table leaked into another family")
	}
	catalog, err := Mappings()
	if err != nil {
		t.Fatal(err)
	}
	for _, frozen := range catalog.Tables {
		if frozen.Name == lendingmarket.TableName {
			t.Fatal("runtime table must stay outside the frozen migration catalog")
		}
	}
	for _, want := range []string{
		"PRIMARY KEY (`observation_time`,`market_address`)",
		"`borrow_rate_apr` DECIMAL(38,18) NOT NULL", "`payload_hash` BINARY(32) NOT NULL",
		"`observation_time` DATETIME(3) NOT NULL", "ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8",
	} {
		if !strings.Contains(table.DDL, want) {
			t.Fatalf("DDL lacks %q:\n%s", want, table.DDL)
		}
	}
	if strings.Count(table.DDL, "NOT NULL") != len(table.Columns) {
		t.Fatal("every column of a complete-batch row must be NOT NULL")
	}
}

func TestJustLendMarketModelMatchesMapping(t *testing.T) {
	tables, err := justLendMarketMappings()
	if err != nil {
		t.Fatal(err)
	}
	r, err := modelRecord(lendingmarket.Observation{ObservationTime: time.Now().UTC()})
	if err != nil {
		t.Fatal(err)
	}
	if len(r) != len(tables[0].Columns) {
		t.Fatalf("model has %d fields, table %d columns", len(r), len(tables[0].Columns))
	}
	for _, col := range tables[0].Columns {
		if _, ok := r[col.Name]; !ok {
			t.Fatalf("model lacks column %s", col.Name)
		}
	}
}
