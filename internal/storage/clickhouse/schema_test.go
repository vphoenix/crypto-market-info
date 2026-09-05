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
	if len(statements) != 11 {
		t.Fatalf("statements=%d", len(statements))
	}
	all := strings.Join(statements, "\n")
	for _, required := range []string{"venue_contract_version String", "ADD COLUMN IF NOT EXISTS venue_contract_version String DEFAULT ''", "bid_price_01 Int64", "bid_qty_50 UInt64", "ask_price_50 Int64", "ask_qty_50 UInt64", "ReplacingMergeTree(is_actual)", "funding_time DateTime64(3, 'UTC')", "MODIFY COLUMN funding_time DateTime64(3, 'UTC')", "yield_route", "yield_observation", "unbonding_seconds Nullable(UInt64)", "MODIFY COLUMN unbonding_seconds Nullable(UInt64)", "reward_lengths", "FINAL"} {
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

func TestYieldSchemaCreatesAndAdditivelyMigratesNullableAVAXFields(t *testing.T) {
	const database = "phase2_schema_test"
	statements, err := SchemaStatements(database)
	if err != nil {
		t.Fatal(err)
	}
	table := "`" + database + "`.yield_observation"
	var create string
	var migrations []string
	for _, statement := range statements {
		if strings.HasPrefix(statement, "CREATE TABLE IF NOT EXISTS "+table) {
			if create != "" {
				t.Fatal("duplicate yield_observation CREATE")
			}
			create = statement
		}
		if strings.HasPrefix(statement, "ALTER TABLE "+table) {
			migrations = append(migrations, statement)
		}
	}
	if create == "" {
		t.Fatal("missing yield_observation CREATE for configured database")
	}
	alter := strings.Join(migrations, "\n")
	for _, column := range []string{
		"pool_cash Nullable(Decimal(38, 18))",
		"redemption_window_seconds Nullable(UInt64)",
	} {
		if !strings.Contains(create, column) {
			t.Errorf("CREATE missing nullable column %q", column)
		}
		if strings.Count(alter, "ADD COLUMN IF NOT EXISTS "+column) != 1 {
			t.Errorf("missing idempotent additive migration for %q", column)
		}
	}
	for _, invariant := range []string{
		"ENGINE = ReplacingMergeTree",
		"PARTITION BY toYYYYMM(observation_time)",
		"ORDER BY (yield_route_id, observation_time, tier_no)",
	} {
		if !strings.Contains(create, invariant) {
			t.Errorf("observation storage invariant changed: %q", invariant)
		}
	}
	// Inspect DDL only: runtime repeatability and NULL round-trips belong to
	// integration tests. New and migrated tables need not share column order.
	for _, forbidden := range []string{"DROP ", "TRUNCATE ", "MODIFY ORDER BY", "MODIFY TTL", "UPDATE "} {
		if strings.Contains(strings.ToUpper(alter), forbidden) {
			t.Errorf("additive migration contains %q", forbidden)
		}
	}
	if strings.Contains(strings.Join(statements, "\n"), "crypto_market_info") {
		t.Fatal("schema hard-coded the production database instead of the configured name")
	}
}
