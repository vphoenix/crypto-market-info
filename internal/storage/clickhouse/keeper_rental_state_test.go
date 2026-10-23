package clickhouse

import (
	"strings"
	"testing"
)

func TestKeeperRentalStateSchemaSeparateFromLegacy(t *testing.T) {
	old, e := KeeperSchemaStatements("fixture")
	if e != nil {
		t.Fatal(e)
	}
	for _, s := range old {
		if strings.Contains(s, "rental_state") {
			t.Fatal("legacy seven-table schema changed")
		}
	}
	ss, e := KeeperRentalStateSchemaStatements("fixture")
	if e != nil || len(ss) != 2 {
		t.Fatal(e, len(ss))
	}
	if _, e = KeeperRentalStateSchemaStatements("bad-name"); e == nil {
		t.Fatal("unsafe database accepted")
	}
	all := strings.Join(ss, "\n")
	for _, want := range []string{"security_deposit_sun Nullable(UInt256)", "rent_index Nullable(UInt256)", "liquidatable Nullable(Bool)", "row_count UInt32", "digest FixedString(32)", "ORDER BY (capture_id, observation_index)"} {
		if !strings.Contains(all, want) {
			t.Fatal("missing schema semantics", want)
		}
	}
}
