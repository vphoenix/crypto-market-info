package mysql

import (
	"context"
	"testing"

	"github.com/google/uuid"
)

func TestAcceptedBootstrapGapIsPinnedAndNeverMakesBadBatchReadable(t *testing.T) {
	c := familyClient(t, "crypto_market_info_justlend_keeper")
	ctx := context.Background()
	b := keeperFixture(t)
	if e := c.WriteKeeperBatch(ctx, b); e != nil {
		t.Fatal(e)
	}
	if _, e := c.db.ExecContext(ctx, "DELETE FROM "+c.table("jl_keeper_rental_event")+" WHERE capture_id=?", b.Capture.CaptureId[:]); e != nil {
		t.Fatal(e)
	}
	if e := c.VerifyMigratedDomain(ctx, "jl_keeper_capture"); e == nil {
		t.Fatal("unacknowledged loss passed")
	}
	r, e := modelRecord(b.Capture)
	if e != nil {
		t.Fatal(e)
	}
	g := AcceptedBootstrapGap{Run: uuid.New(), Database: c.database, Family: "crypto_market_info_justlend_keeper", Table: "jl_keeper_capture", Row: MigrationRow(r), Reason: "user accepts fixture historical loss", Evidence: "fixture-original-declaration"}
	t.Cleanup(func() {
		c.db.ExecContext(ctx, "DELETE FROM cmi_storage.bootstrap_source_gap WHERE database_name=?", c.database)
	})
	if e = RegisterBootstrapGap(ctx, c.db, g); e != nil {
		t.Fatal(e)
	}
	if e = RegisterBootstrapGap(ctx, c.db, g); e != nil {
		t.Fatal("idempotent acknowledgement", e)
	}
	if e = c.VerifyMigratedDomain(ctx, g.Table); e != nil {
		t.Fatal("exact accepted historical gap", e)
	}
	if _, e = c.KeeperBatch(ctx, b.Capture); e == nil {
		t.Fatal("accepted gap fabricated a complete batch")
	}
	if e = finalizeMigrationTables(ctx, c.db, g.Run, []Table{}); e == nil {
		t.Fatal("accepted loss opened complete history")
	}
	bad := g
	bad.Row = MigrationRow{}
	for k, v := range g.Row {
		bad.Row[k] = v
	}
	bad.Row["reason"] = "changed original"
	if e = RegisterBootstrapGap(ctx, c.db, bad); e == nil {
		t.Fatal("changed original acknowledgement accepted")
	}
	if _, e = c.db.ExecContext(ctx, "UPDATE "+c.table(g.Table)+" SET reason=? WHERE capture_id=?", []byte("changed original"), b.Capture.CaptureId[:]); e != nil {
		t.Fatal(e)
	}
	if e = c.VerifyMigratedDomain(ctx, g.Table); e == nil {
		t.Fatal("changed acknowledged marker bypassed validation")
	}
}

func TestBootstrapGapRejectsUnreviewedFactTables(t *testing.T) {
	g := AcceptedBootstrapGap{Run: uuid.New(), Database: "crypto_market_info", Family: "crypto_market_info", Table: "instrument", Reason: "fixture", Evidence: "fixture"}
	if e := RegisterBootstrapGap(context.Background(), nil, g); e == nil {
		t.Fatal("identity loss accepted as a historical marker gap")
	}
}
