package mysql

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
)

func TestDiscoveryGroupRollbackRetryAndPhysicalMultiplicity(t *testing.T) {
	c := integrationClient(t)
	ctx := context.Background()
	if err := c.InitOKXPairSchema(ctx); err != nil {
		t.Fatal(err)
	}
	f := discoveryFixture(t, c)
	b := model.TradeBar{ID: "test-group-bar", InstrumentID: f.InstrumentID, ObservedMS: f.ObservedMS, SourceMS: f.SourceMS, MinuteMS: (f.SourceMS/60000 - 1) * 60000, BaseVolume: decimal.NewFromInt(7), Final: true, ConversionHash: model.ReferenceHash("test-conversion"), Sources: []model.PublicSource{{URL: "https://www.okx.com/api/v5/market/candles?instId=TEST-USDT-SWAP&bar=1m", PayloadHash: model.ReferenceHash("test-bar"), TimeBasis: "request_without_source_clock", SourceMS: f.SourceMS, ObservedMS: f.ObservedMS}}}
	facts := []model.DiscoveryFact{f, b}
	// The sorted bar insert succeeds before the later funding conflict. The
	// entire fact/provenance/status transaction must nevertheless roll back.
	bad := f
	bad.Rate = decimal.Zero
	body := bad.Body()
	body["row_hash"] = model.DiscoveryHash(body)
	if err := c.transaction(ctx, func(tx *sql.Tx) error {
		tab, _ := c.mapping(f.Kind())
		return putImmutable(ctx, tx, c.table(tab.Name), tab, record(body))
	}); err != nil {
		t.Fatal(err)
	}
	if err := c.WriteDiscoveryFacts(ctx, facts); !errors.Is(err, contracts.ErrConflict) {
		t.Fatalf("late conflict: %v", err)
	}
	for _, name := range []string{b.Kind(), "public_observation_provenance", "source_batch_status"} {
		rows, err := c.records(ctx, c.db, name, "")
		if err != nil || len(rows) != 0 {
			t.Fatalf("partial %s: %d %v", name, len(rows), err)
		}
	}
	intents, err := c.records(ctx, c.db, "source_publication_intents", "")
	if err != nil || len(intents) != 2 {
		t.Fatalf("intents: %d %v", len(intents), err)
	}
	known := map[string]any{}
	for _, r := range intents {
		known[r["source_kind"].(string)] = r["known_from_ms"]
	}
	if _, err = c.db.ExecContext(ctx, "DELETE FROM "+c.table(f.Kind())); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err = c.WriteDiscoveryFacts(ctx, facts); err != nil {
			t.Fatal(err)
		}
	}
	statuses, err := c.records(ctx, c.db, "source_batch_status", "")
	if err != nil || len(statuses) != 2 {
		t.Fatalf("status %v", err)
	}
	before := map[string]any{}
	for _, r := range statuses {
		kind := r["source_kind"].(string)
		if r["known_from_ms"] != known[kind] {
			t.Fatal("first knowledge moved")
		}
		before[kind] = r["published_ms"]
	}
	provenance, err := c.records(ctx, c.db, "public_observation_provenance", "")
	if err != nil || len(provenance) != 5 {
		t.Fatalf("provenance %d %v", len(provenance), err)
	}
	if err = c.WriteDiscoveryFacts(ctx, []model.DiscoveryFact{b, f}); err != nil {
		t.Fatal(err)
	}
	statuses, _ = c.records(ctx, c.db, "source_batch_status", "")
	for _, r := range statuses {
		if r["published_ms"] != before[r["source_kind"].(string)] {
			t.Fatal("retry changed publication")
		}
	}
	// Every historical physical occurrence must participate in the check.
	if _, err = c.db.ExecContext(ctx, "UPDATE "+c.table("source_batch_status")+" SET complete=0 WHERE source_kind=? AND source_id=?", []byte(b.Kind()), []byte(b.ID)); err != nil {
		t.Fatal(err)
	}
	if err = c.WriteDiscoveryFacts(ctx, facts); !errors.Is(err, contracts.ErrConflict) {
		t.Fatalf("incomplete status accepted: %v", err)
	}
	if _, err = c.db.ExecContext(ctx, "UPDATE "+c.table("source_batch_status")+" SET complete=1 WHERE source_kind=? AND source_id=?", []byte(b.Kind()), []byte(b.ID)); err != nil {
		t.Fatal(err)
	}
	tab, _ := c.mapping("source_batch_status")
	cols := []string{}
	for _, col := range tab.Columns {
		cols = append(cols, "`"+col.Name+"`")
	}
	cols = append(cols, "`_key_hash`")
	columns := strings.Join(cols, ",")
	if _, err = c.db.ExecContext(ctx, "INSERT INTO "+c.table(tab.Name)+" ("+columns+",`_occurrence`) SELECT "+columns+",1 FROM "+c.table(tab.Name)+" WHERE source_kind=? AND source_id=?", []byte(b.Kind()), []byte(b.ID)); err != nil {
		t.Fatal(err)
	}
	if err = c.WriteDiscoveryFacts(ctx, facts); !errors.Is(err, contracts.ErrConflict) {
		t.Fatalf("duplicate status accepted: %v", err)
	}
}
