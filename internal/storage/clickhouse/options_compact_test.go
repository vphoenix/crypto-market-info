package clickhouse

import (
	"context"
	"encoding/hex"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/options"
)

func TestOptionCompactPlansCoverAllTables(t *testing.T) {
	plans, err := OptionsCompactMigrations("crypto_option_plan")
	if err != nil {
		t.Fatal(err)
	}
	if len(plans) != 5 {
		t.Fatalf("got %d tables, want five", len(plans))
	}
	seen := map[string]bool{}
	for _, p := range plans {
		if seen[p.Table] || len(p.Columns) != len(p.SelectExpressions) {
			t.Fatal("invalid plan", p.Table)
		}
		seen[p.Table] = true
		if !strings.Contains(p.CreateSQL, "compact_batch_id FixedString(32)") {
			t.Fatal("missing compact identity", p.Table)
		}
		_, _, suffix := compactDeclarations(p.CreateSQL)
		if strings.Contains(suffix, ",batch_id") {
			t.Fatal("ALIAS sorting key", suffix)
		}
		if !strings.Contains(strings.Join(p.LegacyColumns, ","), "batch_id") {
			t.Fatal("missing legacy projection", p.Table)
		}
	}
}

func TestOptionCompactNullableAndChangingValues(t *testing.T) {
	minute := time.Date(2026, 10, 6, 14, 57, 0, 0, time.UTC)
	times := make([]*time.Time, 60)
	past := minute.Add(-24*time.Hour + 137*time.Microsecond)
	later := minute.Add(59*time.Second + 42*time.Microsecond)
	for i := 5; i < 31; i++ {
		x := past
		times[i] = &x
	}
	for i := 40; i < 60; i++ {
		x := later
		times[i] = &x
	}
	offsets, raw, err := compactArray(compactField{"source_times", "", "time"}, times, minute)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(offsets, []uint8{0, 5, 31, 40}) {
		t.Fatal(offsets)
	}
	values := raw.([]*int64)
	for i, want := range times {
		index := 0
		for n, off := range offsets {
			if int(off) <= i {
				index = n
			}
		}
		if want == nil {
			if values[index] != nil {
				t.Fatal("NULL changed to zero")
			}
			continue
		}
		if values[index] == nil || minute.UnixMicro()+*values[index] != want.UnixMicro() {
			t.Fatal("time changed", i)
		}
	}
	ids := make([]*uuid.UUID, 60)
	id := uuid.New()
	for i := 30; i < 60; i++ {
		x := id
		ids[i] = &x
	}
	offsets, raw, err = compactArray(compactField{"epoch", "Array(Nullable(UUID))", "rle"}, ids, minute)
	if err != nil || !reflect.DeepEqual(offsets, []uint8{0, 30}) || !reflect.DeepEqual(raw, []*uuid.UUID{nil, &id}) {
		t.Fatal(offsets, raw, err)
	}
	digest := strings.Repeat("00", 32)
	x, err := compactBinaryDigest(digest)
	if err != nil || len(x) != 32 || hex.EncodeToString([]byte(x)) != digest {
		t.Fatal("digest changed", err)
	}
	if _, err = compactBinaryDigest(strings.Repeat("AA", 32)); err == nil {
		t.Fatal("noncanonical digest accepted")
	}
}

// Copy expanded legacy tables through the actual migration SELECT, compare
// every field, then use normal readers after table replacement. Hashes include
// all retained quality fields, so a changed microsecond/NULL fails this test.
func TestOptionsCompactClickHouseMigration(t *testing.T) {
	c := derivativeIntegrationClient(t)
	ctx := context.Background()
	r, catalog := catalogDBFixture(t, c)
	if err := c.WriteOptionsCatalogMinute(ctx, catalog); err != nil {
		t.Fatal(err)
	}
	_, live := liveDBFixture(t, c)
	if err := c.WriteOptionsMinute(ctx, live); err != nil {
		t.Fatal(err)
	}
	offline := options.BookEnvelope{RunID: uuid.New(), MinuteTime: live.MinuteTime, PreparedAt: live.PreparedAt, Origin: "synthetic", EvidenceHash: options.PayloadHash([]byte("compact migration fixture")), Batches: live.Books}
	if err := c.WriteDerivativeBookEnvelope(ctx, offline); err != nil {
		t.Fatal(err)
	}
	plans, err := OptionsCompactMigrations(c.database)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range plans {
		legacy := p.Table + "_expanded_test"
		target := p.Table + "_compact_test"
		key := "run_id,minute_time"
		if p.Table == "derivative_book_quality_minute" {
			key = "instrument_id,minute_time,batch_id"
		}
		if p.Table == "options_catalog_quality_evidence_minute" {
			key = "run_id,instrument_id,minute_time,batch_id"
		}
		cols := strings.Join(p.LegacyColumns, ",")
		statements := []string{
			"CREATE TABLE " + c.table(legacy) + " ENGINE=ReplacingMergeTree ORDER BY (" + key + ") AS SELECT " + cols + " FROM " + c.table(p.Table) + " WHERE 0",
			"INSERT INTO " + c.table(legacy) + " (" + cols + ") SELECT " + cols + " FROM " + c.table(p.Table) + " FINAL",
			strings.Replace(p.CreateSQL, p.Table, target, 1),
			"INSERT INTO " + c.table(target) + " (" + strings.Join(p.Columns, ",") + ") SELECT " + strings.Join(p.SelectExpressions, ",") + " FROM " + c.table(legacy) + " FINAL",
		}
		for _, sql := range statements {
			if err := c.conn.Exec(ctx, sql); err != nil {
				t.Fatalf("%s: %v", p.Table, err)
			}
		}
		left, right := make([]string, len(p.LegacyColumns)), make([]string, len(p.LegacyColumns))
		for i, col := range p.LegacyColumns {
			left[i] = "a." + col
			right[i] = "b." + col
		}
		var count, matched uint64
		sql := "SELECT count(),countIf(toJSONString(tuple(" + strings.Join(left, ",") + "))=toJSONString(tuple(" + strings.Join(right, ",") + "))) FROM " + c.table(legacy) + " AS a FINAL INNER JOIN " + c.table(target) + " AS b FINAL USING (" + key + ")"
		if err := c.conn.QueryRow(ctx, sql).Scan(&count, &matched); err != nil {
			t.Fatal(err)
		}
		if count == 0 || count != matched {
			t.Fatal("migration changed fields", p.Table, count, matched)
		}
		if err := c.conn.Exec(ctx, "EXCHANGE TABLES "+c.table(p.Table)+" AND "+c.table(target)); err != nil {
			t.Fatal(err)
		}
	}
	got, err := c.LoadOptionsCatalogMinute(ctx, r.ID, catalog.Live.MinuteTime)
	if err != nil || got.ID() != catalog.ID() {
		t.Fatal("catalog identity changed", err)
	}
	gotLive, err := c.LoadOptionsMinute(ctx, live.RunID, live.MinuteTime)
	if err != nil || gotLive.ID() != live.ID() {
		t.Fatal("live identity changed", err)
	}
	gotOffline, err := c.LoadDerivativeBookEnvelope(ctx, offline.RunID, offline.MinuteTime)
	if err != nil || gotOffline.ID() != offline.ID() {
		t.Fatal("offline identity changed", err)
	}
	for i, b := range got.Live.Books {
		if model.DerivativeBatchHash(b) != model.DerivativeBatchHash(catalog.Live.Books[i]) {
			t.Fatal("quality changed", i)
		}
	}
	// Verify the same writer also handles an expanded legacy installation.
	if err := c.conn.Exec(ctx, "EXCHANGE TABLES "+c.table("derivative_book_quality_minute")+" AND "+c.table("derivative_book_quality_minute_expanded_test")); err != nil {
		t.Fatal(err)
	}
	c.optionCompactTables = nil
	ids := make([]uint32, len(live.Books))
	for i, b := range live.Books {
		ids[i] = b.InstrumentID
	}
	specs, err := c.LoadDerivativeSpecs(ctx, ids)
	if err != nil {
		t.Fatal(err)
	}
	legacyEnvelope := derivativeTestEnvelope(t, specs, live.MinuteTime.Add(time.Minute))
	if err := c.WriteDerivativeBookEnvelope(ctx, legacyEnvelope); err != nil {
		t.Fatal("legacy writer", err)
	}
	if _, err := c.LoadDerivativeBookEnvelope(ctx, legacyEnvelope.RunID, legacyEnvelope.MinuteTime); err != nil {
		t.Fatal("legacy reader", err)
	}
	t.Log(fmt.Sprintf("verified %d compact tables and legacy writer", len(plans)))
}
