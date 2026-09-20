package clickhouse

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/model"
)

func TestClickHousePerpetualCheckScopesRunsAndIgnoresOrphanDeltas(t *testing.T) {
	c := commonUniverseTestClient(t)
	ctx := context.Background()
	start := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	definitions := []model.Instrument{perpDefinition("BTC"), perpDefinition("BTC")}
	definitions[0].Exchange, definitions[1].Exchange = "TestA", "TestB"
	instruments, err := c.RegisterInstruments(ctx, definitions)
	if err != nil {
		t.Fatal(err)
	}
	revision := strings.Repeat("d", 64)
	var mappings []model.CanonicalMapping
	for _, i := range instruments {
		mappings = append(mappings, model.CanonicalMapping{InstrumentID: i.ID, MappingRevision: revision, CanonicalMarketKey: "BTC-USDT-PERP", CanonicalBaseAsset: "BTC", CanonicalQuoteAsset: "USDT", CanonicalSettleAsset: "USDT", CanonicalBaseUnitsPerVenueBaseUnit: decimal.NewFromInt(1), MappingKind: "identity", RecordedAt: start})
	}
	if err = c.WriteCanonicalMappings(ctx, mappings); err != nil {
		t.Fatal(err)
	}
	makeRun := func(at time.Time) model.PerpetualUniverseRun {
		run := model.PerpetualUniverseRun{RunID: uuid.New(), SelectionRevision: revision, MappingRevision: revision, StartedAt: at, SelectionConfigJSON: `{"Capacity":{"FundingEnabled":true}}`, EnabledVenues: []string{"TestA", "TestB"}, CanonicalGroupCount: 1, InstrumentCount: 2}
		var members []model.PerpetualUniverseMember
		for _, i := range instruments {
			members = append(members, model.PerpetualUniverseMember{RunID: run.RunID, InstrumentID: i.ID, CanonicalMarketKey: "BTC-USDT-PERP"})
		}
		if err := c.WritePerpetualUniverseRun(ctx, run, members); err != nil {
			t.Fatal(err)
		}
		return run
	}
	run := makeRun(start)
	var expectedChanges uint64
	for _, at := range []time.Time{start, start.Add(time.Minute), start.Add(91 * time.Minute)} {
		var batches []model.MinuteBatch
		for _, i := range instruments {
			batches = append(batches, measuredBatch(i.ID, at, true))
		}
		if at.Before(start.Add(90 * time.Minute)) {
			for _, d := range batches[0].Deltas {
				expectedChanges += uint64(len(d.BidChangePrice) + len(d.AskChangePrice))
			}
		}
		if err = c.WriteCompletedMinute(ctx, model.CompletedMinute{MinuteTime: at, Batches: batches}); err != nil {
			t.Fatal(err)
		}
	}
	// This delta batch intentionally has no minute visibility marker.
	orphan := measuredBatch(instruments[0].ID, start.Add(3*time.Minute), true)
	if err = c.insertDeltas(ctx, orphan.Deltas); err != nil {
		t.Fatal(err)
	}
	for _, i := range instruments {
		for hour := range 3 {
			at := start.Add(time.Duration(hour) * time.Hour)
			if err = c.UpsertFundingRate(ctx, model.FundingRate{InstrumentID: i.ID, HourTime: at, FundingTime: at, Rate: decimal.RequireFromString("0.0001"), IsActual: true}); err != nil {
				t.Fatal(err)
			}
		}
	}
	readonly, err := OpenReadOnly(ctx, Config{Addresses: []string{envOr("CLICKHOUSE_TEST_ADDR", "127.0.0.1:9000")}, Database: c.database})
	if err != nil {
		t.Fatal(err)
	}
	defer readonly.Close()
	if err = readonly.conn.Exec(ctx, "CREATE TABLE "+c.table("must_not_create")+" (id UInt32) ENGINE=Memory"); err == nil {
		t.Fatal("readonly checker allowed a schema mutation")
	}
	before, err := readonly.CheckPerpetualData(ctx, run.RunID, start.Add(2*time.Hour), 10*time.Minute)
	if err != nil || before.RunSupersededAt != nil || before.Sources[0].MinuteRows != 3 || before.Sources[0].FundingRows != 3 {
		t.Fatalf("current run query: %+v %v", before, err)
	}
	nextStart := start.Add(90 * time.Minute)
	makeRun(nextStart)
	after, err := readonly.CheckPerpetualData(ctx, run.RunID, start.Add(2*time.Hour), 10*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if after.RunSupersededAt == nil || !after.RunSupersededAt.Equal(nextStart) || !after.BookThroughExclusive.Equal(nextStart) {
		t.Fatalf("old run was not bounded by next run: %+v", after)
	}
	for _, s := range after.Sources {
		if s.MinuteRows != 2 || s.ValidSeconds != 120 || s.ChangedPriceLevels != expectedChanges || s.FundingRows != 1 || s.ActualFundingRows != 1 {
			t.Fatalf("orphan/later-run facts leaked into committed scope: %+v expectedChanges=%d", s, expectedChanges)
		}
	}
	first, err := readonly.PerpetualReplaySeconds(ctx, instruments[0].ID, start, nextStart, 100, 1234)
	if err != nil || len(first) != 100 {
		t.Fatalf("reservoir size=%d err=%v", len(first), err)
	}
	second, err := readonly.PerpetualReplaySeconds(ctx, instruments[0].ID, start, nextStart, 100, 1234)
	if err != nil || !slices.Equal(first, second) {
		t.Fatal("fixed seed replay is not reproducible", err)
	}
	seen := map[time.Time]bool{}
	for _, at := range first {
		if at.Before(start) || !at.Before(start.Add(2*time.Minute)) || seen[at] {
			t.Fatalf("sample duplicated or outside committed valid seconds: %s", at)
		}
		seen[at] = true
	}
	if _, err = readonly.PerpetualReplaySeconds(ctx, instruments[0].ID, start.Add(time.Second), nextStart, 100, 1234); err == nil {
		t.Fatal("unaligned replay range accepted")
	}
	invalid := measuredBatch(instruments[0].ID, start.Add(5*time.Minute), false)
	invalid.Minute.ValidBitmap = uint64(1) << 62
	if err = c.insertMinute(ctx, invalid.Minute); err != nil {
		t.Fatal(err)
	}
	invalidCheck, err := readonly.CheckPerpetualData(ctx, run.RunID, start.Add(2*time.Hour), 10*time.Minute)
	if err != nil || invalidCheck.Sources[0].InvalidMinuteRows != 1 {
		t.Fatalf("invalid bitmap was not detected: %+v %v", invalidCheck, err)
	}
}
