package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/options"
	"github.com/vphoenix/crypto-market-info/internal/replay"
	"github.com/vphoenix/crypto-market-info/internal/sampler"
)

func derivativeIntegrationClient(t *testing.T) *Client {
	t.Helper()
	if os.Getenv("CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set CLICKHOUSE_INTEGRATION=1 for isolated derivative database tests")
	}
	db := fmt.Sprintf("crypto_options_it_%d", time.Now().UnixNano())
	t.Logf("isolated derivative database=%s", db)
	c, err := Open(context.Background(), Config{Addresses: []string{envOr("CLICKHOUSE_TEST_ADDR", "127.0.0.1:9000")}, Database: db, MaxAttempts: 1, WriteTimeout: 20 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = c.Close()
		admin, err := ch.Open(&ch.Options{Addr: []string{envOr("CLICKHOUSE_TEST_ADDR", "127.0.0.1:9000")}, Auth: ch.Auth{Database: "default", Username: "default"}})
		if err == nil {
			_ = admin.Exec(context.Background(), "DROP DATABASE IF EXISTS `"+db+"` SYNC")
			_ = admin.Close()
		}
	})
	if err := c.InitSchema(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := c.InitDerivativeSchema(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := c.InitDerivativeSchema(context.Background()); err != nil {
		t.Fatal("repeat migration", err)
	}
	return c
}

func derivativeTestSpec(symbol string, native uint64) options.ContractSpec {
	settle := "BTC"
	expiry := time.Date(2026, 10, 30, 8, 0, 0, 0, time.UTC)
	s := options.ContractSpec{Instrument: model.Instrument{Exchange: "Deribit", MarketType: model.MarketOption, ExchangeSymbol: symbol, BaseAsset: "BTC", QuoteAsset: "BTC", SettleAsset: &settle, ExpiryTime: &expiry, ContractMultiplier: decimal.NewFromInt(1), PriceTickSize: options.StorageUnit(), QuantityStepSize: options.StorageUnit()}, NativeID: native, CreatedAt: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC), SourceInstrumentType: "reversed", NativeAmountKind: "base", NativeAmountCurrency: "BTC", ContractSize: decimal.NewFromInt(1), IndexID: "btc_usd", SettlementSemanticsID: "deribit-option-reversed-v1", OptionType: "call", Strike: decimal.NewFromInt(80000), StrikeCurrency: "USD", EvidenceHash: options.PayloadHash([]byte("synthetic definition"))}
	s.Instrument.VenueContractVersion = s.Version()
	return s
}

func derivativeSyntheticMinute(t *testing.T, id uint32, at time.Time, signed, active, anchor bool) model.DerivativeMinuteBatch {
	t.Helper()
	return derivativeSyntheticMinuteDepth(t, id, at, signed, active, anchor, 10)
}

func derivativeSyntheticMinuteDepth(t *testing.T, id uint32, at time.Time, signed, active, anchor bool, depth int) model.DerivativeMinuteBatch {
	t.Helper()
	buf, err := sampler.NewDerivativeMinuteBufferWithDepth(id, at, signed, depth)
	if err != nil {
		t.Fatal(err)
	}
	epoch := uuid.MustParse("00000000-0000-4000-8000-000000000001")
	for second := 0; second < 60; second++ {
		tm := at.Add(time.Duration(second) * time.Second)
		received := at
		if active {
			received = tm
		}
		qty := uint64(7)
		if active {
			qty += uint64(second)
		}
		bid := int64(100)
		if signed {
			bid = 0
		}
		sequence := uint64(1)
		if active {
			sequence = uint64(second + 1)
		}
		s := model.DerivativeSnapshot{InstrumentID: id, SourceTime: received.Add(-time.Millisecond), ReceivedAt: received, Epoch: epoch, ChangeID: sequence}
		for level := int64(0); level < int64(depth); level++ {
			s.Bids = append(s.Bids, model.Level{PriceTick: bid - level, QtyLot: 7})
			s.Asks = append(s.Asks, model.Level{PriceTick: 101 + level, QtyLot: 9})
		}
		s.Bids[0].QtyLot = qty
		q := model.DerivativeQuality{Sampled: true, StreamValid: true, MarketKnown: true, MarketOpen: true, SourceTime: s.SourceTime, ReceivedAt: received, CapturedAt: tm, Epoch: epoch, ChangeID: s.ChangeID, LastSnapshotAt: at, MarketStateAt: at, MarketStateBasis: 2, BidLevels: uint8(depth), AskLevels: uint8(depth)}
		if !anchor && second == 0 {
			s = model.DerivativeSnapshot{}
			q.StreamValid = false
			q.Reason = model.DerivativeNotReady
		}
		if err := buf.Sample(tm, s, q); err != nil {
			t.Fatal(err)
		}
	}
	b, err := buf.Complete()
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func derivativeTestEnvelope(t *testing.T, specs []options.ContractSpec, at time.Time) options.BookEnvelope {
	e := options.BookEnvelope{RunID: uuid.New(), MinuteTime: at, PreparedAt: at.Add(time.Minute), Origin: "synthetic", EvidenceHash: options.PayloadHash([]byte("synthetic offline test generator v1"))}
	for i, s := range specs {
		e.Batches = append(e.Batches, derivativeSyntheticMinute(t, s.Instrument.ID, at, s.Instrument.MarketType == model.MarketOptionCombo, i%2 == 0, true))
	}
	sort.Slice(e.Batches, func(i, j int) bool { return e.Batches[i].InstrumentID < e.Batches[j].InstrumentID })
	return e
}

func TestDerivativeClickHouseRoundTripFailuresAndVisibility(t *testing.T) {
	c := derivativeIntegrationClient(t)
	ctx := context.Background()
	specs, err := c.RegisterDerivativeSpecs(ctx, []options.ContractSpec{derivativeTestSpec("OFFLINE-CALL-A", 1001), derivativeTestSpec("OFFLINE-CALL-B", 1002)})
	if err != nil {
		t.Fatal(err)
	}
	combo := derivativeTestSpec("OFFLINE-COMBO", 1003)
	combo.Instrument.MarketType = model.MarketOptionCombo
	combo.Instrument.ExpiryTime = nil
	combo.OptionType = ""
	combo.Strike = decimal.Zero
	combo.StrikeCurrency = ""
	combo.NativeAmountKind = "combo"
	combo.Legs = []options.ComboLeg{{InstrumentID: specs[0].Instrument.ID, SignedRatio: 1, AmountPerCombo: decimal.NewFromInt(1)}, {InstrumentID: specs[1].Instrument.ID, SignedRatio: -1, AmountPerCombo: decimal.NewFromInt(1)}}
	combo.Instrument.VenueContractVersion = combo.Version()
	registered, err := c.RegisterDerivativeSpecs(ctx, []options.ContractSpec{combo})
	if err != nil {
		t.Fatal(err)
	}
	specs = append(specs, registered...)
	// Same economic definition, refreshed response evidence: keep first evidence.
	refresh := specs[0]
	refresh.EvidenceHash = options.PayloadHash([]byte("new response"))
	same, err := c.RegisterDerivativeSpecs(ctx, []options.ContractSpec{refresh})
	if err != nil || same[0].Instrument.ID != specs[0].Instrument.ID || same[0].EvidenceHash != specs[0].EvidenceHash {
		t.Fatalf("spec refresh %v %v", same, err)
	}
	at := time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)
	rule := options.TradingRule{InstrumentID: specs[0].Instrument.ID, SourceTick: decimal.RequireFromString("0.0001"), MinAmount: decimal.RequireFromString("0.1"), AmountStep: decimal.RequireFromString("0.1"), ObservedAt: at.Add(-time.Second), KnownFrom: at.Add(-time.Second), EffectiveFrom: at.Add(-time.Second), EffectiveTimeBasis: "first_observed", SourceURL: "https://example.invalid/offline-fixture", PayloadHash: options.PayloadHash([]byte("synthetic rule"))}
	if err := c.WriteDerivativeTradingRule(ctx, rule); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{"future_knowledge", "future_effective"} {
		future := rule
		future.EffectiveTimeBasis = "published"
		if kind == "future_knowledge" {
			future.ObservedAt = at.Add(time.Hour)
			future.KnownFrom = future.ObservedAt
		} else {
			future.EffectiveFrom = at.Add(time.Hour)
		}
		if err := c.WriteDerivativeTradingRule(ctx, future); err != nil {
			t.Fatal(err)
		}
		e := derivativeTestEnvelope(t, specs[:1], at)
		for i := range e.Batches[0].Quality {
			e.Batches[0].Quality[i].TradingRuleID = future.ID()
			e.Batches[0].Quality[i].RulePublishedAt = at
		}
		if err := c.WriteDerivativeBookEnvelope(ctx, e); err == nil {
			t.Fatal("accepted", kind)
		}
		var count uint64
		if err := c.conn.QueryRow(ctx, `SELECT count() FROM `+c.table("derivative_book_quality_minute")+` WHERE batch_id=?`, e.ID()).Scan(&count); err != nil || count != 0 {
			t.Fatalf("invalid reference partly written: count=%d err=%v", count, err)
		}
	}
	for n, stage := range []string{"delta", "minute", "quality", "commit"} {
		e := derivativeTestEnvelope(t, specs, at.Add(time.Duration(n)*time.Minute))
		for i := range e.Batches[0].Quality {
			e.Batches[0].Quality[i].TradingRuleID = rule.ID()
			e.Batches[0].Quality[i].RulePublishedAt = rule.KnownFrom
		}
		c.derivativeAfterInsert = func(atStage string) error {
			if atStage == stage {
				return context.DeadlineExceeded
			}
			return nil
		}
		if err := c.WriteDerivativeBookEnvelope(ctx, e); err == nil {
			t.Fatal("injected failure did not fail")
		}
		c.derivativeAfterInsert = nil
		_, loadErr := c.LoadDerivativeBookEnvelope(ctx, e.RunID, e.MinuteTime)
		if stage != "commit" && !errors.Is(loadErr, ErrNotFound) {
			t.Fatalf("orphan visibility after %s: %v", stage, loadErr)
		}
		if stage == "commit" && loadErr != nil {
			t.Fatal("ambiguous commit missing", loadErr)
		}
		if err := c.WriteDerivativeBookEnvelope(ctx, e); err != nil {
			t.Fatalf("stable retry %s: %v", stage, err)
		}
		got, err := c.LoadDerivativeBookEnvelope(ctx, e.RunID, e.MinuteTime)
		if err != nil {
			t.Fatal(err)
		}
		if got.ID() != e.ID() {
			t.Fatal("roundtrip digest changed")
		}
		for _, book := range got.Batches {
			for second := uint8(0); second < 60; second++ {
				r, err := replay.ReplayDerivative(book, second)
				if err != nil || !r.Quality.ReplayValid {
					t.Fatalf("replay %d %v", second, err)
				}
			}
		}
		conflict := e.Clone()
		conflict.EvidenceHash = options.PayloadHash([]byte("changed envelope"))
		if err := c.WriteDerivativeBookEnvelope(ctx, conflict); err == nil {
			t.Fatal("overwrote existing minute")
		}
	}
	// No anchor still has 60 quality slots; known valid later seconds are not replayable.
	e := derivativeTestEnvelope(t, specs[:1], at.Add(10*time.Minute))
	e.Batches[0] = derivativeSyntheticMinute(t, specs[0].Instrument.ID, e.MinuteTime, false, true, false)
	if err := c.WriteDerivativeBookEnvelope(ctx, e); err != nil {
		t.Fatal(err)
	}
	got, err := c.LoadDerivativeBookEnvelope(ctx, e.RunID, e.MinuteTime)
	if err != nil {
		t.Fatal(err)
	}
	if got.Batches[0].Minute != nil {
		t.Fatal("invented anchor")
	}
	// A present commit cannot hide a missing required delta.
	e = derivativeTestEnvelope(t, specs[:1], at.Add(11*time.Minute))
	if err := c.WriteDerivativeBookEnvelope(ctx, e); err != nil {
		t.Fatal(err)
	}
	if err := c.conn.Exec(ctx, `ALTER TABLE `+c.table("derivative_book_second_delta")+` DELETE WHERE batch_id=? AND second_offset=3 SETTINGS mutations_sync=2`, e.ID()); err != nil {
		t.Fatal(err)
	}
	if _, err := c.LoadDerivativeBookEnvelope(ctx, e.RunID, e.MinuteTime); !errors.Is(err, replay.ErrIncompleteDerivativeBatch) {
		t.Fatalf("missing delta: %v", err)
	}
	reader, err := OpenReadOnly(ctx, Config{Addresses: []string{envOr("CLICKHOUSE_TEST_ADDR", "127.0.0.1:9000")}, Database: c.database})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if err := reader.conn.Exec(ctx, `CREATE TABLE `+reader.table("forbidden_readonly_probe")+` (x UInt8) ENGINE=Memory`); err == nil {
		t.Fatal("read-only connection accepted DDL")
	}
}

func TestDerivativeFoundationDDLIsOptIn(t *testing.T) {
	base, _ := SchemaStatements("check_only")
	for _, s := range base {
		if strings.Contains(s, "derivative_") {
			t.Fatal("derivatives enabled in existing schema path")
		}
	}
	statements, err := DerivativeSchemaStatements("check_only")
	if err != nil {
		t.Fatal(err)
	}
	all := strings.Join(statements, "\n")
	for _, needle := range []string{"origin IN ('fixture','synthetic')", "stored_depth IN (5,10)", "Array(Int64)", "Array(Nullable(DateTime64(6, 'UTC')))", "member_hashes", "delta_bitmap"} {
		if !strings.Contains(all, needle) {
			t.Fatalf("missing schema invariant %s", needle)
		}
	}
	if _, err := DerivativeSchemaStatements("unsafe-db"); err == nil {
		t.Fatal("unsafe identifier")
	}
}

func TestDerivativeClickHouseDayMeasurement(t *testing.T) {
	if os.Getenv("OPTIONS_DAY_MEASUREMENT") != "1" {
		t.Skip("set OPTIONS_DAY_MEASUREMENT=1 for full synthetic day measurement")
	}
	for _, active := range []bool{true, false} {
		name := "inactive"
		if active {
			name = "active"
		}
		t.Run(name, func(t *testing.T) { measureDerivativeDay(t, active) })
	}
}

func measureDerivativeDay(t *testing.T, active bool) {
	c := derivativeIntegrationClient(t)
	ctx := context.Background()
	specs, err := c.RegisterDerivativeSpecs(ctx, []options.ContractSpec{derivativeTestSpec("OFFLINE-MEASURED", 2001)})
	if err != nil {
		t.Fatal(err)
	}
	day := time.Date(2026, 9, 19, 0, 0, 0, 0, time.UTC)
	run := uuid.New()
	for m := 0; m < 1440; m++ {
		e := derivativeTestEnvelope(t, specs, day.Add(time.Duration(m)*time.Minute))
		e.RunID = run
		e.Batches[0] = derivativeSyntheticMinute(t, specs[0].Instrument.ID, e.MinuteTime, false, active, true)
		if err := c.WriteDerivativeBookEnvelope(ctx, e); err != nil {
			t.Fatalf("minute %d: %v", m, err)
		}
	}
	for _, table := range []string{"derivative_book_minute", "derivative_book_second_delta", "derivative_book_quality_minute", "derivative_book_foundation_commit"} {
		if err := c.conn.Exec(ctx, `OPTIMIZE TABLE `+c.table(table)+` FINAL`); err != nil {
			t.Fatal(err)
		}
		var bytes, rows uint64
		if err := c.conn.QueryRow(ctx, `SELECT sum(data_compressed_bytes),sum(rows) FROM system.parts WHERE active AND database=? AND table=?`, c.database, table).Scan(&bytes, &rows); err != nil {
			t.Fatal(err)
		}
		t.Logf("synthetic day table=%s compressed_bytes=%d rows=%d", table, bytes, rows)
	}
	latencies := make([]time.Duration, 100)
	for i := range latencies {
		at := day.Add(time.Duration((i*137)%1440) * time.Minute)
		start := time.Now()
		e, err := c.LoadDerivativeBookEnvelope(ctx, run, at)
		if err != nil {
			t.Fatal(err)
		}
		for _, b := range e.Batches {
			if _, err := replay.ReplayDerivative(b, uint8(i%60)); err != nil {
				t.Fatal(err)
			}
		}
		latencies[i] = time.Since(start)
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	t.Logf("100 dispersed complete-envelope loads and replays: p50=%s p95=%s p99=%s", latencies[49], latencies[94], latencies[98])
}
