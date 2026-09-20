package main

import (
	"strings"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/model"
	chstore "github.com/vphoenix/crypto-market-info/internal/storage/clickhouse"
)

func TestReplayBookStructuralAssertions(t *testing.T) {
	valid := func() model.BookSnapshot {
		return model.BookSnapshot{InstrumentID: 1, StoredDepth: model.BookDepth, Bids: []model.Level{{PriceTick: 100, QtyLot: 2}, {PriceTick: 99, QtyLot: 3}}, Asks: []model.Level{{PriceTick: 101, QtyLot: 2}, {PriceTick: 102, QtyLot: 3}}}
	}
	if err := validateBook(valid(), 1); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		change func(*model.BookSnapshot)
	}{
		{"wrong instrument", func(b *model.BookSnapshot) { b.InstrumentID = 2 }},
		{"empty side", func(b *model.BookSnapshot) { b.Asks = nil }},
		{"unknown depth", func(b *model.BookSnapshot) { b.StoredDepth = 0 }},
		{"more than stored depth", func(b *model.BookSnapshot) { b.Bids = make([]model.Level, 11) }},
		{"more than 50", func(b *model.BookSnapshot) { b.Bids = make([]model.Level, 51) }},
		{"zero quantity", func(b *model.BookSnapshot) { b.Bids[0].QtyLot = 0 }},
		{"negative price", func(b *model.BookSnapshot) { b.Bids[1].PriceTick = -1 }},
		{"unsorted bids", func(b *model.BookSnapshot) { b.Bids[1].PriceTick = 101 }},
		{"duplicate ask", func(b *model.BookSnapshot) { b.Asks[1].PriceTick = 101 }},
		{"locked", func(b *model.BookSnapshot) { b.Bids[0].PriceTick = 101 }},
		{"crossed", func(b *model.BookSnapshot) { b.Bids[0].PriceTick = 102 }},
	} {
		t.Run(test.name, func(t *testing.T) {
			book := valid()
			test.change(&book)
			if validateBook(book, 1) == nil {
				t.Fatal("invalid replayed book accepted")
			}
		})
	}
}

func TestMinimumDurationRequiresCapturedDataNotOnlyWallClock(t *testing.T) {
	first := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	through := first.Add(24 * time.Hour)
	latest := through.Add(-time.Minute)
	fundingHour := through.Add(-time.Hour)
	s := chstore.PerpetualSourceCheck{InstrumentID: 1, Exchange: "Binance", Symbol: "BTCUSDT", MinuteRows: 1440, ValidSeconds: 86400, FirstMinute: &first, LatestMinute: &latest, FundingRows: 24, ActualFundingRows: 3, LatestFundingHour: &fundingHour}
	if pending := sourcePending(s, through, 24*time.Hour, true); len(pending) != 0 {
		t.Fatalf("complete data rejected: %v", pending)
	}
	s.ValidSeconds = 120
	if pending := strings.Join(sourcePending(s, through, 24*time.Hour, true), " "); !strings.Contains(pending, "captured valid seconds") {
		t.Fatal("24h wall-clock span hid mostly missing data")
	}
	s.ValidSeconds = 86400
	s.FirstMinute = &latest
	if pending := strings.Join(sourcePending(s, through, 24*time.Hour, true), " "); !strings.Contains(pending, "observed span") {
		t.Fatal("short observed duration passed 24h check")
	}
}

func TestFundingRequiresHoursFreshnessAndActualConfirmation(t *testing.T) {
	first := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	through := first.Add(24 * time.Hour)
	latest, fundingHour := through.Add(-time.Minute), through.Add(-time.Hour)
	s := chstore.PerpetualSourceCheck{MinuteRows: 1440, ValidSeconds: 86400, FirstMinute: &first, LatestMinute: &latest, FundingRows: 1, LatestFundingHour: &fundingHour}
	pending := strings.Join(sourcePending(s, through, 24*time.Hour, true), " ")
	if !strings.Contains(pending, "funding hour rows") || !strings.Contains(pending, "no confirmed actual funding") {
		t.Fatalf("sparse or unconfirmed funding passed: %s", pending)
	}
	s.FundingRows, s.ActualFundingRows = 24, 3
	fundingHour = through.Add(-2 * time.Hour)
	if pending = strings.Join(sourcePending(s, through, 24*time.Hour, true), " "); !strings.Contains(pending, "funding hour is stale") {
		t.Fatalf("stopped funding source passed: %s", pending)
	}
}

func TestReadinessReportsNoRowsStaleAndPendingFunding(t *testing.T) {
	through := time.Now().UTC().Truncate(time.Minute)
	if len(sourcePending(chstore.PerpetualSourceCheck{}, through, 0, false)) == 0 {
		t.Fatal("empty dataset passed")
	}
	first := through.Add(-10 * time.Minute)
	latest := through.Add(-time.Minute)
	s := chstore.PerpetualSourceCheck{MinuteRows: 9, ValidSeconds: 540, FirstMinute: &first, LatestMinute: &latest}
	if len(sourcePending(s, through, 0, false)) != 0 {
		t.Fatal("funding-disabled quick check rejected")
	}
	if pending := strings.Join(sourcePending(s, through, 0, true), " "); !strings.Contains(pending, "funding") {
		t.Fatal("missing enabled funding not reported")
	}
	latest = through.Add(-4 * time.Minute)
	if pending := strings.Join(sourcePending(s, through, 0, false), " "); !strings.Contains(pending, "stale") {
		t.Fatal("stopped source not reported")
	}
}
