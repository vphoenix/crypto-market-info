package perpcontext

import (
	"crypto/sha256"
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

var at = time.Date(2026, 11, 2, 15, 40, 0, 0, time.UTC)

func testBatch() Batch {
	hash := sha256.Sum256([]byte("body"))
	asset := func(coin string) Observation {
		return Observation{Dex: "xyz", Coin: coin, ObservationTime: at, UniverseCount: 3, BatchAssetCount: 2, MaxLeverage: 20, SzDecimals: 4,
			MarkPx: decimal.RequireFromString("281.79"), OraclePx: decimal.RequireFromString("281.71"), PrevDayPx: decimal.RequireFromString("280.1"),
			FundingRateHourly: decimal.RequireFromString("-0.0000125"), OpenInterest: decimal.RequireFromString("1003421.5"),
			DayNotionalVolume: decimal.RequireFromString("362700000.12"), DayBaseVolume: decimal.RequireFromString("1288000.3"),
			BookValid: 1, MidPx: decimal.RequireFromString("281.8"), Premium: decimal.RequireFromString("-0.000345"),
			ImpactBidPx: decimal.RequireFromString("281.7"), ImpactAskPx: decimal.RequireFromString("281.9"), PayloadHash: string(hash[:])}
	}
	return Batch{Dex: "xyz", CollectedAt: at, UniverseCount: 3, Assets: []Observation{asset("xyz:GOOGL"), asset("xyz:GOLD")}}
}

func TestBatchValidateAcceptsCompleteBatchWithSignedRates(t *testing.T) {
	if err := testBatch().Validate(); err != nil {
		t.Fatal(err)
	}
	b := testBatch()
	b.Assets[1].BookValid, b.Assets[1].MidPx, b.Assets[1].Premium, b.Assets[1].ImpactBidPx, b.Assets[1].ImpactAskPx = 0, decimal.Zero, decimal.Zero, decimal.Zero, decimal.Zero
	if err := b.Validate(); err != nil {
		t.Fatalf("absent book row rejected: %v", err)
	}
}

func TestBatchValidateRejectsInconsistentOrUnstorableRows(t *testing.T) {
	other := sha256.Sum256([]byte("other"))
	cases := map[string]func(*Batch){
		"bad dex":            func(b *Batch) { b.Dex = "XYZ" },
		"row dex":            func(b *Batch) { b.Assets[1].Dex = "" },
		"zero time":          func(b *Batch) { b.CollectedAt = time.Time{} },
		"sub-millisecond":    func(b *Batch) { b.CollectedAt = at.Add(time.Microsecond) },
		"non-UTC":            func(b *Batch) { b.CollectedAt = at.In(time.FixedZone("UTC+8", 8*3600)) },
		"empty":              func(b *Batch) { b.Assets = nil },
		"more than universe": func(b *Batch) { b.UniverseCount = 1 },
		"row time":           func(b *Batch) { b.Assets[1].ObservationTime = at.Add(time.Millisecond) },
		"asset count":        func(b *Batch) { b.Assets[0].BatchAssetCount = 3 },
		"universe count":     func(b *Batch) { b.Assets[0].UniverseCount = 4 },
		"mixed hash":         func(b *Batch) { b.Assets[1].PayloadHash = string(other[:]) },
		"zero hash":          func(b *Batch) { z := string(make([]byte, 32)); b.Assets[0].PayloadHash, b.Assets[1].PayloadHash = z, z },
		"duplicate":          func(b *Batch) { b.Assets[1].Coin = b.Assets[0].Coin },
		"missing prefix":     func(b *Batch) { b.Assets[0].Coin = "GOOGL" },
		"space in coin":      func(b *Batch) { b.Assets[0].Coin = "xyz:GO OGL" },
		"zero leverage":      func(b *Batch) { b.Assets[0].MaxLeverage = 0 },
		"flag":               func(b *Batch) { b.Assets[0].BookValid = 2 },
		"zero oracle":        func(b *Batch) { b.Assets[0].OraclePx = decimal.Zero },
		"negative OI":        func(b *Batch) { b.Assets[0].OpenInterest = decimal.NewFromInt(-1) },
		"19 digits":          func(b *Batch) { b.Assets[0].FundingRateHourly = decimal.RequireFromString("0.0000000000000000001") },
		"too large":          func(b *Batch) { b.Assets[0].DayNotionalVolume = decimal.New(1, 20) },
		"too negative":       func(b *Batch) { b.Assets[0].Premium = decimal.New(-1, 20) },
		"book without mid":   func(b *Batch) { b.Assets[0].MidPx = decimal.Zero },
		"absent book value":  func(b *Batch) { b.Assets[0].BookValid = 0 },
	}
	for name, mutate := range cases {
		b := testBatch()
		mutate(&b)
		if err := b.Validate(); err == nil {
			t.Fatalf("%s: invalid batch accepted", name)
		}
	}
}

func TestValidCoinRequiresDexPrefixOnlyForDeployerDexes(t *testing.T) {
	for _, ok := range [][2]string{{"", "BTC"}, {"", "kPEPE"}, {"xyz", "xyz:GOOGL"}} {
		if !ValidCoin(ok[0], ok[1]) {
			t.Fatalf("%v rejected", ok)
		}
	}
	for _, bad := range [][2]string{{"", "xyz:GOOGL"}, {"xyz", "GOOGL"}, {"xyz", "xyz:"}, {"xyz", "flx:GOOGL"}, {"xyz", "xyz:A:B"}, {"", ""}} {
		if ValidCoin(bad[0], bad[1]) {
			t.Fatalf("%v accepted", bad)
		}
	}
}
