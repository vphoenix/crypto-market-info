package lendingmarket

import (
	"crypto/sha256"
	"strings"
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

var at = time.Date(2026, 11, 2, 10, 0, 0, 0, time.UTC)

func testBatch() Batch {
	hash := sha256.Sum256([]byte("body"))
	market := func(address, symbol string) Observation {
		return Observation{MarketAddress: address, ObservationTime: at, BatchMarketCount: 2, MarketSymbol: symbol,
			UnderlyingAddress: "TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t", UnderlyingSymbol: "USDT", UnderlyingDecimals: 6,
			SupplyRateAPR: decimal.RequireFromString("0.4626987764"), BorrowRateAPR: decimal.RequireFromString("0.5507187782"),
			Cash: decimal.RequireFromString("10103226.842891"), TotalBorrows: decimal.RequireFromString("137640418.1333"),
			Reserves: decimal.RequireFromString("209210.55274"), TotalSupply: decimal.RequireFromString("13461904497.37706501"),
			ExchangeRate: decimal.RequireFromString("0.010891869372988396"), BorrowIndex: decimal.RequireFromString("1.184253299792460419"),
			CollateralFactor: decimal.RequireFromString("0.75"), ReserveFactor: decimal.RequireFromString("0.1"),
			UnderlyingPriceTRX: decimal.RequireFromString("3.182848"), PayloadHash: string(hash[:])}
	}
	return Batch{CollectedAt: at, Markets: []Observation{market("TXJgMdjVX5dKiQaUi9QobwNxtSQaFqccvd", "jUSDT"), market("TE2RzoSV3wFK99w6J9UnnZ4vLfXYoxvRwP", "jTRX")}}
}

func TestBatchValidateAcceptsCompleteBatch(t *testing.T) {
	if err := testBatch().Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestBatchValidateRejectsInconsistentOrUnstorableRows(t *testing.T) {
	other := sha256.Sum256([]byte("other"))
	cases := map[string]func(*Batch){
		"zero time":         func(b *Batch) { b.CollectedAt = time.Time{} },
		"sub-millisecond":   func(b *Batch) { b.CollectedAt = at.Add(time.Microsecond) },
		"non-UTC":           func(b *Batch) { b.CollectedAt = at.In(time.FixedZone("UTC+8", 8*3600)) },
		"empty":             func(b *Batch) { b.Markets = nil },
		"row time":          func(b *Batch) { b.Markets[1].ObservationTime = at.Add(time.Millisecond) },
		"count":             func(b *Batch) { b.Markets[0].BatchMarketCount = 3 },
		"mixed hash":        func(b *Batch) { b.Markets[1].PayloadHash = string(other[:]) },
		"zero hash":         func(b *Batch) { z := string(make([]byte, 32)); b.Markets[0].PayloadHash, b.Markets[1].PayloadHash = z, z },
		"short hash":        func(b *Batch) { b.Markets[0].PayloadHash, b.Markets[1].PayloadHash = "x", "x" },
		"duplicate":         func(b *Batch) { b.Markets[1].MarketAddress = b.Markets[0].MarketAddress },
		"bad checksum":      func(b *Batch) { b.Markets[0].MarketAddress = "TXJgMdjVX5dKiQaUi9QobwNxtSQaFqccve" },
		"empty symbol":      func(b *Batch) { b.Markets[0].MarketSymbol = "" },
		"space in symbol":   func(b *Batch) { b.Markets[0].UnderlyingSymbol = "US DT" },
		"decimals":          func(b *Batch) { b.Markets[0].UnderlyingDecimals = 37 },
		"negative":          func(b *Batch) { b.Markets[0].Cash = decimal.NewFromInt(-1) },
		"19 digits":         func(b *Batch) { b.Markets[0].BorrowRateAPR = decimal.RequireFromString("0.1000000000000000001") },
		"too large":         func(b *Batch) { b.Markets[0].TotalSupply = decimal.New(1, 20) },
		"collateral factor": func(b *Batch) { b.Markets[0].CollateralFactor = decimal.RequireFromString("1.01") },
		"reserve factor":    func(b *Batch) { b.Markets[0].ReserveFactor = decimal.RequireFromString("1.01") },
	}
	for name, mutate := range cases {
		b := testBatch()
		mutate(&b)
		if err := b.Validate(); err == nil {
			t.Fatalf("%s: invalid batch accepted", name)
		}
	}
}

func TestValidTronAddress(t *testing.T) {
	for _, ok := range []string{"T9yD14Nj9j7xAB4dbGeiX9h8unkKHxuWwb", "TXJgMdjVX5dKiQaUi9QobwNxtSQaFqccvd"} {
		if !ValidTronAddress(ok) {
			t.Fatalf("%s rejected", ok)
		}
	}
	for _, bad := range []string{"", "0x5c0401e81bc07ca70fad469b451682c0d747ef1c", "TXJgMdjVX5dKiQaUi9QobwNxtSQaFqccv0", strings.Repeat("T", 34)} {
		if ValidTronAddress(bad) {
			t.Fatalf("%q accepted", bad)
		}
	}
}
