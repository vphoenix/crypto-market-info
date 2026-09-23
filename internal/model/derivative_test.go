package model

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

func TestOptionTypesPreserveLegacyBookValidation(t *testing.T) {
	settle := "BTC"
	expiry := time.Date(2026, 10, 30, 8, 0, 0, 0, time.UTC)
	i := Instrument{ID: 1, Exchange: "Deribit", MarketType: MarketOption, ExchangeSymbol: "BTC-30OCT26-80000-C", VenueContractVersion: "test", BaseAsset: "BTC", QuoteAsset: "BTC", SettleAsset: &settle, ExpiryTime: &expiry, ContractMultiplier: decimal.NewFromInt(1), PriceTickSize: decimal.New(1, -8), QuantityStepSize: decimal.New(1, -8)}
	if err := i.Validate(); err != nil {
		t.Fatal(err)
	}
	i.ExpiryTime = nil
	if err := i.Validate(); err == nil {
		t.Fatal("option without expiry")
	}
	i.MarketType = MarketOptionCombo
	if err := i.Validate(); err != nil {
		t.Fatal(err)
	}
	i.ExpiryTime = &expiry
	if err := i.Validate(); err == nil {
		t.Fatal("combo expiry inferred from one leg")
	}
	for _, s := range []BookSnapshot{{InstrumentID: 1, SourceTime: expiry}, {InstrumentID: 1, SourceTime: expiry, Bids: []Level{{PriceTick: 0, QtyLot: 1}}, Asks: []Level{{PriceTick: 1, QtyLot: 1}}}} {
		if err := s.Validate(BookDepth); err == nil {
			t.Fatal("legacy validator was relaxed")
		}
	}
}
