package model

import (
	"testing"

	"github.com/shopspring/decimal"
)

func TestInstrumentVersionParticipatesInDefinitionIdentity(t *testing.T) {
	settle := "USDT"
	base := Instrument{Exchange: "Bybit", MarketType: MarketPerpetual, ExchangeSymbol: "BTCUSDT", VenueContractVersion: "5:1585526400000", BaseAsset: "BTC", QuoteAsset: "USDT", SettleAsset: &settle, ContractMultiplier: decimal.NewFromInt(1), PriceTickSize: decimal.RequireFromString("0.1"), QuantityStepSize: decimal.RequireFromString("0.001")}
	if err := base.ValidateDefinition(); err != nil {
		t.Fatal(err)
	}
	same := base
	if !base.SameDefinition(same) {
		t.Fatal("identical venue contract version was not reused")
	}
	different := base
	different.VenueContractVersion = "6:1785526400000"
	if base.SameDefinition(different) {
		t.Fatal("different venue contract version was treated as the same definition")
	}
	legacy := base
	legacy.VenueContractVersion = ""
	if err := legacy.ValidateDefinition(); err != nil {
		t.Fatalf("legacy stored derivative must remain readable: %v", err)
	}
	bad := base
	bad.VenueContractVersion = " 5:1585526400000"
	if err := bad.ValidateDefinition(); err == nil {
		t.Fatal("non-exact venue contract version was accepted")
	}
}
