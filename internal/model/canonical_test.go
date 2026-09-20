package model

import (
	"strings"
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

func TestCanonicalMappingRejectsUnrepresentableFactorsAndIdentityChanges(t *testing.T) {
	valid := CanonicalMapping{InstrumentID: 1, MappingRevision: strings.Repeat("a", 64), CanonicalMarketKey: "PEPE-USDT-PERP", CanonicalBaseAsset: "PEPE",
		CanonicalQuoteAsset: "USDT", CanonicalSettleAsset: "USDT", CanonicalBaseUnitsPerVenueBaseUnit: decimal.NewFromInt(1000), MappingKind: "alias", RecordedAt: time.Now().UTC().Truncate(time.Millisecond)}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, factor := range []string{"0", "-1", "0.0000000000000000001", "100000000000000000000"} {
		candidate := valid
		candidate.CanonicalBaseUnitsPerVenueBaseUnit = decimal.RequireFromString(factor)
		if err := candidate.Validate(); err == nil {
			t.Errorf("accepted factor %s", factor)
		}
	}
	candidate := valid
	candidate.MappingKind = "identity"
	if err := candidate.Validate(); err == nil {
		t.Fatal("identity factor was not 1")
	}
	for _, key := range []string{"PEPE-USDC-PERP", "pepe-USDT-PERP", "A-B-USDT-PERP", "PEPE-USDT-PERP-USDT-PERP"} {
		if err := ValidateCanonicalMarketKey(key); err == nil {
			t.Errorf("accepted key %s", key)
		}
	}
	if err := ValidateMappingRevision(strings.Repeat("A", 64)); err == nil {
		t.Fatal("accepted uppercase revision")
	}
}
