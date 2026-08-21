package yield

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

func TestBatchNormalizesMillisecondsAndRejectsInvalidBeforeWriting(t *testing.T) {
	at := time.Date(2026, 8, 21, 1, 2, 3, 456789123, time.FixedZone("test", 8*3600))
	route := YieldRouteDefinition{ProviderType: "native", Provider: "TRON", ProductCode: "x", ProductName: "x", YieldType: "native_staking", DepositAssetKey: "a", PositionAssetKey: "a", RedeemAssetKey: "a", IncomeSource: "issuance", SourceURL: "https://example.invalid", CollectionEnabled: true}
	rate := decimal.RequireFromString("0.1")
	batch := Batch{Source: "test", CollectedAt: at, Items: []CollectedYield{{Route: route, Observation: YieldObservation{ObservationTime: at, CollectedAt: at, TierNo: 1, TierMinAmount: decimal.Zero, TierMode: "none", Rate: &rate, RateKind: "apr", RateOrigin: "derived", RateMode: "variable", RewardAssetKeys: []string{"a"}, RewardComponentRates: []*decimal.Decimal{&rate}, RulePrincipalLossMode: "none", RuleEligibility: "candidate", Availability: "available"}}}}
	if err := batch.NormalizeAndValidate(); err != nil {
		t.Fatal(err)
	}
	if batch.CollectedAt.Location() != time.UTC || batch.CollectedAt.Nanosecond() != 456000000 || !batch.Items[0].Observation.CollectedAt.Equal(batch.CollectedAt) {
		t.Fatalf("batch was not normalized: %+v", batch)
	}
	batch.Items[0].Observation.RulePrincipalLossMode = "variable"
	if err := batch.NormalizeAndValidate(); err == nil {
		t.Fatal("variable loss candidate was accepted")
	}
}

func TestHashPayloadsFramesNamesAndLengths(t *testing.T) {
	a := HashPayloads(Payload{Name: "a", Body: []byte("bc")})
	b := HashPayloads(Payload{Name: "ab", Body: []byte("c")})
	if a == b || len(a) != 64 {
		t.Fatalf("ambiguous hashes: %q %q", a, b)
	}
}
