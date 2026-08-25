package yield

import (
	"strings"
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

func TestLiveBatchRequiresValidPayloadHashWhileImportMayOmitIt(t *testing.T) {
	at := time.Now().UTC()
	batch := validRunnerBatch(at)
	batch.Items[0].Observation.SourcePayloadHash = nil
	if err := batch.NormalizeAndValidate(); err != nil {
		t.Fatalf("historical or manual import rejected: %v", err)
	}
	if err := batch.NormalizeAndValidateForLiveCollection(); err == nil {
		t.Fatal("live batch without source_payload_hash was accepted")
	}

	for _, invalid := range []string{strings.Repeat("a", 63), strings.Repeat("A", 64), strings.Repeat("g", 64)} {
		batch = validRunnerBatch(at)
		batch.Items[0].Observation.SourcePayloadHash = &invalid
		if err := batch.NormalizeAndValidateForLiveCollection(); err == nil {
			t.Fatalf("invalid live hash %q was accepted", invalid)
		}
	}

	batch = validRunnerBatch(at)
	if err := batch.NormalizeAndValidateForLiveCollection(); err != nil {
		t.Fatalf("valid live batch rejected: %v", err)
	}
}

func TestBatchRequiresOneRouteDefinitionAcrossTiers(t *testing.T) {
	at := time.Now().UTC()
	consistent := validRunnerBatch(at)
	secondTier := consistent.Items[0]
	secondTier.Observation.TierNo = 2
	consistent.Items = append(consistent.Items, secondTier)
	if err := consistent.NormalizeAndValidate(); err != nil {
		t.Fatalf("same route definition across tiers rejected: %v", err)
	}

	for name, change := range map[string]func(*YieldRouteDefinition){
		"metadata": func(route *YieldRouteDefinition) { route.ProductName = "different name" },
		"stable definition": func(route *YieldRouteDefinition) {
			route.IncomeSource = "protocol_fee"
		},
	} {
		t.Run(name, func(t *testing.T) {
			batch := validRunnerBatch(at)
			conflictingTier := batch.Items[0]
			conflictingTier.Observation.TierNo = 2
			change(&conflictingTier.Route)
			batch.Items = append(batch.Items, conflictingTier)
			if err := batch.NormalizeAndValidate(); err == nil {
				t.Fatal("conflicting route definition across tiers was accepted")
			}
		})
	}
}

func TestBatchAllowsRouteHistoryButRejectsSameTimeAndTier(t *testing.T) {
	at := time.Now().UTC()
	batch := validRunnerBatch(at)
	history := batch.Items[0]
	history.Observation.ObservationTime = at.Add(-time.Hour)
	batch.Items = append(batch.Items, history)
	if err := batch.NormalizeAndValidate(); err != nil {
		t.Fatalf("different history points rejected: %v", err)
	}

	duplicate := validRunnerBatch(at)
	duplicate.Items = append(duplicate.Items, duplicate.Items[0])
	if err := duplicate.NormalizeAndValidate(); err == nil {
		t.Fatal("same route, observation time, and tier accepted")
	}
}
