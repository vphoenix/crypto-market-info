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

func TestYieldObservationPreservesOptionalPoolCashAndRedemptionWindow(t *testing.T) {
	at := time.Date(2026, 8, 27, 0, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name   string
		cash   *decimal.Decimal
		window *uint64
	}{
		{name: "legacy NULL fields"},
		{name: "observed zero", cash: Ptr(decimal.Zero), window: Ptr(uint64(0))},
		{name: "positive cash above TVL", cash: Ptr(decimal.RequireFromString("2.123456789012345678")), window: Ptr(uint64(172800))},
		{name: "cash upper bound", cash: Ptr(decimal.RequireFromString("99999999999999999999.999999999999999999"))},
		{name: "one wei", cash: Ptr(decimal.RequireFromString("0.000000000000000001"))},
		{name: "maximum uint64 window", window: Ptr(^uint64(0))},
	} {
		t.Run(test.name, func(t *testing.T) {
			batch := validRunnerBatch(at)
			batch.Items[0].Observation.PoolCash = test.cash
			batch.Items[0].Observation.RedemptionWindowSeconds = test.window
			batch.Items[0].Observation.TVL = Ptr(decimal.NewFromInt(1))
			if err := batch.NormalizeAndValidateForLiveCollection(); err != nil {
				t.Fatalf("valid optional fields rejected: %v", err)
			}
			got := batch.Items[0].Observation
			if (got.PoolCash == nil) != (test.cash == nil) || (test.cash != nil && !got.PoolCash.Equal(*test.cash)) {
				t.Fatalf("pool cash changed: got=%v want=%v", got.PoolCash, test.cash)
			}
			if (got.RedemptionWindowSeconds == nil) != (test.window == nil) ||
				(test.window != nil && *got.RedemptionWindowSeconds != *test.window) {
				t.Fatalf("redemption window changed: got=%v want=%v", got.RedemptionWindowSeconds, test.window)
			}
		})
	}
}

func TestYieldObservationRejectsPoolCashOutsideExactDecimalRange(t *testing.T) {
	for _, invalid := range []string{
		"-0.000000000000000001",
		"100000000000000000000",
		"100000000000000000001",
		"0.0000000000000000001",
		"1.0000000000000000001",
	} {
		t.Run(invalid, func(t *testing.T) {
			batch := validRunnerBatch(time.Date(2026, 8, 27, 0, 0, 0, 0, time.UTC))
			batch.Items[0].Observation.PoolCash = Ptr(decimal.RequireFromString(invalid))
			if err := batch.NormalizeAndValidateForLiveCollection(); err == nil || !strings.Contains(err.Error(), "pool_cash") {
				t.Fatalf("invalid pool cash %s: err=%v", invalid, err)
			}
		})
	}
}
