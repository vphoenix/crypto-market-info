package aave

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/exchange"
	marketyield "github.com/vphoenix/crypto-market-info/internal/yield"
)

var testNow = time.Date(2026, 8, 27, 0, 0, 0, 123456789, time.UTC)

func v3Fixture(history string) string {
	return fmt.Sprintf(`{"data":{"market":{"address":%q,"chain":{"chainId":43114},"reserves":[{"underlyingToken":{"address":%q,"decimals":18},"aToken":{"address":%q,"decimals":18}}]},"supplyAPYHistory":%s}}`, v3Pool, wavax, v3AToken, history)
}

func v3Point(date, raw string) string {
	return fmt.Sprintf(`{"date":%q,"avgRate":{"raw":%q,"decimals":27}}`, date, raw)
}

func collectV3Fixture(t *testing.T, body string) (marketyield.Batch, error) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, body) }))
	defer server.Close()
	c := NewV3Collector(server.URL)
	c.Now = func() time.Time { return testNow }
	c.Retry.MaxAttempts = 1
	c.Retry.Cooldown = exchange.NewRequestGate(0)
	return c.Collect(context.Background())
}

func TestV3HistoryMappingAndEvidence(t *testing.T) {
	response := v3Fixture("[" + v3Point("2026-08-26T23:15:00.123Z", "6668427376540457481976400") + "," +
		v3Point("2026-08-26T20:07:00Z", "0") + "," + v3Point("2026-08-27T06:30:00+08:00", "2000000000000000000000000000") + "]")
	var request []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request, _ = io.ReadAll(r.Body)
		var body struct{ Query string }
		if r.Method != http.MethodPost || r.Header.Get("Content-Type") != "application/json" || json.Unmarshal(request, &body) != nil || body.Query != v3Query {
			t.Error("invalid GraphQL request")
		}
		fmt.Fprint(w, response)
	}))
	defer server.Close()
	c := NewV3Collector(server.URL)
	c.Now = func() time.Time { return testNow.In(time.FixedZone("CST", 8*3600)) }
	batch, err := c.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err = batch.NormalizeAndValidateForLiveCollection(); err != nil {
		t.Fatal(err)
	}
	if batch.Source != "aave-v3-avax" || len(batch.Items) != 3 || !batch.CollectedAt.Equal(testNow.Truncate(time.Millisecond)) {
		t.Fatalf("batch=%+v", batch)
	}
	for i, want := range []string{"0", "2", "0.006668427376540457"} {
		if got := batch.Items[i].Observation.Rate.String(); got != want {
			t.Fatalf("rate[%d]=%s want %s", i, got, want)
		}
	}
	item := batch.Items[2]
	if item.Route.ProductCode != "avalanche-v3-wavax-supply" || item.Route.DepositAssetKey != wavaxAsset || item.Route.RedeemAssetKey != wavaxAsset ||
		item.Route.PositionAssetKey != "eip155:43114:erc20:"+v3AToken || *item.Route.ContractAddress != v3Pool || *item.Route.Network != "avalanche-c-mainnet" || *item.Route.PriceExposureAsset != "AVAX" {
		t.Fatalf("route=%+v", item.Route)
	}
	if item.Observation.ObservationTime.Format(time.RFC3339Nano) != "2026-08-26T23:15:00.123Z" || item.Observation.ExposureRatio == nil || item.Observation.ExposureRatio.String() != "1" {
		t.Fatalf("observation=%+v", item.Observation)
	}
	wantHash := marketyield.HashPayloads(marketyield.Payload{Name: "request", Body: request}, marketyield.Payload{Name: "response", Body: []byte(response)})
	assertHistoryObservations(t, batch, wantHash)
}

func assertHistoryObservations(t *testing.T, batch marketyield.Batch, hash string) {
	t.Helper()
	for _, item := range batch.Items {
		o := item.Observation
		if o.RateKind != "apy" || o.RateOrigin != "reported" || o.RateMode != "variable" || o.TierNo != 1 || !o.TierMinAmount.IsZero() || o.TierMaxAmount != nil || o.TierMode != "none" ||
			o.RulePrincipalLossMode != "none" || o.RuleEligibility != "candidate" || o.Availability != "unknown" || o.SourcePayloadHash == nil || *o.SourcePayloadHash != hash {
			t.Fatalf("observation mapping=%+v", o)
		}
		if len(o.RewardAssetKeys) != 1 || o.RewardAssetKeys[0] != wavaxAsset || !o.RewardComponentRates[0].Equal(*o.Rate) {
			t.Fatalf("reward components=%+v", o)
		}
		if o.EntryFeeRate != nil || o.ExitFeeRate != nil || o.FixedPenaltyRate != nil || o.PerformanceFeeRate != nil || o.EntryFeeAmount != nil || o.ExitFeeAmount != nil ||
			o.FixedFeeAssetKey != nil || o.FixedPrincipalLossRate != nil || o.UnbondingSeconds != nil || o.LockSeconds != 0 || o.Capacity != nil || o.RemainingCapacity != nil || o.TVL != nil ||
			o.BlockHeight != nil || o.BlockHash != nil || o.Finality != nil || !o.CollectedAt.Equal(batch.CollectedAt) {
			t.Fatalf("historical unknown fields fabricated: %+v", o)
		}
	}
}

func TestV3RejectsIdentityAndGraphQLFailures(t *testing.T) {
	valid := v3Fixture("[" + v3Point("2026-08-26T23:00:00Z", "10000000000000000000000000") + "]")
	fixtures := map[string]string{
		"wrong chain":    strings.Replace(valid, "43114", "1", 1),
		"wrong pool":     strings.Replace(valid, v3Pool, v4Spoke, 1),
		"wrong mint":     strings.Replace(valid, wavax, v3AToken, 1),
		"wrong aToken":   strings.Replace(valid, v3AToken, wavax, 1),
		"wrong decimals": strings.Replace(valid, `"decimals":18`, `"decimals":6`, 1),
		"missing chain":  strings.Replace(valid, `"chain":{"chainId":43114}`, `"chain":null`, 1),
		"null market":    `{"data":{"market":null,"supplyAPYHistory":[]}}`,
		"null data":      `{"data":null}`,
		"missing data":   `{}`,
		"partial errors": strings.TrimSuffix(valid, "}") + `,"errors":[{"message":"partial response"}]}`,
		"malformed":      strings.TrimSuffix(valid, "}"),
		"trailing JSON":  valid + `{}`,
	}
	reserve := fmt.Sprintf(`{"underlyingToken":{"address":%q,"decimals":18},"aToken":{"address":%q,"decimals":18}}`, wavax, v3AToken)
	fixtures["duplicate target"] = strings.Replace(valid, reserve, reserve+","+reserve, 1)
	for name, body := range fixtures {
		t.Run(name, func(t *testing.T) {
			if batch, err := collectV3Fixture(t, body); err == nil || len(batch.Items) != 0 {
				t.Fatalf("accepted invalid response: batch=%+v err=%v", batch, err)
			}
		})
	}
}

func TestV3RejectsInvalidRates(t *testing.T) {
	for _, raw := range []string{"", "-1", "-0.00001", "0.1", "1e27", "NaN", "100000000000000000000000000000000000000000000000"} {
		t.Run(raw, func(t *testing.T) {
			if _, err := collectV3Fixture(t, v3Fixture("["+v3Point("2026-08-26T23:00:00Z", raw)+"]")); err == nil {
				t.Fatal("invalid V3 raw accepted")
			}
		})
	}
	valid := v3Fixture("[" + v3Point("2026-08-26T23:00:00Z", "0") + "]")
	for _, body := range []string{strings.Replace(valid, `"decimals":27`, `"decimals":18`, 1), strings.Replace(valid, `"raw":"0"`, `"raw":null`, 1), strings.Replace(valid, `"raw":"0"`, `"raw":0`, 1)} {
		if _, err := collectV3Fixture(t, body); err == nil {
			t.Fatal("invalid V3 rate shape accepted")
		}
	}
}
