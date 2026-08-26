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

func v4Fixture(history string) string {
	return fmt.Sprintf(`{"data":{"reserve":{"id":%q,"onChainId":"0","chain":{"chainId":43114},"spoke":{"address":%q},"asset":{"underlying":{"address":%q,"info":{"decimals":18}}}},"supplyApyHistory":%s}}`, v4ReserveID, v4Spoke, wavax, history)
}

func v4Point(date, normalized string) string {
	return fmt.Sprintf(`{"date":%q,"avgRate":{"normalized":%q}}`, date, normalized)
}

func collectV4Fixture(t *testing.T, body string) (marketyield.Batch, error) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, body) }))
	defer server.Close()
	c := NewV4Collector(server.URL)
	c.Now = func() time.Time { return testNow }
	c.Retry.MaxAttempts = 1
	c.Retry.Cooldown = exchange.NewRequestGate(0)
	return c.Collect(context.Background())
}

func TestV4HistoryMappingExplicitlyExcludesRewards(t *testing.T) {
	response := v4Fixture("[" + v4Point("2026-08-26T23:00:00Z", "1.36082551852043339681246800") + "]")
	var request []byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request, _ = io.ReadAll(r.Body)
		var body struct{ Query string }
		if json.Unmarshal(request, &body) != nil || body.Query != v4Query || !strings.Contains(body.Query, "includeRewards: false") || !strings.Contains(body.Query, "window: LAST_WEEK") {
			t.Error("V4 request does not explicitly select weekly base APY")
		}
		fmt.Fprint(w, response)
	}))
	defer server.Close()
	c := NewV4Collector(server.URL)
	c.Now = func() time.Time { return testNow }
	batch, err := c.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err = batch.NormalizeAndValidateForLiveCollection(); err != nil {
		t.Fatal(err)
	}
	if batch.Source != "aave-v4-avax" || len(batch.Items) != 1 {
		t.Fatalf("batch=%+v", batch)
	}
	item := batch.Items[0]
	if item.Observation.Rate.String() != "0.013608255185204333" || item.Observation.ExposureRatio != nil || item.Route.ProductCode != "avalanche-v4-main-wavax-supply" ||
		item.Route.PositionAssetKey != "eip155:43114:protocol-position:aave-v4:"+v4Spoke+":0" || *item.Route.ContractAddress != v4Spoke {
		t.Fatalf("item=%+v observation=%+v", item, item.Observation)
	}
	wantHash := marketyield.HashPayloads(marketyield.Payload{Name: "request", Body: request}, marketyield.Payload{Name: "response", Body: []byte(response)})
	assertHistoryObservations(t, batch, wantHash)
}

func TestV4RejectsIdentityAndGraphQLFailures(t *testing.T) {
	valid := v4Fixture("[" + v4Point("2026-08-26T23:00:00Z", "1.3") + "]")
	fixtures := map[string]string{
		"wrong chain":      strings.Replace(valid, "43114", "1", 1),
		"wrong spoke":      strings.Replace(valid, v4Spoke, v3Pool, 1),
		"wrong mint":       strings.Replace(valid, wavax, v3AToken, 1),
		"wrong reserve ID": strings.Replace(valid, v4ReserveID, "other", 1),
		"wrong onChainId":  strings.Replace(valid, `"onChainId":"0"`, `"onChainId":"1"`, 1),
		"wrong decimals":   strings.Replace(valid, `"decimals":18`, `"decimals":6`, 1),
		"null reserve":     `{"data":{"reserve":null,"supplyApyHistory":[]}}`,
		"null data":        `{"data":null}`,
		"missing data":     `{}`,
		"partial errors":   strings.TrimSuffix(valid, "}") + `,"errors":[{"message":"partial response"}]}`,
		"null chain":       strings.Replace(valid, `"chain":{"chainId":43114}`, `"chain":null`, 1),
		"null underlying":  strings.Replace(valid, `"address":"`+wavax+`"`, `"address":null`, 1),
		"malformed":        strings.TrimSuffix(valid, "}"),
		"trailing JSON":    valid + `{}`,
	}
	for name, body := range fixtures {
		t.Run(name, func(t *testing.T) {
			if batch, err := collectV4Fixture(t, body); err == nil || len(batch.Items) != 0 {
				t.Fatalf("accepted invalid response: batch=%+v err=%v", batch, err)
			}
		})
	}
}

func TestV4RatePrecisionAndNegativeBeforeTruncation(t *testing.T) {
	for _, rate := range []string{"", "-1", "-0.000000000000000001", "NaN", "Infinity", "1e-2147483648", "1e20", "10000000000000000000000"} {
		t.Run(rate, func(t *testing.T) {
			if _, err := collectV4Fixture(t, v4Fixture("["+v4Point("2026-08-26T23:00:00Z", rate)+"]")); err == nil {
				t.Fatal("invalid normalized rate accepted")
			}
		})
	}
	for input, want := range map[string]string{"0": "0", "0.000000000000000001": "0", "150": "1.5", "1.123456789012345678999": "0.011234567890123456"} {
		batch, err := collectV4Fixture(t, v4Fixture("["+v4Point("2026-08-26T23:00:00Z", input)+"]"))
		if err != nil || batch.Items[0].Observation.Rate.String() != want {
			t.Fatalf("rate %q batch=%+v err=%v", input, batch, err)
		}
	}
	valid := v4Fixture("[" + v4Point("2026-08-26T23:00:00Z", "0") + "]")
	for _, body := range []string{strings.Replace(valid, `"normalized":"0"`, `"normalized":null`, 1), strings.Replace(valid, `"normalized":"0"`, `"normalized":0`, 1)} {
		if _, err := collectV4Fixture(t, body); err == nil {
			t.Fatal("invalid V4 rate shape accepted")
		}
	}
}

func TestAaveHistoricalRangeCompletenessAndNoSyntheticPoints(t *testing.T) {
	for _, version := range []string{"v3", "v4"} {
		t.Run(version, func(t *testing.T) {
			point, fixture, collect := v3Point, v3Fixture, collectV3Fixture
			rate := "10000000000000000000000000"
			if version == "v4" {
				point, fixture, collect, rate = v4Point, v4Fixture, collectV4Fixture, "1"
			}
			validPoint := point("2026-08-26T23:00:00Z", rate)
			fixtures := map[string]string{
				"null": "null", "empty": "[]", "null point": "[null]", "missing date": `[{"avgRate":null}]`,
				"duplicate": "[" + validPoint + "," + validPoint + "]",
				"same UTC":  "[" + validPoint + "," + point("2026-08-27T07:00:00+08:00", rate) + "]",
				"future":    "[" + point("2026-08-27T00:06:00Z", rate) + "]",
				"too old":   "[" + validPoint + "," + point("2026-08-01T00:00:00Z", rate) + "]",
				"stale":     "[" + point("2026-08-26T17:00:00Z", rate) + "]",
				"too many":  "[" + strings.Repeat(validPoint+",", 512) + validPoint + "]",
			}
			for name, history := range fixtures {
				t.Run(name, func(t *testing.T) {
					if batch, err := collect(t, fixture(history)); err == nil || len(batch.Items) != 0 {
						t.Fatalf("invalid curve accepted, err=%v", err)
					}
				})
			}
			batch, err := collect(t, fixture("["+validPoint+","+point("2026-08-26T20:17:03Z", rate)+"]"))
			if err != nil || len(batch.Items) != 2 || batch.Items[0].Observation.ObservationTime.Format(time.RFC3339) != "2026-08-26T20:17:03Z" {
				t.Fatalf("sparse unsorted source history was not preserved: %+v err=%v", batch, err)
			}
		})
	}
}

func TestAaveHashesRawResponseNotLocalClock(t *testing.T) {
	for _, version := range []string{"v3", "v4"} {
		t.Run(version, func(t *testing.T) {
			fixture := v3Fixture("[" + v3Point("2026-08-26T23:00:00Z", "0") + "]")
			if version == "v4" {
				fixture = v4Fixture("[" + v4Point("2026-08-26T23:00:00Z", "0") + "]")
			}
			response := fixture
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, response) }))
			defer server.Close()
			now := testNow
			var collector marketyield.Collector
			if version == "v3" {
				c := NewV3Collector(server.URL)
				c.Now, c.Retry.Cooldown = func() time.Time { return now }, exchange.NewRequestGate(0)
				collector = c
			} else {
				c := NewV4Collector(server.URL)
				c.Now, c.Retry.Cooldown = func() time.Time { return now }, exchange.NewRequestGate(0)
				collector = c
			}
			first, err := collector.Collect(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			now = now.Add(time.Hour)
			second, err := collector.Collect(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if *first.Items[0].Observation.SourcePayloadHash != *second.Items[0].Observation.SourcePayloadHash || !first.Items[0].Observation.ObservationTime.Equal(second.Items[0].Observation.ObservationTime) || first.CollectedAt.Equal(second.CollectedAt) {
				t.Fatal("hash or source timestamp depends on local clock")
			}
			response = fixture + "\n"
			third, err := collector.Collect(context.Background())
			if err != nil || *first.Items[0].Observation.SourcePayloadHash == *third.Items[0].Observation.SourcePayloadHash {
				t.Fatalf("hash omitted raw response bytes: %v", err)
			}
		})
	}
}
