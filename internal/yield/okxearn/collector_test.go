package okxearn

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/exchange"
	marketyield "github.com/vphoenix/crypto-market-info/internal/yield"
)

var testNow = time.Date(2026, 8, 27, 0, 0, 0, 123456789, time.UTC)

func point(at time.Time, rate string) string {
	return fmt.Sprintf(`{"ccy":"AVAX","ts":%q,"lendingRate":%q,"rate":"99","amt":"999999"}`, strconv.FormatInt(at.UnixMilli(), 10), rate)
}

func page(points ...string) string {
	return `{"code":"0","data":[` + strings.Join(points, ",") + `]}`
}

func collectFixture(t *testing.T, pages []string) (marketyield.Batch, []string, error) {
	t.Helper()
	requests := make(chan string, 16)
	index := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != historyPath || r.URL.Query().Get("ccy") != "AVAX" || r.URL.Query().Get("limit") != "100" {
			t.Error("invalid history request", r.URL)
		}
		requests <- "http://" + r.Host + r.URL.String()
		if index >= len(pages) {
			t.Error("unexpected additional request")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		body := pages[index]
		index++
		if body == "HTTP_ERROR" {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		fmt.Fprint(w, body)
	}))
	defer server.Close()
	c := NewCollector(server.URL)
	c.Now = func() time.Time { return testNow.In(time.FixedZone("CST", 8*3600)) }
	c.Retry.MaxAttempts = 1
	c.Retry.Cooldown = exchange.NewRequestGate(0)
	batch, err := c.Collect(context.Background())
	var urls []string
	for len(requests) > 0 {
		urls = append(urls, <-requests)
	}
	return batch, urls, err
}

func TestCollectorPaginatesMapsLendingAPRAndHashesAllPages(t *testing.T) {
	last, middle, oldest := testNow.Add(-time.Hour), testNow.Add(-2*time.Hour), testNow.Add(-3*time.Hour)
	old := fmt.Sprintf(`{"ccy":"AVAX","ts":%q,"rate":"999"}`, strconv.FormatInt(testNow.Add(-8*24*time.Hour).UnixMilli(), 10))
	pages := []string{
		page(point(middle, "0.0206"), point(last, "0.020612345678901234999")),
		page(point(oldest, "0"), point(middle, "0.0206000"), old),
	}
	batch, urls, err := collectFixture(t, pages)
	if err != nil {
		t.Fatal(err)
	}
	if err = batch.NormalizeAndValidateForLiveCollection(); err != nil {
		t.Fatal(err)
	}
	if len(urls) != 2 || strings.Contains(urls[0], "after=") || !strings.Contains(urls[1], "after="+strconv.FormatInt(middle.UnixMilli(), 10)) {
		t.Fatalf("URLs=%v", urls)
	}
	if batch.Source != "okx-avax-flexible" || len(batch.Items) != 3 || !batch.CollectedAt.Equal(testNow.Truncate(time.Millisecond)) {
		t.Fatalf("batch=%+v", batch)
	}
	var payloads []marketyield.Payload
	for i := range pages {
		payloads = append(payloads, marketyield.Payload{Name: fmt.Sprintf("page-%d-request", i+1), Body: []byte(urls[i])}, marketyield.Payload{Name: fmt.Sprintf("page-%d-response", i+1), Body: []byte(pages[i])})
	}
	hash := marketyield.HashPayloads(payloads...)
	for i, want := range []string{"0", "0.0206", "0.020612345678901234"} {
		item, o := batch.Items[i], batch.Items[i].Observation
		if o.Rate.String() != want || o.RateKind != "apr" || o.RateOrigin != "reported" || o.RateMode != "variable" || o.TierMode != "unknown" ||
			o.RulePrincipalLossMode != "unknown" || o.RuleEligibility != "unknown" || *o.EligibilityReason != "public_lending_rate_only" || o.Availability != "unknown" || *o.SourcePayloadHash != hash {
			t.Fatalf("observation=%+v", o)
		}
		if item.Route.ProductCode != "simple-earn-flexible-avax" || item.Route.Provider != "OKX" || item.Route.ProviderType != "cex" || item.Route.Network != nil || item.Route.ContractAddress != nil ||
			item.Route.DepositAssetKey != assetKey || item.Route.RedeemAssetKey != assetKey || item.Route.PositionAssetKey != "cex:okx:earn-flexible:AVAX" || *item.Route.PriceExposureAsset != "AVAX" || strings.Contains(item.Route.SourceURL, "after=") {
			t.Fatalf("route=%+v", item.Route)
		}
		if o.TierNo != 1 || !o.TierMinAmount.IsZero() || o.TierMaxAmount != nil || o.ExposureRatio.String() != "1" || len(o.RewardAssetKeys) != 1 || o.RewardAssetKeys[0] != assetKey || !o.RewardComponentRates[0].Equal(*o.Rate) {
			t.Fatalf("tier/exposure/reward=%+v", o)
		}
		if o.EntryFeeRate != nil || o.ExitFeeRate != nil || o.PerformanceFeeRate != nil || o.FixedPenaltyRate != nil || o.EntryFeeAmount != nil || o.ExitFeeAmount != nil || o.FixedFeeAssetKey != nil ||
			o.FixedPrincipalLossRate != nil || o.Capacity != nil || o.RemainingCapacity != nil || o.TVL != nil || o.LockSeconds != 0 || o.UnbondingSeconds != nil || o.BlockHeight != nil || o.BlockHash != nil || o.Finality != nil {
			t.Fatalf("invented historical fields=%+v", o)
		}
	}
	if !batch.Items[2].Observation.ObservationTime.Equal(last.Truncate(time.Millisecond)) || batch.Items[2].Observation.ObservationTime.Location() != time.UTC {
		t.Fatal("source milliseconds not preserved")
	}
}

func TestCollectorCompletesAtEmptyPageAndDeduplicatesEqualDecimals(t *testing.T) {
	row := point(testNow.Add(-time.Hour), "1.5") // >100% is not a parse error.
	batch, urls, err := collectFixture(t, []string{page(row, strings.Replace(row, "1.5", "1.500", 1)), page()})
	if err != nil || len(batch.Items) != 1 || len(urls) != 2 || batch.Items[0].Observation.Rate.String() != "1.5" {
		t.Fatalf("batch=%+v urls=%v err=%v", batch, urls, err)
	}
	want := marketyield.HashPayloads(
		marketyield.Payload{Name: "page-1-request", Body: []byte(urls[0])},
		marketyield.Payload{Name: "page-1-response", Body: []byte(page(row, strings.Replace(row, "1.5", "1.500", 1)))},
		marketyield.Payload{Name: "page-2-request", Body: []byte(urls[1])},
		marketyield.Payload{Name: "page-2-response", Body: []byte(page())},
	)
	if *batch.Items[0].Observation.SourcePayloadHash != want {
		t.Fatal("hash omitted terminal empty page")
	}
}

func TestCollectorRejectsInvalidRatesAndResponseShapes(t *testing.T) {
	validPoint := point(testNow.Add(-time.Hour), "0.0206")
	validPage := page(validPoint)
	fixtures := map[string]string{
		"missing code": `{"data":[]}`, "error code": `{"code":"1","data":[]}`, "null data": `{"code":"0","data":null}`, "missing data": `{"code":"0"}`,
		"empty": page(), "malformed": `{"code":`, "trailing": validPage + `{}`, "null point": page("null"),
		"wrong currency": strings.Replace(validPage, "AVAX", "BTC", 1),
		"missing rate":   strings.Replace(validPage, `"lendingRate":"0.0206",`, "", 1),
		"null rate":      strings.Replace(validPage, `"lendingRate":"0.0206"`, `"lendingRate":null`, 1),
		"numeric rate":   strings.Replace(validPage, `"lendingRate":"0.0206"`, `"lendingRate":0.0206`, 1),
		"no timestamp":   `{"code":"0","data":[{"ccy":"AVAX","lendingRate":"0.01"}]}`,
		"future":         page(point(testNow.Add(6*time.Minute), "0.02")),
	}
	for _, rate := range []string{"", "-1", "-0.0000000000000000001", "1e-2147483648", "1e20", " 1", "NaN", "100000000000000000000"} {
		fixtures["invalid rate "+rate] = page(point(testNow.Add(-time.Hour), rate))
	}
	for _, ts := range []string{"0", "-1", "1.2", "1e10", "9223372036854775808"} {
		fixtures["invalid timestamp "+ts] = page(fmt.Sprintf(`{"ccy":"AVAX","ts":%q,"lendingRate":"0.1"}`, ts))
	}
	for name, body := range fixtures {
		t.Run(name, func(t *testing.T) {
			if batch, _, err := collectFixture(t, []string{body, page()}); err == nil || len(batch.Items) != 0 {
				t.Fatalf("accepted invalid response batch=%+v err=%v", batch, err)
			}
		})
	}
}

func TestCollectorRejectsIncompletePaginationAndUntruncatedConflicts(t *testing.T) {
	newer, boundary, earlier := testNow.Add(-time.Hour), testNow.Add(-2*time.Hour), testNow.Add(-3*time.Hour)
	first := page(point(newer, "0.01"), point(boundary, "0.020000000000000000001"))
	validBoundary := point(boundary, "0.0200000000000000000010")
	cases := map[string][]string{
		"boundary conflict past precision":  {first, page(point(boundary, "0.020000000000000000002"), point(earlier, "0.03"))},
		"duplicate conflict past precision": {page(point(newer, "0.020000000000000000001"), point(newer, "0.020000000000000000002"))},
		"cursor did not advance":            {first, page(validBoundary)},
		"row beyond cursor despite old min": {first, page(point(newer, "0.01"), point(testNow.Add(-8*24*time.Hour), "0.01"))},
		"boundary wrong currency":           {first, page(strings.Replace(validBoundary, "AVAX", "BTC", 1), point(earlier, "0.03"))},
		"boundary missing rate":             {first, page(fmt.Sprintf(`{"ccy":"AVAX","ts":%q}`, strconv.FormatInt(boundary.UnixMilli(), 10)), point(earlier, "0.03"))},
		"midway HTTP error":                 {first, "HTTP_ERROR"},
		"midway JSON error":                 {first, `{"code":"0","data":null}`},
		"stale":                             {page(point(testNow.Add(-7*time.Hour), "0.02")), page()},
	}
	for i := range 8 {
		cases["page limit"] = append(cases["page limit"], page(point(testNow.Add(-time.Duration(i+1)*time.Hour), "0.02")))
	}
	for name, pages := range cases {
		t.Run(name, func(t *testing.T) {
			batch, urls, err := collectFixture(t, pages)
			if err == nil || len(batch.Items) != 0 {
				t.Fatalf("accepted partial history=%+v err=%v", batch, err)
			}
			if name == "page limit" && len(urls) != 8 {
				t.Fatalf("page limit requested %d pages", len(urls))
			}
		})
	}
}

func TestCollectorFiltersOldRatesButValidatesAllTimesAndCurrencies(t *testing.T) {
	current := point(testNow.Add(-time.Hour), "0.02")
	old := fmt.Sprintf(`{"ccy":"AVAX","ts":%q,"lendingRate":null}`, strconv.FormatInt(testNow.Add(-8*24*time.Hour).UnixMilli(), 10))
	batch, urls, err := collectFixture(t, []string{page(current, old)})
	if err != nil || len(batch.Items) != 1 || len(urls) != 1 {
		t.Fatalf("old null lendingRate invalidated usable data: %v", err)
	}
	for _, row := range []string{strings.Replace(old, "AVAX", "BTC", 1), `{"ccy":"AVAX","ts":"bad"}`} {
		if batch, _, err = collectFixture(t, []string{page(current, row)}); err == nil || len(batch.Items) != 0 {
			t.Fatal("old invalid currency/timestamp ignored")
		}
	}
	// The lower boundary is inclusive. It must have the new lender rate and
	// requires one further page before a complete response can be claimed.
	lower := point(testNow.Add(-7*24*time.Hour), "0.03")
	batch, urls, err = collectFixture(t, []string{page(current, lower), page()})
	if err != nil || len(batch.Items) != 2 || len(urls) != 2 {
		t.Fatalf("inclusive lower bound failed: %v", err)
	}
}
