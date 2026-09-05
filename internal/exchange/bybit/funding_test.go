package bybit

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/model"
)

func tickerPayload(kind string, ts int64, fields string) []byte {
	return []byte(fmt.Sprintf(`{"topic":"tickers.BTCUSDT","type":%q,"ts":%d,"data":{"symbol":"BTCUSDT"%s}}`, kind, ts, fields))
}

func TestTickerSparseStatePublishesAndRefreshesCompleteEstimate(t *testing.T) {
	instrument := bybitTestInstrument()
	instruments := map[string]model.Instrument{instrument.ExchangeSymbol: instrument}
	cache := newTickerCache()
	target := int64(1787097600123)
	snapshot, matched, err := ParseTickerUpdate(tickerPayload("snapshot", 1000, fmt.Sprintf(`,"fundingRate":"0.001","nextFundingTime":"%d","fundingIntervalHour":"8"`, target)), instruments)
	if err != nil || matched.ID != instrument.ID {
		t.Fatalf("snapshot=%+v instrument=%+v err=%v", snapshot, matched, err)
	}
	estimate, complete, err := cache.Apply(snapshot, instrument)
	if err != nil || !complete || estimate.FundingTime.UnixMilli() != target || estimate.SourceTime.UnixMilli() != 1000 {
		t.Fatalf("estimate=%+v complete=%v err=%v", estimate, complete, err)
	}
	delta, _, err := ParseTickerUpdate(tickerPayload("delta", 1100, `,"lastPrice":"100"`), instruments)
	if err != nil {
		t.Fatal(err)
	}
	refreshed, complete, err := cache.Apply(delta, instrument)
	if err != nil || !complete || refreshed.SourceTime.UnixMilli() != 1100 || !refreshed.Rate.Equal(estimate.Rate) {
		t.Fatalf("refreshed=%+v complete=%v err=%v", refreshed, complete, err)
	}
	rateDelta, _, _ := ParseTickerUpdate(tickerPayload("delta", 1200, `,"fundingRate":"0.002"`), instruments)
	updated, complete, err := cache.Apply(rateDelta, instrument)
	if err != nil || !complete || updated.Rate.String() != "0.002" || updated.FundingTime.UnixMilli() != target {
		t.Fatalf("updated=%+v complete=%v err=%v", updated, complete, err)
	}
}

func TestTickerRequiresSnapshotAndClearsStateOnNewSnapshot(t *testing.T) {
	instrument := bybitTestInstrument()
	instruments := map[string]model.Instrument{instrument.ExchangeSymbol: instrument}
	cache := newTickerCache()
	delta, _, _ := ParseTickerUpdate(tickerPayload("delta", 1000, `,"fundingRate":"0.001"`), instruments)
	if _, _, err := cache.Apply(delta, instrument); err == nil {
		t.Fatal("delta before snapshot was accepted")
	}
	full, _, _ := ParseTickerUpdate(tickerPayload("snapshot", 1100, `,"fundingRate":"0.001","nextFundingTime":"1787097600123","fundingIntervalHour":"8"`), instruments)
	if _, complete, err := cache.Apply(full, instrument); err != nil || !complete {
		t.Fatalf("complete snapshot result complete=%v err=%v", complete, err)
	}
	partial, _, _ := ParseTickerUpdate(tickerPayload("snapshot", 1200, `,"fundingRate":"0.003"`), instruments)
	if _, complete, err := cache.Apply(partial, instrument); err != nil || complete {
		t.Fatalf("partial replacement snapshot reused stale fields: complete=%v err=%v", complete, err)
	}
	fill, _, _ := ParseTickerUpdate(tickerPayload("delta", 1300, `,"nextFundingTime":"1787126400456","fundingIntervalHour":"4"`), instruments)
	estimate, complete, err := cache.Apply(fill, instrument)
	if err != nil || !complete || estimate.FundingTime.UnixMilli() != 1787126400456 || estimate.Rate.String() != "0.003" {
		t.Fatalf("filled estimate=%+v complete=%v err=%v", estimate, complete, err)
	}
}

func TestTickerRejectsInvalidIdentityFieldsAndTimeRollback(t *testing.T) {
	instrument := bybitTestInstrument()
	instruments := map[string]model.Instrument{instrument.ExchangeSymbol: instrument}
	for name, payload := range map[string][]byte{
		"topic":     []byte(`{"topic":"tickers.ETHUSDT","type":"snapshot","ts":1000,"data":{"symbol":"BTCUSDT"}}`),
		"symbol":    []byte(`{"topic":"tickers.ETHUSDT","type":"snapshot","ts":1000,"data":{"symbol":"ETHUSDT"}}`),
		"null rate": tickerPayload("snapshot", 1000, `,"fundingRate":null`),
		"bad time":  tickerPayload("snapshot", 1000, `,"nextFundingTime":"bad"`),
		"interval":  tickerPayload("snapshot", 1000, `,"fundingIntervalHour":"0"`),
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, err := ParseTickerUpdate(payload, instruments); err == nil {
				t.Fatal("invalid ticker was accepted")
			}
		})
	}
	cache := newTickerCache()
	first, _, _ := ParseTickerUpdate(tickerPayload("snapshot", 2000, `,"fundingRate":"0.001","nextFundingTime":"1787097600123","fundingIntervalHour":"1"`), instruments)
	_, _, _ = cache.Apply(first, instrument)
	rollback, _, _ := ParseTickerUpdate(tickerPayload("delta", 1999, `,"fundingRate":"0.002"`), instruments)
	if _, _, err := cache.Apply(rollback, instrument); err == nil {
		t.Fatal("ticker timestamp rollback was accepted")
	}
}

func TestTickerSubscriptionBatchesHonorEncodedArgsLimit(t *testing.T) {
	topics := []string{"tickers.A", "tickers.B", "tickers.C"}
	batches, err := tickerSubscriptionBatches(topics, 29)
	if err != nil || len(batches) != 2 {
		t.Fatalf("batches=%v err=%v", batches, err)
	}
	for _, batch := range batches {
		encoded, marshalErr := json.Marshal(batch)
		if marshalErr != nil || len(encoded) > 29 {
			t.Fatalf("batch=%v encoded=%q err=%v", batch, encoded, marshalErr)
		}
	}
	if _, err = tickerSubscriptionBatches([]string{strings.Repeat("x", 40)}, 10); err == nil {
		t.Fatal("oversize single topic was accepted")
	}
}

func TestParseFundingHistoryRequiresExactIdentityAndTimestamp(t *testing.T) {
	instrument := bybitTestInstrument()
	target := time.UnixMilli(1787097600123).UTC()
	payload := []byte(`{"retCode":0,"retMsg":"OK","result":{"category":"linear","list":[{"symbol":"BTCUSDT","fundingRate":"0.001","fundingRateTimestamp":"1787097600123"}]}}`)
	actual, found, err := ParseFundingHistory(payload, instrument, target)
	if err != nil || !found || actual.InstrumentID != instrument.ID || actual.FundingTime.UnixMilli() != target.UnixMilli() || !actual.IsActual {
		t.Fatalf("actual=%+v found=%v err=%v", actual, found, err)
	}
	for name, body := range map[string]string{
		"empty":        `{"retCode":0,"result":{"category":"linear","list":[]}}`,
		"other time":   `{"retCode":0,"result":{"category":"linear","list":[{"symbol":"BTCUSDT","fundingRate":"0.001","fundingRateTimestamp":"1787097600122"}]}}`,
		"other symbol": `{"retCode":0,"result":{"category":"linear","list":[{"symbol":"ETHUSDT","fundingRate":"0.001","fundingRateTimestamp":"1787097600123"}]}}`,
	} {
		t.Run(name, func(t *testing.T) {
			_, gotFound, gotErr := ParseFundingHistory([]byte(body), instrument, target)
			if name == "other symbol" {
				if gotErr == nil {
					t.Fatal("wrong symbol was treated as not found")
				}
				return
			}
			if gotErr != nil || gotFound {
				t.Fatalf("found=%v err=%v", gotFound, gotErr)
			}
		})
	}
}

func TestParseFundingHistoryRejectsNonMillisecondTarget(t *testing.T) {
	instrument := bybitTestInstrument()
	target := time.UnixMilli(1787097600123).UTC().Add(time.Nanosecond)
	payload := []byte(`{"retCode":0,"retMsg":"OK","result":{"category":"linear","list":[]}}`)
	if _, _, err := ParseFundingHistory(payload, instrument, target); err == nil {
		t.Fatal("non-millisecond target was accepted")
	}
}

func TestActualFundingRateBuildsDefensiveEndTime(t *testing.T) {
	instrument := bybitTestInstrument()
	target := time.UnixMilli(1787097600123).UTC()
	client := NewClient()
	client.BaseURL = "https://bybit.test"
	client.HTTP = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.Path != "/v5/market/funding/history" || request.URL.Query().Get("category") != "linear" || request.URL.Query().Get("symbol") != instrument.ExchangeSymbol || request.URL.Query().Get("endTime") != strconv.FormatInt(target.UnixMilli()+1, 10) || request.URL.Query().Get("limit") != "1" {
			t.Fatalf("request URL=%s", request.URL)
		}
		body := fmt.Sprintf(`{"retCode":0,"result":{"category":"linear","list":[{"symbol":"BTCUSDT","fundingRate":"0.001","fundingRateTimestamp":%q}]}}`, strconv.FormatInt(target.UnixMilli(), 10))
		return testHTTPResponse(http.StatusOK, body, nil), nil
	})}
	if _, found, err := client.ActualFundingRate(context.Background(), instrument, target); err != nil || !found {
		t.Fatalf("found=%v err=%v", found, err)
	}
}
