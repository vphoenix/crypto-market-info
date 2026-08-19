package binance

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/model"
)

func TestFundingHistoryMatchesExactMillisecondTarget(t *testing.T) {
	instrument := testInstrument()
	target := time.UnixMilli(1787097600123).UTC()
	payload := []byte(`[{"symbol":"BTCUSDT","fundingRate":"-0.0001","fundingTime":1787097600123}]`)
	actual, found, err := ParseFundingHistory(payload, instrument, target)
	if err != nil || !found || !actual.IsActual || actual.FundingTime.UnixMilli() != target.UnixMilli() {
		t.Fatalf("actual=%+v found=%v err=%v", actual, found, err)
	}
	if _, found, err = ParseFundingHistory(payload, instrument, target.Add(time.Millisecond)); err != nil || found {
		t.Fatalf("millisecond-mismatched response found=%v err=%v", found, err)
	}
}

func TestActualFundingRequestDoesNotImmediatelyRetry(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		http.Error(w, "temporary", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	client := NewClient()
	client.FuturesBaseURL = server.URL
	client.HTTP = server.Client()
	_, _, err := client.ActualFundingRate(context.Background(), testInstrument(), time.UnixMilli(1787097600123).UTC())
	if err == nil {
		t.Fatal("failed funding history request unexpectedly succeeded")
	}
	if calls.Load() != 1 {
		t.Fatalf("actual funding request retried immediately: calls=%d", calls.Load())
	}
}

func TestParseFundingWebSocketUpdate(t *testing.T) {
	instrument := testInstrument()
	estimate, matched, err := ParseFundingUpdate(
		[]byte(`{"e":"markPriceUpdate","E":1787097599123,"s":"BTCUSDT","r":"0.0002","T":1787097600456}`),
		map[string]model.Instrument{"BTCUSDT": instrument},
	)
	if err != nil {
		t.Fatal(err)
	}
	if matched.ID != instrument.ID || estimate.FundingTime.UnixMilli() != 1787097600456 || estimate.SourceTime.UnixMilli() != 1787097599123 || estimate.Rate.String() != "0.0002" {
		t.Fatalf("estimate=%+v matched=%+v", estimate, matched)
	}
}
