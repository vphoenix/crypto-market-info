package binance

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/model"
)

func TestFundingHistoryMatchesSmallSourceTimestampOffset(t *testing.T) {
	instrument := testInstrument()
	target := time.UnixMilli(1787097600123).UTC()
	payload := []byte(`[{"symbol":"BTCUSDT","fundingRate":"-0.0001","fundingTime":1787097600127}]`)
	actual, found, err := ParseFundingHistory(payload, instrument, target)
	if err != nil || !found || !actual.IsActual || actual.FundingTime.UnixMilli() != target.UnixMilli()+4 || actual.HourTime != target.Truncate(time.Hour) {
		t.Fatalf("actual=%+v found=%v err=%v", actual, found, err)
	}
	if _, found, err = ParseFundingHistory(payload, instrument, target.Add(5*time.Millisecond)); err != nil || found {
		t.Fatalf("earlier source row matched: found=%v err=%v", found, err)
	}
	if _, found, err = ParseFundingHistory([]byte(`[{"symbol":"BTCUSDT","fundingRate":"-0.0001","fundingTime":1787097601123}]`), instrument, target); err != nil || found {
		t.Fatalf("later settlement matched: found=%v err=%v", found, err)
	}
	if _, found, err = ParseFundingHistory([]byte(`[{"symbol":"BTCUSDT","fundingRate":"-0.0001","fundingTime":1787097600123},{"symbol":"BTCUSDT","fundingRate":"-0.0002","fundingTime":1787097600127}]`), instrument, target); err == nil || found {
		t.Fatalf("ambiguous settlement accepted: found=%v err=%v", found, err)
	}
}

func TestActualFundingRequestIncludesDelayedSourceMillisecond(t *testing.T) {
	target := time.UnixMilli(1787097600000).UTC()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if q.Get("startTime") != strconv.FormatInt(target.UnixMilli(), 10) || q.Get("endTime") != strconv.FormatInt(target.UnixMilli()+999, 10) {
			t.Errorf("wrong funding window: %s", r.URL.RawQuery)
		}
		w.Write([]byte(`[{"symbol":"BTCUSDT","fundingRate":"0.0001","fundingTime":1787097600004}]`))
	}))
	defer server.Close()
	client := NewClient()
	client.FuturesBaseURL = server.URL
	client.HTTP = server.Client()
	rate, found, err := client.ActualFundingRate(context.Background(), testInstrument(), target)
	if err != nil || !found || rate.FundingTime.UnixMilli() != target.UnixMilli()+4 || rate.Rate.String() != "0.0001" {
		t.Fatalf("rate=%+v found=%v err=%v", rate, found, err)
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
