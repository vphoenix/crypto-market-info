package okx

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/model"
)

func TestFundingHistoryUsesRealizedRateAndExactMilliseconds(t *testing.T) {
	instrument := okxInstrument()
	target := time.UnixMilli(1787097600123).UTC()
	payload := []byte(`{"code":"0","msg":"","data":[{"instId":"BTC-USDT-SWAP","fundingRate":"0.2","realizedRate":"0.1","fundingTime":"1787097600123"}]}`)
	rate, found, err := ParseFundingHistory(payload, instrument, target)
	if err != nil || !found || rate.Rate.String() != "0.1" || !rate.IsActual || rate.FundingTime.UnixMilli() != target.UnixMilli() {
		t.Fatalf("rate=%+v found=%v err=%v", rate, found, err)
	}
	if _, found, err = ParseFundingHistory(payload, instrument, target.Add(time.Millisecond)); err != nil || found {
		t.Fatalf("millisecond-mismatched response found=%v err=%v", found, err)
	}
}

func TestFundingHistoryDoesNotPromoteEstimateWhenRealizedRateIsEmpty(t *testing.T) {
	instrument := okxInstrument()
	target := time.UnixMilli(1787097600123).UTC()
	payload := []byte(`{"code":"0","msg":"","data":[{"instId":"BTC-USDT-SWAP","fundingRate":"0.2","realizedRate":"","fundingTime":"1787097600123"}]}`)
	rate, found, err := ParseFundingHistory(payload, instrument, target)
	if err != nil {
		t.Fatal(err)
	}
	if found || rate != (model.FundingRate{}) {
		t.Fatalf("estimated funding was promoted to actual: rate=%+v found=%v", rate, found)
	}
}

func TestActualFundingRequestDoesNotImmediatelyRetry(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		http.Error(w, "temporary", http.StatusTooManyRequests)
	}))
	defer server.Close()
	client := NewClient()
	client.BaseURL = server.URL
	client.HTTP = server.Client()
	_, _, err := client.ActualFundingRate(context.Background(), okxInstrument(), time.UnixMilli(1787097600123).UTC())
	if err == nil {
		t.Fatal("failed funding history request unexpectedly succeeded")
	}
	if calls.Load() != 1 {
		t.Fatalf("actual funding request retried immediately: calls=%d", calls.Load())
	}
}

func TestActualFundingRequestCoversMoreThanTwentyHourlySettlements(t *testing.T) {
	instrument := okxInstrument()
	newest := time.UnixMilli(1787184000123).UTC()
	target := newest.Add(-24 * time.Hour)
	rows := make([]fundingWire, 25)
	for index := range rows {
		settlement := newest.Add(-time.Duration(index) * time.Hour)
		rows[index] = fundingWire{
			InstID:       instrument.ExchangeSymbol,
			FundingRate:  "0.2",
			RealizedRate: "0.1",
			FundingTime:  strconv.FormatInt(settlement.UnixMilli(), 10),
		}
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Query().Get("limit") != fundingHistoryLimit {
			http.Error(w, "history limit does not cover the startup window", http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(struct {
			Code string        `json:"code"`
			Msg  string        `json:"msg"`
			Data []fundingWire `json:"data"`
		}{Code: "0", Data: rows})
	}))
	defer server.Close()
	client := NewClient()
	client.BaseURL = server.URL
	client.HTTP = server.Client()
	rate, found, err := client.ActualFundingRate(context.Background(), instrument, target)
	if err != nil || !found {
		t.Fatalf("older hourly settlement was not found: found=%v err=%v", found, err)
	}
	if rate.FundingTime.UnixMilli() != target.UnixMilli() || rate.Rate.String() != "0.1" || !rate.IsActual {
		t.Fatalf("rate=%+v", rate)
	}
}

func TestParseFundingWebSocketUpdate(t *testing.T) {
	instrument := okxInstrument()
	payload := []byte(`{"arg":{"channel":"funding-rate","instId":"BTC-USDT-SWAP"},"data":[{"instId":"BTC-USDT-SWAP","instType":"SWAP","fundingRate":"0.0003","fundingTime":"1787097600456","ts":"1787097599123"}]}`)
	estimates, matched, err := ParseFundingUpdates(payload, map[string]model.Instrument{instrument.ExchangeSymbol: instrument})
	if err != nil {
		t.Fatal(err)
	}
	if len(estimates) != 1 || len(matched) != 1 || estimates[0].FundingTime.UnixMilli() != 1787097600456 || estimates[0].SourceTime.UnixMilli() != 1787097599123 || estimates[0].Rate.String() != "0.0003" {
		t.Fatalf("estimates=%+v matched=%+v", estimates, matched)
	}
}
