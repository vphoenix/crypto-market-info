package okx

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/vphoenix/crypto-market-info/internal/exchange"
	"github.com/vphoenix/crypto-market-info/internal/model"
)

type estimateCapture struct{ values chan model.FundingEstimate }

func (c estimateCapture) Put(value model.FundingEstimate) error {
	c.values <- value
	return nil
}

func TestFundingBatchesTwentyArgsAndWaitsForEachACK(t *testing.T) {
	instruments := make([]model.Instrument, 21)
	for i := range instruments {
		instruments[i] = okxInstrument()
		instruments[i].ID = uint32(i + 1)
		instruments[i].ExchangeSymbol = fmt.Sprintf("A%02d-USDT-SWAP", i)
	}
	estimates := estimateCapture{values: make(chan model.FundingEstimate, 1)}
	confirmations := confirmationCapture{values: make(chan struct {
		instrument model.Instrument
		target     time.Time
	}, 1)}
	upgrader := websocket.Upgrader{}
	observed := make(chan []int, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		counts := make([]int, 0, 2)
		for batch := 0; batch < 2; batch++ {
			var request struct {
				ID   string              `json:"id"`
				Args []map[string]string `json:"args"`
			}
			if err := conn.ReadJSON(&request); err != nil {
				return
			}
			counts = append(counts, len(request.Args))
			if batch == 0 {
				symbol := request.Args[len(request.Args)-1]["instId"]
				_ = conn.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf(`{"arg":{"channel":"funding-rate","instId":%q},"data":[{"instId":%q,"instType":"SWAP","fundingRate":"0.1","fundingTime":"1787097600123","ts":"1787097599000"}]}`, symbol, symbol)))
			}
			for i, arg := range request.Args {
				if batch == 0 && i == len(request.Args)-1 {
					time.Sleep(10 * time.Millisecond)
					if len(estimates.values) != 0 {
						t.Error("funding became visible before its own ACK")
					}
				}
				_ = conn.WriteJSON(map[string]any{"id": request.ID, "event": "subscribe", "arg": arg})
			}
		}
		observed <- counts
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()
	runtime := FundingRuntime{Instruments: instruments, Estimates: estimates, Confirmations: confirmations, WSEndpoint: "ws" + strings.TrimPrefix(server.URL, "http"), ConnectGate: exchange.NewRequestGate(0), PingInterval: time.Hour}
	if err := runtime.Validate(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runtime.runConnection(ctx) }()
	select {
	case counts := <-observed:
		if len(counts) != 2 || counts[0] != 20 || counts[1] != 1 {
			t.Fatal(counts)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("funding did not subscribe two batches")
	}
	select {
	case estimate := <-estimates.values:
		if estimate.InstrumentID != 20 {
			t.Fatal(estimate.InstrumentID)
		}
	case <-time.After(time.Second):
		t.Fatal("funding did not replay acknowledged target")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if runtime.HealthSnapshot().ReadyInstruments != 0 {
		t.Fatal("stopped funding kept ready estimates")
	}
}

func (c estimateCapture) MarkUnavailable(_ []uint32) {}

type confirmationCapture struct {
	values chan struct {
		instrument model.Instrument
		target     time.Time
	}
}

func (c confirmationCapture) Schedule(_ context.Context, instrument model.Instrument, target time.Time) error {
	c.values <- struct {
		instrument model.Instrument
		target     time.Time
	}{instrument: instrument, target: target}
	return nil
}

func TestFundingRuntimeUsesOneConnectionForConfiguredInstruments(t *testing.T) {
	btc := okxInstrument()
	eth := btc
	eth.ID, eth.ExchangeSymbol, eth.BaseAsset = 3, "ETH-USDT-SWAP", "ETH"
	upgrader := websocket.Upgrader{}
	subscription := make(chan []map[string]string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		var request struct {
			Operation string              `json:"op"`
			Args      []map[string]string `json:"args"`
		}
		if err = conn.ReadJSON(&request); err != nil {
			return
		}
		if request.Operation != "subscribe" {
			return
		}
		subscription <- request.Args
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"event":"subscribe","arg":{"channel":"funding-rate","instId":"BTC-USDT-SWAP"}}`))
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"event":"subscribe","arg":{"channel":"funding-rate","instId":"ETH-USDT-SWAP"}}`))
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"arg":{"channel":"funding-rate","instId":"BTC-USDT-SWAP"},"data":[{"instId":"BTC-USDT-SWAP","instType":"SWAP","fundingRate":"0.1","fundingTime":"1787097600123","ts":"1787097599000"}]}`))
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"arg":{"channel":"funding-rate","instId":"ETH-USDT-SWAP"},"data":[{"instId":"ETH-USDT-SWAP","instType":"SWAP","fundingRate":"0.2","fundingTime":"1787097600456","ts":"1787097599001"}]}`))
		for {
			if _, _, err = conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()
	estimates := estimateCapture{values: make(chan model.FundingEstimate, 2)}
	confirmations := confirmationCapture{values: make(chan struct {
		instrument model.Instrument
		target     time.Time
	}, 2)}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	runtime := FundingRuntime{Instruments: []model.Instrument{btc, eth}, Estimates: estimates, Confirmations: confirmations, WSEndpoint: "ws" + strings.TrimPrefix(server.URL, "http"), PingInterval: time.Hour}
	go func() { done <- runtime.Run(ctx) }()
	select {
	case args := <-subscription:
		encoded, _ := json.Marshal(args)
		if len(args) != 2 || !strings.Contains(string(encoded), "BTC-USDT-SWAP") || !strings.Contains(string(encoded), "ETH-USDT-SWAP") {
			t.Fatalf("subscription=%s", encoded)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("funding subscription was not received")
	}
	for index := 0; index < 2; index++ {
		select {
		case <-estimates.values:
		case <-time.After(2 * time.Second):
			t.Fatal("funding estimate was not delivered")
		}
		select {
		case <-confirmations.values:
		case <-time.After(2 * time.Second):
			t.Fatal("funding confirmation was not scheduled")
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
