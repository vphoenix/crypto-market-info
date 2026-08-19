package binance

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/vphoenix/crypto-market-info/internal/model"
)

type estimateCapture struct{ values chan model.FundingEstimate }

func (c estimateCapture) Put(value model.FundingEstimate) error {
	c.values <- value
	return nil
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
	btc := testInstrument()
	eth := btc
	eth.ID, eth.ExchangeSymbol, eth.BaseAsset = 2, "ETHUSDT", "ETH"
	upgrader := websocket.Upgrader{}
	subscription := make(chan []string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		var request struct {
			Method string   `json:"method"`
			Params []string `json:"params"`
		}
		if err = conn.ReadJSON(&request); err != nil {
			return
		}
		if request.Method != "SUBSCRIBE" {
			return
		}
		subscription <- request.Params
		_ = conn.WriteJSON(map[string]any{"result": nil, "id": 1})
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"e":"markPriceUpdate","E":1787097599000,"s":"BTCUSDT","r":"0.1","T":1787097600123}`))
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"e":"markPriceUpdate","E":1787097599001,"s":"ETHUSDT","r":"0.2","T":1787097600456}`))
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
	runtime := FundingRuntime{Instruments: []model.Instrument{btc, eth}, Estimates: estimates, Confirmations: confirmations, WSEndpoint: "ws" + strings.TrimPrefix(server.URL, "http")}
	go func() { done <- runtime.Run(ctx) }()
	select {
	case params := <-subscription:
		encoded, _ := json.Marshal(params)
		if len(params) != 2 || !strings.Contains(string(encoded), "btcusdt@markPrice@1s") || !strings.Contains(string(encoded), "ethusdt@markPrice@1s") {
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
