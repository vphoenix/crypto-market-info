package okx

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/vphoenix/crypto-market-info/internal/exchange"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/orderbook"
)

func TestBookAndFundingConnectionsShareDialGate(t *testing.T) {
	connections := make(chan time.Time, 2)
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		connections <- time.Now()
		for {
			if _, _, err = conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()

	instrument := okxInstrument()
	book, _ := orderbook.New(instrument.ID, 400)
	estimates := estimateCapture{values: make(chan model.FundingEstimate, 1)}
	confirmations := confirmationCapture{values: make(chan struct {
		instrument model.Instrument
		target     time.Time
	}, 1)}
	endpoint := "ws" + strings.TrimPrefix(server.URL, "http")
	gate := exchange.NewRequestGate(80 * time.Millisecond)
	bookRuntime := Runtime{Instrument: instrument, Book: book, WSEndpoint: endpoint, ConnectGate: gate, PingInterval: time.Hour}
	fundingRuntime := FundingRuntime{Instruments: []model.Instrument{instrument}, Estimates: estimates, Confirmations: confirmations, WSEndpoint: endpoint, ConnectGate: gate, PingInterval: time.Hour}

	ctx, cancel := context.WithCancel(context.Background())
	results := make(chan error, 2)
	go func() { results <- bookRuntime.Run(ctx) }()
	go func() { results <- fundingRuntime.Run(ctx) }()
	times := []time.Time{<-connections, <-connections}
	sort.Slice(times, func(i, j int) bool { return times[i].Before(times[j]) })
	if spacing := times[1].Sub(times[0]); spacing < 70*time.Millisecond {
		cancel()
		t.Fatalf("OKX websocket dials were only %s apart", spacing)
	}
	cancel()
	for range 2 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
}
