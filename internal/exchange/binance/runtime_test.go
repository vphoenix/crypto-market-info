package binance

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/vphoenix/crypto-market-info/internal/exchange"
	"github.com/vphoenix/crypto-market-info/internal/orderbook"
)

func TestRuntimeBuffersDiffBeforeRESTSnapshot(t *testing.T) {
	instrument := testInstrument()
	book, _ := orderbook.New(instrument.ID, 1000)
	rest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(50 * time.Millisecond)
		_, _ = w.Write([]byte(`{"lastUpdateId":100,"T":1000,"bids":[["100","0.001"]],"asks":[["101","0.001"]]}`))
	}))
	defer rest.Close()
	upgrader := websocket.Upgrader{}
	wsServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"e":"depthUpdate","E":1100,"T":1100,"s":"BTCUSDT","U":100,"u":101,"pu":99,"b":[["100","0.007"]],"a":[]}`))
		for {
			if _, _, err = conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer wsServer.Close()
	client := NewClient()
	client.FuturesBaseURL, client.HTTP, client.Retry = rest.URL, rest.Client(), exchange.DefaultHTTPRetryConfig()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	runtime := Runtime{Instrument: instrument, Book: book, Client: client, WSEndpoint: "ws" + strings.TrimPrefix(wsServer.URL, "http")}
	go func() { done <- runtime.Run(ctx) }()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		snapshot, valid := book.Snapshot(50)
		if valid && snapshot.Sequence == 101 && snapshot.Bids[0].QtyLot == 7 {
			cancel()
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done
	t.Fatalf("runtime did not bridge snapshot and buffered diff: view=%+v", book.View())
}

func TestPerpetualRuntimeDefaultsToPublicEndpoint(t *testing.T) {
	instrument := testInstrument()
	book, _ := orderbook.New(instrument.ID, 1000)
	runtime := Runtime{Instrument: instrument, Book: book}
	if err := runtime.defaults(); err != nil {
		t.Fatal(err)
	}
	if runtime.WSEndpoint != "wss://fstream.binance.com/public/ws" {
		t.Fatalf("endpoint=%q", runtime.WSEndpoint)
	}
}
