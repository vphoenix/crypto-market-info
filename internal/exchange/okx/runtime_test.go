package okx

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/vphoenix/crypto-market-info/internal/orderbook"
)

func TestRuntimeSubscribesAppliesSnapshotAndUpdate(t *testing.T) {
	instrument := okxInstrument()
	book, _ := orderbook.New(instrument.ID, 400)
	upgrader := websocket.Upgrader{}
	subscriptionID := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		var request struct {
			ID string `json:"id"`
		}
		if err = conn.ReadJSON(&request); err != nil {
			return
		}
		subscriptionID <- request.ID
		messages := []string{
			`{"event":"subscribe","arg":{"channel":"books","instId":"BTC-USDT-SWAP"}}`,
			`{"arg":{"channel":"books","instId":"BTC-USDT-SWAP"},"action":"snapshot","data":[{"asks":[["101","1","0","1"]],"bids":[["100","2","0","1"]],"ts":"1000","prevSeqId":-1,"seqId":10,"checksum":0}]}`,
			`{"arg":{"channel":"books","instId":"BTC-USDT-SWAP"},"action":"update","data":[{"asks":[],"bids":[["100","3","0","1"]],"ts":"1100","prevSeqId":10,"seqId":11,"checksum":0}]}`,
		}
		for _, message := range messages {
			if err = conn.WriteMessage(websocket.TextMessage, []byte(message)); err != nil {
				return
			}
		}
		for {
			if _, _, err = conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	runtime := Runtime{Instrument: instrument, Book: book, WSEndpoint: "ws" + strings.TrimPrefix(server.URL, "http"), PingInterval: time.Hour}
	go func() { done <- runtime.Run(ctx) }()
	select {
	case id := <-subscriptionID:
		if id != "book2" {
			t.Fatalf("subscription id=%q", id)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("subscription was not received")
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		snapshot, valid := book.Snapshot(50)
		if valid && snapshot.Sequence == 11 && snapshot.Bids[0].QtyLot == 3 {
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
	t.Fatalf("runtime did not apply OKX stream: view=%+v", book.View())
}
