package deribit

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/vphoenix/crypto-market-info/internal/exchange"
)

func TestSessionHeartbeatTakesControlSlotAlreadyReservedByOrdinaryRequest(t *testing.T) {
	upgrader := websocket.Upgrader{}
	first := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		var req struct {
			ID     int    `json:"id"`
			Method string `json:"method"`
		}
		if conn.ReadJSON(&req) != nil {
			return
		}
		_ = conn.WriteJSON(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": "ok"})
		_ = conn.WriteJSON(map[string]any{"jsonrpc": "2.0", "method": "heartbeat", "params": map[string]any{"type": "test_request"}})
		if conn.ReadJSON(&req) == nil {
			first <- req.Method
		}
	}))
	defer server.Close()
	c := NewClient(server.URL, "ws"+strings.TrimPrefix(server.URL, "http"))
	c.ControlGate = exchange.NewRequestGate(150 * time.Millisecond)
	c.SubscriptionGate = exchange.NewRequestGate(0)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.Session(ctx, []string{"book.a.100ms"}, nil, func(StreamEvent) error { return nil }) }()
	select {
	case method := <-first:
		if method != "public/test" {
			t.Fatalf("reserved ordinary slot delayed heartbeat: %s", method)
		}
	case <-ctx.Done():
		t.Fatal("heartbeat never sent")
	}
	cancel()
	<-done
}

func TestSessionReceivedAtIsSocketReceiptBeforeSlowCallback(t *testing.T) {
	upgrader := websocket.Upgrader{}
	secondWritten := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for n := 0; n < 2; n++ {
			var req struct {
				ID     int    `json:"id"`
				Method string `json:"method"`
			}
			if conn.ReadJSON(&req) != nil {
				return
			}
			var result any = "ok"
			if req.Method == "public/subscribe" {
				result = []string{"book.a.100ms"}
			}
			if conn.WriteJSON(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": result}) != nil {
				return
			}
		}
		for n := 0; n < 2; n++ {
			if conn.WriteJSON(map[string]any{"jsonrpc": "2.0", "method": "subscription", "params": map[string]any{"channel": "book.a.100ms", "data": map[string]any{"marker": n}}}) != nil {
				return
			}
		}
		close(secondWritten)
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()
	c := NewClient(server.URL, "ws"+strings.TrimPrefix(server.URL, "http"))
	c.ControlGate = exchange.NewRequestGate(0)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	done := make(chan error, 1)
	observed := make(chan time.Time, 1)
	count := 0
	var resumed time.Time
	go func() {
		done <- c.Session(ctx, []string{"book.a.100ms"}, nil, func(e StreamEvent) error {
			if e.Kind != "message" {
				return nil
			}
			count++
			if count == 1 {
				<-secondWritten
				time.Sleep(500 * time.Millisecond)
				resumed = time.Now()
			} else {
				observed <- e.ReceivedAt
			}
			return nil
		})
	}()
	select {
	case at := <-observed:
		if !at.Before(resumed) {
			t.Fatal("callback backlog was recorded as a fresh socket receipt")
		}
	case <-ctx.Done():
		t.Fatal("no second frame")
	}
	cancel()
	<-done
}
