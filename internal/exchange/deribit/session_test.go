package deribit

import (
	"context"
	"github.com/gorilla/websocket"
	"github.com/vphoenix/crypto-market-info/internal/exchange"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestSessionHeartbeatBypassesWaitingSubscription(t *testing.T) {
	up := websocket.Upgrader{}
	reply := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		var request struct {
			ID     int    `json:"id"`
			Method string `json:"method"`
		}
		if conn.ReadJSON(&request) != nil {
			return
		}
		_ = conn.WriteJSON(map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": "ok"})
		_ = conn.WriteJSON(map[string]any{"jsonrpc": "2.0", "method": "heartbeat", "params": map[string]any{"type": "test_request"}})
		if conn.ReadJSON(&request) == nil {
			reply <- request.Method
		}
	}))
	defer server.Close()
	c := NewClient(server.URL, "ws"+strings.TrimPrefix(server.URL, "http"))
	c.SubscriptionGate = exchange.NewRequestGate(5 * time.Second)
	if err := c.SubscriptionGate.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.Session(ctx, []string{"book.a.100ms"}, nil, func(StreamEvent) error { return nil }) }()
	select {
	case method := <-reply:
		if method != "public/test" {
			t.Fatalf("heartbeat displaced by %s", method)
		}
	case <-ctx.Done():
		t.Fatal("waiting subscription blocked heartbeat response")
	}
	cancel()
	<-done
}

func TestSessionDynamicACKAndCanceledChannelFence(t *testing.T) {
	up := websocket.Upgrader{}
	var wg sync.WaitGroup
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		wg.Add(1)
		defer wg.Done()
		defer conn.Close()
		for {
			var req struct {
				ID     int    `json:"id"`
				Method string `json:"method"`
				Params struct {
					Channels []string `json:"channels"`
				} `json:"params"`
			}
			if conn.ReadJSON(&req) != nil {
				return
			}
			var result any = "ok"
			if req.Method == "public/subscribe" || req.Method == "public/unsubscribe" {
				result = req.Params.Channels
			}
			if req.Method == "public/status" {
				result = map[string]any{"locked": "false"}
			}
			if conn.WriteJSON(map[string]any{"jsonrpc": "2.0", "id": req.ID, "result": result}) != nil {
				return
			}
		}
	}))
	defer server.Close()
	defer wg.Wait()
	c := NewClient(server.URL, "ws"+strings.TrimPrefix(server.URL, "http"))
	c.ControlGate = exchange.NewRequestGate(time.Millisecond)
	c.SubscriptionGate = exchange.NewRequestGate(time.Millisecond)
	c.FrameBudget = &exchange.BufferBudget{Limit: 1 << 20}
	commands := make(chan SessionCommand, 4)
	events := make(chan StreamEvent, 32)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	finished := make(chan error, 1)
	go func() {
		finished <- c.Session(ctx, []string{"book.a.100ms"}, commands, func(f StreamEvent) error {
			select {
			case events <- f:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}()
	waitKind := func(kind, ch string) {
		t.Helper()
		for {
			select {
			case f := <-events:
				if f.Kind == kind && (ch == "" || f.Channel == ch) {
					return
				}
			case <-ctx.Done():
				t.Fatal("session event timeout")
			}
		}
	}
	waitKind("ready", "")
	commands <- SessionCommand{Method: "public/subscribe", Channels: []string{"book.b.100ms"}}
	waitKind("subscribed", "book.b.100ms")
	commands <- SessionCommand{Method: "public/unsubscribe", Channels: []string{"book.b.100ms"}}
	waitKind("unsubscribed", "book.b.100ms")
	commands <- SessionCommand{Method: "public/status", Generation: 42}
	for {
		f := <-events
		if f.Kind == "status" {
			if f.Generation != 42 {
				t.Fatal("status generation lost")
			}
			break
		}
	}
	commands <- SessionCommand{Method: "public/subscribe", Channels: []string{"book.b.100ms"}}
	err := <-finished
	if err == nil || !strings.Contains(err.Error(), "fresh physical epoch") {
		t.Fatal(err)
	}
	cancel()
	if c.FrameBudget.Used() != 0 {
		t.Fatal("frame reservation leaked")
	}
}
