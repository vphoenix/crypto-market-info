package wsstream

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/vphoenix/crypto-market-info/internal/exchange"
)

func TestQuietActiveBookKeepsValidityButMissingSnapshotAndGapResync(t *testing.T) {
	var first, second, missingResync atomic.Int64
	var sendGap atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, e := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if e != nil {
			return
		}
		defer conn.Close()
		initial := true
		gapSent := false
		for {
			var req testWire
			if e = conn.ReadJSON(&req); e != nil {
				return
			}
			if req.Operation == "ping" {
				if sendGap.Load() && !gapSent {
					gapSent = true
					if e = conn.WriteJSON(testWire{Topic: "a", Kind: "delta", Value: -1}); e != nil {
						return
					}
				}
				if e = conn.WriteJSON(testWire{Operation: "pong"}); e != nil {
					return
				}
				continue
			}
			for _, topic := range req.Topics {
				if req.Operation == "unsubscribe" && topic == "b" {
					missingResync.Add(1)
				}
				if e = conn.WriteJSON(testWire{Operation: req.Operation, RequestID: req.RequestID, Topic: topic}); e != nil {
					return
				}
			}
			if initial {
				initial = false
				if e = conn.WriteJSON(testWire{Topic: "a", Kind: "snapshot", Value: 1}); e != nil {
					return
				}
			}
		}
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stats := &Statistics{}
	var gapResynced atomic.Bool
	firstTarget := testTarget("a", &first)
	firstTarget.Reset = func(reason string) {
		first.Store(0)
		if reason == "sequence gap" {
			gapResynced.Store(true)
		}
	}
	done := make(chan error, 1)
	go func() {
		done <- RunConnection(ctx, Config{Endpoint: "ws" + strings.TrimPrefix(server.URL, "http"), ConnectGate: exchange.NewRequestGate(0), Protocol: testProtocol{}, Targets: []*Target{firstTarget, testTarget("b", &second)}, BatchSize: 2, AckPerTopic: true, Resync: true, AllowQuietActive: true, ControlInterval: time.Millisecond, PingInterval: 10 * time.Millisecond, SilenceTimeout: 80 * time.Millisecond, SubscribeTimeout: 200 * time.Millisecond, Stats: stats})
	}()
	eventually(t, func() bool { return first.Load() == 1 })
	eventually(t, func() bool { return missingResync.Load() > 0 })
	time.Sleep(180 * time.Millisecond)
	if first.Load() != 1 {
		t.Fatal("healthy quiet book became invalid")
	}
	if second.Load() != 0 {
		t.Fatal("missing initial snapshot became valid")
	}
	if event := stats.LastResync.Load(); event == nil || event.Topic != "b" {
		t.Fatal("missing-snapshot resync reason lost")
	}
	sendGap.Store(true)
	eventually(t, func() bool { return first.Load() == 0 })
	eventually(t, func() bool { return gapResynced.Load() })
	cancel()
	select {
	case e := <-done:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(time.Second):
		t.Fatal("connection did not stop")
	}
	if first.Load() != 0 || second.Load() != 0 {
		t.Fatal("connection cancellation retained valid books")
	}
}
