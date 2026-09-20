package binance

import (
	"context"
	"encoding/json"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/orderbook"
)

// Explicitly opt-in diagnostic: public market-data reads, no credentials/orders.
func TestLiveSubscriptionProbe(t *testing.T) {
	if os.Getenv("BINANCE_LIVE_PROBE") != "1" {
		t.Skip("requires explicit BINANCE_LIVE_PROBE=1")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	client := NewClient()
	items, err := client.Instruments(ctx, model.MarketPerpetual)
	if err != nil {
		t.Fatal(err)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].ExchangeSymbol < items[j].ExchangeSymbol })
	if len(items) < 200 {
		t.Fatal("live catalog contains fewer than 200 instruments")
	}
	if err = client.waitWebsocket(ctx); err != nil {
		t.Fatal(err)
	}
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, "wss://fstream.binance.com/public/ws", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	writer := newSocketWriter(ctx, conn, 250*time.Millisecond)
	go writer.run()
	conn.SetPingHandler(writer.pong)
	streams := make([]string, 200)
	for i := range streams {
		streams[i] = strings.ToLower(items[i].ExchangeSymbol) + "@depth@100ms"
	}
	batches, err := subscriptionBatches(streams)
	if err != nil {
		t.Fatal(err)
	}
	marketMessages := 0
	for i, batch := range batches {
		id := int64(i + 1)
		request := map[string]any{"method": "SUBSCRIBE", "params": batch, "id": id}
		encoded, _ := json.Marshal(request)
		if err = writer.json(request); err != nil {
			t.Fatal(err)
		}
		_ = conn.SetReadDeadline(time.Now().Add(8 * time.Second))
		for {
			_, reply, readErr := conn.ReadMessage()
			if readErr != nil {
				t.Fatal(readErr)
			}
			ack, ackErr := parseSubscriptionACK(reply, id)
			if ackErr != nil {
				t.Fatal(ackErr)
			}
			if ack {
				t.Logf("batch=%d topics=%d bytes=%d ACK=%s", i+1, len(batch), len(encoded), reply)
				break
			}
			marketMessages++
		}
	}
	listID := int64(len(batches) + 1)
	if err = writer.json(map[string]any{"method": "LIST_SUBSCRIPTIONS", "id": listID}); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(8 * time.Second))
	for {
		_, reply, readErr := conn.ReadMessage()
		if readErr != nil {
			t.Fatal(readErr)
		}
		var listing struct {
			ID     int64    `json:"id"`
			Result []string `json:"result"`
		}
		if json.Unmarshal(reply, &listing) == nil && listing.ID == listID {
			sort.Strings(listing.Result)
			sort.Strings(streams)
			if len(listing.Result) != 200 {
				t.Fatalf("actual subscriptions=%d", len(listing.Result))
			}
			for i := range streams {
				if listing.Result[i] != streams[i] {
					t.Fatal("subscription listing differs")
				}
			}
			t.Logf("one connection confirmed %d subscriptions; market messages observed=%d", len(listing.Result), marketMessages)
			break
		}
		marketMessages++
	}
	if marketMessages == 0 {
		t.Fatal("no depth messages observed")
	}
}

// A bounded, isolated reproduction of the real manager's snapshot/bridge path.
// It never starts the reconnect loop or fair scheduler: exactly one public WS,
// at most three depth requests, and a 30-second total context.
func TestLiveSnapshotBridgeProbe(t *testing.T) {
	if os.Getenv("BINANCE_LIVE_BRIDGE_PROBE") != "1" {
		t.Skip("requires explicit BINANCE_LIVE_BRIDGE_PROBE=1")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client := NewClient()
	items, err := client.Instruments(ctx, model.MarketPerpetual)
	if err != nil {
		t.Fatal(err)
	}
	symbol := os.Getenv("BINANCE_BRIDGE_SYMBOL")
	if symbol == "" {
		symbol = "SOLUSDT"
	}
	var instrument model.Instrument
	for _, item := range items {
		if item.ExchangeSymbol == symbol {
			instrument = item
			instrument.ID = 1 // Isolated in-memory identity, never persisted.
			break
		}
	}
	if instrument.ID == 0 {
		t.Fatalf("symbol %q not found in eligible metadata", symbol)
	}
	book, err := orderbook.New(instrument.ID, 1000)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := NewBookManager(client, []model.Instrument{instrument}, map[uint32]*orderbook.Book{instrument.ID: book}, 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	target := manager.targets[0]
	connectionDone := make(chan error, 1)
	go func() { connectionDone <- manager.runShardConnection(ctx, manager.targets) }()
	defer func() { cancel(); t.Logf("connection finished: %v", <-connectionDone) }()
	attempts := 0
	for attempts < 3 && ctx.Err() == nil {
		target.mu.Lock()
		stage, retryAt := target.stage, target.retryAt
		target.mu.Unlock()
		if stage != bookWaiting || time.Now().Before(retryAt) {
			select {
			case <-ctx.Done():
			case <-time.After(20 * time.Millisecond):
			}
			continue
		}
		attempts++
		started := time.Now()
		manager.snapshotAttempt(ctx, target)
		view := book.View()
		t.Logf("symbol=%s attempt=%d elapsed=%s state=%s reason=%q sequence=%d source_time=%s bids=%d asks=%d resyncs=%d", symbol, attempts, time.Since(started), view.State, view.Reason, view.Sequence, view.SourceTime, view.BidLevels, view.AskLevels, manager.resyncs.Load())
		if view.State == orderbook.StateValid {
			select {
			case <-ctx.Done():
			case <-time.After(3 * time.Second):
			}
			t.Logf("symbol=%s followup_view=%+v health=%+v", symbol, book.View(), manager.HealthSnapshot())
			break
		}
	}
	if attempts == 0 {
		t.Fatalf("no snapshot attempt: final_view=%+v context=%v", book.View(), ctx.Err())
	}
}
