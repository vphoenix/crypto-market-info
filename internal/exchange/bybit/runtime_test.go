package bybit

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/vphoenix/crypto-market-info/internal/exchange"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/orderbook"
)

func TestBookRuntimeSubscribesAndAppliesSnapshot(t *testing.T) {
	upgrader := websocket.Upgrader{}
	subscribed := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		conn, err := upgrader.Upgrade(w, request, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		var subscribe struct {
			ReqID string   `json:"req_id"`
			Op    string   `json:"op"`
			Args  []string `json:"args"`
		}
		if err = conn.ReadJSON(&subscribe); err != nil || subscribe.Op != "subscribe" || subscribe.ReqID == "" || len(subscribe.Args) != 1 || subscribe.Args[0] != "orderbook.1000.BTCUSDT" {
			return
		}
		_ = conn.WriteJSON(map[string]any{"success": true, "ret_msg": "", "req_id": subscribe.ReqID, "op": "subscribe"})
		_ = conn.WriteMessage(websocket.TextMessage, depthPayload("snapshot", 10, 20, 1000, 900, `[["100","1"]]`, `[["101","1"]]`))
		close(subscribed)
		for {
			if _, _, err = conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()
	instrument := bybitTestInstrument()
	book, _ := orderbook.New(instrument.ID, orderBookDepth)
	runtime := Runtime{Instrument: instrument, Book: book, WSEndpoint: "ws" + strings.TrimPrefix(server.URL, "http"), ConnectGate: exchange.NewRequestGate(0), PingInterval: time.Hour}
	if err := runtime.defaults(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan error, 1)
	go func() { finished <- runtime.runConnection(ctx) }()
	select {
	case <-subscribed:
	case <-time.After(time.Second):
		t.Fatal("runtime did not subscribe")
	}
	deadline := time.Now().Add(time.Second)
	for {
		if snapshot, valid := book.Snapshot(50); valid && snapshot.SourceTime.UnixMilli() == 900 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("runtime did not apply snapshot")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
}

func TestBookRuntimeBuffersDataUntilSubscriptionAcknowledgement(t *testing.T) {
	upgrader := websocket.Upgrader{}
	sent := make(chan struct{})
	allowAck := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		conn, err := upgrader.Upgrade(w, request, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		var subscribe struct {
			ReqID string `json:"req_id"`
		}
		if err = conn.ReadJSON(&subscribe); err != nil {
			return
		}
		_ = conn.WriteMessage(websocket.TextMessage, depthPayload("snapshot", 10, 20, 1000, 900, `[["100","1"]]`, `[["101","1"]]`))
		_ = conn.WriteMessage(websocket.TextMessage, depthPayload("delta", 11, 21, 1100, 1000, `[["100","2"]]`, `[]`))
		close(sent)
		<-allowAck
		_ = conn.WriteJSON(map[string]any{"success": true, "ret_msg": "", "req_id": subscribe.ReqID, "op": "subscribe"})
		for {
			if _, _, err = conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()
	instrument := bybitTestInstrument()
	book, _ := orderbook.New(instrument.ID, orderBookDepth)
	runtime := Runtime{Instrument: instrument, Book: book, WSEndpoint: "ws" + strings.TrimPrefix(server.URL, "http"), ConnectGate: exchange.NewRequestGate(0), PingInterval: time.Hour}
	if err := runtime.defaults(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan error, 1)
	go func() { finished <- runtime.runConnection(ctx) }()
	select {
	case <-sent:
	case <-time.After(time.Second):
		t.Fatal("server did not send pre-ack book data")
	}
	time.Sleep(20 * time.Millisecond)
	if _, valid := book.Snapshot(50); valid {
		t.Fatal("pre-ack book data mutated the book")
	}
	close(allowAck)
	deadline := time.Now().Add(time.Second)
	for {
		snapshot, valid := book.Snapshot(50)
		if valid && snapshot.SourceTime.UnixMilli() == 1000 && len(snapshot.Bids) == 1 && snapshot.Bids[0].QtyLot == 2000 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("buffered snapshot and delta were not replayed in order: snapshot=%+v valid=%v", snapshot, valid)
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
}

func TestBookRuntimeFailedAcknowledgementDiscardsBufferedData(t *testing.T) {
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		conn, err := upgrader.Upgrade(w, request, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		var subscribe struct {
			ReqID string `json:"req_id"`
		}
		if err = conn.ReadJSON(&subscribe); err != nil {
			return
		}
		_ = conn.WriteMessage(websocket.TextMessage, depthPayload("snapshot", 10, 20, 1000, 900, `[["100","1"]]`, `[["101","1"]]`))
		_ = conn.WriteJSON(map[string]any{"success": false, "ret_msg": "denied", "req_id": subscribe.ReqID, "op": "subscribe"})
	}))
	defer server.Close()
	instrument := bybitTestInstrument()
	book, _ := orderbook.New(instrument.ID, orderBookDepth)
	runtime := Runtime{Instrument: instrument, Book: book, WSEndpoint: "ws" + strings.TrimPrefix(server.URL, "http"), ConnectGate: exchange.NewRequestGate(0), PingInterval: time.Hour}
	if err := runtime.defaults(); err != nil {
		t.Fatal(err)
	}
	err := runtime.runConnection(context.Background())
	if err == nil || !strings.Contains(err.Error(), "failed: denied") {
		t.Fatalf("runConnection error=%v", err)
	}
	if _, valid := book.Snapshot(50); valid || book.View().State != orderbook.StateInvalid {
		t.Fatalf("failed acknowledgement exposed buffered book: valid=%v state=%s", valid, book.View().State)
	}
}

func TestBookRuntimeSubscriptionTimeout(t *testing.T) {
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		conn, err := upgrader.Upgrade(w, request, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		var subscribe map[string]any
		if err = conn.ReadJSON(&subscribe); err != nil {
			return
		}
		_ = conn.WriteMessage(websocket.TextMessage, depthPayload("snapshot", 10, 20, 1000, 900, `[["100","1"]]`, `[["101","1"]]`))
		for {
			if _, _, err = conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()
	instrument := bybitTestInstrument()
	book, _ := orderbook.New(instrument.ID, orderBookDepth)
	runtime := Runtime{Instrument: instrument, Book: book, WSEndpoint: "ws" + strings.TrimPrefix(server.URL, "http"), ConnectGate: exchange.NewRequestGate(0), PingInterval: time.Hour, SubscribeTimeout: 20 * time.Millisecond}
	if err := runtime.defaults(); err != nil {
		t.Fatal(err)
	}
	err := runtime.runConnection(context.Background())
	if err == nil || !strings.Contains(err.Error(), "acknowledgement timed out") {
		t.Fatalf("runConnection error=%v", err)
	}
	if _, valid := book.Snapshot(50); valid || book.View().State != orderbook.StateInvalid {
		t.Fatalf("subscription timeout exposed buffered book: valid=%v state=%s", valid, book.View().State)
	}
}

func TestBookRuntimePreAckBufferOverflowIsTerminal(t *testing.T) {
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		conn, err := upgrader.Upgrade(w, request, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		var subscribe map[string]any
		if err = conn.ReadJSON(&subscribe); err != nil {
			return
		}
		_ = conn.WriteMessage(websocket.TextMessage, depthPayload("snapshot", 10, 20, 1000, 900, `[["100","1"]]`, `[["101","1"]]`))
		_ = conn.WriteMessage(websocket.TextMessage, depthPayload("delta", 11, 21, 1100, 1000, `[]`, `[]`))
		for {
			if _, _, err = conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()
	instrument := bybitTestInstrument()
	book, _ := orderbook.New(instrument.ID, orderBookDepth)
	runtime := Runtime{Instrument: instrument, Book: book, WSEndpoint: "ws" + strings.TrimPrefix(server.URL, "http"), ConnectGate: exchange.NewRequestGate(0), QueueCapacity: 16, PreAckCapacity: 1, PingInterval: time.Hour}
	if err := runtime.defaults(); err != nil {
		t.Fatal(err)
	}
	err := runtime.runConnection(context.Background())
	if err == nil || !strings.Contains(err.Error(), "pre-ack buffer overflow") {
		t.Fatalf("runConnection error=%v", err)
	}
	if _, valid := book.Snapshot(50); valid || book.View().State != orderbook.StateInvalid {
		t.Fatalf("pre-ack overflow exposed buffered book: valid=%v state=%s", valid, book.View().State)
	}
}

type runtimeEstimateSink struct {
	mu          sync.Mutex
	estimates   []model.FundingEstimate
	unavailable []uint32
	available   bool
	notify      chan struct{}
}

func (s *runtimeEstimateSink) Put(estimate model.FundingEstimate) error {
	s.mu.Lock()
	s.estimates = append(s.estimates, estimate)
	s.available = true
	s.mu.Unlock()
	select {
	case s.notify <- struct{}{}:
	default:
	}
	return nil
}

func (s *runtimeEstimateSink) MarkUnavailable(ids []uint32) {
	s.mu.Lock()
	s.unavailable = append([]uint32(nil), ids...)
	s.available = false
	s.mu.Unlock()
}

type runtimeConfirmationSink struct {
	mu     sync.Mutex
	calls  []time.Time
	notify chan struct{}
}

func (s *runtimeConfirmationSink) Schedule(_ context.Context, _ model.Instrument, target time.Time) error {
	s.mu.Lock()
	s.calls = append(s.calls, target)
	s.mu.Unlock()
	if s.notify != nil {
		select {
		case s.notify <- struct{}{}:
		default:
		}
	}
	return nil
}

func TestFundingRuntimeMergesSparseTickerAndRefreshesEstimate(t *testing.T) {
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		conn, err := upgrader.Upgrade(w, request, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		var subscribe struct {
			ReqID string   `json:"req_id"`
			Op    string   `json:"op"`
			Args  []string `json:"args"`
		}
		if err = conn.ReadJSON(&subscribe); err != nil {
			return
		}
		_ = conn.WriteJSON(map[string]any{"success": true, "ret_msg": "", "req_id": subscribe.ReqID, "op": "subscribe"})
		_ = conn.WriteMessage(websocket.TextMessage, tickerPayload("snapshot", 1000, `,"fundingRate":"0.001","nextFundingTime":"1787097600123","fundingIntervalHour":"8"`))
		_ = conn.WriteMessage(websocket.TextMessage, tickerPayload("delta", 1100, `,"lastPrice":"100"`))
		for {
			if _, _, err = conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()
	estimates := &runtimeEstimateSink{notify: make(chan struct{}, 2)}
	confirmations := &runtimeConfirmationSink{}
	runtime := FundingRuntime{Instruments: []model.Instrument{bybitTestInstrument()}, Estimates: estimates, Confirmations: confirmations, WSEndpoint: "ws" + strings.TrimPrefix(server.URL, "http"), ConnectGate: exchange.NewRequestGate(0), PingInterval: time.Hour}
	if err := runtime.defaults(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan error, 1)
	go func() { finished <- runtime.runConnection(ctx) }()
	for range 2 {
		select {
		case <-estimates.notify:
		case <-time.After(time.Second):
			t.Fatal("funding runtime did not publish estimates")
		}
	}
	cancel()
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	estimates.mu.Lock()
	defer estimates.mu.Unlock()
	if len(estimates.estimates) != 2 || estimates.estimates[0].SourceTime.UnixMilli() != 1000 || estimates.estimates[1].SourceTime.UnixMilli() != 1100 || !estimates.estimates[0].Rate.Equal(estimates.estimates[1].Rate) {
		t.Fatalf("estimates=%+v", estimates.estimates)
	}
	confirmations.mu.Lock()
	defer confirmations.mu.Unlock()
	if len(confirmations.calls) != 2 {
		t.Fatalf("confirmation calls=%v", confirmations.calls)
	}
}

func TestFundingRuntimeBuffersDataUntilSubscriptionAcknowledgement(t *testing.T) {
	upgrader := websocket.Upgrader{}
	sent := make(chan struct{})
	allowAck := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		conn, err := upgrader.Upgrade(w, request, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		var subscribe struct {
			ReqID string `json:"req_id"`
		}
		if err = conn.ReadJSON(&subscribe); err != nil {
			return
		}
		_ = conn.WriteMessage(websocket.TextMessage, tickerPayload("snapshot", 1000, `,"fundingRate":"0.001","nextFundingTime":"1787097600123","fundingIntervalHour":"8"`))
		_ = conn.WriteMessage(websocket.TextMessage, tickerPayload("delta", 1100, `,"fundingRate":"0.002"`))
		close(sent)
		<-allowAck
		_ = conn.WriteJSON(map[string]any{"success": true, "ret_msg": "", "req_id": subscribe.ReqID, "op": "subscribe"})
		for {
			if _, _, err = conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()
	estimates := &runtimeEstimateSink{notify: make(chan struct{}, 2)}
	runtime := FundingRuntime{Instruments: []model.Instrument{bybitTestInstrument()}, Estimates: estimates, Confirmations: &runtimeConfirmationSink{}, WSEndpoint: "ws" + strings.TrimPrefix(server.URL, "http"), ConnectGate: exchange.NewRequestGate(0), PingInterval: time.Hour}
	if err := runtime.defaults(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan error, 1)
	go func() { finished <- runtime.runConnection(ctx) }()
	select {
	case <-sent:
	case <-time.After(time.Second):
		t.Fatal("server did not send pre-ack ticker data")
	}
	time.Sleep(20 * time.Millisecond)
	estimates.mu.Lock()
	preAckEstimateCount := len(estimates.estimates)
	estimates.mu.Unlock()
	if preAckEstimateCount != 0 {
		t.Fatalf("pre-ack ticker data published %d estimates", preAckEstimateCount)
	}
	close(allowAck)
	for range 2 {
		select {
		case <-estimates.notify:
		case <-time.After(time.Second):
			t.Fatal("buffered ticker updates were not replayed")
		}
	}
	cancel()
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	estimates.mu.Lock()
	defer estimates.mu.Unlock()
	if len(estimates.estimates) != 2 || estimates.estimates[0].SourceTime.UnixMilli() != 1000 || estimates.estimates[1].SourceTime.UnixMilli() != 1100 || estimates.estimates[1].Rate.String() != "0.002" {
		t.Fatalf("buffered estimates=%+v", estimates.estimates)
	}
}

func TestFundingRuntimeFailedAcknowledgementDiscardsBufferedData(t *testing.T) {
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		conn, err := upgrader.Upgrade(w, request, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		var subscribe struct {
			ReqID string `json:"req_id"`
		}
		if err = conn.ReadJSON(&subscribe); err != nil {
			return
		}
		_ = conn.WriteMessage(websocket.TextMessage, tickerPayload("snapshot", 1000, `,"fundingRate":"0.001","nextFundingTime":"1787097600123","fundingIntervalHour":"8"`))
		_ = conn.WriteJSON(map[string]any{"success": false, "ret_msg": "denied", "req_id": subscribe.ReqID, "op": "subscribe"})
		for {
			if _, _, err = conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()
	estimates := &runtimeEstimateSink{notify: make(chan struct{}, 1)}
	runtime := FundingRuntime{Instruments: []model.Instrument{bybitTestInstrument()}, Estimates: estimates, Confirmations: &runtimeConfirmationSink{}, WSEndpoint: "ws" + strings.TrimPrefix(server.URL, "http"), ConnectGate: exchange.NewRequestGate(0), PingInterval: time.Hour}
	if err := runtime.defaults(); err != nil {
		t.Fatal(err)
	}
	err := runtime.runConnection(context.Background())
	if err == nil || !strings.Contains(err.Error(), "failed: denied") {
		t.Fatalf("runConnection error=%v", err)
	}
	estimates.mu.Lock()
	defer estimates.mu.Unlock()
	if len(estimates.estimates) != 0 || estimates.available {
		t.Fatalf("failed acknowledgement exposed buffered estimates=%+v available=%v", estimates.estimates, estimates.available)
	}
}

func TestFundingRuntimeSubscriptionTimeoutDiscardsBufferedData(t *testing.T) {
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		conn, err := upgrader.Upgrade(w, request, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		var subscribe map[string]any
		if err = conn.ReadJSON(&subscribe); err != nil {
			return
		}
		_ = conn.WriteMessage(websocket.TextMessage, tickerPayload("snapshot", 1000, `,"fundingRate":"0.001","nextFundingTime":"1787097600123","fundingIntervalHour":"8"`))
		for {
			if _, _, err = conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()
	estimates := &runtimeEstimateSink{notify: make(chan struct{}, 1)}
	runtime := FundingRuntime{Instruments: []model.Instrument{bybitTestInstrument()}, Estimates: estimates, Confirmations: &runtimeConfirmationSink{}, WSEndpoint: "ws" + strings.TrimPrefix(server.URL, "http"), ConnectGate: exchange.NewRequestGate(0), PingInterval: time.Hour, SubscribeTimeout: 20 * time.Millisecond}
	if err := runtime.defaults(); err != nil {
		t.Fatal(err)
	}
	err := runtime.runConnection(context.Background())
	if err == nil || !strings.Contains(err.Error(), "acknowledgements timed out") {
		t.Fatalf("runConnection error=%v", err)
	}
	estimates.mu.Lock()
	defer estimates.mu.Unlock()
	if len(estimates.estimates) != 0 || estimates.available {
		t.Fatalf("subscription timeout exposed buffered estimates=%+v available=%v", estimates.estimates, estimates.available)
	}
}

func TestFundingRuntimePreAckBufferOverflowIsTerminal(t *testing.T) {
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		conn, err := upgrader.Upgrade(w, request, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		var subscribe map[string]any
		if err = conn.ReadJSON(&subscribe); err != nil {
			return
		}
		_ = conn.WriteMessage(websocket.TextMessage, tickerPayload("snapshot", 1000, `,"fundingRate":"0.001","nextFundingTime":"1787097600123","fundingIntervalHour":"8"`))
		_ = conn.WriteMessage(websocket.TextMessage, tickerPayload("delta", 1100, `,"fundingRate":"0.002"`))
		for {
			if _, _, err = conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()
	estimates := &runtimeEstimateSink{notify: make(chan struct{}, 1)}
	runtime := FundingRuntime{Instruments: []model.Instrument{bybitTestInstrument()}, Estimates: estimates, Confirmations: &runtimeConfirmationSink{}, WSEndpoint: "ws" + strings.TrimPrefix(server.URL, "http"), ConnectGate: exchange.NewRequestGate(0), QueueCapacity: 16, PreAckCapacity: 1, PingInterval: time.Hour}
	if err := runtime.defaults(); err != nil {
		t.Fatal(err)
	}
	err := runtime.runConnection(context.Background())
	if err == nil || !strings.Contains(err.Error(), "pre-ack buffer overflow") {
		t.Fatalf("runConnection error=%v", err)
	}
	estimates.mu.Lock()
	defer estimates.mu.Unlock()
	if len(estimates.estimates) != 0 || estimates.available {
		t.Fatalf("pre-ack overflow exposed buffered estimates=%+v available=%v", estimates.estimates, estimates.available)
	}
}

func TestFundingRuntimeDisconnectMarksAllUnavailableAndReconnectNeedsSnapshot(t *testing.T) {
	estimates := &runtimeEstimateSink{notify: make(chan struct{}, 2)}
	confirmations := &runtimeConfirmationSink{notify: make(chan struct{}, 1)}
	instrument := bybitTestInstrument()
	var connections atomic.Int32
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		conn, err := upgrader.Upgrade(w, request, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		var subscribe struct {
			ReqID string `json:"req_id"`
		}
		if err = conn.ReadJSON(&subscribe); err != nil {
			return
		}
		_ = conn.WriteJSON(map[string]any{"success": true, "ret_msg": "", "req_id": subscribe.ReqID, "op": "subscribe"})
		if connections.Add(1) == 1 {
			_ = conn.WriteMessage(websocket.TextMessage, tickerPayload("snapshot", 1000, `,"fundingRate":"0.001","nextFundingTime":"1787097600123","fundingIntervalHour":"8"`))
			select {
			case <-confirmations.notify:
			case <-time.After(time.Second):
			}
			return
		}
		_ = conn.WriteMessage(websocket.TextMessage, tickerPayload("delta", 1100, `,"fundingRate":"0.002"`))
		for {
			if _, _, err = conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()
	runtime := FundingRuntime{Instruments: []model.Instrument{instrument}, Estimates: estimates, Confirmations: confirmations, WSEndpoint: "ws" + strings.TrimPrefix(server.URL, "http"), ConnectGate: exchange.NewRequestGate(0), PingInterval: time.Hour}
	if err := runtime.defaults(); err != nil {
		t.Fatal(err)
	}
	if err := runtime.runConnection(context.Background()); err == nil {
		t.Fatal("closed funding connection unexpectedly succeeded")
	}
	estimates.mu.Lock()
	firstCount, firstAvailable := len(estimates.estimates), estimates.available
	estimates.mu.Unlock()
	if firstCount != 1 || firstAvailable {
		t.Fatalf("after disconnect estimates=%d available=%v", firstCount, firstAvailable)
	}
	err := runtime.runConnection(context.Background())
	if err == nil || !strings.Contains(err.Error(), "delta arrived before snapshot") {
		t.Fatalf("reconnect error=%v", err)
	}
	estimates.mu.Lock()
	defer estimates.mu.Unlock()
	if len(estimates.estimates) != firstCount || estimates.available {
		t.Fatalf("reconnect reused old cache: estimates=%d available=%v", len(estimates.estimates), estimates.available)
	}
}

func TestFundingRuntimeActivatesOnlyAcknowledgedSubscriptionBatch(t *testing.T) {
	first := bybitTestInstrument()
	first.ID = 31
	first.ExchangeSymbol = strings.Repeat("A", 10500) + "USDT"
	second := first
	second.ID = 32
	second.ExchangeSymbol = strings.Repeat("B", 10500) + "USDT"
	topics := []string{"tickers." + first.ExchangeSymbol, "tickers." + second.ExchangeSymbol}
	if batches, err := tickerSubscriptionBatches(topics, tickerArgsLimit); err != nil || len(batches) != 2 {
		t.Fatalf("test topics did not form two batches: batches=%d err=%v", len(batches), err)
	}
	estimates := &runtimeEstimateSink{notify: make(chan struct{}, 3)}
	confirmations := &runtimeConfirmationSink{notify: make(chan struct{}, 3)}
	sent := make(chan struct{})
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		conn, err := upgrader.Upgrade(w, request, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		var subscriptions [2]struct {
			ReqID string `json:"req_id"`
		}
		for index := range subscriptions {
			if err = conn.ReadJSON(&subscriptions[index]); err != nil {
				return
			}
		}
		firstPayload := fmt.Sprintf(`{"topic":%q,"type":"snapshot","ts":1000,"data":{"symbol":%q,"fundingRate":"0.001","nextFundingTime":"1787097600123","fundingIntervalHour":"8"}}`, topics[0], first.ExchangeSymbol)
		_ = conn.WriteMessage(websocket.TextMessage, []byte(firstPayload))
		secondPayload := fmt.Sprintf(`{"topic":%q,"type":"snapshot","ts":1100,"data":{"symbol":%q,"fundingRate":"0.002","nextFundingTime":"1787097600123","fundingIntervalHour":"8"}}`, topics[1], second.ExchangeSymbol)
		_ = conn.WriteMessage(websocket.TextMessage, []byte(secondPayload))
		_ = conn.WriteJSON(map[string]any{"success": true, "ret_msg": "", "req_id": subscriptions[0].ReqID, "op": "subscribe"})
		select {
		case <-confirmations.notify:
		case <-time.After(time.Second):
			return
		}
		firstDelta := fmt.Sprintf(`{"topic":%q,"type":"delta","ts":1200,"data":{"symbol":%q,"fundingRate":"0.003"}}`, topics[0], first.ExchangeSymbol)
		_ = conn.WriteMessage(websocket.TextMessage, []byte(firstDelta))
		select {
		case <-confirmations.notify:
		case <-time.After(time.Second):
			return
		}
		_ = conn.WriteJSON(map[string]any{"success": true, "ret_msg": "", "req_id": subscriptions[1].ReqID, "op": "subscribe"})
		close(sent)
		for {
			if _, _, err = conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()
	runtime := FundingRuntime{Instruments: []model.Instrument{first, second}, Estimates: estimates, Confirmations: confirmations, WSEndpoint: "ws" + strings.TrimPrefix(server.URL, "http"), ConnectGate: exchange.NewRequestGate(0), PingInterval: time.Hour}
	if err := runtime.defaults(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan error, 1)
	go func() { finished <- runtime.runConnection(ctx) }()
	select {
	case <-sent:
	case <-time.After(2 * time.Second):
		t.Fatal("funding batches were not independently activated")
	}
	for range 3 {
		select {
		case <-estimates.notify:
		case <-time.After(time.Second):
			t.Fatal("funding runtime did not replay all batch messages")
		}
	}
	cancel()
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	estimates.mu.Lock()
	defer estimates.mu.Unlock()
	if len(estimates.estimates) != 3 ||
		estimates.estimates[0].InstrumentID != first.ID || estimates.estimates[0].SourceTime.UnixMilli() != 1000 ||
		estimates.estimates[1].InstrumentID != first.ID || estimates.estimates[1].SourceTime.UnixMilli() != 1200 ||
		estimates.estimates[2].InstrumentID != second.ID || estimates.estimates[2].SourceTime.UnixMilli() != 1100 {
		t.Fatalf("batch replay estimates=%+v", estimates.estimates)
	}
}

func TestWebsocketControlRequiresSuccessfulMatchingAcknowledgement(t *testing.T) {
	for name, payload := range map[string]string{
		"failed":   `{"success":false,"ret_msg":"denied","req_id":"book3","op":"subscribe"}`,
		"mismatch": `{"success":true,"ret_msg":"","req_id":"other","op":"subscribe"}`,
		"unknown":  `{"success":true,"ret_msg":"","req_id":"book3","op":"other"}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := parseWebsocketControl([]byte(payload), "book3"); err == nil {
				t.Fatal("invalid control message was accepted")
			}
		})
	}
	valid, _ := json.Marshal(map[string]any{"success": true, "ret_msg": "pong", "op": "ping"})
	if kind, err := parseWebsocketControl(valid, ""); err != nil || kind != "pong" {
		t.Fatalf("pong kind=%q err=%v", kind, err)
	}
}

func TestBookReaderOverflowIsTerminalBeforeQueuedSnapshot(t *testing.T) {
	instrument := bybitTestInstrument()
	book, _ := orderbook.New(instrument.ID, orderBookDepth)
	collector, _ := NewCollector(book)
	baseline, _ := ParseDepth(depthPayload("snapshot", 9, 19, 900, 800, `[["99","1"]]`, `[["102","1"]]`), instrument)
	if err := collector.Push(baseline); err != nil {
		t.Fatal(err)
	}
	conn := websocketFixture(t, []byte(depthPayload("snapshot", 10, 20, 1000, 900, `[["100","1"]]`, `[["101","1"]]`)), depthPayload("delta", 11, 21, 1100, 1000, `[]`, `[]`))
	runtime := Runtime{Instrument: instrument, Book: book, QueueCapacity: 1, SilenceTimeout: time.Second}
	events := make(chan bookStreamEvent, 1)
	errors := make(chan error, 1)
	terminal := &connectionTerminal{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go runtime.readLoop(ctx, conn, "book3", events, errors, terminal)
	select {
	case err := <-errors:
		if err == nil || !strings.Contains(err.Error(), "queue overflow") {
			t.Fatalf("reader error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("book reader did not report overflow")
	}
	if book.View().State != orderbook.StateInvalid {
		t.Fatalf("book state=%s after overflow", book.View().State)
	}
	queued := <-events
	applied := false
	err := terminal.withActive(func() error {
		applied = true
		return collector.Push(*queued.update)
	})
	if err == nil || applied || book.View().State != orderbook.StateInvalid {
		t.Fatalf("terminal apply err=%v applied=%v state=%s", err, applied, book.View().State)
	}
}

func TestFundingReaderOverflowKeepsQueuedTickerUnavailable(t *testing.T) {
	instrument := bybitTestInstrument()
	conn := websocketFixture(t,
		tickerPayload("snapshot", 1000, `,"fundingRate":"0.001","nextFundingTime":"1787097600123","fundingIntervalHour":"8"`),
		tickerPayload("delta", 1100, `,"lastPrice":"100"`),
	)
	estimates := &runtimeEstimateSink{available: true, notify: make(chan struct{}, 1)}
	runtime := FundingRuntime{Estimates: estimates, QueueCapacity: 1, SilenceTimeout: time.Second}
	messages := make(chan []byte, 1)
	errors := make(chan error, 1)
	terminal := &connectionTerminal{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go runtime.readFundingLoop(ctx, conn, messages, errors, terminal, []uint32{instrument.ID})
	select {
	case err := <-errors:
		if err == nil || !strings.Contains(err.Error(), "queue overflow") {
			t.Fatalf("reader error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("funding reader did not report overflow")
	}
	estimates.mu.Lock()
	available := estimates.available
	estimates.mu.Unlock()
	if available {
		t.Fatal("funding estimates remained available after overflow")
	}
	queued := <-messages
	applied := false
	err := terminal.withActive(func() error {
		applied = true
		_, _, parseErr := ParseTickerUpdate(queued, map[string]model.Instrument{instrument.ExchangeSymbol: instrument})
		return parseErr
	})
	estimates.mu.Lock()
	available = estimates.available
	estimates.mu.Unlock()
	if err == nil || applied || available {
		t.Fatalf("terminal ticker err=%v applied=%v available=%v", err, applied, available)
	}
}

func websocketFixture(t *testing.T, payloads ...[]byte) *websocket.Conn {
	t.Helper()
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		conn, err := upgrader.Upgrade(w, request, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for _, payload := range payloads {
			if err = conn.WriteMessage(websocket.TextMessage, payload); err != nil {
				return
			}
		}
		for {
			if _, _, err = conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		server.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		server.Close()
	})
	return conn
}
