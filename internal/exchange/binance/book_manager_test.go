package binance

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/vphoenix/crypto-market-info/internal/exchange"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/orderbook"
)

func testManager(t *testing.T, n, topics int) *BookManager {
	t.Helper()
	instruments := make([]model.Instrument, n)
	books := make(map[uint32]*orderbook.Book, n)
	for i := range instruments {
		instrument := testInstrument()
		instrument.ID = uint32(i + 1)
		instrument.ExchangeSymbol = fmt.Sprintf("ASSET%04dUSDT", n-i)
		instrument.BaseAsset = fmt.Sprintf("ASSET%04d", n-i)
		instruments[i] = instrument
		books[instrument.ID], _ = orderbook.New(instrument.ID, 1000)
	}
	client := NewClient()
	client.ConnectionInterval = time.Millisecond
	client.SnapshotInterval = time.Millisecond
	m, err := NewBookManager(client, instruments, books, topics, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	m.ControlInterval = time.Millisecond
	m.ReconnectBase = time.Millisecond
	m.ReconnectMax = 10 * time.Millisecond
	m.ReconnectJitter = func(d time.Duration) time.Duration { return d }
	return m
}

func TestBookManagerStableShardsAndConstruction(t *testing.T) {
	for _, n := range []int{0, 1, 200, 201, 400} {
		m := testManager(t, n, 200)
		if len(m.shards) != (n+199)/200 {
			t.Fatalf("n=%d shards=%d", n, len(m.shards))
		}
		for i, target := range m.targets {
			if i > 0 && m.targets[i-1].instrument.ExchangeSymbol >= target.instrument.ExchangeSymbol {
				t.Fatal("unstable shard order")
			}
			if target.book.View().State != orderbook.StateConnecting {
				t.Fatal("constructor changed book state")
			}
		}
	}
	instrument := testInstrument()
	book, _ := orderbook.New(2, 1000)
	if _, err := NewBookManager(NewClient(), []model.Instrument{instrument}, map[uint32]*orderbook.Book{1: book}, 200, nil); err == nil {
		t.Fatal("mismatched book accepted")
	}
	if _, err := NewBookManager(NewClient(), nil, nil, 201, nil); err == nil {
		t.Fatal("oversized shard accepted")
	}
}

func TestSnapshotSchedulerInitialPriorityAndRetryFairness(t *testing.T) {
	m := testManager(t, 3, 200)
	for _, target := range m.targets {
		target.stage = bookWaiting
	}
	m.targets[0].attempted = true
	if m.nextSnapshot(time.Now()) != m.targets[1] {
		t.Fatal("retry displaced an initial target")
	}
	m.targets[1].attempted = true
	if m.nextSnapshot(time.Now()) != m.targets[2] {
		t.Fatal("initial targets were starved")
	}
	m.targets[2].attempted = true
	for i := 0; i < 6; i++ {
		if m.nextSnapshot(time.Now()) != m.targets[i%3] {
			t.Fatal("retry round robin is unfair")
		}
	}
	m.targets[0].retryAt = time.Now().Add(time.Hour)
	if m.nextSnapshot(time.Now()) != m.targets[1] {
		t.Fatal("backed-off target blocked ready target")
	}
}

type blockingSnapshotGate struct {
	entered chan struct{}
	release chan struct{}
}

func (g *blockingSnapshotGate) Wait(ctx context.Context) error {
	select {
	case g.entered <- struct{}{}:
	default:
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-g.release:
		return nil
	}
}

func managerDiff(target *bookTarget, generation uint64, first, last, previous int64) bookEvent {
	return bookEvent{target: target, generation: generation, received: time.Now().UTC(), payload: []byte(fmt.Sprintf(`{"e":"depthUpdate","E":1100,"T":1100,"s":%q,"U":%d,"u":%d,"pu":%d,"b":[["100","0.007"]],"a":[]}`, target.instrument.ExchangeSymbol, first, last, previous))}
}

func TestSnapshotArmsAfterGateAndPublishesOnlyAfterBridge(t *testing.T) {
	m := testManager(t, 1, 200)
	target := m.targets[0]
	target.stage = bookWaiting
	target.book.MarkResyncing("queued")
	gate := &blockingSnapshotGate{entered: make(chan struct{}, 1), release: make(chan struct{})}
	m.Client.snapshotOnce.Do(func() { m.Client.snapshotGate = gate })
	requested := make(chan struct{})
	respond := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(requested)
		select {
		case <-respond:
		case <-r.Context().Done():
			return
		}
		_, _ = w.Write([]byte(`{"lastUpdateId":100,"T":1000,"bids":[["100","0.001"]],"asks":[["101","0.001"]]}`))
	}))
	defer server.Close()
	m.Client.HTTP = server.Client()
	m.Client.FuturesBaseURL = server.URL
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { m.snapshotAttempt(ctx, target); close(done) }()
	<-gate.entered
	if err := m.processEvent(managerDiff(target, 0, 90, 91, 89)); err != nil {
		t.Fatal(err)
	}
	target.mu.Lock()
	buffered := len(target.buffer)
	stage := target.stage
	target.mu.Unlock()
	if buffered != 0 || stage != bookWaiting {
		t.Fatal("snapshot queued behind gate allocated buffer")
	}
	close(gate.release)
	<-requested
	target.mu.Lock()
	generation := target.generation
	target.mu.Unlock()
	// A diff captured before arm must never contaminate the new generation.
	if err := m.processEvent(managerDiff(target, 0, 100, 500, 499)); err != nil {
		t.Fatal(err)
	}
	close(respond)
	waitUntil(t, func() bool { target.mu.Lock(); defer target.mu.Unlock(); return target.stage == bookBridging })
	if _, valid := target.book.Snapshot(50); valid {
		t.Fatal("unbridged REST snapshot was exposed")
	}
	if err := m.processEvent(managerDiff(target, generation, 100, 101, 99)); err != nil {
		t.Fatal(err)
	}
	<-done
	snapshot, valid := target.book.Snapshot(50)
	if !valid || snapshot.Sequence != 101 || snapshot.Bids[0].QtyLot != 7 {
		t.Fatalf("snapshot=%+v valid=%v", snapshot, valid)
	}
}

func TestSnapshotBufferOverflowIsTargetScoped(t *testing.T) {
	m := testManager(t, 2, 200)
	bad, good := m.targets[0], m.targets[1]
	bad.stage = bookBuffering
	bad.generation = 1
	bad.buffer = make([]DepthUpdate, snapshotBridgeCapacity)
	good.stage = bookLive
	_ = good.book.ApplySnapshot(model.BookSnapshot{InstrumentID: good.instrument.ID, SourceTime: time.Now().UTC(), Sequence: 10, Bids: []model.Level{{PriceTick: 100, QtyLot: 1}}, Asks: []model.Level{{PriceTick: 101, QtyLot: 1}}})
	if err := m.processEvent(managerDiff(bad, 1, 100, 101, 99)); err != nil {
		t.Fatal(err)
	}
	if bad.stage != bookWaiting || bad.buffer != nil || bad.generation != 2 {
		t.Fatal("overflow generation not discarded")
	}
	if _, valid := good.book.Snapshot(50); !valid {
		t.Fatal("overflow invalidated unrelated book")
	}
}

func TestManagerSharedShardBridgesAndUnknownRouteInvalidatesAll(t *testing.T) {
	m := testManager(t, 2, 200)
	var sequence atomic.Int64
	sequence.Store(100)
	var snapshots atomic.Int64
	rest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		snapshots.Add(1)
		_, _ = fmt.Fprintf(w, `{"lastUpdateId":%d,"T":1000,"bids":[["100","0.001"]],"asks":[["101","0.001"]]}`, sequence.Load())
	}))
	defer rest.Close()
	m.Client.HTTP = rest.Client()
	m.Client.FuturesBaseURL = rest.URL
	unknown := make(chan struct{})
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		var request struct {
			Method string   `json:"method"`
			Params []string `json:"params"`
			ID     int64    `json:"id"`
		}
		if conn.ReadJSON(&request) != nil {
			return
		}
		if len(request.Params) != 2 || request.Method != "SUBSCRIBE" {
			return
		}
		_ = conn.WriteJSON(map[string]any{"result": nil, "id": request.ID})
		ticker := time.NewTicker(5 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-unknown:
				_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"e":"depthUpdate","s":"UNKNOWN"}`))
				return
			case <-ticker.C:
				n := sequence.Add(1)
				for _, target := range m.targets {
					event := managerDiff(target, 0, n-1, n, n-1)
					if conn.WriteMessage(websocket.TextMessage, event.payload) != nil {
						return
					}
				}
			}
		}
	}))
	defer server.Close()
	m.WSEndpoint = "ws" + strings.TrimPrefix(server.URL, "http")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	schedulerDone := make(chan struct{})
	go func() { m.snapshotScheduler(ctx); close(schedulerDone) }()
	done := make(chan error, 1)
	go func() { done <- m.runShardConnection(ctx, m.shards[0]) }()
	waitUntil(t, func() bool {
		select {
		case err := <-done:
			t.Fatalf("shard ended before ready: %v", err)
		default:
		}
		return m.HealthSnapshot().ReadyInstruments == 2
	})
	if snapshots.Load() != 2 || m.HealthSnapshot().ConnectedShards != 1 {
		t.Fatal("unexpected snapshot or connection count")
	}
	close(unknown)
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("unknown route accepted")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("unknown route did not close shard")
	}
	if h := m.HealthSnapshot(); h.ReadyInstruments != 0 || h.ConnectedShards != 0 {
		t.Fatalf("health after failure=%+v", h)
	}
	cancel()
	<-schedulerDone
}

func TestManagerSubscriptionACKValidation(t *testing.T) {
	for _, payload := range []string{`{"result":null,"id":2}`, `{"result":false,"id":1}`, `{"code":2,"msg":"bad"}`, `{"result":null}`} {
		if _, err := parseSubscriptionACK([]byte(payload), 1); err == nil {
			t.Fatalf("accepted %s", payload)
		}
	}
	if _, err := parseSubscriptionACK([]byte(`{"result":null,"id":1}`), 0); err == nil {
		t.Fatal("duplicate ACK accepted")
	}
}

func TestBinancePerpetualRequiresUSDTQuote(t *testing.T) {
	payload := []byte(`{"symbols":[{"symbol":"BTCUSDC","status":"TRADING","contractType":"PERPETUAL","baseAsset":"BTC","quoteAsset":"USDC","marginAsset":"USDT","onboardDate":1585526400000,"filters":[{"filterType":"PRICE_FILTER","tickSize":"0.10"},{"filterType":"LOT_SIZE","stepSize":"0.001"}]}]}`)
	instruments, err := ParseExchangeInfo(payload, model.MarketPerpetual)
	if err != nil || len(instruments) != 0 {
		t.Fatalf("non-USDT quote selected: %+v %v", instruments, err)
	}
	spot, err := ParseExchangeInfo(payload, model.MarketSpot)
	if err != nil || len(spot) != 1 {
		t.Fatal("spot eligibility changed")
	}
}

func TestClientSnapshotCooldownPrecedesArm(t *testing.T) {
	m := testManager(t, 1, 200)
	m.Client.Retry.Cooldown = exchange.NewRequestGate(0)
	m.Client.Retry.Cooldown.Cooldown(time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	var armed bool
	_, err := m.Client.depthSnapshot(ctx, m.targets[0].instrument, func() error { armed = true; return nil })
	if err == nil || armed {
		t.Fatal("snapshot armed during HTTP cooldown")
	}
}

func waitUntil(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition did not become true")
}
