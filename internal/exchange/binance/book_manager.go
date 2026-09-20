package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/gorilla/websocket"
	"github.com/vphoenix/crypto-market-info/internal/exchange"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/orderbook"
)

const MaxBookTopicsPerConnection = 200
const snapshotBridgeCapacity = 4096

type BookHealthSnapshot struct {
	ExpectedInstruments  int
	ReadyInstruments     int
	InvalidInstruments   int
	ResyncingInstruments int
	ConnectedShards      int
	OldestSourceTime     time.Time
	Reconnects           uint64
	Resyncs              uint64
	QueuePeak            uint64
	QueueOverflows       uint64
}

type bookStage uint8

const (
	bookOffline bookStage = iota
	bookWaiting
	bookBuffering
	bookBridging
	bookLive
)

type bookTarget struct {
	mu            sync.Mutex
	instrument    model.Instrument
	book          *orderbook.Book
	stage         bookStage
	generation    uint64
	attempted     bool
	retryAt       time.Time
	failures      int
	buffer        []DepthUpdate
	collector     *Collector
	attemptCancel context.CancelFunc
	done          chan error
	lastReceive   time.Time
}

type bookEvent struct {
	payload    []byte
	target     *bookTarget
	generation uint64
	received   time.Time
}

type BookManager struct {
	Client              *Client
	WSEndpoint          string
	Dialer              *websocket.Dialer
	SilenceTimeout      time.Duration
	SubscriptionTimeout time.Duration
	SnapshotTimeout     time.Duration
	BridgeTimeout       time.Duration
	ReconnectBase       time.Duration
	ReconnectMax        time.Duration
	ReconnectJitter     func(time.Duration) time.Duration
	ControlInterval     time.Duration
	Logger              *slog.Logger
	targets             []*bookTarget
	shards              [][]*bookTarget
	wake                chan struct{}
	connected           atomic.Int64
	reconnects          atomic.Uint64
	resyncs             atomic.Uint64
	queuePeak           atomic.Uint64
	queueOverflows      atomic.Uint64
	cursor              int
}

func NewBookManager(client *Client, instruments []model.Instrument, books map[uint32]*orderbook.Book, topicsPerConnection int, logger *slog.Logger) (*BookManager, error) {
	if client == nil || topicsPerConnection < 1 || topicsPerConnection > MaxBookTopicsPerConnection {
		return nil, fmt.Errorf("Binance manager requires client and topics per connection in 1..%d", MaxBookTopicsPerConnection)
	}
	if logger == nil {
		logger = slog.Default()
	}
	m := &BookManager{Client: client, WSEndpoint: "wss://fstream.binance.com/public/ws", Dialer: websocket.DefaultDialer,
		SilenceTimeout: 45 * time.Second, SubscriptionTimeout: 10 * time.Second, SnapshotTimeout: 20 * time.Second, BridgeTimeout: 5 * time.Second,
		ReconnectBase: time.Second, ReconnectMax: 30 * time.Second, ReconnectJitter: exchange.AddJitter, ControlInterval: 250 * time.Millisecond, Logger: logger, wake: make(chan struct{}, 1)}
	ordered := append([]model.Instrument(nil), instruments...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].ExchangeSymbol < ordered[j].ExchangeSymbol })
	seenIDs := make(map[uint32]bool)
	seenSymbols := make(map[string]bool)
	for _, instrument := range ordered {
		if err := instrument.Validate(); err != nil || instrument.Exchange != "Binance" || instrument.MarketType != model.MarketPerpetual {
			return nil, fmt.Errorf("Binance manager received invalid perpetual instrument %q", instrument.ExchangeSymbol)
		}
		book := books[instrument.ID]
		if book == nil || book.InstrumentID() != instrument.ID || seenIDs[instrument.ID] || seenSymbols[instrument.ExchangeSymbol] {
			return nil, fmt.Errorf("Binance manager duplicate instrument or missing/mismatched book %q", instrument.ExchangeSymbol)
		}
		seenIDs[instrument.ID], seenSymbols[instrument.ExchangeSymbol] = true, true
		m.targets = append(m.targets, &bookTarget{instrument: instrument, book: book})
	}
	for offset := 0; offset < len(m.targets); offset += topicsPerConnection {
		m.shards = append(m.shards, m.targets[offset:min(offset+topicsPerConnection, len(m.targets))])
	}
	for _, shard := range m.shards {
		streams := make([]string, len(shard))
		for i, target := range shard {
			streams[i] = strings.ToLower(target.instrument.ExchangeSymbol) + "@depth@100ms"
		}
		if _, err := subscriptionBatches(streams); err != nil {
			return nil, err
		}
	}
	return m, nil
}

func BookBufferedEventSlots(instruments, topics int) int {
	if instruments <= 0 || topics <= 0 {
		return 0
	}
	slots := snapshotBridgeCapacity
	for remaining := instruments; remaining > 0; remaining -= topics {
		slots += max(4096, 64*min(remaining, topics)) + 2*controlQueueCapacity + 2
	}
	return slots
}

// Slots account for Go queue envelopes. Variable payload/level allocations are
// reported separately from this lower-bound resident envelope size.
func BufferedEventSlotBytes() uintptr {
	return max(unsafe.Sizeof(bookEvent{}), unsafe.Sizeof(DepthUpdate{}))
}

func (m *BookManager) HealthSnapshot() BookHealthSnapshot {
	h := BookHealthSnapshot{ExpectedInstruments: len(m.targets), ConnectedShards: int(m.connected.Load()), Reconnects: m.reconnects.Load(), Resyncs: m.resyncs.Load(), QueuePeak: m.queuePeak.Load(), QueueOverflows: m.queueOverflows.Load()}
	for _, target := range m.targets {
		view := target.book.View()
		switch view.State {
		case orderbook.StateValid:
			h.ReadyInstruments++
			if h.OldestSourceTime.IsZero() || view.SourceTime.Before(h.OldestSourceTime) {
				h.OldestSourceTime = view.SourceTime
			}
		case orderbook.StateResyncing, orderbook.StateConnecting:
			h.ResyncingInstruments++
		default:
			h.InvalidInstruments++
		}
	}
	return h
}

func (m *BookManager) Run(ctx context.Context) error {
	if err := m.Validate(); err != nil {
		return err
	}
	var workers sync.WaitGroup
	workers.Add(1)
	go func() { defer workers.Done(); m.snapshotScheduler(ctx) }()
	for _, shard := range m.shards {
		workers.Add(1)
		go func(targets []*bookTarget) { defer workers.Done(); m.runShard(ctx, targets) }(shard)
	}
	workers.Wait()
	return nil
}

func (m *BookManager) Validate() error {
	if m == nil || m.Client == nil || m.Dialer == nil || m.Logger == nil || m.ReconnectJitter == nil {
		return fmt.Errorf("Binance manager missing local dependency")
	}
	u, err := url.Parse(m.WSEndpoint)
	if err != nil || u.Host == "" || (u.Scheme != "ws" && u.Scheme != "wss") {
		return fmt.Errorf("Binance manager invalid websocket endpoint")
	}
	if m.SilenceTimeout <= 0 || m.SubscriptionTimeout <= 0 || m.SnapshotTimeout <= 0 || m.BridgeTimeout <= 0 || m.ControlInterval <= 0 || m.ReconnectBase <= 0 || m.ReconnectMax < m.ReconnectBase {
		return fmt.Errorf("Binance manager invalid timeout or interval")
	}
	return nil
}

func (m *BookManager) notify() {
	select {
	case m.wake <- struct{}{}:
	default:
	}
}

func (m *BookManager) invalidateShard(targets []*bookTarget, reason string) {
	for _, target := range targets {
		target.mu.Lock()
		target.generation++
		target.stage = bookOffline
		target.buffer, target.collector = nil, nil
		if target.attemptCancel != nil {
			target.attemptCancel()
		}
		if target.done != nil {
			select {
			case target.done <- fmt.Errorf("%s", reason):
			default:
			}
		}
		target.book.MarkInvalid(reason)
		target.mu.Unlock()
	}
	m.notify()
}

func (m *BookManager) runShard(ctx context.Context, targets []*bookTarget) {
	delay := m.ReconnectBase
	for ctx.Err() == nil {
		err := m.runShardConnection(ctx, targets)
		if ctx.Err() != nil {
			m.invalidateShard(targets, "Binance manager stopped")
			return
		}
		m.reconnects.Add(1)
		m.Logger.Error("Binance book shard disconnected", "topics", len(targets), "first_symbol", targets[0].instrument.ExchangeSymbol, "error", err)
		if !exchange.Wait(ctx, m.ReconnectJitter(delay)) {
			return
		}
		delay = min(delay*2, m.ReconnectMax)
	}
}

func (m *BookManager) runShardConnection(ctx context.Context, targets []*bookTarget) error {
	if err := m.Client.waitWebsocket(ctx); err != nil {
		return err
	}
	conn, response, err := m.Dialer.DialContext(ctx, m.WSEndpoint, http.Header{})
	if err != nil {
		if response != nil {
			return fmt.Errorf("Binance shard handshake %s: %w", response.Status, err)
		}
		return err
	}
	defer conn.Close()
	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	fence := &connectionFence{}
	stop := context.AfterFunc(readCtx, func() { _ = conn.Close() })
	defer stop()
	m.connected.Add(1)
	defer m.connected.Add(-1)
	writer := newSocketWriter(readCtx, conn, m.ControlInterval)
	writerDone := make(chan struct{})
	go func() { defer close(writerDone); writer.run() }()
	conn.SetPingHandler(func(payload string) error {
		_ = conn.SetReadDeadline(time.Now().Add(m.SilenceTimeout))
		return writer.pong(payload)
	})
	conn.SetPongHandler(func(string) error { return conn.SetReadDeadline(time.Now().Add(m.SilenceTimeout)) })
	conn.SetCloseHandler(func(code int, text string) error { return &websocket.CloseError{Code: code, Text: text} })
	conn.SetReadLimit(8 << 20)
	_ = conn.SetReadDeadline(time.Now().Add(m.SilenceTimeout))
	queue := make(chan bookEvent, max(4096, 64*len(targets)))
	errors := make(chan error, 1)
	bySymbol := make(map[string]*bookTarget, len(targets))
	streams := make([]string, len(targets))
	for i, target := range targets {
		bySymbol[target.instrument.ExchangeSymbol] = target
		streams[i] = strings.ToLower(target.instrument.ExchangeSymbol) + "@depth@100ms"
	}
	batches, err := subscriptionBatches(streams)
	if err != nil {
		return err
	}
	readerDone := make(chan struct{})
	go func() { defer close(readerDone); m.readShard(readCtx, conn, bySymbol, targets, queue, errors, fence) }()
	defer func() {
		fence.fail(fmt.Errorf("Binance shard connection ended"), func() { m.invalidateShard(targets, "Binance shard connection ended") })
		cancel()
		_ = conn.Close()
		<-readerDone
		<-writerDone
	}()
	var id int64
	batchIndex, batchStart, batchEnd := 0, 0, 0
	sendBatch := func() error {
		id = m.Client.requestID.Add(1)
		batchEnd = batchStart + len(batches[batchIndex])
		return writer.json(map[string]any{"method": "SUBSCRIBE", "params": batches[batchIndex], "id": id})
	}
	if err = sendBatch(); err != nil {
		return err
	}
	ackTimer := time.NewTimer(m.SubscriptionTimeout)
	defer ackTimer.Stop()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err = <-errors:
			return err
		case err = <-writer.errors:
			return err
		case <-ackTimer.C:
			return fmt.Errorf("Binance shard subscription ACK timeout")
		case now := <-ticker.C:
			for _, target := range targets {
				target.mu.Lock()
				if target.stage == bookLive && now.Sub(target.lastReceive) >= m.SilenceTimeout {
					m.retryTargetLocked(target, fmt.Errorf("Binance target silence timeout"))
				}
				target.mu.Unlock()
			}
		case event := <-queue:
			if event.target == nil {
				if err = fence.apply(func() error { return m.acknowledgeShard(targets[batchStart:batchEnd], event.payload, id) }); err != nil {
					return err
				}
				id = 0
				ackTimer.Stop()
				batchIndex++
				batchStart = batchEnd
				if batchIndex < len(batches) {
					if err = sendBatch(); err != nil {
						return err
					}
					ackTimer.Reset(m.SubscriptionTimeout)
				}
				continue
			}
			if err = fence.apply(func() error { return m.processEvent(event) }); err != nil {
				return err
			}
		}
	}
}

func (m *BookManager) acknowledgeShard(targets []*bookTarget, payload []byte, id int64) error {
	ack, err := parseSubscriptionACK(payload, id)
	if err != nil {
		return err
	}
	if !ack {
		return fmt.Errorf("Binance unrouteable websocket message")
	}
	for _, target := range targets {
		target.mu.Lock()
		target.stage = bookWaiting
		target.generation++
		target.book.MarkResyncing("awaiting Binance snapshot slot")
		target.mu.Unlock()
	}
	m.notify()
	return nil
}

func (m *BookManager) readShard(ctx context.Context, conn *websocket.Conn, bySymbol map[string]*bookTarget, targets []*bookTarget, queue chan<- bookEvent, errors chan<- error, fence *connectionFence) {
	fail := func(err error) {
		fence.fail(err, func() { m.invalidateShard(targets, err.Error()) })
		select {
		case errors <- err:
		default:
		}
	}
	for {
		_, payload, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() == nil {
				fail(err)
			}
			return
		}
		now := time.Now().UTC()
		_ = conn.SetReadDeadline(now.Add(m.SilenceTimeout))
		var identity struct {
			Event     string          `json:"e"`
			EventTime json.RawMessage `json:"E"` // Keep encoding/json's case folding from treating E as e.
			Symbol    string          `json:"s"`
		}
		if err = json.Unmarshal(payload, &identity); err != nil {
			fail(err)
			return
		}
		event := bookEvent{payload: payload, received: now}
		if identity.Event != "" || identity.Symbol != "" {
			target := bySymbol[identity.Symbol]
			if target == nil || identity.Event != "depthUpdate" {
				fail(fmt.Errorf("Binance unknown book route %q/%q", identity.Event, identity.Symbol))
				return
			}
			target.mu.Lock()
			event.target, event.generation = target, target.generation
			stage := target.stage
			target.mu.Unlock()
			if stage == bookWaiting || stage == bookOffline {
				continue
			}
		}
		select {
		case queue <- event:
			for peak := m.queuePeak.Load(); uint64(len(queue)) > peak; peak = m.queuePeak.Load() {
				if m.queuePeak.CompareAndSwap(peak, uint64(len(queue))) {
					break
				}
			}
		case <-ctx.Done():
			return
		default:
			m.queueOverflows.Add(1)
			fail(fmt.Errorf("Binance shard queue overflow"))
			return
		}
	}
}

func (m *BookManager) processEvent(event bookEvent) error {
	target := event.target
	target.mu.Lock()
	defer target.mu.Unlock()
	if event.generation != target.generation || target.stage == bookOffline || target.stage == bookWaiting {
		return nil
	}
	update, err := ParseDepthUpdate(event.payload, target.instrument, true, event.received)
	if err != nil {
		return err
	} // A malformed source message invalidates the shared transport.
	target.lastReceive = event.received
	switch target.stage {
	case bookBuffering:
		if len(target.buffer) >= snapshotBridgeCapacity {
			m.queueOverflows.Add(1)
			m.retryTargetLocked(target, fmt.Errorf("Binance snapshot bridge buffer overflow"))
			return nil
		}
		target.buffer = append(target.buffer, update)
	case bookBridging, bookLive:
		if err = target.collector.Push(update); err != nil {
			m.retryTargetLocked(target, err)
			return nil
		}
		if target.stage == bookBridging && target.collector.firstAccepted {
			return m.activateLocked(target)
		}
	}
	return nil
}

func (m *BookManager) retryTargetLocked(target *bookTarget, err error) {
	m.resyncs.Add(1)
	target.generation++
	target.stage = bookWaiting
	target.buffer, target.collector = nil, nil
	target.failures++
	delay := m.ReconnectBase
	for i := 1; i < target.failures && delay < m.ReconnectMax; i++ {
		delay = min(delay*2, m.ReconnectMax)
	}
	target.retryAt = time.Now().Add(m.ReconnectJitter(delay))
	target.book.MarkResyncing(err.Error())
	if target.attemptCancel != nil {
		target.attemptCancel()
	}
	if target.done != nil {
		select {
		case target.done <- err:
		default:
		}
	}
	m.notify()
}

func (m *BookManager) activateLocked(target *bookTarget) error {
	// Build/bridge on a private Book: the sampler must never see an unbridged
	// REST snapshot, even for the brief interval between ApplySnapshot and Push.
	snapshot, valid := target.collector.book.Snapshot(1000)
	if !valid {
		m.retryTargetLocked(target, fmt.Errorf("Binance bridged snapshot invalid"))
		return nil
	}
	if err := target.book.ApplySnapshot(snapshot); err != nil {
		m.retryTargetLocked(target, err)
		return nil
	}
	target.collector.book = target.book
	target.stage, target.failures = bookLive, 0
	target.buffer = nil
	if target.done != nil {
		select {
		case target.done <- nil:
		default:
		}
	}
	return nil
}

// nextSnapshot gives every not-yet-attempted online target priority. Retries are
// round-robin and respect per-target backoff; a failing symbol cannot monopolize
// the global REST request slot.
func (m *BookManager) nextSnapshot(now time.Time) *bookTarget {
	for _, initialOnly := range []bool{true, false} {
		for offset := 0; offset < len(m.targets); offset++ {
			index := (m.cursor + offset) % len(m.targets)
			target := m.targets[index]
			target.mu.Lock()
			eligible := target.stage == bookWaiting && !now.Before(target.retryAt) && (!initialOnly || !target.attempted)
			target.mu.Unlock()
			if eligible {
				m.cursor = (index + 1) % len(m.targets)
				return target
			}
		}
	}
	return nil
}

func (m *BookManager) snapshotScheduler(ctx context.Context) {
	for ctx.Err() == nil {
		target := m.nextSnapshot(time.Now())
		if target == nil {
			timer := time.NewTimer(100 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-m.wake:
				timer.Stop()
			case <-timer.C:
			}
			continue
		}
		m.snapshotAttempt(ctx, target)
	}
}

func (m *BookManager) snapshotAttempt(ctx context.Context, target *bookTarget) {
	attemptCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var generation uint64
	var done chan error
	var stopTimeout func() bool
	snapshot, err := m.Client.depthSnapshot(attemptCtx, target.instrument, func() error {
		target.mu.Lock()
		defer target.mu.Unlock()
		if target.stage != bookWaiting {
			return fmt.Errorf("Binance snapshot target no longer queued")
		}
		target.generation++
		generation = target.generation
		target.stage, target.attempted = bookBuffering, true
		target.buffer = make([]DepthUpdate, 0, snapshotBridgeCapacity)
		target.collector = nil
		target.attemptCancel = cancel
		done = make(chan error, 1)
		target.done = done
		// The timeout starts at the send boundary, not during a global cooldown.
		timer := time.AfterFunc(m.SnapshotTimeout, cancel)
		stopTimeout = timer.Stop
		return nil
	})
	if stopTimeout != nil {
		stopTimeout()
	}
	target.mu.Lock()
	if generation == 0 || generation != target.generation {
		if generation == 0 && err != nil && ctx.Err() == nil && target.stage == bookWaiting {
			target.attempted = true
			m.retryTargetLocked(target, err)
		}
		target.mu.Unlock()
		return
	}
	if err != nil {
		m.retryTargetLocked(target, err)
		target.mu.Unlock()
		return
	}
	scratch, _ := orderbook.New(target.instrument.ID, 1000)
	collector, _ := NewCollector(scratch, true)
	if err = collector.ApplySnapshot(snapshot); err == nil {
		for _, update := range target.buffer {
			if err = collector.Push(update); err != nil {
				break
			}
		}
	}
	if err != nil {
		m.retryTargetLocked(target, err)
		target.mu.Unlock()
		return
	}
	target.collector, target.stage = collector, bookBridging
	target.buffer = nil
	if collector.firstAccepted {
		_ = m.activateLocked(target)
	}
	target.mu.Unlock()
	timer := time.NewTimer(m.BridgeTimeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-done:
	case <-timer.C:
	}
	target.mu.Lock()
	if target.generation == generation {
		if target.stage != bookLive {
			m.retryTargetLocked(target, fmt.Errorf("Binance snapshot bridge timeout"))
		}
		target.attemptCancel, target.done = nil, nil
	}
	target.mu.Unlock()
}
