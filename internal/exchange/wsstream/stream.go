// Package wsstream supplies bounded multiplexed transport. Venue adapters retain
// all message parsing, snapshot and sequence rules.
package wsstream

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/gorilla/websocket"
	"github.com/vphoenix/crypto-market-info/internal/exchange"
)

type Message struct {
	Topic, Operation, RequestID string
	Acknowledgement, Pong       bool
}

type Protocol interface {
	Decode([]byte) (Message, error)
	Subscription(id, operation string, topics []string) any
	Ping() (int, []byte)
}

// Apply returns whether the target has established its own snapshot. An adapter
// must ignore deltas while awaitingSnapshot, but still strictly validate them.
type Target struct {
	Topic       string
	Apply       func(payload []byte, awaitingSnapshot bool) (ready bool, err error)
	Reset       func(reason string)
	Invalidate  func(reason string)
	generation  atomic.Uint64
	phase       phase
	lastMessage time.Time
}

type phase uint8

const (
	awaitingSubscribe phase = iota
	awaitingSnapshot
	active
	awaitingUnsubscribe
)

type Statistics struct {
	Connected                                      atomic.Int64
	Reconnects, Resyncs, QueuePeak, QueueOverflows atomic.Uint64
	StaleTargets                                   atomic.Uint64
}

type Config struct {
	Endpoint                                 string
	Dialer                                   *websocket.Dialer
	ConnectGate                              exchange.WaitGate
	Protocol                                 Protocol
	Targets                                  []*Target
	Batches                                  [][]string
	BatchSize, QueueCapacity, PreAckCapacity int
	AckPerTopic, RequireAckID, Resync        bool
	// Funding topics may legitimately be quiet while other topics and the
	// heartbeat prove that the shared connection is healthy. Expire only that
	// observation; retain the uninterrupted venue snapshot/delta cache.
	StaleTargetOnly                                                 bool
	OnStale                                                         func(topic string)
	ControlInterval, PingInterval, SilenceTimeout, SubscribeTimeout time.Duration
	MaxControlsPerHour                                              int
	Stats                                                           *Statistics
}

type streamEvent struct {
	payload      []byte
	message      Message
	generation   uint64
	receiveOrder uint64
	sentID       string
}

// EventSlotBytes estimates queue bookkeeping only; payload backing memory is
// additional and depends on actual depth/message sizes.
func EventSlotBytes() uintptr { return unsafe.Sizeof(streamEvent{}) }

// BufferedEventSlots includes incoming events, pre-ACK entries, queued controls,
// RFC pong requests, and the writer's one active command.
func BufferedEventSlots(targets, queueCapacity, preAckCapacity int) int {
	if targets <= 0 {
		return 0
	}
	if queueCapacity <= 0 {
		queueCapacity = max(4096, 64*targets)
	}
	if preAckCapacity <= 0 {
		preAckCapacity = queueCapacity
	}
	return queueCapacity + preAckCapacity + max(64, targets*2) + 16 + 1
}

type terminal struct {
	mu  sync.Mutex
	err error
}

func (t *terminal) fail(err error, targets []*Target) error {
	if err == nil {
		err = fmt.Errorf("websocket reader stopped")
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.err == nil {
		t.err = err
		for _, target := range targets {
			target.Invalidate(err.Error())
		}
	}
	return t.err
}
func (t *terminal) apply(fn func() error) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.err != nil {
		return t.err
	}
	return fn()
}

type pendingRequest struct {
	operation string
	topics    map[string]bool
	deadline  time.Time
}
type writeCommand struct {
	id, operation string
	topics        []string
}
type pongCommand struct{ data string }

// RunConnection runs one generation of a shard. All target state becomes
// invalid before it returns, including cancellation and writer failures.
func RunConnection(ctx context.Context, cfg Config) (result error) {
	if err := cfg.validate(); err != nil {
		return err
	}
	for _, target := range cfg.Targets {
		target.generation.Add(1)
		target.phase = awaitingSubscribe
		target.Reset("connecting websocket shard")
	}
	defer func() {
		for _, target := range cfg.Targets {
			target.Invalidate("websocket shard stopped")
		}
	}()
	if err := cfg.ConnectGate.Wait(ctx); err != nil {
		return err
	}
	conn, response, err := cfg.Dialer.DialContext(ctx, cfg.Endpoint, http.Header{})
	if err != nil {
		if response != nil {
			if response.Body != nil {
				response.Body.Close()
			}
			return fmt.Errorf("websocket handshake %s: %w", response.Status, err)
		}
		return err
	}
	readCtx, cancel := context.WithCancel(ctx)
	var workers sync.WaitGroup
	defer func() { cancel(); conn.Close(); workers.Wait() }()
	cfg.Stats.Connected.Add(1)
	defer cfg.Stats.Connected.Add(-1)
	byTopic := make(map[string]*Target, len(cfg.Targets))
	for _, target := range cfg.Targets {
		byTopic[target.Topic] = target
	}
	events := make(chan streamEvent, cfg.QueueCapacity)
	failures := make(chan error, 1)
	commands := make(chan writeCommand, max(64, len(cfg.Targets)*2))
	pongs := make(chan pongCommand, 16)
	end := &terminal{}
	fail := func(err error) error {
		err = end.fail(err, cfg.Targets)
		select {
		case failures <- err:
		default:
		}
		return err
	}
	push := func(event streamEvent) bool {
		select {
		case events <- event:
			peak := uint64(len(events))
			for old := cfg.Stats.QueuePeak.Load(); peak > old; old = cfg.Stats.QueuePeak.Load() {
				if cfg.Stats.QueuePeak.CompareAndSwap(old, peak) {
					break
				}
			}
			return true
		case <-readCtx.Done():
			return false
		default:
			cfg.Stats.QueueOverflows.Add(1)
			fail(fmt.Errorf("websocket shard reader queue overflow"))
			return false
		}
	}
	conn.SetPingHandler(func(data string) error {
		select {
		case pongs <- pongCommand{data}:
			return nil
		default:
			return fail(fmt.Errorf("websocket pong queue overflow"))
		}
	})
	// Gorilla's default close handler writes from the reader; the serial writer
	// owns every frame, so termination simply closes the transport.
	conn.SetCloseHandler(func(code int, text string) error { return &websocket.CloseError{Code: code, Text: text} })
	conn.SetReadLimit(4 << 20)
	workers.Add(2)
	go func() {
		defer workers.Done()
		var receiveOrder uint64
		_ = conn.SetReadDeadline(time.Now().Add(cfg.SilenceTimeout))
		for {
			_, payload, readErr := conn.ReadMessage()
			if readErr != nil {
				if readCtx.Err() == nil {
					fail(readErr)
				}
				return
			}
			_ = conn.SetReadDeadline(time.Now().Add(cfg.SilenceTimeout))
			message, decodeErr := cfg.Protocol.Decode(payload)
			if decodeErr != nil {
				fail(decodeErr)
				return
			}
			event := streamEvent{payload: payload, message: message}
			receiveOrder++
			event.receiveOrder = receiveOrder
			if message.Topic != "" {
				target := byTopic[message.Topic]
				if target == nil {
					fail(fmt.Errorf("websocket unexpected topic %q", message.Topic))
					return
				}
				event.generation = target.generation.Load()
			}
			if !push(event) {
				return
			}
		}
	}()
	go func() {
		defer workers.Done()
		if writeErr := runWriter(readCtx, conn, cfg, commands, pongs, push); writeErr != nil && readCtx.Err() == nil {
			fail(writeErr)
		}
	}()
	pending := make(map[string]*pendingRequest)
	var nextID uint64
	enqueue := func(operation string, topics []string) error {
		nextID++
		id := fmt.Sprintf("r%d", nextID)
		request := &pendingRequest{operation: operation, topics: make(map[string]bool, len(topics))}
		for _, topic := range topics {
			request.topics[topic] = true
		}
		pending[id] = request
		select {
		case commands <- writeCommand{id, operation, append([]string(nil), topics...)}:
			return nil
		default:
			return fmt.Errorf("websocket control queue overflow")
		}
	}
	for _, topics := range cfg.Batches {
		if err := enqueue("subscribe", topics); err != nil {
			return fail(err)
		}
	}
	preAck := make(map[string][]streamEvent)
	preAckCount := 0
	resync := func(target *Target, cause error) error {
		if !cfg.Resync {
			return cause
		}
		cfg.Stats.Resyncs.Add(1)
		target.generation.Add(1)
		target.phase = awaitingUnsubscribe
		target.lastMessage = time.Time{}
		target.Reset(cause.Error())
		preAckCount -= len(preAck[target.Topic])
		delete(preAck, target.Topic)
		return enqueue("unsubscribe", []string{target.Topic})
	}
	apply := func(event streamEvent) error {
		target := byTopic[event.message.Topic]
		if event.generation != target.generation.Load() || target.phase == awaitingUnsubscribe {
			return nil
		}
		if target.phase == awaitingSubscribe {
			if preAckCount >= cfg.PreAckCapacity {
				cfg.Stats.QueueOverflows.Add(1)
				return fmt.Errorf("websocket shard pre-ack buffer overflow")
			}
			preAck[target.Topic] = append(preAck[target.Topic], event)
			preAckCount++
			return nil
		}
		ready, applyErr := target.Apply(event.payload, target.phase == awaitingSnapshot)
		if applyErr != nil {
			return resync(target, applyErr)
		}
		if ready {
			target.phase = active
			target.lastMessage = time.Now()
		}
		return nil
	}
	ticker := time.NewTicker(min(100*time.Millisecond, cfg.SubscribeTimeout))
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-failures:
			return err
		case event := <-events:
			err := end.apply(func() error {
				if event.sentID != "" {
					if request := pending[event.sentID]; request != nil {
						request.deadline = time.Now().Add(cfg.SubscribeTimeout)
					}
					return nil
				}
				msg := event.message
				if msg.Pong {
					return nil
				}
				if !msg.Acknowledgement {
					return apply(event)
				}
				id := msg.RequestID
				if cfg.AckPerTopic && id == "" && !cfg.RequireAckID {
					for candidate, request := range pending {
						if request.operation == msg.Operation && request.topics[msg.Topic] {
							if id != "" {
								return fmt.Errorf("ambiguous websocket acknowledgement")
							}
							id = candidate
						}
					}
				}
				request := pending[id]
				if request == nil || request.operation != msg.Operation {
					return fmt.Errorf("unexpected or duplicate websocket %s acknowledgement id=%q topic=%q", msg.Operation, msg.RequestID, msg.Topic)
				}
				topics := make([]string, 0, len(request.topics))
				if cfg.AckPerTopic {
					if !request.topics[msg.Topic] {
						return fmt.Errorf("unexpected websocket acknowledgement topic %q", msg.Topic)
					}
					topics = append(topics, msg.Topic)
					delete(request.topics, msg.Topic)
				} else {
					for topic := range request.topics {
						topics = append(topics, topic)
					}
					clear(request.topics)
					sort.Strings(topics)
				}
				if len(request.topics) == 0 {
					delete(pending, id)
				}
				var replay []streamEvent
				for _, topic := range topics {
					target := byTopic[topic]
					if msg.Operation == "unsubscribe" {
						if target.phase != awaitingUnsubscribe {
							return fmt.Errorf("unexpected unsubscribe state")
						}
						target.generation.Add(1)
						target.phase = awaitingSubscribe
						if err := enqueue("subscribe", []string{topic}); err != nil {
							return err
						}
						continue
					}
					if target.phase != awaitingSubscribe {
						return fmt.Errorf("unexpected subscribe state")
					}
					target.phase = awaitingSnapshot
					target.lastMessage = time.Now()
					buffered := preAck[topic]
					delete(preAck, topic)
					preAckCount -= len(buffered)
					replay = append(replay, buffered...)
				}
				sort.Slice(replay, func(i, j int) bool { return replay[i].receiveOrder < replay[j].receiveOrder })
				for _, early := range replay {
					if err := apply(early); err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				return fail(err)
			}
		case now := <-ticker.C:
			err := end.apply(func() error {
				for id, request := range pending {
					if !request.deadline.IsZero() && !now.Before(request.deadline) {
						return fmt.Errorf("websocket %s acknowledgements timed out id=%s", request.operation, id)
					}
				}
				for _, target := range cfg.Targets {
					if (target.phase == active || target.phase == awaitingSnapshot) && !target.lastMessage.IsZero() && now.Sub(target.lastMessage) > cfg.SilenceTimeout {
						if cfg.StaleTargetOnly {
							target.Invalidate("topic stale or missing initial snapshot")
							target.lastMessage = time.Time{}
							cfg.Stats.StaleTargets.Add(1)
							if cfg.OnStale != nil {
								cfg.OnStale(target.Topic)
							}
							continue
						}
						if err := resync(target, fmt.Errorf("websocket target %s stale or missing snapshot", target.Topic)); err != nil {
							return err
						}
					}
				}
				return nil
			})
			if err != nil {
				return fail(err)
			}
		}
	}
}

func (c *Config) validate() error {
	if c.Endpoint == "" || c.Protocol == nil || c.ConnectGate == nil || c.Stats == nil || len(c.Targets) == 0 {
		return fmt.Errorf("websocket shard missing dependencies")
	}
	if c.Dialer == nil {
		c.Dialer = websocket.DefaultDialer
	}
	if c.BatchSize <= 0 && len(c.Batches) == 0 {
		return fmt.Errorf("websocket batch size must be positive")
	}
	if c.QueueCapacity <= 0 {
		c.QueueCapacity = max(4096, 64*len(c.Targets))
	}
	if c.PreAckCapacity <= 0 {
		c.PreAckCapacity = c.QueueCapacity
	}
	if c.ControlInterval <= 0 {
		c.ControlInterval = 250 * time.Millisecond
	}
	if c.PingInterval <= 0 {
		c.PingInterval = 20 * time.Second
	}
	if c.SilenceTimeout <= 0 {
		c.SilenceTimeout = 45 * time.Second
	}
	if c.SubscribeTimeout <= 0 {
		c.SubscribeTimeout = 10 * time.Second
	}
	seen := make(map[string]bool)
	for _, target := range c.Targets {
		if target == nil || target.Topic == "" || target.Apply == nil || target.Reset == nil || target.Invalidate == nil || seen[target.Topic] {
			return fmt.Errorf("invalid or duplicate websocket target")
		}
		seen[target.Topic] = true
	}
	if len(c.Batches) == 0 {
		for offset := 0; offset < len(c.Targets); offset += c.BatchSize {
			var topics []string
			for _, target := range c.Targets[offset:min(offset+c.BatchSize, len(c.Targets))] {
				topics = append(topics, target.Topic)
			}
			c.Batches = append(c.Batches, topics)
		}
	}
	for _, topics := range c.Batches {
		if len(topics) == 0 {
			return fmt.Errorf("empty websocket subscription batch")
		}
		for _, topic := range topics {
			if !seen[topic] {
				return fmt.Errorf("unexpected or duplicated websocket subscription topic %q", topic)
			}
			delete(seen, topic)
		}
	}
	if len(seen) != 0 {
		return fmt.Errorf("incomplete websocket subscription batches")
	}
	return nil
}

// controlReady applies a sliding hour window without reserving future permits.
func controlReady(now, last time.Time, history []time.Time, interval time.Duration, hourly int) time.Time {
	ready := last.Add(interval)
	if hourly > 0 && len(history) >= hourly {
		if hourReady := history[len(history)-hourly].Add(time.Hour); hourReady.After(ready) {
			ready = hourReady
		}
	}
	if ready.Before(now) {
		ready = now
	}
	return ready
}

type frameWriter interface {
	SetWriteDeadline(time.Time) error
	WriteMessage(int, []byte) error
	WriteJSON(any) error
}

func runWriter(ctx context.Context, conn frameWriter, cfg Config, commands <-chan writeCommand, pongs <-chan pongCommand, push func(streamEvent) bool) error {
	var waiting *writeCommand
	var lastControl time.Time
	var history []time.Time
	nextPing := time.Now().Add(cfg.PingInterval)
	timer := time.NewTimer(0)
	defer timer.Stop()
	write := func(kind int, payload []byte) error {
		if err := conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
			return err
		}
		return conn.WriteMessage(kind, payload)
	}
	for {
		now := time.Now()
		for len(history) > 0 && !history[0].Add(time.Hour).After(now) {
			history = history[1:]
		}
		// Heartbeats bypass the hourly subscription budget; they remain serialized
		// and cannot starve behind a target repeatedly requesting resynchronization.
		pingReady := nextPing
		if cfg.MaxControlsPerHour == 0 && lastControl.Add(cfg.ControlInterval).After(pingReady) {
			pingReady = lastControl.Add(cfg.ControlInterval)
		}
		if !now.Before(pingReady) {
			kind, payload := cfg.Protocol.Ping()
			if err := write(kind, payload); err != nil {
				return err
			}
			nextPing = time.Now().Add(cfg.PingInterval)
			// JSON heartbeat counts against Bybit's inter-control spacing.
			if cfg.MaxControlsPerHour == 0 {
				lastControl = time.Now()
			}
			pingReady = nextPing
		}
		wake := pingReady
		if waiting != nil {
			ready := controlReady(time.Now(), lastControl, history, cfg.ControlInterval, cfg.MaxControlsPerHour)
			if !ready.After(time.Now()) {
				if err := conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
					return err
				}
				if err := conn.WriteJSON(cfg.Protocol.Subscription(waiting.id, waiting.operation, waiting.topics)); err != nil {
					return err
				}
				lastControl = time.Now()
				if cfg.MaxControlsPerHour > 0 {
					history = append(history, lastControl)
				}
				if !push(streamEvent{sentID: waiting.id}) {
					return nil
				}
				waiting = nil
				continue
			}
			if ready.Before(wake) {
				wake = ready
			}
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(max(time.Until(wake), time.Nanosecond))
		input := commands
		if waiting != nil {
			input = nil
		}
		select {
		case <-ctx.Done():
			return nil
		case command := <-input:
			waiting = &command
		case pong := <-pongs:
			if err := write(websocket.PongMessage, []byte(pong.data)); err != nil {
				return err
			}
		case <-timer.C:
		}
	}
}
