package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const controlQueueCapacity = 16

// Live USD-M endpoints reject JSON requests truncated at byte 4096. Keep a
// conservative cap on the complete encoded request, not merely topic count.
const maxSubscriptionPayloadBytes = 4000

func subscriptionBatches(streams []string) ([][]string, error) {
	if len(streams) == 0 {
		return nil, fmt.Errorf("Binance subscription requires streams")
	}
	var batches [][]string
	var batch []string
	fits := func(candidate []string) bool {
		// Reserve the longest positive request ID; actual IDs may grow on reconnect.
		payload, _ := json.Marshal(map[string]any{"method": "SUBSCRIBE", "params": candidate, "id": int64(math.MaxInt64)})
		return len(candidate) <= MaxBookTopicsPerConnection && len(payload) <= maxSubscriptionPayloadBytes
	}
	for _, stream := range streams {
		candidate := append(append([]string(nil), batch...), stream)
		if fits(candidate) {
			batch = candidate
			continue
		}
		if len(batch) == 0 {
			return nil, fmt.Errorf("Binance subscription stream exceeds %d-byte request budget", maxSubscriptionPayloadBytes)
		}
		batches = append(batches, batch)
		batch = []string{stream}
		if !fits(batch) {
			return nil, fmt.Errorf("Binance subscription stream exceeds %d-byte request budget", maxSubscriptionPayloadBytes)
		}
	}
	return append(batches, batch), nil
}

// connectionFence prevents queued messages/ACKs from reviving a transport after
// the reader has invalidated it. Checking and publishing share the same lock.
type connectionFence struct {
	mu  sync.Mutex
	err error
}

func (f *connectionFence) apply(action func() error) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	return action()
}

func (f *connectionFence) fail(err error, invalidate func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return
	}
	f.err = err
	invalidate()
}

type controlWrite struct {
	payload []byte
	done    chan error
}

// socketWriter is the only websocket writer, including pong responses. Heartbeat
// requests have priority over queued JSON after the minimum send interval.
type socketWriter struct {
	ctx      context.Context
	conn     *websocket.Conn
	controls chan controlWrite
	pongs    chan []byte
	errors   chan error
	done     chan struct{}
	err      error
	interval time.Duration
}

func newSocketWriter(ctx context.Context, conn *websocket.Conn, interval time.Duration) *socketWriter {
	return &socketWriter{ctx: ctx, conn: conn, controls: make(chan controlWrite, controlQueueCapacity), pongs: make(chan []byte, controlQueueCapacity), errors: make(chan error, 1), done: make(chan struct{}), interval: interval}
}

func (w *socketWriter) json(value any) error {
	payload, err := json.Marshal(value)
	if err != nil {
		return err
	}
	request := controlWrite{payload: payload, done: make(chan error, 1)}
	select {
	case w.controls <- request:
	case <-w.ctx.Done():
		return w.ctx.Err()
	case <-w.done:
		return w.stoppedError()
	}
	select {
	case err = <-request.done:
		return err
	case <-w.ctx.Done():
		return w.ctx.Err()
	case <-w.done:
		return w.stoppedError()
	}
}

func (w *socketWriter) pong(payload string) error {
	select {
	case w.pongs <- []byte(payload):
		return nil
	case <-w.ctx.Done():
		return w.ctx.Err()
	default:
		return fmt.Errorf("Binance pong queue overflow")
	}
}

func (w *socketWriter) run() {
	defer close(w.done)
	var pending *controlWrite
	var last time.Time
	for {
		var pong []byte
		select {
		case pong = <-w.pongs:
		default:
		}
		if pong == nil && pending == nil {
			select {
			case <-w.ctx.Done():
				return
			case pong = <-w.pongs:
			case request := <-w.controls:
				pending = &request
			}
		}
		if delay := time.Until(last.Add(w.interval)); delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-w.ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
		}
		// If a JSON send waited for its slot, a newly arrived heartbeat goes first.
		if pong == nil {
			select {
			case pong = <-w.pongs:
			default:
			}
		}
		var err error
		if pong != nil {
			err = w.conn.WriteControl(websocket.PongMessage, pong, time.Now().Add(5*time.Second))
		} else {
			_ = w.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			err = w.conn.WriteMessage(websocket.TextMessage, pending.payload)
			pending.done <- err
			pending = nil
		}
		last = time.Now()
		if err != nil {
			w.err = err
			select {
			case w.errors <- err:
			default:
			}
			_ = w.conn.Close()
			return
		}
	}
}

func (w *socketWriter) stoppedError() error {
	if w.err != nil {
		return w.err
	}
	if err := w.ctx.Err(); err != nil {
		return err
	}
	return fmt.Errorf("Binance websocket writer stopped")
}

// parseSubscriptionACK distinguishes ACKs from market messages and accepts only
// the exact outstanding request. A second ACK after completion is an error.
func parseSubscriptionACK(payload []byte, expected int64) (bool, error) {
	var wire map[string]json.RawMessage
	if err := json.Unmarshal(payload, &wire); err != nil {
		return false, err
	}
	if _, exists := wire["code"]; exists {
		return true, fmt.Errorf("Binance websocket control error: %s", payload)
	}
	if _, exists := wire["error"]; exists {
		return true, fmt.Errorf("Binance websocket control error: %s", payload)
	}
	result, exists := wire["result"]
	if !exists {
		return false, nil
	}
	var id int64
	if expected <= 0 || json.Unmarshal(wire["id"], &id) != nil || id != expected || string(result) != "null" {
		return true, fmt.Errorf("Binance websocket unexpected or failed subscription ACK: %s", payload)
	}
	return true, nil
}
