package deribit

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

// Commands are bounded by the owner. A channel that has been canceled is never
// reused in the same physical epoch: book messages do not carry generation IDs.
type SessionCommand struct {
	Method     string
	Channels   []string
	Generation uint64
}

func (c *Client) Session(ctx context.Context, initial []string, commands <-chan SessionCommand, emit func(StreamEvent) error) (resultErr error) {
	if len(initial) == 0 || len(initial) > 512 {
		return fmt.Errorf("invalid initial session channels")
	}
	if err := c.ControlGate.Wait(ctx); err != nil {
		return err
	}
	d := *websocket.DefaultDialer
	d.HandshakeTimeout = 15 * time.Second
	conn, _, err := d.DialContext(ctx, c.WSURL, nil)
	if err != nil {
		return err
	}
	defer conn.Close()
	diag := &sessionDiagnostics{started: time.Now()}
	defer func() {
		if resultErr != nil && resultErr != context.Canceled {
			resultErr = fmt.Errorf("%w; %s", resultErr, diag.summary())
		}
	}()
	originalEmit := emit
	emit = func(e StreamEvent) error {
		started := time.Now()
		err := originalEmit(e)
		diag.emitMaxNsMax(time.Since(started).Nanoseconds())
		return err
	}
	conn.SetReadLimit(8 << 20)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	epoch := uuid.New()
	var seq atomic.Uint64
	if err = emit(StreamEvent{Kind: "connected", Epoch: epoch}); err != nil {
		return err
	}
	defer func() { _ = emit(StreamEvent{Kind: "disconnected", Epoch: epoch, Sequence: seq.Load() + 1}) }()
	type request struct {
		id             int
		cmd            SessionCommand
		testReceivedAt time.Time
	}
	ordinary := make(chan request, 64)
	priority := make(chan request, 16)
	readyOrdinary := make(chan request, 1)
	gatedDone := make(chan struct{})
	sent := make(chan request, 64)
	fail := make(chan error, 1)
	done := make(chan struct{})
	// Subscription pacing must not hold the socket writer: a heartbeat test
	// request can arrive while an ordinary subscription waits for its slot.
	go func() {
		defer close(gatedDone)
		for {
			var r request
			select {
			case <-ctx.Done():
				return
			case r = <-ordinary:
			}
			if c.SubscriptionGate != nil && (r.cmd.Method == "public/subscribe" || r.cmd.Method == "public/unsubscribe") {
				if err := c.SubscriptionGate.Wait(ctx); err != nil {
					return
				}
			}
			select {
			case readyOrdinary <- r:
			case <-ctx.Done():
				return
			}
		}
	}()
	go func() {
		defer close(done)
		var deferred *request
		for {
			var r request
			select {
			case r = <-priority:
			default:
				if deferred != nil {
					r, deferred = *deferred, nil
				} else {
					select {
					case <-ctx.Done():
						return
					case r = <-priority:
					case r = <-readyOrdinary:
					}
				}
			}
			gateStarted := time.Now()
			if err := c.ControlGate.Wait(ctx); err != nil {
				return
			}
			atomicMax(&diag.gateMaxNs, time.Since(gateStarted).Nanoseconds())
			// A test_request may arrive while the chosen ordinary command waits
			// for the shared control slot. Give that already-reserved slot to the
			// heartbeat, retain the ordinary command, and pace it on the next slot.
			if r.cmd.Method != "public/test" {
				select {
				case heartbeat := <-priority:
					old := r
					deferred, r = &old, heartbeat
				default:
				}
			}
			var params any = struct{}{}
			if r.cmd.Method == "public/set_heartbeat" {
				params = struct {
					Interval int `json:"interval"`
				}{10}
			} else if r.cmd.Method == "public/subscribe" || r.cmd.Method == "public/unsubscribe" {
				params = struct {
					Channels []string `json:"channels"`
				}{r.cmd.Channels}
			}
			if err := conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
				select {
				case fail <- err:
				default:
				}
				conn.Close()
				return
			}
			if err := conn.WriteJSON(struct {
				JSONRPC string `json:"jsonrpc"`
				ID      int    `json:"id"`
				Method  string `json:"method"`
				Params  any    `json:"params"`
			}{"2.0", r.id, r.cmd.Method, params}); err != nil {
				select {
				case fail <- err:
				default:
				}
				conn.Close()
				return
			}
			diag.writes.Add(1)
			diag.lastWriteNs.Store(time.Now().UnixNano())
			if !r.testReceivedAt.IsZero() {
				diag.testsSent.Add(1)
				atomicMax(&diag.testDelayMaxNs, time.Since(r.testReceivedAt).Nanoseconds())
			}
			select {
			case sent <- r:
			case <-ctx.Done():
				return
			}
		}
	}()
	defer func() { cancel(); conn.Close(); <-done; <-gatedDone }()
	go func() { <-ctx.Done(); conn.Close() }()
	nextID := 2
	ordinary <- request{id: 1, cmd: SessionCommand{Method: "public/set_heartbeat"}}
	ordinary <- request{id: 2, cmd: SessionCommand{Method: "public/subscribe", Channels: initial}}
	pending := map[int]request{1: {id: 1, cmd: SessionCommand{Method: "public/set_heartbeat"}}, 2: {id: 2, cmd: SessionCommand{Method: "public/subscribe", Channels: initial}}}
	active, used := map[string]bool{}, map[string]bool{}
	for _, s := range initial {
		used[s] = true
	}
	heartReady, subReady, ready := false, false, false
	// Reader runs independently of gated writes. Bounded frames are consumed by
	// this single owner in source order, including subscription acknowledgments.
	type frame struct {
		raw    []byte
		err    error
		readAt time.Time
	}
	frames := make(chan frame, 64)
	readerDone := make(chan struct{})
	go func() {
		defer close(frames)
		defer close(readerDone)
		for {
			conn.SetReadDeadline(time.Now().Add(30 * time.Second))
			_, raw, err := conn.ReadMessage()
			readAt := time.Now()
			if err == nil {
				diag.reads.Add(1)
				diag.bytes.Add(int64(len(raw)))
				diag.lastReadNs.Store(readAt.UnixNano())
			}
			if err != nil {
				raw = nil
			}
			if err == nil && !c.FrameBudget.Reserve(int64(len(raw))) {
				raw = nil
				err = fmt.Errorf("session frame byte budget exceeded")
			}
			select {
			case frames <- frame{raw: raw, err: err, readAt: readAt}:
				atomicMax(&diag.queuePeak, int64(len(frames)))
			case <-ctx.Done():
				c.FrameBudget.Release(int64(len(raw)))
				return
			}
			if err != nil {
				return
			}
		}
	}()
	ackTimer := time.NewTicker(time.Second)
	defer func() {
		cancel()
		conn.Close()
		<-readerDone
		for f := range frames {
			c.FrameBudget.Release(int64(len(f.raw)))
		}
	}()
	defer ackTimer.Stop()
	// Queue age and exchange ACK time are different budgets. The shared rate
	// gate may delay initial subscriptions for every physical connection.
	queued := map[int]bool{1: true, 2: true}
	deadlines := map[int]time.Time{1: time.Now().Add(90 * time.Second), 2: time.Now().Add(90 * time.Second)}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-fail:
			return err
		case r := <-sent:
			if _, ok := pending[r.id]; ok {
				delete(queued, r.id)
				deadlines[r.id] = time.Now().Add(15 * time.Second)
			}
		case <-ackTimer.C:
			for id, at := range deadlines {
				if time.Now().After(at) {
					if queued[id] {
						return fmt.Errorf("session request queue timeout id %d", id)
					}
					return fmt.Errorf("session ACK timeout id %d method %s", id, pending[id].cmd.Method)
				}
			}
		case cmd := <-commands:
			if cmd.Method != "public/subscribe" && cmd.Method != "public/unsubscribe" && cmd.Method != "public/status" {
				return fmt.Errorf("unsupported session command")
			}
			if len(pending) >= 64 {
				return fmt.Errorf("session request budget exceeded")
			}
			if cmd.Method == "public/subscribe" {
				for _, s := range cmd.Channels {
					if used[s] {
						return fmt.Errorf("channel reuse requires fresh physical epoch")
					}
					used[s] = true
				}
			}
			if cmd.Method == "public/unsubscribe" {
				for _, s := range cmd.Channels {
					delete(active, s)
				}
			}
			nextID++
			r := request{id: nextID, cmd: cmd}
			pending[nextID] = r
			queued[nextID] = true
			deadlines[nextID] = time.Now().Add(90 * time.Second)
			select {
			case ordinary <- r:
			default:
				return fmt.Errorf("session write queue full")
			}
		case f, ok := <-frames:
			if !ok {
				return fmt.Errorf("session reader stopped")
			}
			if f.err != nil {
				return f.err
			}
			atomicMax(&diag.frameAgeMaxNs, time.Since(f.readAt).Nanoseconds())
			c.FrameBudget.Release(int64(len(f.raw)))
			n := seq.Add(1)
			var wire struct {
				JSONRPC string          `json:"jsonrpc"`
				ID      *int            `json:"id"`
				Method  string          `json:"method"`
				Result  json.RawMessage `json:"result"`
				Error   *struct {
					Code int `json:"code"`
				} `json:"error"`
				Params struct {
					Channel string `json:"channel"`
					Type    string `json:"type"`
				} `json:"params"`
			}
			if err := decode(f.raw, &wire); err != nil {
				return err
			}
			if wire.JSONRPC != "2.0" {
				return fmt.Errorf("invalid session RPC")
			}
			if wire.Error != nil {
				if wire.Error.Code == 10028 {
					c.cooldown(time.Minute)
				}
				return fmt.Errorf("session RPC error %d", wire.Error.Code)
			}
			received := f.readAt.UTC().Truncate(time.Microsecond)
			e := StreamEvent{Kind: "confirm", Epoch: epoch, Sequence: n, Raw: f.raw, ReceivedAt: received}
			switch {
			case wire.ID != nil:
				r, ok := pending[*wire.ID]
				if !ok {
					return fmt.Errorf("unexpected session response")
				}
				delete(pending, *wire.ID)
				delete(queued, *wire.ID)
				delete(deadlines, *wire.ID)
				switch r.cmd.Method {
				case "public/set_heartbeat":
					var v string
					if err := json.Unmarshal(wire.Result, &v); err != nil || v != "ok" {
						return fmt.Errorf("heartbeat ACK rejected")
					}
					heartReady = true
				case "public/subscribe", "public/unsubscribe":
					var got []string
					if err := json.Unmarshal(wire.Result, &got); err != nil {
						return err
					}
					want := slices.Clone(r.cmd.Channels)
					slices.Sort(want)
					slices.Sort(got)
					if !slices.Equal(got, want) {
						return fmt.Errorf("incomplete subscription ACK")
					}
					for _, s := range got {
						kind := "unsubscribed"
						if r.cmd.Method == "public/subscribe" {
							active[s] = true
							kind = "subscribed"
						}
						if err := emit(StreamEvent{Kind: kind, Channel: s, Epoch: epoch, Sequence: n, ReceivedAt: received}); err != nil {
							return err
						}
					}
					if *wire.ID == 2 {
						subReady = true
					}
				case "public/status":
					e.Kind = "status"
					e.Channel = "public/status"
					e.Generation = r.cmd.Generation
				case "public/test":
					if len(wire.Result) == 0 || string(wire.Result) == "null" {
						return fmt.Errorf("invalid heartbeat test response")
					}
				}
			case wire.Method == "heartbeat":
				if wire.Params.Type != "heartbeat" && wire.Params.Type != "test_request" {
					return fmt.Errorf("unknown heartbeat")
				}
				if wire.Params.Type == "test_request" {
					nextID++
					diag.testsReceived.Add(1)
					r := request{id: nextID, cmd: SessionCommand{Method: "public/test"}, testReceivedAt: f.readAt}
					pending[nextID] = r
					queued[nextID] = true
					deadlines[nextID] = time.Now().Add(90 * time.Second)
					select {
					case priority <- r:
					default:
						return fmt.Errorf("heartbeat queue full")
					}
				}
			case wire.Method == "subscription":
				if !used[wire.Params.Channel] {
					return fmt.Errorf("unrequested session channel")
				}
				// Pre-ACK frames are delivered but the engine keeps them invalid.
				if _, live := active[wire.Params.Channel]; !live {
					found := false
					for _, r := range pending {
						if r.cmd.Method == "public/subscribe" && slices.Contains(r.cmd.Channels, wire.Params.Channel) {
							found = true
						}
					}
					if !found {
						continue
					}
				}
				e.Kind = "message"
				e.Channel = wire.Params.Channel
			default:
				return fmt.Errorf("unknown session frame")
			}
			if err := emit(e); err != nil {
				return err
			}
			if !ready && heartReady && subReady {
				ready = true
				if err := emit(StreamEvent{Kind: "ready", Epoch: epoch, Sequence: n}); err != nil {
					return err
				}
			}
		}
	}
}

// Numeric diagnostics only: no payloads, credentials or protocol state dumps.
type sessionDiagnostics struct {
	started                                                      time.Time
	reads, bytes, writes, queuePeak                              atomic.Int64
	lastReadNs, lastWriteNs, gateMaxNs, emitMaxNs, frameAgeMaxNs atomic.Int64
	testsReceived, testsSent, testDelayMaxNs                     atomic.Int64
}

func atomicMax(v *atomic.Int64, n int64) {
	for old := v.Load(); n > old; old = v.Load() {
		if v.CompareAndSwap(old, n) {
			return
		}
	}
}
func (s *sessionDiagnostics) emitMaxNsMax(n int64) { atomicMax(&s.emitMaxNs, n) }
func (s *sessionDiagnostics) summary() string {
	age := func(ns int64) int64 {
		if ns == 0 {
			return -1
		}
		return time.Since(time.Unix(0, ns)).Milliseconds()
	}
	return fmt.Sprintf("session_diag elapsed_ms=%d reads=%d bytes=%d writes=%d frame_queue_peak=%d frame_age_max_ms=%d emit_max_ms=%d control_gate_max_ms=%d last_read_age_ms=%d last_write_age_ms=%d tests_received=%d tests_sent=%d test_reply_max_ms=%d", time.Since(s.started).Milliseconds(), s.reads.Load(), s.bytes.Load(), s.writes.Load(), s.queuePeak.Load(), s.frameAgeMaxNs.Load()/int64(time.Millisecond), s.emitMaxNs.Load()/int64(time.Millisecond), s.gateMaxNs.Load()/int64(time.Millisecond), age(s.lastReadNs.Load()), age(s.lastWriteNs.Load()), s.testsReceived.Load(), s.testsSent.Load(), s.testDelayMaxNs.Load()/int64(time.Millisecond))
}
