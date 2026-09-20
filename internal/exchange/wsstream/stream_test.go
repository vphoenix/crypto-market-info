package wsstream

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/vphoenix/crypto-market-info/internal/exchange"
)

type testWire struct {
	Topic, Operation, RequestID, Kind string
	Value                             int64
	Topics                            []string
}
type testProtocol struct{}

func (testProtocol) Decode(payload []byte) (Message, error) {
	var wire testWire
	if err := json.Unmarshal(payload, &wire); err != nil {
		return Message{}, err
	}
	if wire.Operation == "pong" {
		return Message{Pong: true}, nil
	}
	if wire.Operation != "" {
		return Message{Topic: wire.Topic, Operation: wire.Operation, RequestID: wire.RequestID, Acknowledgement: true}, nil
	}
	if wire.Topic == "" {
		return Message{}, fmt.Errorf("missing topic")
	}
	return Message{Topic: wire.Topic}, nil
}
func (testProtocol) Subscription(id, op string, topics []string) any {
	return testWire{RequestID: id, Operation: op, Topics: topics}
}
func (testProtocol) Ping() (int, []byte) {
	return websocket.TextMessage, []byte(`{"Operation":"ping"}`)
}

func eventually(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition did not become true")
		}
		time.Sleep(time.Millisecond)
	}
}

func testTarget(topic string, state *atomic.Int64) *Target {
	return &Target{Topic: topic, Reset: func(string) { state.Store(0) }, Invalidate: func(string) { state.Store(0) }, Apply: func(payload []byte, awaiting bool) (bool, error) {
		var wire testWire
		if err := json.Unmarshal(payload, &wire); err != nil {
			return false, err
		}
		if awaiting && wire.Kind != "snapshot" {
			return false, nil
		}
		if wire.Value < 0 {
			return false, fmt.Errorf("sequence gap")
		}
		state.Store(wire.Value)
		return true, nil
	}}
}

func TestSharedShardACKResyncAndTerminalBoundaries(t *testing.T) {
	for _, perTopic := range []bool{false, true} {
		t.Run(fmt.Sprintf("per_topic_%v", perTopic), func(t *testing.T) {
			var first, second atomic.Int64
			upgrader := websocket.Upgrader{}
			serverDone := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := upgrader.Upgrade(w, r, nil)
				if err != nil {
					return
				}
				defer conn.Close()
				defer close(serverDone)
				var request testWire
				if err := conn.ReadJSON(&request); err != nil {
					return
				}
				if request.Operation != "subscribe" || len(request.Topics) != 2 {
					t.Errorf("initial request=%+v", request)
					return
				}
				_ = conn.WriteJSON(testWire{Topic: "a", Kind: "snapshot", Value: 1})
				_ = conn.WriteJSON(testWire{Topic: "b", Kind: "snapshot", Value: 1})
				time.Sleep(10 * time.Millisecond)
				if first.Load() != 0 || second.Load() != 0 {
					t.Error("pre-ack snapshot became valid")
				}
				ack := func(op, id, topic string) { _ = conn.WriteJSON(testWire{Operation: op, RequestID: id, Topic: topic}) }
				if perTopic {
					ack("subscribe", request.RequestID, "a")
					eventually(t, func() bool { return first.Load() == 1 })
					if second.Load() != 0 {
						t.Error("unacknowledged target became valid")
					}
					ack("subscribe", request.RequestID, "b")
				} else {
					ack("subscribe", request.RequestID, "")
				}
				eventually(t, func() bool { return first.Load() == 1 && second.Load() == 1 })
				_ = conn.WriteJSON(testWire{Topic: "a", Kind: "delta", Value: -1})
				if err := conn.ReadJSON(&request); err != nil {
					return
				}
				if request.Operation != "unsubscribe" || len(request.Topics) != 1 || request.Topics[0] != "a" {
					t.Errorf("resync request=%+v", request)
					return
				}
				_ = conn.WriteJSON(testWire{Topic: "a", Kind: "snapshot", Value: 99})
				_ = conn.WriteJSON(testWire{Topic: "b", Kind: "delta", Value: 2})
				eventually(t, func() bool { return second.Load() == 2 })
				if first.Load() != 0 {
					t.Error("old subscription snapshot restored failed target")
				}
				topic := ""
				if perTopic {
					topic = "a"
				}
				ack("unsubscribe", request.RequestID, topic)
				if err := conn.ReadJSON(&request); err != nil {
					return
				}
				if request.Operation != "subscribe" || len(request.Topics) != 1 {
					t.Errorf("resubscribe request=%+v", request)
					return
				}
				_ = conn.WriteJSON(testWire{Topic: "a", Kind: "delta", Value: 99})
				ack("subscribe", request.RequestID, topic)
				_ = conn.WriteJSON(testWire{Topic: "a", Kind: "delta", Value: 99})
				time.Sleep(10 * time.Millisecond)
				if first.Load() != 0 {
					t.Error("delta before new snapshot restored target")
				}
				_ = conn.WriteJSON(testWire{Topic: "a", Kind: "snapshot", Value: 3})
				eventually(t, func() bool { return first.Load() == 3 && second.Load() == 2 })
				_ = conn.WriteJSON(testWire{Topic: "unknown", Kind: "snapshot", Value: 5})
				for {
					if _, _, err := conn.ReadMessage(); err != nil {
						return
					}
				}
			}))
			defer server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			stats := &Statistics{}
			err := RunConnection(ctx, Config{Endpoint: "ws" + strings.TrimPrefix(server.URL, "http"), ConnectGate: exchange.NewRequestGate(0), Protocol: testProtocol{}, Targets: []*Target{testTarget("a", &first), testTarget("b", &second)}, BatchSize: 20, AckPerTopic: perTopic, RequireAckID: true, Resync: true, ControlInterval: time.Millisecond, PingInterval: time.Hour, Stats: stats})
			if err == nil || !strings.Contains(err.Error(), "unexpected topic") {
				t.Fatalf("terminal error=%v", err)
			}
			if first.Load() != 0 || second.Load() != 0 {
				t.Fatal("terminal shard left valid target")
			}
			if stats.Resyncs.Load() != 1 {
				t.Fatalf("resyncs=%d", stats.Resyncs.Load())
			}
			<-serverDone
		})
	}
}

func TestPreAckOverflowDuplicateMissingAndUnexpectedACK(t *testing.T) {
	for _, failure := range []string{"preack", "duplicate", "missing", "wrong_id"} {
		t.Run(failure, func(t *testing.T) {
			var state atomic.Int64
			upgrader := websocket.Upgrader{}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := upgrader.Upgrade(w, r, nil)
				if err != nil {
					return
				}
				defer conn.Close()
				var request testWire
				if conn.ReadJSON(&request) != nil {
					return
				}
				_ = conn.WriteJSON(testWire{Topic: "a", Kind: "snapshot", Value: 1})
				switch failure {
				case "preack":
					_ = conn.WriteJSON(testWire{Topic: "a", Kind: "delta", Value: 2})
				case "duplicate":
					_ = conn.WriteJSON(testWire{Operation: "subscribe", RequestID: request.RequestID})
					_ = conn.WriteJSON(testWire{Operation: "subscribe", RequestID: request.RequestID})
				case "wrong_id":
					_ = conn.WriteJSON(testWire{Operation: "subscribe", RequestID: "wrong"})
				}
				for {
					if _, _, err := conn.ReadMessage(); err != nil {
						return
					}
				}
			}))
			defer server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			err := RunConnection(ctx, Config{Endpoint: "ws" + strings.TrimPrefix(server.URL, "http"), ConnectGate: exchange.NewRequestGate(0), Protocol: testProtocol{}, Targets: []*Target{testTarget("a", &state)}, BatchSize: 20, PreAckCapacity: 1, SubscribeTimeout: 20 * time.Millisecond, Stats: &Statistics{}})
			if err == nil {
				t.Fatal("bad acknowledgement did not terminate shard")
			}
			if state.Load() != 0 {
				t.Fatal("terminal failure left valid state")
			}
		})
	}
}

func TestTerminalFencesQueuedApplication(t *testing.T) {
	var state atomic.Int64
	target := testTarget("a", &state)
	end := &terminal{}
	end.fail(fmt.Errorf("overflow"), []*Target{target})
	if err := end.apply(func() error { state.Store(10); return nil }); err == nil || state.Load() != 0 {
		t.Fatal("terminal allowed stale queued mutation")
	}
}

func TestReaderOverflowInvalidatesBeforeQueuedMutation(t *testing.T) {
	var state atomic.Int64
	entered, release := make(chan struct{}), make(chan struct{})
	target := testTarget("a", &state)
	original := target.Apply
	target.Apply = func(payload []byte, awaiting bool) (bool, error) {
		if awaiting {
			close(entered)
			<-release
		}
		return original(payload, awaiting)
	}
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		var request testWire
		if conn.ReadJSON(&request) != nil {
			return
		}
		_ = conn.WriteJSON(testWire{Operation: "subscribe", RequestID: request.RequestID})
		time.Sleep(10 * time.Millisecond)
		_ = conn.WriteJSON(testWire{Topic: "a", Kind: "snapshot", Value: 1})
		<-entered
		for i := 0; i < 10; i++ {
			_ = conn.WriteJSON(testWire{Topic: "a", Kind: "delta", Value: int64(i + 2)})
		}
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	stats := &Statistics{}
	done := make(chan error, 1)
	go func() {
		done <- RunConnection(ctx, Config{Endpoint: "ws" + strings.TrimPrefix(server.URL, "http"), ConnectGate: exchange.NewRequestGate(0), Protocol: testProtocol{}, Targets: []*Target{target}, BatchSize: 20, QueueCapacity: 4, Stats: stats})
	}()
	eventually(t, func() bool { return stats.QueueOverflows.Load() > 0 })
	close(release)
	if err := <-done; err == nil || !strings.Contains(err.Error(), "queue overflow") {
		t.Fatalf("error=%v", err)
	}
	if state.Load() != 0 {
		t.Fatal("queued event restored terminal target")
	}
}

func TestControlReadyIntervalAndRollingHour(t *testing.T) {
	now := time.Unix(100000, 0)
	if got := controlReady(now, now, nil, 250*time.Millisecond, 400); !got.Equal(now.Add(250 * time.Millisecond)) {
		t.Fatal(got)
	}
	history := make([]time.Time, 400)
	for i := range history {
		history[i] = now.Add(-30*time.Minute + time.Duration(i)*time.Second)
	}
	if got := controlReady(now, now.Add(-time.Second), history, 250*time.Millisecond, 400); !got.Equal(history[0].Add(time.Hour)) {
		t.Fatal(got)
	}
}

func TestWriterReservesHeartbeatWhileHourlyBudgetBlocked(t *testing.T) {
	upgrader := websocket.Upgrader{}
	observed := make(chan testWire, 16)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			var wire testWire
			if conn.ReadJSON(&wire) != nil {
				return
			}
			observed <- wire
		}
	}))
	defer server.Close()
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	commands := make(chan writeCommand, 2)
	commands <- writeCommand{"1", "subscribe", []string{"a"}}
	commands <- writeCommand{"2", "subscribe", []string{"b"}}
	done := make(chan error, 1)
	go func() {
		done <- runWriter(ctx, conn, Config{Protocol: testProtocol{}, ControlInterval: time.Millisecond, MaxControlsPerHour: 1, PingInterval: 20 * time.Millisecond}, commands, make(chan pongCommand), func(streamEvent) bool { return true })
	}()
	for i := 0; i < 3; i++ {
		select {
		case wire := <-observed:
			if (i == 0 && wire.Operation != "subscribe") || (i > 0 && wire.Operation != "ping") {
				t.Fatalf("message %d=%+v", i, wire)
			}
		case <-time.After(time.Second):
			t.Fatal("hour budget starved heartbeat")
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

type recordingFrameWriter struct{ observed chan time.Time }

func (w recordingFrameWriter) SetWriteDeadline(time.Time) error { return nil }
func (w recordingFrameWriter) WriteMessage(int, []byte) error   { w.observed <- time.Now(); return nil }
func (w recordingFrameWriter) WriteJSON(any) error              { w.observed <- time.Now(); return nil }

func TestWriterJSONHeartbeatSharesControlSpacing(t *testing.T) {
	observed := make(chan time.Time, 16)
	writer := recordingFrameWriter{observed: observed}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	commands := make(chan writeCommand, 2)
	commands <- writeCommand{"1", "subscribe", []string{"a"}}
	commands <- writeCommand{"2", "subscribe", []string{"b"}}
	done := make(chan error, 1)
	go func() {
		done <- runWriter(ctx, writer, Config{Protocol: testProtocol{}, ControlInterval: 20 * time.Millisecond, PingInterval: 25 * time.Millisecond}, commands, make(chan pongCommand), func(streamEvent) bool { return true })
	}()
	var last time.Time
	for i := 0; i < 4; i++ {
		select {
		case now := <-observed:
			if !last.IsZero() && now.Sub(last) < 20*time.Millisecond {
				t.Fatalf("writer send interval=%s", now.Sub(last))
			}
			last = now
		case <-time.After(time.Second):
			t.Fatal("writer stalled")
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
