package binance

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/vphoenix/crypto-market-info/internal/model"
)

func TestFundingBatchesAndPreACKBarrier(t *testing.T) {
	instruments := make([]model.Instrument, 201)
	for i := range instruments {
		instruments[i] = testInstrument()
		instruments[i].ID = uint32(i + 1)
		instruments[i].ExchangeSymbol = fmt.Sprintf("ASSET%04dUSDT", i)
		instruments[i].BaseAsset = fmt.Sprintf("ASSET%04d", i)
	}
	preACKSent := make(chan struct{})
	allowACK := make(chan struct{})
	counts := make(chan int, 2)
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		var previous int64
		for batch := 0; batch < 2; batch++ {
			var request struct {
				ID     int64    `json:"id"`
				Params []string `json:"params"`
			}
			if conn.ReadJSON(&request) != nil {
				return
			}
			counts <- len(request.Params)
			if request.ID == previous {
				return
			}
			previous = request.ID
			symbol := "ASSET0000USDT"
			if batch == 1 {
				symbol = "ASSET0200USDT"
			}
			if conn.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf(`{"e":"markPriceUpdate","E":1787097599000,"s":%q,"r":"0.1","T":1787097600123}`, symbol))) != nil {
				return
			}
			if batch == 0 {
				close(preACKSent)
				<-allowACK
			}
			if conn.WriteJSON(map[string]any{"result": nil, "id": request.ID}) != nil {
				return
			}
		}
		for {
			if _, _, err = conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()
	estimates := estimateCapture{values: make(chan model.FundingEstimate, 2)}
	confirmations := confirmationCapture{values: make(chan struct {
		instrument model.Instrument
		target     time.Time
	}, 2)}
	r := FundingRuntime{Instruments: instruments, Estimates: estimates, Confirmations: confirmations, WSEndpoint: "ws" + strings.TrimPrefix(server.URL, "http"), ControlInterval: time.Millisecond}
	if err := r.Validate(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.runConnection(ctx) }()
	<-preACKSent
	select {
	case <-estimates.values:
		t.Fatal("funding published before batch ACK")
	case <-time.After(20 * time.Millisecond):
	}
	close(allowACK)
	// 201 long symbols exceed the byte cap before the 200-topic count cap.
	for _, want := range []int{135, 66} {
		select {
		case got := <-counts:
			if got != want {
				t.Fatalf("batch got %d want %d", got, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("batch not sent")
		}
	}
	for i := 0; i < 2; i++ {
		select {
		case <-estimates.values:
		case <-time.After(2 * time.Second):
			t.Fatal("pre-ACK estimate not replayed")
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestControlWriterPongPriorityAndPacing(t *testing.T) {
	type sent struct {
		kind string
		when time.Time
	}
	observed := make(chan sent, 3)
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		conn.SetPongHandler(func(string) error { observed <- sent{"pong", time.Now()}; return nil })
		for {
			_, _, err = conn.ReadMessage()
			if err != nil {
				return
			}
			observed <- sent{"json", time.Now()}
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
	writer := newSocketWriter(ctx, conn, 50*time.Millisecond)
	go writer.run()
	if err = writer.json(map[string]int{"id": 1}); err != nil {
		t.Fatal(err)
	}
	first := <-observed
	done := make(chan error, 1)
	go func() { done <- writer.json(map[string]int{"id": 2}) }()
	time.Sleep(5 * time.Millisecond)
	if err = writer.pong("heartbeat"); err != nil {
		t.Fatal(err)
	}
	var events [2]sent
	for i := range events {
		select {
		case events[i] = <-observed:
		case <-time.After(time.Second):
			t.Fatal("control writer stalled")
		}
	}
	if events[0].kind != "pong" || events[1].kind != "json" {
		t.Fatalf("heartbeat did not get priority: %+v", events)
	}
	if events[0].when.Sub(first.when) < 45*time.Millisecond || events[1].when.Sub(events[0].when) < 45*time.Millisecond {
		t.Fatal("control/pong pacing violated")
	}
	if err = <-done; err != nil {
		t.Fatal(err)
	}
}

func TestWebsocketClientGateSharedAcrossCallers(t *testing.T) {
	client := NewClient()
	client.ConnectionInterval = 60 * time.Millisecond
	if client.WebsocketConnectGate() != client.WebsocketConnectGate() {
		t.Fatal("client gate not shared")
	}
	done := make(chan time.Time, 2)
	for range 2 {
		go func() {
			if err := client.waitWebsocket(context.Background()); err != nil {
				panic(err)
			}
			done <- time.Now()
		}()
	}
	first, second := <-done, <-done
	if second.Sub(first) < 55*time.Millisecond {
		t.Fatal("shared client dial interval violated")
	}
}

func TestBookSubscriptionTimeoutDoesNotRequestSnapshot(t *testing.T) {
	m := testManager(t, 1, 200)
	m.SubscriptionTimeout = 20 * time.Millisecond
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			if _, _, err = conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()
	m.WSEndpoint = "ws" + strings.TrimPrefix(server.URL, "http")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := m.runShardConnection(ctx, m.shards[0])
	if err == nil || !strings.Contains(err.Error(), "ACK timeout") {
		t.Fatalf("err=%v", err)
	}
	if m.targets[0].attempted || m.HealthSnapshot().ReadyInstruments != 0 {
		t.Fatal("target became ready before ACK")
	}
}

func TestConnectionFenceRejectsQueuedACKAndFundingAfterFailure(t *testing.T) {
	m := testManager(t, 1, 200)
	fence := &connectionFence{}
	fence.fail(fmt.Errorf("reader overflow"), func() { m.invalidateShard(m.targets, "reader overflow") })
	if err := fence.apply(func() error { return m.acknowledgeShard(m.targets, []byte(`{"result":null,"id":1}`), 1) }); err == nil {
		t.Fatal("queued ACK revived failed connection")
	}
	if m.targets[0].stage != bookOffline || m.nextSnapshot(time.Now()) != nil {
		t.Fatal("snapshot scheduler revived failed target")
	}
	put := false
	if err := fence.apply(func() error { put = true; return nil }); err == nil || put {
		t.Fatal("queued funding publication revived failed connection")
	}
}
