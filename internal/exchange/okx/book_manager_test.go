package okx

import (
	"context"
	"fmt"
	"github.com/gorilla/websocket"
	"github.com/vphoenix/crypto-market-info/internal/exchange"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/orderbook"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestBookManagerAppliesTwoRealVenueTopics(t *testing.T) {
	first := okxInstrument()
	second := first
	second.ID = first.ID + 1
	second.ExchangeSymbol = "BTC-USDT"
	second.MarketType = model.MarketSpot
	second.SettleAsset = nil
	second.VenueContractVersion = ""
	second.ContractMultiplier = first.QuantityStepSize
	books := make(map[uint32]*orderbook.Book)
	for _, instrument := range []model.Instrument{first, second} {
		books[instrument.ID], _ = orderbook.New(instrument.ID, 400)
	}
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		var req struct {
			ID   string `json:"id"`
			Op   string `json:"op"`
			Args []struct {
				InstID string `json:"instId"`
			} `json:"args"`
		}
		if err := conn.ReadJSON(&req); err != nil {
			return
		}
		if req.Op != "subscribe" || len(req.Args) != 2 {
			t.Errorf("request=%+v", req)
			return
		}
		for _, arg := range req.Args {
			_ = conn.WriteJSON(map[string]any{"id": req.ID, "event": "subscribe", "arg": map[string]string{"channel": "books", "instId": arg.InstID}})
			_ = conn.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf(`{"arg":{"channel":"books","instId":%q},"action":"snapshot","data":[{"asks":[["101","1","0","1"]],"bids":[["100","2","0","1"]],"ts":"1000","prevSeqId":-1,"seqId":10}]}`, arg.InstID)))
		}
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer server.Close()
	manager, err := NewBookManager(NewClient(), []model.Instrument{second, first}, books, 20, nil)
	if err != nil {
		t.Fatal(err)
	}
	manager.WSEndpoint = "ws" + strings.TrimPrefix(server.URL, "http")
	manager.ConnectGate = exchange.NewRequestGate(0)
	manager.PingInterval = time.Hour
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- manager.Run(ctx) }()
	deadline := time.Now().Add(2 * time.Second)
	for manager.HealthSnapshot().ReadyInstruments != 2 {
		if time.Now().After(deadline) {
			t.Fatalf("health=%+v", manager.HealthSnapshot())
		}
		time.Sleep(time.Millisecond)
	}
	if manager.HealthSnapshot().ConnectedShards != 1 {
		t.Fatal("did not share one connection")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if manager.HealthSnapshot().ReadyInstruments != 0 {
		t.Fatal("cancellation left valid books")
	}
}

func TestBookManagerStableShardsAndConstructorValidation(t *testing.T) {
	for _, count := range []int{0, 1, 20, 21, 40} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			instruments := make([]model.Instrument, 0, count)
			books := make(map[uint32]*orderbook.Book)
			for i := count - 1; i >= 0; i-- {
				instrument := okxInstrument()
				instrument.ID = uint32(i + 1)
				instrument.ExchangeSymbol = fmt.Sprintf("A%03d-USDT-SWAP", i)
				instruments = append(instruments, instrument)
				books[instrument.ID], _ = orderbook.New(instrument.ID, 400)
			}
			manager, err := NewBookManager(NewClient(), instruments, books, 20, nil)
			if err != nil {
				t.Fatal(err)
			}
			if len(manager.shards) != (count+19)/20 {
				t.Fatalf("shards=%d", len(manager.shards))
			}
			if len(manager.instruments) > 1 && manager.instruments[0].ID != 1 {
				t.Fatal("unstable instrument order")
			}
			health := manager.HealthSnapshot()
			if health.ExpectedInstruments != count || health.ReadyInstruments != 0 || health.ResyncingInstruments != count || health.ConnectedShards != 0 {
				t.Fatalf("constructor health=%+v", health)
			}
			if count > 0 && BookBufferedEventSlots(count, 20) < 2*4096*((count+19)/20) {
				t.Fatal("underestimated event slots")
			}
		})
	}
	instrument := okxInstrument()
	book, _ := orderbook.New(instrument.ID, 400)
	books := map[uint32]*orderbook.Book{instrument.ID: book}
	for _, topics := range []int{0, 21} {
		if _, err := NewBookManager(NewClient(), []model.Instrument{instrument}, books, topics, nil); err == nil {
			t.Fatal("invalid topic cap accepted")
		}
	}
	if _, err := NewBookManager(NewClient(), []model.Instrument{instrument, instrument}, books, 20, nil); err == nil {
		t.Fatal("duplicate route accepted")
	}
	if _, err := NewBookManager(NewClient(), []model.Instrument{instrument}, nil, 20, nil); err == nil {
		t.Fatal("missing book accepted")
	}
	manager, err := NewBookManager(NewClient(), []model.Instrument{instrument}, books, 20, nil)
	if err != nil {
		t.Fatal(err)
	}
	manager.WSEndpoint = "https://wrong-scheme.invalid"
	if err := manager.Validate(); err == nil {
		t.Fatal("invalid endpoint override accepted")
	}
}

func TestBookManagerControlRejectsFailedOrMismatchedIdentity(t *testing.T) {
	protocol := streamProtocol{channel: "books"}
	for _, payload := range []string{`{"event":"error","code":"60012"}`, `{"event":"subscribe","arg":{"channel":"funding-rate","instId":"BTC-USDT-SWAP"}}`, `{"arg":{"channel":"books"}}`} {
		if _, err := protocol.Decode([]byte(payload)); err == nil {
			t.Fatalf("invalid control/identity accepted: %s", payload)
		}
	}
}
