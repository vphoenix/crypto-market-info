package bybit

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/vphoenix/crypto-market-info/internal/exchange"
	"github.com/vphoenix/crypto-market-info/internal/funding"
	"github.com/vphoenix/crypto-market-info/internal/model"
)

func TestFundingQuietTargetExpiresWithoutDisconnectAndRecoversFromRealDelta(t *testing.T) {
	quiet := bybitTestInstrument()
	busy := quiet
	busy.ID++
	busy.ExchangeSymbol = "ETHUSDT"
	busy.BaseAsset = "ETH"
	store := funding.NewEstimateStore()
	var connections, subscriptions, pings atomic.Int64
	recoverQuiet := make(chan struct{}, 1)
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		connections.Add(1)
		var subscribe struct {
			ReqID string `json:"req_id"`
			Op    string `json:"op"`
		}
		if conn.ReadJSON(&subscribe) != nil {
			return
		}
		subscriptions.Add(1)
		_ = conn.WriteJSON(map[string]any{"success": true, "op": "subscribe", "req_id": subscribe.ReqID})
		for _, symbol := range []string{quiet.ExchangeSymbol, busy.ExchangeSymbol} {
			payload := fmt.Sprintf(`{"topic":"tickers.%s","type":"snapshot","ts":1000,"data":{"symbol":%q,"fundingRate":"0.001","nextFundingTime":"1787097600123","fundingIntervalHour":"8"}}`, symbol, symbol)
			if conn.WriteMessage(websocket.TextMessage, []byte(payload)) != nil {
				return
			}
		}
		reads := make(chan string, 16)
		readerDone := make(chan struct{})
		go func() {
			defer close(readerDone)
			for {
				var command struct {
					Op string `json:"op"`
				}
				if conn.ReadJSON(&command) != nil {
					return
				}
				select {
				case reads <- command.Op:
				case <-r.Context().Done():
					return
				}
			}
		}()
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		sourceMS := int64(1000)
		for {
			select {
			case <-readerDone:
				return
			case operation := <-reads:
				if operation == "ping" {
					pings.Add(1)
					if conn.WriteJSON(map[string]any{"success": true, "op": "ping", "ret_msg": "pong"}) != nil {
						return
					}
				} else {
					subscriptions.Add(1)
					t.Errorf("quiet target triggered control %s", operation)
				}
			case <-ticker.C:
				sourceMS++
				payload := fmt.Sprintf(`{"topic":"tickers.%s","type":"delta","ts":%d,"data":{"symbol":%q,"lastPrice":"100"}}`, busy.ExchangeSymbol, sourceMS, busy.ExchangeSymbol)
				if conn.WriteMessage(websocket.TextMessage, []byte(payload)) != nil {
					return
				}
			case <-recoverQuiet:
				payload := fmt.Sprintf(`{"topic":"tickers.%s","type":"delta","ts":2000,"data":{"symbol":%q,"lastPrice":"101"}}`, quiet.ExchangeSymbol, quiet.ExchangeSymbol)
				if conn.WriteMessage(websocket.TextMessage, []byte(payload)) != nil {
					return
				}
			}
		}
	}))
	defer server.Close()
	runtime := FundingRuntime{Instruments: []model.Instrument{quiet, busy}, Estimates: store, Confirmations: &runtimeConfirmationSink{}, WSEndpoint: "ws" + strings.TrimPrefix(server.URL, "http"), ConnectGate: exchange.NewRequestGate(0), ControlInterval: 5 * time.Millisecond, PingInterval: 20 * time.Millisecond, SilenceTimeout: 60 * time.Millisecond}
	if err := runtime.Validate(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runtime.runConnection(ctx) }()
	wait := func(condition func() bool) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for !condition() {
			if time.Now().After(deadline) {
				t.Fatalf("condition timed out, health=%+v", runtime.HealthSnapshot())
			}
			time.Sleep(time.Millisecond)
		}
	}
	wait(func() bool { return runtime.HealthSnapshot().ReadyInstruments == 2 })
	wait(func() bool {
		return runtime.HealthSnapshot().StaleTargets == 1 && runtime.HealthSnapshot().ReadyInstruments == 1
	})
	cutoff := time.UnixMilli(10000)
	if _, ok := store.At(quiet.ID, cutoff, time.Hour); ok {
		t.Fatal("scheduler can still read quiet target after expiry")
	}
	if _, ok := store.At(busy.ID, cutoff, time.Hour); !ok {
		t.Fatal("quiet target invalidated active peer")
	}
	pingCount := pings.Load()
	wait(func() bool { return pings.Load() >= pingCount+2 })
	if _, ok := store.At(quiet.ID, cutoff, time.Hour); ok {
		t.Fatal("pong refreshed unavailable estimate")
	}
	if connections.Load() != 1 || subscriptions.Load() != 1 || runtime.HealthSnapshot().ConnectedShards != 1 {
		t.Fatal("quiet topic disconnected or resubscribed shared connection")
	}
	recoverQuiet <- struct{}{}
	wait(func() bool {
		estimate, ok := store.At(quiet.ID, cutoff, time.Hour)
		return ok && estimate.SourceTime.UnixMilli() == 2000
	})
	estimate, _ := store.At(quiet.ID, cutoff, time.Hour)
	if estimate.Rate.String() != "0.001" || estimate.FundingTime.UnixMilli() != 1787097600123 {
		t.Fatal("real sparse delta lost continuous ticker cache")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, ok := store.At(busy.ID, cutoff, time.Hour); ok {
		t.Fatal("connection shutdown did not invalidate peers")
	}
}
