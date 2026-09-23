package deribit

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/vphoenix/crypto-market-info/internal/exchange"
)

func TestScopeEvidenceAndFailure(t *testing.T) {
	raw, err := os.ReadFile("testdata/metadata-BTC-option.json")
	if err != nil {
		t.Fatal(err)
	}
	body := raw
	status := http.StatusOK
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(status); _, _ = w.Write(body) }))
	defer server.Close()
	c := NewClient(server.URL, "ws://example.invalid")
	c.RESTGate = exchange.NewRequestGate(0)
	ctx := context.Background()
	result, err := c.FetchScope(ctx, Scope{"BTC", "option"})
	if err != nil {
		t.Fatal(err)
	}
	if result.RequestID == uuid.Nil || result.RawCount != uint32(len(result.Instruments)) || result.AcceptedCount == 0 || result.Status != "complete" || result.PayloadHash == "" {
		t.Fatal("missing scope evidence")
	}
	body = []byte(`{"jsonrpc":"2.0","result":[{}]}`)
	failed, err := c.FetchScope(ctx, Scope{"BTC", "option"})
	if err == nil || failed.Status != "parse_error" || failed.PayloadHash == "" || failed.RawCount != 0 {
		t.Fatal("failed scope fabricated facts")
	}
	status = http.StatusTooManyRequests
	body = []byte(`rate limited`)
	if _, err = c.FetchScope(ctx, Scope{"BTC", "option"}); err == nil {
		t.Fatal("429 accepted")
	}
	wait, cancel := context.WithTimeout(ctx, 5*time.Millisecond)
	defer cancel()
	if err = c.ControlGate.Wait(wait); err == nil {
		t.Fatal("HTTP limit did not cool WS controls")
	}
}

func TestStreamACKAndHeartbeat(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(map[bool]string{false: "complete", true: "partial"}[partial], func(t *testing.T) {
			serverDone := make(chan error, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
				if err != nil {
					serverDone <- err
					return
				}
				defer conn.Close()
				_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
				var request struct {
					ID     int    `json:"id"`
					Method string `json:"method"`
					Params struct {
						Channels []string `json:"channels"`
					} `json:"params"`
				}
				if err = conn.ReadJSON(&request); err != nil {
					serverDone <- err
					return
				}
				if err = conn.WriteJSON(struct {
					JSONRPC string `json:"jsonrpc"`
					ID      int    `json:"id"`
					Result  string `json:"result"`
				}{"2.0", request.ID, "ok"}); err != nil {
					serverDone <- err
					return
				}
				if err = conn.ReadJSON(&request); err != nil {
					serverDone <- err
					return
				}
				channels := request.Params.Channels
				if partial {
					channels = []string{}
				}
				if err = conn.WriteJSON(struct {
					JSONRPC string   `json:"jsonrpc"`
					ID      int      `json:"id"`
					Result  []string `json:"result"`
				}{"2.0", request.ID, channels}); err != nil {
					serverDone <- err
					return
				}
				if partial {
					serverDone <- nil
					return
				}
				if err = conn.WriteMessage(websocket.TextMessage, []byte(`{"jsonrpc":"2.0","method":"heartbeat","params":{"type":"test_request"}}`)); err != nil {
					serverDone <- err
					return
				}
				if err = conn.ReadJSON(&request); err != nil {
					serverDone <- err
					return
				}
				if request.Method != "public/test" {
					serverDone <- context.Canceled
					return
				}
				if err = conn.WriteMessage(websocket.TextMessage, []byte(`{"jsonrpc":"2.0","id":3,"result":{"version":"test-finished"}}`)); err != nil {
					serverDone <- err
					return
				}
				serverDone <- nil
			}))
			defer server.Close()
			c := NewClient(server.URL, "ws"+strings.TrimPrefix(server.URL, "http"))
			c.ControlGate = exchange.NewRequestGate(0)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			ready, testReply, disconnected := false, false, false
			err := c.Stream(ctx, []string{"deribit_price_index.btc_usd"}, make(chan struct{}), func(e StreamEvent) error {
				if e.Kind == "ready" {
					ready = true
				}
				if e.Kind == "disconnected" {
					disconnected = true
				}
				if strings.Contains(string(e.Raw), "test-finished") {
					testReply = true
					cancel()
				}
				return nil
			})
			if err == nil {
				t.Fatal("closed stream returned success")
			}
			if partial && ready {
				t.Fatal("partial ACK admitted")
			}
			if !partial && (!ready || !testReply) {
				t.Fatal("heartbeat/ACK not handled")
			}
			if !disconnected {
				t.Fatal("disconnect not emitted")
			}
			if err = <-serverDone; err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestIndexExactDecimalAndAliases(t *testing.T) {
	raw := []byte(`{"jsonrpc":"2.0","method":"subscription","params":{"channel":"deribit_price_index.btc_usd","data":{"index_name":"btc_usd","price":81000.123456789012345678,"timestamp":1789948800000}}}`)
	s, err := DecodeIndex(raw, "btc_usd", uuid.New(), time.Date(2026, 9, 21, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if s.Price.String() != "81000.123456789012345678" {
		t.Fatal("price precision lost")
	}
	for _, bad := range []string{strings.Replace(string(raw), `"price":`, `"Price":`, 1), strings.Replace(string(raw), "81000.123456789012345678", "null", 1), strings.Replace(string(raw), "81000.123456789012345678", "-1", 1)} {
		if _, err = DecodeIndex(json.RawMessage(bad), "btc_usd", uuid.New(), time.Now()); err == nil {
			t.Fatal("invalid index admitted")
		}
	}
}
