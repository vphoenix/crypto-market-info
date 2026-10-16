package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/gorilla/websocket"
)

// Tiny, finite public subscription probes. They send no credentials, orders or
// writes to storage; only response shapes, hashes and timing are retained.
func probePublicControls(ctx context.Context) error {
	type probe struct {
		URL     string
		Deribit bool
	}
	results := []map[string]any{}
	for _, p := range []probe{{"wss://ws.okx.com:8443/ws/v5/public", false}, {"wss://ws.okx.com/ws/v5/public", false}, {"wss://www.deribit.com/ws/api/v2", true}} {
		row := map[string]any{"url": p.URL, "started_at": time.Now().UTC(), "credentials_sent": false, "orders_sent": 0}
		d := *websocket.DefaultDialer
		d.HandshakeTimeout = 10 * time.Second
		conn, _, e := d.DialContext(ctx, p.URL, nil)
		if e != nil {
			row["error"] = e.Error()
			results = append(results, row)
			continue
		}
		conn.SetReadLimit(1 << 20)
		steps := []map[string]any{}
		for n := 1; n <= 3; n++ {
			if !p.Deribit && n == 3 {
				break
			}
			op := "subscribe"
			topic := "BTC-USDT"
			id := fmt.Sprint("r", n)
			var command any
			if p.Deribit {
				topic = "deribit_price_index.btc_usd"
				if n >= 2 {
					topic = "book.BTC-PERPETUAL.none.10.100ms"
				}
				if n == 3 {
					op = "unsubscribe"
				}
				command = map[string]any{"jsonrpc": "2.0", "id": n, "method": "public/" + op, "params": map[string]any{"channels": []string{topic}}}
			} else {
				if n == 2 {
					op = "unsubscribe"
				}
				command = map[string]any{"id": id, "op": op, "args": []map[string]string{{"channel": "books", "instId": topic}}}
			}
			started := time.Now()
			step := map[string]any{"operation": op, "topic": topic, "id": n}
			conn.SetWriteDeadline(started.Add(5 * time.Second))
			e = conn.WriteJSON(command)
			if e == nil {
				conn.SetReadDeadline(started.Add(10 * time.Second))
				for {
					_, raw, err := conn.ReadMessage()
					if err != nil {
						e = err
						break
					}
					var wire struct {
						Event, ID, Action string
						JSONRPC           string          `json:"jsonrpc"`
						Result            json.RawMessage `json:"result"`
						Error             json.RawMessage `json:"error"`
					}
					if p.Deribit {
						var r struct {
							ID     int             `json:"id"`
							Result json.RawMessage `json:"result"`
							Error  json.RawMessage `json:"error"`
						}
						if err = json.Unmarshal(raw, &r); err != nil {
							e = err
							break
						}
						if r.ID != n {
							continue
						}
						step["rpc_error"] = string(r.Error)
						step["result_present"] = len(r.Result) > 0
					} else {
						if err = json.Unmarshal(raw, &wire); err != nil {
							continue
						}
						if wire.Event != op {
							continue
						}
						step["response_id"] = wire.ID
						step["echo_matches"] = wire.ID == id
					}
					h := sha256.Sum256(raw)
					step["response_sha256"] = hex.EncodeToString(h[:])
					break
				}
			}
			step["elapsed_ms"] = time.Since(started).Milliseconds()
			if e != nil {
				step["error"] = e.Error()
			}
			steps = append(steps, step)
			if e != nil {
				break
			}
		}
		conn.Close()
		row["steps"] = steps
		results = append(results, row)
	}
	return save(base+"/public-controls-probe.json", results)
}
