package deribit

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/vphoenix/crypto-market-info/internal/options"
)

type StreamEvent struct {
	ReceivedAt    time.Time
	Kind, Channel string
	Epoch         uuid.UUID
	Raw           []byte
	Sequence      uint64
	Generation    uint64
}

// Stream runs one connection generation. Its owner retries with backoff.
func (c *Client) Stream(ctx context.Context, channels []string, reset <-chan struct{}, emit func(StreamEvent) error) error {
	if len(channels) == 0 || len(channels) > 32 {
		return fmt.Errorf("invalid subscription count")
	}
	if err := c.ControlGate.Wait(ctx); err != nil {
		return err
	}
	dialer := *websocket.DefaultDialer
	dialer.HandshakeTimeout = 15 * time.Second
	conn, _, err := dialer.DialContext(ctx, c.WSURL, nil)
	if err != nil {
		return err
	}
	defer conn.Close()
	conn.SetReadLimit(8 << 20)
	epoch := uuid.New()
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			conn.Close()
		case <-reset:
			conn.Close()
		case <-done:
		}
	}()
	if err = emit(StreamEvent{Kind: "connected", Epoch: epoch}); err != nil {
		return err
	}
	defer func() { _ = emit(StreamEvent{Kind: "disconnected", Epoch: epoch}) }()
	send := func(id int, method string, params any) error {
		if err := c.ControlGate.Wait(ctx); err != nil {
			return err
		}
		if err := conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
			return err
		}
		return conn.WriteJSON(struct {
			JSONRPC string `json:"jsonrpc"`
			ID      int    `json:"id"`
			Method  string `json:"method"`
			Params  any    `json:"params"`
		}{"2.0", id, method, params})
	}
	if err = send(1, "public/set_heartbeat", struct {
		Interval int `json:"interval"`
	}{10}); err != nil {
		return err
	}
	if err = send(2, "public/subscribe", struct {
		Channels []string `json:"channels"`
	}{channels}); err != nil {
		return err
	}
	heartReady, subReady, ready := false, false, false
	want := append([]string(nil), channels...)
	slices.Sort(want)
	started := time.Now()
	for {
		deadline := time.Now().Add(30 * time.Second)
		if !ready {
			deadline = started.Add(15 * time.Second)
		}
		if err = conn.SetReadDeadline(deadline); err != nil {
			return err
		}
		_, raw, readErr := conn.ReadMessage()
		if readErr != nil {
			return readErr
		}
		var f struct {
			JSONRPC string          `json:"jsonrpc"`
			ID      *int            `json:"id"`
			Method  string          `json:"method"`
			Result  json.RawMessage `json:"result"`
			Error   *struct {
				Code int `json:"code"`
			} `json:"error"`
			Params struct {
				Channel string          `json:"channel"`
				Type    string          `json:"type"`
				Data    json.RawMessage `json:"data"`
			} `json:"params"`
		}
		if err = decode(raw, &f); err != nil {
			return err
		}
		if f.JSONRPC != "2.0" {
			return fmt.Errorf("invalid RPC version")
		}
		if f.Error != nil {
			if f.Error.Code == 10028 {
				c.cooldown(time.Minute)
			}
			return fmt.Errorf("Deribit RPC error %d", f.Error.Code)
		}
		kind := "confirm"
		switch {
		case f.ID != nil:
			switch *f.ID {
			case 1:
				var result string
				if err = json.Unmarshal(f.Result, &result); err != nil || result != "ok" {
					return fmt.Errorf("heartbeat ACK rejected")
				}
				heartReady = true
			case 2:
				var got []string
				if err = json.Unmarshal(f.Result, &got); err != nil {
					return err
				}
				slices.Sort(got)
				if !slices.Equal(got, want) {
					return fmt.Errorf("incomplete subscription ACK")
				}
				subReady = true
			case 3:
				if len(f.Result) == 0 || string(f.Result) == "null" {
					return fmt.Errorf("invalid test response")
				}
			default:
				return fmt.Errorf("unexpected RPC response")
			}
		case f.Method == "heartbeat":
			if f.Params.Type != "heartbeat" && f.Params.Type != "test_request" {
				return fmt.Errorf("unknown heartbeat")
			}
		case f.Method == "subscription":
			if !slices.Contains(channels, f.Params.Channel) {
				return fmt.Errorf("unrequested channel")
			}
			kind = "message"
		default:
			return fmt.Errorf("unknown public message")
		}
		if err = emit(StreamEvent{Kind: kind, Channel: f.Params.Channel, Epoch: epoch, Raw: raw}); err != nil {
			return err
		}
		if !ready && heartReady && subReady {
			ready = true
			if err = emit(StreamEvent{Kind: "ready", Epoch: epoch}); err != nil {
				return err
			}
		}
		if f.Method == "heartbeat" && f.Params.Type == "test_request" {
			if err = send(3, "public/test", struct{}{}); err != nil {
				return err
			}
		}
	}
}

func DecodeIndex(raw []byte, index string, epoch uuid.UUID, received time.Time) (options.IndexSample, error) {
	var e struct {
		JSONRPC string `json:"jsonrpc"`
		Method  string `json:"method"`
		Params  struct {
			Channel string `json:"channel"`
			Data    struct {
				Index     string          `json:"index_name"`
				Price     json.RawMessage `json:"price"`
				Timestamp *int64          `json:"timestamp"`
			} `json:"data"`
		} `json:"params"`
	}
	if err := decode(raw, &e); err != nil {
		return options.IndexSample{}, err
	}
	d := e.Params.Data
	if !options.ValidIndex(index) || e.JSONRPC != "2.0" || e.Method != "subscription" || e.Params.Channel != "deribit_price_index."+index || d.Index != index || d.Timestamp == nil || *d.Timestamp <= 0 || epoch == uuid.Nil || received.IsZero() {
		return options.IndexSample{}, fmt.Errorf("invalid index envelope")
	}
	p, err := options.ParseNumber(string(d.Price))
	if err != nil || !p.IsPositive() {
		return options.IndexSample{}, fmt.Errorf("invalid index price")
	}
	return options.IndexSample{Price: &p, SourceTime: time.UnixMilli(*d.Timestamp).UTC(), ReceivedAt: received, Epoch: epoch, State: options.IndexObserved}, nil
}
