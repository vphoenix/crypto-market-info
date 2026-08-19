package okx

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/orderbook"
)

type Runtime struct {
	Instrument     model.Instrument
	Book           *orderbook.Book
	WSEndpoint     string
	Dialer         *websocket.Dialer
	QueueCapacity  int
	PingInterval   time.Duration
	SilenceTimeout time.Duration
	ReconnectBase  time.Duration
	ReconnectMax   time.Duration
	Logger         *slog.Logger
}

func (r *Runtime) Run(ctx context.Context) error {
	if err := r.defaults(); err != nil {
		return err
	}
	delay := r.ReconnectBase
	for ctx.Err() == nil {
		err := r.runConnection(ctx)
		if ctx.Err() != nil {
			return nil
		}
		r.Book.MarkInvalid(err.Error())
		r.Logger.Error("OKX book connection invalidated", "instrument_id", r.Instrument.ID, "symbol", r.Instrument.ExchangeSymbol, "error", err)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
		delay *= 2
		if delay > r.ReconnectMax {
			delay = r.ReconnectMax
		}
	}
	return nil
}

func (r *Runtime) defaults() error {
	if r.Book == nil || r.Instrument.ID == 0 {
		return fmt.Errorf("OKX runtime requires instrument and book")
	}
	if r.WSEndpoint == "" {
		r.WSEndpoint = "wss://ws.okx.com:8443/ws/v5/public"
	}
	if r.Dialer == nil {
		r.Dialer = websocket.DefaultDialer
	}
	if r.QueueCapacity <= 0 {
		r.QueueCapacity = 4096
	}
	if r.PingInterval <= 0 {
		r.PingInterval = 20 * time.Second
	}
	if r.SilenceTimeout <= 0 {
		r.SilenceTimeout = 45 * time.Second
	}
	if r.ReconnectBase <= 0 {
		r.ReconnectBase = time.Second
	}
	if r.ReconnectMax <= 0 {
		r.ReconnectMax = 30 * time.Second
	}
	if r.Logger == nil {
		r.Logger = slog.Default()
	}
	return nil
}

func (r *Runtime) runConnection(ctx context.Context) error {
	r.Book.MarkResyncing("connecting OKX websocket")
	conn, response, err := r.Dialer.DialContext(ctx, r.WSEndpoint, http.Header{})
	if err != nil {
		if response != nil {
			return fmt.Errorf("OKX websocket handshake %s: %w", response.Status, err)
		}
		return err
	}
	defer conn.Close()
	subscribe := map[string]any{"id": fmt.Sprintf("book-%d", r.Instrument.ID), "op": "subscribe", "args": []map[string]string{{"channel": "books", "instId": r.Instrument.ExchangeSymbol}}}
	if err = conn.WriteJSON(subscribe); err != nil {
		return err
	}
	updates := make(chan DepthUpdate, r.QueueCapacity)
	readerErrors := make(chan error, 1)
	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go r.readLoop(readCtx, conn, updates, readerErrors)
	collector, _ := NewCollector(r.Book)
	ticker := time.NewTicker(r.PingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case err = <-readerErrors:
			if err == nil {
				err = fmt.Errorf("OKX websocket reader stopped")
			}
			return err
		case update := <-updates:
			if err = collector.Push(update); err != nil {
				return err
			}
		case <-ticker.C:
			if err = conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
				return err
			}
			if err = conn.WriteMessage(websocket.TextMessage, []byte("ping")); err != nil {
				return err
			}
		}
	}
}

func (r *Runtime) readLoop(ctx context.Context, conn *websocket.Conn, updates chan<- DepthUpdate, errors chan<- error) {
	fail := func(err error) {
		select {
		case errors <- err:
		default:
		}
	}
	_ = conn.SetReadDeadline(time.Now().Add(r.SilenceTimeout))
	for {
		_, payload, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() == nil {
				fail(err)
			}
			return
		}
		_ = conn.SetReadDeadline(time.Now().Add(r.SilenceTimeout))
		if bytes.Equal(bytes.TrimSpace(payload), []byte("pong")) {
			continue
		}
		var control struct {
			Event string `json:"event"`
			Code  string `json:"code"`
			Msg   string `json:"msg"`
		}
		if json.Unmarshal(payload, &control) == nil && control.Event != "" {
			if control.Event == "subscribe" && (control.Code == "" || control.Code == "0") {
				continue
			}
			if control.Event != "error" && control.Code == "" {
				fail(fmt.Errorf("OKX websocket unsupported control event=%s", control.Event))
				return
			}
			fail(fmt.Errorf("OKX websocket control event=%s code=%s msg=%s", control.Event, control.Code, control.Msg))
			return
		}
		update, err := ParseDepth(payload, r.Instrument)
		if err != nil {
			fail(err)
			return
		}
		select {
		case updates <- update:
		case <-ctx.Done():
			return
		default:
			fail(fmt.Errorf("OKX update queue overflow"))
			return
		}
	}
}
