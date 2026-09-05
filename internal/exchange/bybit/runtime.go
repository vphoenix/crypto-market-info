package bybit

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"github.com/vphoenix/crypto-market-info/internal/exchange"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/orderbook"
)

const defaultWSEndpoint = "wss://stream.bybit.com/v5/public/linear"

type Runtime struct {
	Instrument       model.Instrument
	Book             *orderbook.Book
	WSEndpoint       string
	Dialer           *websocket.Dialer
	QueueCapacity    int
	PreAckCapacity   int
	PingInterval     time.Duration
	SubscribeTimeout time.Duration
	SilenceTimeout   time.Duration
	ReconnectBase    time.Duration
	ReconnectMax     time.Duration
	ConnectGate      exchange.WaitGate
	ReconnectJitter  func(time.Duration) time.Duration
	Logger           *slog.Logger
}

type bookStreamEvent struct {
	subscribed bool
	pong       bool
	update     *DepthUpdate
}

type websocketControl struct {
	Success *bool  `json:"success"`
	RetMsg  string `json:"ret_msg"`
	ReqID   string `json:"req_id"`
	Op      string `json:"op"`
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
		r.Logger.Error("Bybit book connection invalidated", "instrument_id", r.Instrument.ID, "symbol", r.Instrument.ExchangeSymbol, "error", err)
		if !exchange.Wait(ctx, r.ReconnectJitter(delay)) {
			return nil
		}
		delay *= 2
		if delay > r.ReconnectMax {
			delay = r.ReconnectMax
		}
	}
	return nil
}

func (r *Runtime) defaults() error {
	if r.Book == nil || r.Instrument.ID == 0 || r.Instrument.Exchange != "Bybit" || r.Instrument.MarketType != model.MarketPerpetual {
		return fmt.Errorf("Bybit runtime requires a registered Bybit perpetual instrument and book")
	}
	if r.WSEndpoint == "" {
		r.WSEndpoint = defaultWSEndpoint
	}
	if r.Dialer == nil {
		r.Dialer = websocket.DefaultDialer
	}
	if r.QueueCapacity <= 0 {
		r.QueueCapacity = 4096
	}
	if r.PreAckCapacity <= 0 {
		r.PreAckCapacity = r.QueueCapacity
	}
	if r.PingInterval <= 0 {
		r.PingInterval = 20 * time.Second
	}
	if r.SubscribeTimeout <= 0 {
		r.SubscribeTimeout = 10 * time.Second
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
	if r.ConnectGate == nil {
		r.ConnectGate = exchange.NewRequestGate(time.Second)
	}
	if r.ReconnectJitter == nil {
		r.ReconnectJitter = exchange.AddJitter
	}
	if r.Logger == nil {
		r.Logger = slog.Default()
	}
	return nil
}

func (r *Runtime) runConnection(ctx context.Context) error {
	r.Book.MarkResyncing("connecting Bybit websocket")
	if err := r.ConnectGate.Wait(ctx); err != nil {
		return err
	}
	conn, response, err := r.Dialer.DialContext(ctx, r.WSEndpoint, http.Header{})
	if err != nil {
		if response != nil {
			return fmt.Errorf("Bybit websocket handshake %s: %w", response.Status, err)
		}
		return err
	}
	defer conn.Close()
	reqID := fmt.Sprintf("book%d", r.Instrument.ID)
	topic := fmt.Sprintf("orderbook.%d.%s", orderBookDepth, r.Instrument.ExchangeSymbol)
	if err = conn.WriteJSON(map[string]any{"req_id": reqID, "op": "subscribe", "args": []string{topic}}); err != nil {
		return err
	}
	events := make(chan bookStreamEvent, r.QueueCapacity)
	readerErrors := make(chan error, 1)
	terminal := &connectionTerminal{}
	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go r.readLoop(readCtx, conn, reqID, events, readerErrors, terminal)
	collector, _ := NewCollector(r.Book)
	ticker := time.NewTicker(r.PingInterval)
	defer ticker.Stop()
	subscriptionTimer := time.NewTimer(r.SubscribeTimeout)
	defer subscriptionTimer.Stop()
	subscriptionTimeout := subscriptionTimer.C
	subscribed := false
	preAck := make([]DepthUpdate, 0, min(r.PreAckCapacity, 64))
	fail := func(failure error) error {
		return terminal.fail(failure, func(err error) { r.Book.MarkInvalid(err.Error()) })
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case err = <-readerErrors:
			if terminalErr := terminal.current(); terminalErr != nil {
				return terminalErr
			}
			return terminal.fail(err, func(failure error) { r.Book.MarkInvalid(failure.Error()) })
		case event := <-events:
			err = terminal.withActive(func() error {
				if event.subscribed {
					if subscribed {
						return fmt.Errorf("Bybit websocket duplicate subscription acknowledgement")
					}
					subscribed = true
					subscriptionTimer.Stop()
					subscriptionTimeout = nil
					for index := range preAck {
						if applyErr := collector.Push(preAck[index]); applyErr != nil {
							return applyErr
						}
					}
					preAck = nil
					return nil
				}
				if event.pong {
					return nil
				}
				if event.update == nil {
					return fmt.Errorf("Bybit websocket delivered an empty event")
				}
				if !subscribed {
					if len(preAck) >= r.PreAckCapacity {
						return fmt.Errorf("Bybit orderbook pre-ack buffer overflow")
					}
					preAck = append(preAck, *event.update)
					return nil
				}
				return collector.Push(*event.update)
			})
			if err != nil {
				return fail(err)
			}
		case <-ticker.C:
			if err = conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
				return err
			}
			if err = conn.WriteJSON(map[string]string{"op": "ping"}); err != nil {
				return err
			}
		case <-subscriptionTimeout:
			return fail(fmt.Errorf("Bybit websocket subscription acknowledgement timed out"))
		}
	}
}

func (r *Runtime) readLoop(ctx context.Context, conn *websocket.Conn, reqID string, events chan<- bookStreamEvent, errors chan<- error, terminal *connectionTerminal) {
	fail := func(err error) {
		err = terminal.fail(err, func(failure error) { r.Book.MarkInvalid(failure.Error()) })
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
		var identity struct {
			Topic string `json:"topic"`
			Op    string `json:"op"`
		}
		if err = json.Unmarshal(payload, &identity); err != nil {
			fail(fmt.Errorf("Bybit websocket JSON: %w", err))
			return
		}
		event := bookStreamEvent{}
		if identity.Topic != "" {
			update, parseErr := ParseDepth(payload, r.Instrument)
			if parseErr != nil {
				fail(parseErr)
				return
			}
			event.update = &update
		} else {
			control, parseErr := parseWebsocketControl(payload, reqID)
			if parseErr != nil {
				fail(parseErr)
				return
			}
			event.subscribed = control == "subscribe"
			event.pong = control == "pong"
		}
		_ = conn.SetReadDeadline(time.Now().Add(r.SilenceTimeout))
		select {
		case events <- event:
		case <-ctx.Done():
			return
		default:
			fail(fmt.Errorf("Bybit update queue overflow"))
			return
		}
	}
}

func parseWebsocketControl(payload []byte, expectedReqID string) (string, error) {
	var control websocketControl
	if err := json.Unmarshal(payload, &control); err != nil {
		return "", fmt.Errorf("Bybit websocket control JSON: %w", err)
	}
	if control.Success == nil || !*control.Success {
		return "", fmt.Errorf("Bybit websocket control op=%q failed: %s", control.Op, control.RetMsg)
	}
	switch control.Op {
	case "subscribe":
		if control.ReqID != expectedReqID {
			return "", fmt.Errorf("Bybit websocket subscription req_id=%q, want %q", control.ReqID, expectedReqID)
		}
		return "subscribe", nil
	case "ping":
		if control.RetMsg != "pong" {
			return "", fmt.Errorf("Bybit websocket ping response ret_msg=%q", control.RetMsg)
		}
		return "pong", nil
	default:
		return "", fmt.Errorf("Bybit websocket unsupported control op=%q", control.Op)
	}
}
