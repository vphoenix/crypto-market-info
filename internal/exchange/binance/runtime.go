package binance

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/orderbook"
)

type Runtime struct {
	Instrument     model.Instrument
	Book           *orderbook.Book
	Client         *Client
	WSEndpoint     string
	Dialer         *websocket.Dialer
	QueueCapacity  int
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
		r.Logger.Error("Binance book connection invalidated", "instrument_id", r.Instrument.ID, "symbol", r.Instrument.ExchangeSymbol, "error", err)
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
		return fmt.Errorf("Binance runtime requires instrument and book")
	}
	if r.Client == nil {
		r.Client = NewClient()
	}
	if r.Dialer == nil {
		r.Dialer = websocket.DefaultDialer
	}
	if r.QueueCapacity <= 0 {
		r.QueueCapacity = 4096
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
	if r.WSEndpoint == "" {
		if r.Instrument.MarketType == model.MarketPerpetual {
			// Binance routes high-frequency USDⓈ-M depth streams through /public.
			r.WSEndpoint = "wss://fstream.binance.com/public/ws"
		} else {
			r.WSEndpoint = "wss://stream.binance.com:443/ws"
		}
	}
	return nil
}

func (r *Runtime) runConnection(ctx context.Context) error {
	r.Book.MarkResyncing("connecting Binance websocket")
	stream := strings.ToLower(r.Instrument.ExchangeSymbol) + "@depth@100ms"
	endpoint := strings.TrimRight(r.WSEndpoint, "/") + "/" + stream
	conn, response, err := r.Dialer.DialContext(ctx, endpoint, http.Header{})
	if err != nil {
		if response != nil {
			return fmt.Errorf("Binance websocket handshake %s: %w", response.Status, err)
		}
		return err
	}
	defer conn.Close()
	updates := make(chan DepthUpdate, r.QueueCapacity)
	readerErrors := make(chan error, 1)
	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go r.readLoop(readCtx, conn, updates, readerErrors)
	snapshot, err := r.Client.DepthSnapshot(ctx, r.Instrument)
	if err != nil {
		return fmt.Errorf("Binance depth snapshot: %w", err)
	}
	collector, _ := NewCollector(r.Book, r.Instrument.MarketType == model.MarketPerpetual)
	if err = collector.ApplySnapshot(snapshot); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case err = <-readerErrors:
			if err == nil {
				err = fmt.Errorf("Binance websocket reader stopped")
			}
			return err
		case update := <-updates:
			if err = collector.Push(update); err != nil {
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
	conn.SetPongHandler(func(string) error { return conn.SetReadDeadline(time.Now().Add(r.SilenceTimeout)) })
	for {
		_, payload, err := conn.ReadMessage()
		if err != nil {
			if ctx.Err() == nil {
				fail(err)
			}
			return
		}
		_ = conn.SetReadDeadline(time.Now().Add(r.SilenceTimeout))
		update, err := ParseDepthUpdate(payload, r.Instrument, r.Instrument.MarketType == model.MarketPerpetual, time.Now().UTC())
		if err != nil {
			fail(err)
			return
		}
		select {
		case updates <- update:
		case <-ctx.Done():
			return
		default:
			fail(fmt.Errorf("Binance update queue overflow"))
			return
		}
	}
}
