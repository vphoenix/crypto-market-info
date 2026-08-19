package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/vphoenix/crypto-market-info/internal/model"
)

type FundingEstimateSink interface {
	Put(model.FundingEstimate) error
	MarkUnavailable([]uint32)
}

type FundingConfirmationScheduler interface {
	Schedule(context.Context, model.Instrument, time.Time) error
}

type markPriceFundingWire struct {
	Event       string `json:"e"`
	EventTime   *int64 `json:"E"`
	Symbol      string `json:"s"`
	FundingRate string `json:"r"`
	FundingTime *int64 `json:"T"`
}

func ParseFundingUpdate(payload []byte, instruments map[string]model.Instrument) (model.FundingEstimate, model.Instrument, error) {
	var row markPriceFundingWire
	if err := json.Unmarshal(payload, &row); err != nil {
		return model.FundingEstimate{}, model.Instrument{}, fmt.Errorf("Binance funding websocket JSON: %w", err)
	}
	if row.Event != "markPriceUpdate" || row.EventTime == nil || *row.EventTime <= 0 || row.FundingTime == nil || *row.FundingTime <= 0 {
		return model.FundingEstimate{}, model.Instrument{}, fmt.Errorf("Binance funding websocket required fields are invalid")
	}
	instrument, exists := instruments[row.Symbol]
	if !exists || instrument.MarketType != model.MarketPerpetual {
		return model.FundingEstimate{}, model.Instrument{}, fmt.Errorf("Binance funding websocket has unexpected symbol %q", row.Symbol)
	}
	rate, err := model.ParseStrictDecimal(row.FundingRate, "fundingRate")
	if err != nil {
		return model.FundingEstimate{}, model.Instrument{}, err
	}
	estimate := model.FundingEstimate{
		InstrumentID: instrument.ID,
		FundingTime:  time.UnixMilli(*row.FundingTime).UTC(),
		Rate:         rate,
		SourceTime:   time.UnixMilli(*row.EventTime).UTC(),
	}
	return estimate, instrument, estimate.Validate()
}

type FundingRuntime struct {
	Instruments    []model.Instrument
	Estimates      FundingEstimateSink
	Confirmations  FundingConfirmationScheduler
	WSEndpoint     string
	Dialer         *websocket.Dialer
	SilenceTimeout time.Duration
	ReconnectBase  time.Duration
	ReconnectMax   time.Duration
	Logger         *slog.Logger
}

func (r *FundingRuntime) Run(ctx context.Context) error {
	if err := r.defaults(); err != nil {
		return err
	}
	delay := r.ReconnectBase
	for ctx.Err() == nil {
		err := r.runConnection(ctx)
		if ctx.Err() != nil {
			return nil
		}
		r.Estimates.MarkUnavailable(r.instrumentIDs())
		r.Logger.Error("Binance funding websocket disconnected", "error", err)
		if !waitFunding(ctx, delay) {
			return nil
		}
		delay *= 2
		if delay > r.ReconnectMax {
			delay = r.ReconnectMax
		}
	}
	return nil
}

func (r *FundingRuntime) instrumentIDs() []uint32 {
	ids := make([]uint32, len(r.Instruments))
	for index, instrument := range r.Instruments {
		ids[index] = instrument.ID
	}
	return ids
}

func (r *FundingRuntime) defaults() error {
	if len(r.Instruments) == 0 || r.Estimates == nil || r.Confirmations == nil {
		return fmt.Errorf("Binance funding runtime requires instruments, estimates and confirmations")
	}
	seen := make(map[string]struct{}, len(r.Instruments))
	for _, instrument := range r.Instruments {
		if err := instrument.Validate(); err != nil || instrument.Exchange != "Binance" || instrument.MarketType != model.MarketPerpetual {
			return fmt.Errorf("Binance funding runtime received an invalid instrument")
		}
		if _, exists := seen[instrument.ExchangeSymbol]; exists {
			return fmt.Errorf("Binance funding runtime has duplicate symbol %q", instrument.ExchangeSymbol)
		}
		seen[instrument.ExchangeSymbol] = struct{}{}
	}
	if r.WSEndpoint == "" {
		r.WSEndpoint = "wss://fstream.binance.com/ws"
	}
	if r.Dialer == nil {
		r.Dialer = websocket.DefaultDialer
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

func (r *FundingRuntime) runConnection(ctx context.Context) error {
	conn, response, err := r.Dialer.DialContext(ctx, r.WSEndpoint, http.Header{})
	if err != nil {
		if response != nil {
			return fmt.Errorf("Binance funding websocket handshake %s: %w", response.Status, err)
		}
		return err
	}
	defer conn.Close()
	instruments := make(map[string]model.Instrument, len(r.Instruments))
	streams := make([]string, 0, len(r.Instruments))
	for _, instrument := range r.Instruments {
		instruments[instrument.ExchangeSymbol] = instrument
		streams = append(streams, strings.ToLower(instrument.ExchangeSymbol)+"@markPrice@1s")
	}
	if err = conn.WriteJSON(map[string]any{"method": "SUBSCRIBE", "params": streams, "id": 1}); err != nil {
		return err
	}
	_ = conn.SetReadDeadline(time.Now().Add(r.SilenceTimeout))
	conn.SetPongHandler(func(string) error { return conn.SetReadDeadline(time.Now().Add(r.SilenceTimeout)) })
	type readResult struct {
		payload []byte
		err     error
	}
	reads := make(chan readResult, 1)
	go func() {
		for {
			_, payload, readErr := conn.ReadMessage()
			select {
			case reads <- readResult{payload: payload, err: readErr}:
			case <-ctx.Done():
				return
			}
			if readErr != nil {
				return
			}
		}
	}()
	for {
		var payload []byte
		select {
		case <-ctx.Done():
			return nil
		case result := <-reads:
			if result.err != nil {
				return result.err
			}
			payload = result.payload
		}
		_ = conn.SetReadDeadline(time.Now().Add(r.SilenceTimeout))
		var control map[string]json.RawMessage
		if json.Unmarshal(payload, &control) == nil {
			if result, exists := control["result"]; exists {
				if string(result) == "null" {
					continue
				}
				return fmt.Errorf("Binance funding websocket subscription failed: %s", payload)
			}
			if _, exists := control["code"]; exists {
				return fmt.Errorf("Binance funding websocket control error: %s", payload)
			}
		}
		estimate, instrument, parseErr := ParseFundingUpdate(payload, instruments)
		if parseErr != nil {
			return parseErr
		}
		if err = r.Estimates.Put(estimate); err != nil {
			return err
		}
		if err = r.Confirmations.Schedule(ctx, instrument, estimate.FundingTime); err != nil {
			return err
		}
	}
}

func waitFunding(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
