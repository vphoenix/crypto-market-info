package okx

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
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

type fundingPushEnvelope struct {
	Arg struct {
		Channel string `json:"channel"`
		InstID  string `json:"instId"`
	} `json:"arg"`
	Data []fundingPushWire `json:"data"`
}

type fundingPushWire struct {
	InstID      string `json:"instId"`
	InstType    string `json:"instType"`
	FundingRate string `json:"fundingRate"`
	FundingTime string `json:"fundingTime"`
	Timestamp   string `json:"ts"`
}

func ParseFundingUpdates(payload []byte, instruments map[string]model.Instrument) ([]model.FundingEstimate, []model.Instrument, error) {
	var envelope fundingPushEnvelope
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return nil, nil, fmt.Errorf("OKX funding websocket JSON: %w", err)
	}
	if envelope.Arg.Channel != "funding-rate" || len(envelope.Data) == 0 {
		return nil, nil, fmt.Errorf("OKX funding websocket channel/data is invalid")
	}
	estimates := make([]model.FundingEstimate, 0, len(envelope.Data))
	matched := make([]model.Instrument, 0, len(envelope.Data))
	for _, row := range envelope.Data {
		if row.InstID == "" {
			row.InstID = envelope.Arg.InstID
		}
		instrument, exists := instruments[row.InstID]
		if !exists || instrument.MarketType != model.MarketPerpetual || (row.InstType != "" && row.InstType != "SWAP") {
			return nil, nil, fmt.Errorf("OKX funding websocket has unexpected instrument %q", row.InstID)
		}
		fundingMS, err := strconv.ParseInt(row.FundingTime, 10, 64)
		if err != nil || fundingMS <= 0 {
			return nil, nil, fmt.Errorf("OKX funding websocket fundingTime must be a positive integer string")
		}
		sourceMS, err := strconv.ParseInt(row.Timestamp, 10, 64)
		if err != nil || sourceMS <= 0 {
			return nil, nil, fmt.Errorf("OKX funding websocket ts must be a positive integer string")
		}
		rate, err := model.ParseStrictDecimal(row.FundingRate, "fundingRate")
		if err != nil {
			return nil, nil, err
		}
		estimate := model.FundingEstimate{InstrumentID: instrument.ID, FundingTime: time.UnixMilli(fundingMS).UTC(), Rate: rate, SourceTime: time.UnixMilli(sourceMS).UTC()}
		if err = estimate.Validate(); err != nil {
			return nil, nil, err
		}
		estimates = append(estimates, estimate)
		matched = append(matched, instrument)
	}
	return estimates, matched, nil
}

type FundingRuntime struct {
	Instruments    []model.Instrument
	Estimates      FundingEstimateSink
	Confirmations  FundingConfirmationScheduler
	WSEndpoint     string
	Dialer         *websocket.Dialer
	PingInterval   time.Duration
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
		r.Logger.Error("OKX funding websocket disconnected", "error", err)
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
		return fmt.Errorf("OKX funding runtime requires instruments, estimates and confirmations")
	}
	seen := make(map[string]struct{}, len(r.Instruments))
	for _, instrument := range r.Instruments {
		if err := instrument.Validate(); err != nil || instrument.Exchange != "OKX" || instrument.MarketType != model.MarketPerpetual {
			return fmt.Errorf("OKX funding runtime received an invalid instrument")
		}
		if _, exists := seen[instrument.ExchangeSymbol]; exists {
			return fmt.Errorf("OKX funding runtime has duplicate symbol %q", instrument.ExchangeSymbol)
		}
		seen[instrument.ExchangeSymbol] = struct{}{}
	}
	if r.WSEndpoint == "" {
		r.WSEndpoint = "wss://ws.okx.com:8443/ws/v5/public"
	}
	if r.Dialer == nil {
		r.Dialer = websocket.DefaultDialer
	}
	if r.PingInterval <= 0 {
		r.PingInterval = 20 * time.Second
	}
	if r.SilenceTimeout <= 0 {
		r.SilenceTimeout = 2 * time.Minute
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
			return fmt.Errorf("OKX funding websocket handshake %s: %w", response.Status, err)
		}
		return err
	}
	defer conn.Close()
	instruments := make(map[string]model.Instrument, len(r.Instruments))
	args := make([]map[string]string, 0, len(r.Instruments))
	for _, instrument := range r.Instruments {
		instruments[instrument.ExchangeSymbol] = instrument
		args = append(args, map[string]string{"channel": "funding-rate", "instId": instrument.ExchangeSymbol})
	}
	if err = conn.WriteJSON(map[string]any{"id": "funding", "op": "subscribe", "args": args}); err != nil {
		return err
	}
	_ = conn.SetReadDeadline(time.Now().Add(r.SilenceTimeout))
	ticker := time.NewTicker(r.PingInterval)
	defer ticker.Stop()
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
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err = conn.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
				return err
			}
			if err = conn.WriteMessage(websocket.TextMessage, []byte("ping")); err != nil {
				return err
			}
		case result := <-reads:
			if result.err != nil {
				return result.err
			}
			_ = conn.SetReadDeadline(time.Now().Add(r.SilenceTimeout))
			if err = r.handlePayload(ctx, result.payload, instruments); err != nil {
				return err
			}
		}
	}
}

func (r *FundingRuntime) handlePayload(ctx context.Context, payload []byte, instruments map[string]model.Instrument) error {
	if bytes.Equal(bytes.TrimSpace(payload), []byte("pong")) {
		return nil
	}
	var control struct {
		Event string `json:"event"`
		Code  string `json:"code"`
		Msg   string `json:"msg"`
	}
	if json.Unmarshal(payload, &control) == nil && control.Event != "" {
		if control.Event == "subscribe" && (control.Code == "" || control.Code == "0") {
			return nil
		}
		return fmt.Errorf("OKX funding websocket control event=%s code=%s msg=%s", control.Event, control.Code, control.Msg)
	}
	estimates, matched, err := ParseFundingUpdates(payload, instruments)
	if err != nil {
		return err
	}
	for index, estimate := range estimates {
		if err = r.Estimates.Put(estimate); err != nil {
			return err
		}
		if err = r.Confirmations.Schedule(ctx, matched[index], estimate.FundingTime); err != nil {
			return err
		}
	}
	return nil
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
