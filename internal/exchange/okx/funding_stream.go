package okx

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/vphoenix/crypto-market-info/internal/exchange"
	"github.com/vphoenix/crypto-market-info/internal/exchange/wsstream"
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
		if !exists || row.InstID != envelope.Arg.InstID || instrument.MarketType != model.MarketPerpetual || (row.InstType != "" && row.InstType != "SWAP") {
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
	Instruments                       []model.Instrument
	Estimates                         FundingEstimateSink
	Confirmations                     FundingConfirmationScheduler
	WSEndpoint                        string
	Dialer                            *websocket.Dialer
	PingInterval                      time.Duration
	SilenceTimeout                    time.Duration
	ReconnectBase                     time.Duration
	ReconnectMax                      time.Duration
	ConnectGate                       exchange.WaitGate
	ReconnectJitter                   func(time.Duration) time.Duration
	Logger                            *slog.Logger
	QueueCapacity, PreAckCapacity     int
	SubscribeTimeout, ControlInterval time.Duration
	stats                             wsstream.Statistics
	healthMu                          sync.Mutex
	available                         map[uint32]time.Time
}

// Validate performs local dependency validation without starting any workers.
func (r *FundingRuntime) Validate() error {
	if err := r.defaults(); err != nil {
		return err
	}
	return validateWSEndpoint(r.WSEndpoint)
}

func (r *FundingRuntime) HealthSnapshot() BookHealthSnapshot {
	r.healthMu.Lock()
	defer r.healthMu.Unlock()
	h := BookHealthSnapshot{ExpectedInstruments: len(r.Instruments), ReadyInstruments: len(r.available), InvalidInstruments: len(r.Instruments) - len(r.available), ConnectedShards: int(r.stats.Connected.Load()), Reconnects: r.stats.Reconnects.Load(), QueuePeak: r.stats.QueuePeak.Load(), QueueOverflows: r.stats.QueueOverflows.Load()}
	h.StaleTargets = r.stats.StaleTargets.Load()
	for _, ts := range r.available {
		if h.OldestSourceTime.IsZero() || ts.Before(h.OldestSourceTime) {
			h.OldestSourceTime = ts
		}
	}
	return h
}

func (r *FundingRuntime) Run(ctx context.Context) error {
	if err := r.Validate(); err != nil {
		return err
	}
	delay := r.ReconnectBase
	for ctx.Err() == nil {
		err := r.runConnection(ctx)
		if ctx.Err() != nil {
			return nil
		}
		r.Estimates.MarkUnavailable(r.instrumentIDs())
		r.stats.Reconnects.Add(1)
		r.Logger.Error("OKX funding websocket disconnected", "error", err)
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
	seenIDs := make(map[uint32]bool, len(r.Instruments))
	for _, instrument := range r.Instruments {
		if err := instrument.Validate(); err != nil || instrument.Exchange != "OKX" || instrument.MarketType != model.MarketPerpetual {
			return fmt.Errorf("OKX funding runtime received an invalid instrument")
		}
		if _, exists := seen[instrument.ExchangeSymbol]; exists || seenIDs[instrument.ID] {
			return fmt.Errorf("OKX funding runtime has duplicate symbol %q", instrument.ExchangeSymbol)
		}
		seen[instrument.ExchangeSymbol] = struct{}{}
		seenIDs[instrument.ID] = true
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
	if r.PreAckCapacity <= 0 {
		r.PreAckCapacity = r.QueueCapacity
	}
	if r.SubscribeTimeout <= 0 {
		r.SubscribeTimeout = 10 * time.Second
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
	if r.ConnectGate == nil {
		r.ConnectGate = exchange.NewRequestGate(500 * time.Millisecond)
	}
	if r.ReconnectJitter == nil {
		r.ReconnectJitter = exchange.AddJitter
	}
	if r.Logger == nil {
		r.Logger = slog.Default()
	}
	return nil
}

func (r *FundingRuntime) runConnection(ctx context.Context) error {
	instruments := make(map[string]model.Instrument, len(r.Instruments))
	for _, instrument := range r.Instruments {
		instruments[instrument.ExchangeSymbol] = instrument
	}
	targets := make([]*wsstream.Target, 0, len(r.Instruments))
	for _, instrument := range r.Instruments {
		unavailable := func(string) {
			r.Estimates.MarkUnavailable([]uint32{instrument.ID})
			r.healthMu.Lock()
			delete(r.available, instrument.ID)
			r.healthMu.Unlock()
		}
		target := &wsstream.Target{Topic: instrument.ExchangeSymbol, Reset: unavailable, Invalidate: unavailable}
		target.Apply = func(payload []byte, _ bool) (bool, error) {
			estimates, matched, err := ParseFundingUpdates(payload, instruments)
			if err != nil {
				return false, err
			}
			for i, estimate := range estimates {
				if err := r.Estimates.Put(estimate); err != nil {
					return false, err
				}
				if err := r.Confirmations.Schedule(ctx, matched[i], estimate.FundingTime); err != nil {
					return false, err
				}
				r.healthMu.Lock()
				if r.available == nil {
					r.available = make(map[uint32]time.Time)
				}
				r.available[estimate.InstrumentID] = estimate.SourceTime
				r.healthMu.Unlock()
			}
			return true, nil
		}
		targets = append(targets, target)
	}
	return wsstream.RunConnection(ctx, wsstream.Config{Endpoint: r.WSEndpoint, Dialer: r.Dialer, ConnectGate: r.ConnectGate, Protocol: streamProtocol{channel: "funding-rate"}, Targets: targets, BatchSize: 20, QueueCapacity: r.QueueCapacity, PreAckCapacity: r.PreAckCapacity, AckPerTopic: true, ControlInterval: r.ControlInterval, MaxControlsPerHour: 400, PingInterval: r.PingInterval, SilenceTimeout: r.SilenceTimeout, SubscribeTimeout: r.SubscribeTimeout, Stats: &r.stats,
		StaleTargetOnly: true, OnStale: func(topic string) { r.Logger.Warn("OKX funding topic stale; estimate unavailable", "topic", topic) },
	})
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
