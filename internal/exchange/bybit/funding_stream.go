package bybit

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/exchange"
	"github.com/vphoenix/crypto-market-info/internal/exchange/wsstream"
	"github.com/vphoenix/crypto-market-info/internal/model"
)

const tickerArgsLimit = 21000

type FundingEstimateSink interface {
	Put(model.FundingEstimate) error
	MarkUnavailable([]uint32)
}

type FundingConfirmationScheduler interface {
	Schedule(context.Context, model.Instrument, time.Time) error
}

type TickerUpdate struct {
	Snapshot            bool
	SourceTime          time.Time
	FundingRate         *decimal.Decimal
	NextFundingTime     *time.Time
	FundingIntervalHour *int64
}

type tickerEnvelope struct {
	Topic     string          `json:"topic"`
	Type      string          `json:"type"`
	Timestamp *int64          `json:"ts"`
	Data      json.RawMessage `json:"data"`
}

type tickerWire struct {
	Symbol              string          `json:"symbol"`
	FundingRate         json.RawMessage `json:"fundingRate"`
	NextFundingTime     json.RawMessage `json:"nextFundingTime"`
	FundingIntervalHour json.RawMessage `json:"fundingIntervalHour"`
}

func ParseTickerUpdate(payload []byte, instruments map[string]model.Instrument) (TickerUpdate, model.Instrument, error) {
	var envelope tickerEnvelope
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return TickerUpdate{}, model.Instrument{}, fmt.Errorf("Bybit ticker JSON: %w", err)
	}
	snapshot := envelope.Type == "snapshot"
	if !snapshot && envelope.Type != "delta" {
		return TickerUpdate{}, model.Instrument{}, fmt.Errorf("Bybit ticker type must be snapshot or delta")
	}
	if envelope.Timestamp == nil || *envelope.Timestamp <= 0 {
		return TickerUpdate{}, model.Instrument{}, fmt.Errorf("Bybit ticker ts must be positive milliseconds")
	}
	trimmed := bytes.TrimSpace(envelope.Data)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return TickerUpdate{}, model.Instrument{}, fmt.Errorf("Bybit ticker data is required")
	}
	var wire tickerWire
	if err := json.Unmarshal(trimmed, &wire); err != nil {
		return TickerUpdate{}, model.Instrument{}, fmt.Errorf("Bybit ticker data: %w", err)
	}
	instrument, exists := instruments[wire.Symbol]
	if !exists || instrument.Exchange != "Bybit" || instrument.MarketType != model.MarketPerpetual {
		return TickerUpdate{}, model.Instrument{}, fmt.Errorf("Bybit ticker has unexpected symbol %q", wire.Symbol)
	}
	if envelope.Topic != "tickers."+wire.Symbol {
		return TickerUpdate{}, model.Instrument{}, fmt.Errorf("Bybit ticker topic=%q does not match symbol %q", envelope.Topic, wire.Symbol)
	}
	update := TickerUpdate{Snapshot: snapshot, SourceTime: time.UnixMilli(*envelope.Timestamp).UTC()}
	if value, present, err := optionalString(wire.FundingRate, "fundingRate"); err != nil {
		return TickerUpdate{}, model.Instrument{}, err
	} else if present {
		rate, parseErr := model.ParseStrictDecimal(value, "fundingRate")
		if parseErr != nil {
			return TickerUpdate{}, model.Instrument{}, parseErr
		}
		update.FundingRate = &rate
	}
	if value, present, err := optionalString(wire.NextFundingTime, "nextFundingTime"); err != nil {
		return TickerUpdate{}, model.Instrument{}, err
	} else if present {
		milliseconds, parseErr := strconv.ParseInt(value, 10, 64)
		if parseErr != nil || milliseconds <= 0 {
			return TickerUpdate{}, model.Instrument{}, fmt.Errorf("Bybit ticker nextFundingTime must be a positive integer string")
		}
		fundingTime := time.UnixMilli(milliseconds).UTC()
		update.NextFundingTime = &fundingTime
	}
	if value, present, err := optionalString(wire.FundingIntervalHour, "fundingIntervalHour"); err != nil {
		return TickerUpdate{}, model.Instrument{}, err
	} else if present {
		interval, parseErr := strconv.ParseInt(value, 10, 64)
		if parseErr != nil || interval <= 0 {
			return TickerUpdate{}, model.Instrument{}, fmt.Errorf("Bybit ticker fundingIntervalHour must be a positive integer string")
		}
		update.FundingIntervalHour = &interval
	}
	return update, instrument, nil
}

func optionalString(raw json.RawMessage, field string) (string, bool, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 {
		return "", false, nil
	}
	if bytes.Equal(trimmed, []byte("null")) {
		return "", false, fmt.Errorf("Bybit ticker %s cannot be null", field)
	}
	var value string
	if err := json.Unmarshal(trimmed, &value); err != nil || value == "" || strings.TrimSpace(value) != value {
		return "", false, fmt.Errorf("Bybit ticker %s must be an exact non-empty string", field)
	}
	return value, true, nil
}

type tickerState struct {
	hasSnapshot         bool
	lastSourceTime      time.Time
	rate                *decimal.Decimal
	nextFundingTime     *time.Time
	fundingIntervalHour *int64
}

type tickerCache struct {
	states map[uint32]tickerState
}

func newTickerCache() *tickerCache {
	return &tickerCache{states: make(map[uint32]tickerState)}
}

func (c *tickerCache) Apply(update TickerUpdate, instrument model.Instrument) (model.FundingEstimate, bool, error) {
	state := c.states[instrument.ID]
	if update.Snapshot {
		state = tickerState{hasSnapshot: true}
	} else if !state.hasSnapshot {
		return model.FundingEstimate{}, false, fmt.Errorf("Bybit ticker delta arrived before snapshot for %s", instrument.ExchangeSymbol)
	}
	if !state.lastSourceTime.IsZero() && update.SourceTime.Before(state.lastSourceTime) {
		return model.FundingEstimate{}, false, fmt.Errorf("Bybit ticker ts moved backwards for %s", instrument.ExchangeSymbol)
	}
	state.lastSourceTime = update.SourceTime
	if update.FundingRate != nil {
		value := *update.FundingRate
		state.rate = &value
	}
	if update.NextFundingTime != nil {
		value := *update.NextFundingTime
		state.nextFundingTime = &value
	}
	if update.FundingIntervalHour != nil {
		value := *update.FundingIntervalHour
		state.fundingIntervalHour = &value
	}
	c.states[instrument.ID] = state
	if state.rate == nil || state.nextFundingTime == nil || state.fundingIntervalHour == nil {
		return model.FundingEstimate{}, false, nil
	}
	estimate := model.FundingEstimate{InstrumentID: instrument.ID, FundingTime: *state.nextFundingTime, Rate: *state.rate, SourceTime: update.SourceTime}
	if err := estimate.Validate(); err != nil {
		return model.FundingEstimate{}, false, err
	}
	return estimate, true, nil
}

type FundingRuntime struct {
	Instruments      []model.Instrument
	Estimates        FundingEstimateSink
	Confirmations    FundingConfirmationScheduler
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
	ControlInterval  time.Duration
	stats            wsstream.Statistics
	healthMu         sync.Mutex
	available        map[uint32]time.Time
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
		r.Logger.Error("Bybit funding websocket disconnected", "error", err)
		r.stats.Reconnects.Add(1)
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
		return fmt.Errorf("Bybit funding runtime requires instruments, estimates and confirmations")
	}
	seen := make(map[string]struct{}, len(r.Instruments))
	seenIDs := make(map[uint32]bool, len(r.Instruments))
	var topics []string
	for _, instrument := range r.Instruments {
		if err := instrument.Validate(); err != nil || instrument.Exchange != "Bybit" || instrument.MarketType != model.MarketPerpetual {
			return fmt.Errorf("Bybit funding runtime received an invalid instrument")
		}
		if _, exists := seen[instrument.ExchangeSymbol]; exists || seenIDs[instrument.ID] {
			return fmt.Errorf("Bybit funding runtime has duplicate symbol %q", instrument.ExchangeSymbol)
		}
		seen[instrument.ExchangeSymbol] = struct{}{}
		seenIDs[instrument.ID] = true
		topics = append(topics, "tickers."+instrument.ExchangeSymbol)
	}
	if _, err := tickerSubscriptionBatches(topics, tickerArgsLimit); err != nil {
		return err
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

func (r *FundingRuntime) runConnection(ctx context.Context) error {
	instruments := make(map[string]model.Instrument, len(r.Instruments))
	topics := make([]string, 0, len(r.Instruments))
	for _, instrument := range r.Instruments {
		instruments[instrument.ExchangeSymbol] = instrument
		topics = append(topics, "tickers."+instrument.ExchangeSymbol)
	}
	batches, err := tickerSubscriptionBatches(topics, tickerArgsLimit)
	if err != nil {
		return err
	}
	cache := newTickerCache()
	targets := make([]*wsstream.Target, 0, len(r.Instruments))
	for _, instrument := range r.Instruments {
		unavailable := func(string) {
			r.Estimates.MarkUnavailable([]uint32{instrument.ID})
			r.healthMu.Lock()
			delete(r.available, instrument.ID)
			r.healthMu.Unlock()
		}
		target := &wsstream.Target{Topic: "tickers." + instrument.ExchangeSymbol, Reset: unavailable, Invalidate: unavailable}
		target.Apply = func(payload []byte, _ bool) (bool, error) {
			update, matched, err := ParseTickerUpdate(payload, instruments)
			if err != nil {
				return false, err
			}
			estimate, complete, err := cache.Apply(update, matched)
			if err != nil || !complete {
				return false, err
			}
			if err := r.Estimates.Put(estimate); err != nil {
				return false, err
			}
			if err := r.Confirmations.Schedule(ctx, matched, estimate.FundingTime); err != nil {
				return false, err
			}
			r.healthMu.Lock()
			if r.available == nil {
				r.available = make(map[uint32]time.Time)
			}
			r.available[estimate.InstrumentID] = estimate.SourceTime
			r.healthMu.Unlock()
			return true, nil
		}
		targets = append(targets, target)
	}
	return wsstream.RunConnection(ctx, wsstream.Config{Endpoint: r.WSEndpoint, Dialer: r.Dialer, ConnectGate: r.ConnectGate, Protocol: tickerStreamProtocol{}, Targets: targets, Batches: batches, QueueCapacity: r.QueueCapacity, PreAckCapacity: r.PreAckCapacity, RequireAckID: true, ControlInterval: r.ControlInterval, PingInterval: r.PingInterval, SilenceTimeout: r.SilenceTimeout, SubscribeTimeout: r.SubscribeTimeout, Stats: &r.stats,
		StaleTargetOnly: true, OnStale: func(topic string) { r.Logger.Warn("Bybit funding topic stale; estimate unavailable", "topic", topic) },
	})
}

type tickerStreamProtocol struct{ streamProtocol }

func (p tickerStreamProtocol) Decode(payload []byte) (wsstream.Message, error) {
	var envelope struct {
		Topic string `json:"topic"`
		Data  struct {
			Symbol string `json:"symbol"`
		} `json:"data"`
	}
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return wsstream.Message{}, fmt.Errorf("Bybit funding websocket JSON: %w", err)
	}
	if envelope.Topic == "" {
		return p.streamProtocol.Decode(payload)
	}
	if envelope.Data.Symbol == "" || envelope.Topic != "tickers."+envelope.Data.Symbol {
		return wsstream.Message{}, fmt.Errorf("Bybit funding websocket topic/symbol mismatch")
	}
	return wsstream.Message{Topic: envelope.Topic}, nil
}

func (r *FundingRuntime) readFundingLoop(ctx context.Context, conn *websocket.Conn, messages chan<- []byte, errors chan<- error, terminal *connectionTerminal, instrumentIDs []uint32) {
	fail := func(err error) {
		err = terminal.fail(err, func(error) { r.Estimates.MarkUnavailable(instrumentIDs) })
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
		select {
		case messages <- payload:
		case <-ctx.Done():
			return
		default:
			fail(fmt.Errorf("Bybit funding update queue overflow"))
			return
		}
	}
}

func tickerSubscriptionBatches(topics []string, limit int) ([][]string, error) {
	if len(topics) == 0 {
		return nil, fmt.Errorf("Bybit ticker subscription requires topics")
	}
	if limit <= 0 {
		limit = tickerArgsLimit
	}
	var batches [][]string
	current := make([]string, 0, len(topics))
	for _, topic := range topics {
		candidate := append(append([]string(nil), current...), topic)
		encoded, err := json.Marshal(candidate)
		if err != nil {
			return nil, err
		}
		if len(encoded) <= limit {
			current = candidate
			continue
		}
		if len(current) == 0 {
			return nil, fmt.Errorf("Bybit ticker topic %q exceeds args limit %d", topic, limit)
		}
		batches = append(batches, current)
		current = []string{topic}
		encoded, err = json.Marshal(current)
		if err != nil || len(encoded) > limit {
			return nil, fmt.Errorf("Bybit ticker topic %q exceeds args limit %d", topic, limit)
		}
	}
	if len(current) > 0 {
		batches = append(batches, current)
	}
	return batches, nil
}
