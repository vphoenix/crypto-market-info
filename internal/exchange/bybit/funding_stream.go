package bybit

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/exchange"
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
		r.Logger.Error("Bybit funding websocket disconnected", "error", err)
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
	for _, instrument := range r.Instruments {
		if err := instrument.Validate(); err != nil || instrument.Exchange != "Bybit" || instrument.MarketType != model.MarketPerpetual {
			return fmt.Errorf("Bybit funding runtime received an invalid instrument")
		}
		if _, exists := seen[instrument.ExchangeSymbol]; exists {
			return fmt.Errorf("Bybit funding runtime has duplicate symbol %q", instrument.ExchangeSymbol)
		}
		seen[instrument.ExchangeSymbol] = struct{}{}
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
	instrumentIDs := r.instrumentIDs()
	r.Estimates.MarkUnavailable(instrumentIDs)
	defer r.Estimates.MarkUnavailable(instrumentIDs)
	if err := r.ConnectGate.Wait(ctx); err != nil {
		return err
	}
	conn, response, err := r.Dialer.DialContext(ctx, r.WSEndpoint, http.Header{})
	if err != nil {
		if response != nil {
			return fmt.Errorf("Bybit funding websocket handshake %s: %w", response.Status, err)
		}
		return err
	}
	defer conn.Close()
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
	pending := make(map[string][]string, len(batches))
	topicBatch := make(map[string]string, len(topics))
	for index, batch := range batches {
		reqID := fmt.Sprintf("funding%d", index+1)
		pending[reqID] = batch
		for _, topic := range batch {
			topicBatch[topic] = reqID
		}
		if err = conn.WriteJSON(map[string]any{"req_id": reqID, "op": "subscribe", "args": batch}); err != nil {
			return err
		}
	}
	messages := make(chan []byte, r.QueueCapacity)
	readerErrors := make(chan error, 1)
	terminal := &connectionTerminal{}
	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go r.readFundingLoop(readCtx, conn, messages, readerErrors, terminal, instrumentIDs)
	ticker := time.NewTicker(r.PingInterval)
	defer ticker.Stop()
	subscriptionTimer := time.NewTimer(r.SubscribeTimeout)
	defer subscriptionTimer.Stop()
	subscriptionTimeout := subscriptionTimer.C
	subscribed := make(map[string]struct{}, len(topics))
	preAck := make(map[string][][]byte, len(batches))
	preAckCount := 0
	cache := newTickerCache()
	applyTicker := func(payload []byte) error {
		update, instrument, parseErr := ParseTickerUpdate(payload, instruments)
		if parseErr != nil {
			return parseErr
		}
		estimate, complete, applyErr := cache.Apply(update, instrument)
		if applyErr != nil || !complete {
			return applyErr
		}
		if putErr := r.Estimates.Put(estimate); putErr != nil {
			return putErr
		}
		return r.Confirmations.Schedule(ctx, instrument, estimate.FundingTime)
	}
	fail := func(failure error) error {
		return terminal.fail(failure, func(error) { r.Estimates.MarkUnavailable(instrumentIDs) })
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case err = <-readerErrors:
			if terminalErr := terminal.current(); terminalErr != nil {
				return terminalErr
			}
			return terminal.fail(err, func(error) { r.Estimates.MarkUnavailable(instrumentIDs) })
		case payload := <-messages:
			err = terminal.withActive(func() error {
				var identity struct {
					Topic string `json:"topic"`
					Op    string `json:"op"`
				}
				if unmarshalErr := json.Unmarshal(payload, &identity); unmarshalErr != nil {
					return fmt.Errorf("Bybit funding websocket JSON: %w", unmarshalErr)
				}
				if identity.Topic == "" {
					var control websocketControl
					if unmarshalErr := json.Unmarshal(payload, &control); unmarshalErr != nil {
						return unmarshalErr
					}
					if control.Op == "ping" {
						_, controlErr := parseWebsocketControl(payload, "")
						return controlErr
					}
					batch, exists := pending[control.ReqID]
					if !exists {
						return fmt.Errorf("Bybit funding websocket unexpected subscription req_id=%q", control.ReqID)
					}
					if _, controlErr := parseWebsocketControl(payload, control.ReqID); controlErr != nil {
						return controlErr
					}
					delete(pending, control.ReqID)
					for _, topic := range batch {
						subscribed[topic] = struct{}{}
					}
					for _, buffered := range preAck[control.ReqID] {
						if applyErr := applyTicker(buffered); applyErr != nil {
							return applyErr
						}
						preAckCount--
					}
					delete(preAck, control.ReqID)
					if len(pending) == 0 {
						subscriptionTimer.Stop()
						subscriptionTimeout = nil
					}
					return nil
				}
				reqID, expected := topicBatch[identity.Topic]
				if !expected {
					return fmt.Errorf("Bybit funding websocket unexpected topic %q", identity.Topic)
				}
				if _, active := subscribed[identity.Topic]; !active {
					if _, stillPending := pending[reqID]; !stillPending {
						return fmt.Errorf("Bybit funding websocket topic %q has inconsistent subscription state", identity.Topic)
					}
					if preAckCount >= r.PreAckCapacity {
						return fmt.Errorf("Bybit funding pre-ack buffer overflow")
					}
					preAck[reqID] = append(preAck[reqID], append([]byte(nil), payload...))
					preAckCount++
					return nil
				}
				return applyTicker(payload)
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
			return fail(fmt.Errorf("Bybit funding websocket subscription acknowledgements timed out"))
		}
	}
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
