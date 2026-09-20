package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/vphoenix/crypto-market-info/internal/exchange"
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
	Client              *Client
	Instruments         []model.Instrument
	Estimates           FundingEstimateSink
	Confirmations       FundingConfirmationScheduler
	WSEndpoint          string
	Dialer              *websocket.Dialer
	SilenceTimeout      time.Duration
	ReconnectBase       time.Duration
	ReconnectMax        time.Duration
	ReconnectJitter     func(time.Duration) time.Duration
	SubscriptionTimeout time.Duration
	ControlInterval     time.Duration
	Logger              *slog.Logger
	healthMu            sync.Mutex
	health              FundingHealthSnapshot
	reconnects          atomic.Uint64
	queuePeak           atomic.Uint64
	queueOverflows      atomic.Uint64
}

type FundingHealthSnapshot struct {
	ExpectedInstruments                   int
	ReadyInstruments                      int
	InvalidInstruments                    int
	ConnectedShards                       int
	OldestSourceTime                      time.Time
	Reconnects, QueuePeak, QueueOverflows uint64
}

func (r *FundingRuntime) HealthSnapshot() FundingHealthSnapshot {
	r.healthMu.Lock()
	defer r.healthMu.Unlock()
	h := r.health
	h.ExpectedInstruments = len(r.Instruments)
	h.InvalidInstruments = h.ExpectedInstruments - h.ReadyInstruments
	h.Reconnects, h.QueuePeak, h.QueueOverflows = r.reconnects.Load(), r.queuePeak.Load(), r.queueOverflows.Load()
	return h
}

func FundingBufferedEventSlots(instruments int) int {
	if instruments <= 0 {
		return 0
	}
	return max(4096, 64*instruments) + 4096 + 2*controlQueueCapacity + 2
}

// Validate performs all constructor checks without opening a socket.
func (r *FundingRuntime) Validate() error { return r.defaults() }

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
		r.reconnects.Add(1)
		r.Logger.Error("Binance funding websocket disconnected", "error", err)
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
		return fmt.Errorf("Binance funding runtime requires instruments, estimates and confirmations")
	}
	seen := make(map[string]struct{}, len(r.Instruments))
	seenIDs := make(map[uint32]bool, len(r.Instruments))
	streams := make([]string, 0, len(r.Instruments))
	for _, instrument := range r.Instruments {
		if err := instrument.Validate(); err != nil || instrument.Exchange != "Binance" || instrument.MarketType != model.MarketPerpetual {
			return fmt.Errorf("Binance funding runtime received an invalid instrument")
		}
		if _, exists := seen[instrument.ExchangeSymbol]; exists || seenIDs[instrument.ID] {
			return fmt.Errorf("Binance funding runtime has duplicate symbol %q", instrument.ExchangeSymbol)
		}
		seen[instrument.ExchangeSymbol] = struct{}{}
		seenIDs[instrument.ID] = true
		streams = append(streams, strings.ToLower(instrument.ExchangeSymbol)+"@markPrice@1s")
	}
	if _, err := subscriptionBatches(streams); err != nil {
		return err
	}
	if r.Client == nil {
		r.Client = NewClient()
	}
	if r.WSEndpoint == "" {
		// Binance routes mark price through /market; the legacy /ws only emits public streams.
		r.WSEndpoint = "wss://fstream.binance.com/market/ws"
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
	if r.ReconnectJitter == nil {
		r.ReconnectJitter = exchange.AddJitter
	}
	if r.SubscriptionTimeout <= 0 {
		r.SubscriptionTimeout = 10 * time.Second
	}
	if r.ControlInterval <= 0 {
		r.ControlInterval = 250 * time.Millisecond
	}
	u, err := url.Parse(r.WSEndpoint)
	if err != nil || u.Host == "" || (u.Scheme != "ws" && u.Scheme != "wss") {
		return fmt.Errorf("Binance funding invalid websocket endpoint")
	}
	return nil
}

func (r *FundingRuntime) runConnection(ctx context.Context) error {
	if err := r.Client.waitWebsocket(ctx); err != nil {
		return err
	}
	conn, response, err := r.Dialer.DialContext(ctx, r.WSEndpoint, http.Header{})
	if err != nil {
		if response != nil {
			return fmt.Errorf("Binance funding websocket handshake %s: %w", response.Status, err)
		}
		return err
	}
	defer conn.Close()
	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(readCtx, func() { _ = conn.Close() })
	defer stop()
	r.healthMu.Lock()
	r.health.ConnectedShards = 1
	r.healthMu.Unlock()
	defer func() {
		r.Estimates.MarkUnavailable(r.instrumentIDs())
		r.healthMu.Lock()
		r.health = FundingHealthSnapshot{}
		r.healthMu.Unlock()
	}()
	instruments := make(map[string]model.Instrument, len(r.Instruments))
	ordered := append([]model.Instrument(nil), r.Instruments...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].ExchangeSymbol < ordered[j].ExchangeSymbol })
	for _, instrument := range ordered {
		instruments[instrument.ExchangeSymbol] = instrument
	}
	streams := make([]string, len(ordered))
	for i, instrument := range ordered {
		streams[i] = strings.ToLower(instrument.ExchangeSymbol) + "@markPrice@1s"
	}
	batches, err := subscriptionBatches(streams)
	if err != nil {
		return err
	}
	writer := newSocketWriter(readCtx, conn, r.ControlInterval)
	fence := &connectionFence{}
	invalidate := func() {
		r.Estimates.MarkUnavailable(r.instrumentIDs())
		r.healthMu.Lock()
		r.health = FundingHealthSnapshot{}
		r.healthMu.Unlock()
	}
	writerDone := make(chan struct{})
	go func() { defer close(writerDone); writer.run() }()
	_ = conn.SetReadDeadline(time.Now().Add(r.SilenceTimeout))
	conn.SetPongHandler(func(string) error { return conn.SetReadDeadline(time.Now().Add(r.SilenceTimeout)) })
	conn.SetPingHandler(func(payload string) error {
		_ = conn.SetReadDeadline(time.Now().Add(r.SilenceTimeout))
		return writer.pong(payload)
	})
	conn.SetCloseHandler(func(code int, text string) error { return &websocket.CloseError{Code: code, Text: text} })
	conn.SetReadLimit(1 << 20)
	type readResult struct {
		payload  []byte
		err      error
		received time.Time
	}
	reads := make(chan readResult, max(4096, 64*len(ordered)))
	readerErrors := make(chan error, 1)
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		for {
			_, payload, readErr := conn.ReadMessage()
			if readErr != nil {
				fence.fail(readErr, invalidate)
				select {
				case readerErrors <- readErr:
				default:
				}
				return
			}
			now := time.Now().UTC()
			_ = conn.SetReadDeadline(now.Add(r.SilenceTimeout))
			select {
			case reads <- readResult{payload: payload, received: now}:
				peak := uint64(len(reads))
				for old := r.queuePeak.Load(); peak > old; old = r.queuePeak.Load() {
					if r.queuePeak.CompareAndSwap(old, peak) {
						break
					}
				}
			case <-readCtx.Done():
				return
			default:
				r.queueOverflows.Add(1)
				overflow := fmt.Errorf("Binance funding queue overflow")
				fence.fail(overflow, invalidate)
				select {
				case readerErrors <- overflow:
				default:
				}
				_ = conn.Close()
				return
			}
		}
	}()
	defer func() {
		fence.fail(fmt.Errorf("Binance funding connection ended"), invalidate)
		cancel()
		_ = conn.Close()
		<-readerDone
		<-writerDone
	}()
	acked := make(map[string]bool, len(ordered))
	seen := make(map[string]time.Time, len(ordered))
	lastReceived := make(map[string]time.Time, len(ordered))
	preACK := make([]readResult, 0)
	pendingSymbols := make(map[string]bool)
	publish := func(payload []byte, received time.Time) error {
		estimate, instrument, parseErr := ParseFundingUpdate(payload, instruments)
		if parseErr != nil {
			return parseErr
		}
		return fence.apply(func() error {
			if err := r.Estimates.Put(estimate); err != nil {
				return err
			}
			if err := r.Confirmations.Schedule(ctx, instrument, estimate.FundingTime); err != nil {
				return err
			}
			seen[instrument.ExchangeSymbol] = estimate.SourceTime
			lastReceived[instrument.ExchangeSymbol] = received
			r.updateFundingHealth(seen)
			return nil
		})
	}
	batchStart, batchEnd := 0, 0
	batchIndex := 0
	var outstanding int64
	sendBatch := func() error {
		batchEnd = batchStart + len(batches[batchIndex])
		clear(pendingSymbols)
		for _, instrument := range ordered[batchStart:batchEnd] {
			pendingSymbols[instrument.ExchangeSymbol] = true
		}
		outstanding = r.Client.requestID.Add(1)
		return writer.json(map[string]any{"method": "SUBSCRIBE", "params": batches[batchIndex], "id": outstanding})
	}
	if err = sendBatch(); err != nil {
		return err
	}
	ackTimer := time.NewTimer(r.SubscriptionTimeout)
	defer ackTimer.Stop()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		var payload []byte
		var received time.Time
		select {
		case <-ctx.Done():
			return nil
		case err = <-readerErrors:
			return err
		case err = <-writer.errors:
			return err
		case <-ackTimer.C:
			return fmt.Errorf("Binance funding subscription ACK timeout")
		case now := <-ticker.C:
			for symbol, when := range lastReceived {
				if now.Sub(when) >= r.SilenceTimeout {
					r.Estimates.MarkUnavailable([]uint32{instruments[symbol].ID})
					delete(seen, symbol)
				}
			}
			if err = fence.apply(func() error { r.updateFundingHealth(seen); return nil }); err != nil {
				return err
			}
			continue
		case result := <-reads:
			if result.err != nil {
				return result.err
			}
			payload = result.payload
			received = result.received
		}
		ack, ackErr := parseSubscriptionACK(payload, outstanding)
		if ackErr != nil {
			return ackErr
		}
		if ack {
			for _, instrument := range ordered[batchStart:batchEnd] {
				acked[instrument.ExchangeSymbol] = true
			}
			ackTimer.Stop()
			for _, event := range preACK {
				if err = publish(event.payload, event.received); err != nil {
					return err
				}
			}
			preACK = nil
			batchStart = batchEnd
			batchIndex++
			outstanding = 0
			if batchStart < len(ordered) {
				if err = sendBatch(); err != nil {
					return err
				}
				ackTimer.Reset(r.SubscriptionTimeout)
			}
			continue
		}
		_, instrument, parseErr := ParseFundingUpdate(payload, instruments)
		if parseErr != nil {
			return parseErr
		}
		if !acked[instrument.ExchangeSymbol] {
			if !pendingSymbols[instrument.ExchangeSymbol] {
				return fmt.Errorf("Binance funding message before subscription for %q", instrument.ExchangeSymbol)
			}
			if len(preACK) >= 4096 {
				r.queueOverflows.Add(1)
				return fmt.Errorf("Binance funding pre-ACK buffer overflow")
			}
			preACK = append(preACK, readResult{payload: payload, received: received})
			continue
		}
		if err = publish(payload, received); err != nil {
			return err
		}
	}
}

func (r *FundingRuntime) updateFundingHealth(seen map[string]time.Time) {
	r.healthMu.Lock()
	defer r.healthMu.Unlock()
	r.health.ReadyInstruments = len(seen)
	r.health.OldestSourceTime = time.Time{}
	for _, when := range seen {
		if r.health.OldestSourceTime.IsZero() || when.Before(r.health.OldestSourceTime) {
			r.health.OldestSourceTime = when
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
