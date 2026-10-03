package bybit

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"sort"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/vphoenix/crypto-market-info/internal/exchange"
	"github.com/vphoenix/crypto-market-info/internal/exchange/wsstream"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/orderbook"
)

type BookHealthSnapshot struct {
	ExpectedInstruments, ReadyInstruments, InvalidInstruments, ResyncingInstruments, ConnectedShards int
	OldestSourceTime                                                                                 time.Time
	Reconnects, Resyncs, QueuePeak, QueueOverflows                                                   uint64
	StaleTargets                                                                                     uint64
}

const MaxBookTopicsPerConnection = 20

func BookBufferedEventSlots(instruments, topics int) int {
	if instruments <= 0 || topics <= 0 {
		return 0
	}
	total := 0
	for remaining := instruments; remaining > 0; remaining -= topics {
		total += wsstream.BufferedEventSlots(min(remaining, topics), 0, 0)
	}
	return total
}
func BufferedEventSlotBytes() uintptr { return wsstream.EventSlotBytes() }
func FundingBufferedEventSlots(instruments int) int {
	return wsstream.BufferedEventSlots(instruments, 4096, 4096)
}

type BookManager struct {
	WSEndpoint                                                      string
	Dialer                                                          *websocket.Dialer
	ConnectGate                                                     exchange.WaitGate
	QueueCapacity, PreAckCapacity                                   int
	SubscribeTimeout, PingInterval, SilenceTimeout, ControlInterval time.Duration
	ReconnectBase, ReconnectMax                                     time.Duration
	ReconnectJitter                                                 func(time.Duration) time.Duration
	Logger                                                          *slog.Logger
	instruments                                                     []model.Instrument
	books                                                           map[uint32]*orderbook.Book
	shards                                                          [][]*wsstream.Target
	stats                                                           wsstream.Statistics
}

func NewBookManager(client *Client, instruments []model.Instrument, books map[uint32]*orderbook.Book, topicsPerConnection int, logger *slog.Logger) (*BookManager, error) {
	if client == nil || topicsPerConnection < 1 || topicsPerConnection > 20 {
		return nil, fmt.Errorf("Bybit book manager requires client and topics per connection in 1..20")
	}
	if logger == nil {
		logger = slog.Default()
	}
	// The public order book can arrive before a delayed subscription acknowledgement.
	// Allow the acknowledgement time to arrive while wsstream buffers pre-ACK events.
	m := &BookManager{WSEndpoint: defaultWSEndpoint, Dialer: websocket.DefaultDialer, ConnectGate: client.WebsocketConnectGate(), Logger: logger, SubscribeTimeout: 30 * time.Second, ReconnectBase: time.Second, ReconnectMax: 30 * time.Second, ReconnectJitter: exchange.AddJitter, books: make(map[uint32]*orderbook.Book)}
	m.instruments = append([]model.Instrument(nil), instruments...)
	sort.Slice(m.instruments, func(i, j int) bool { return m.instruments[i].ExchangeSymbol < m.instruments[j].ExchangeSymbol })
	seen := make(map[string]bool)
	for _, instrument := range m.instruments {
		book := books[instrument.ID]
		if err := instrument.Validate(); err != nil || instrument.ID == 0 || instrument.Exchange != "Bybit" || instrument.MarketType != model.MarketPerpetual || book == nil || book.InstrumentID() != instrument.ID || seen[instrument.ExchangeSymbol] || m.books[instrument.ID] != nil {
			return nil, fmt.Errorf("Bybit book manager invalid or duplicate instrument %q", instrument.ExchangeSymbol)
		}
		seen[instrument.ExchangeSymbol] = true
		m.books[instrument.ID] = book
		collector, _ := NewCollector(book)
		target := &wsstream.Target{Topic: fmt.Sprintf("orderbook.%d.%s", orderBookDepth, instrument.ExchangeSymbol), Reset: collector.Reset, Invalidate: book.MarkInvalid}
		target.Apply = func(payload []byte, awaiting bool) (bool, error) {
			update, err := ParseDepth(payload, instrument)
			if err != nil {
				return false, err
			}
			if awaiting && !update.Snapshot {
				return false, nil
			}
			if err := collector.Push(update); err != nil {
				return false, err
			}
			return true, nil
		}
		if len(m.shards) == 0 || len(m.shards[len(m.shards)-1]) == topicsPerConnection {
			m.shards = append(m.shards, nil)
		}
		m.shards[len(m.shards)-1] = append(m.shards[len(m.shards)-1], target)
	}
	for _, targets := range m.shards {
		topics := make([]string, len(targets))
		for i, target := range targets {
			topics[i] = target.Topic
		}
		encoded, _ := json.Marshal(topics)
		if len(encoded) > tickerArgsLimit {
			return nil, fmt.Errorf("Bybit book args exceeds %d bytes", tickerArgsLimit)
		}
	}
	return m, nil
}

func (m *BookManager) Run(ctx context.Context) error {
	if err := m.Validate(); err != nil {
		return err
	}
	var workers sync.WaitGroup
	for index, targets := range m.shards {
		workers.Add(1)
		go func() {
			defer workers.Done()
			delay := m.ReconnectBase
			for ctx.Err() == nil {
				err := m.runConnection(ctx, targets)
				if ctx.Err() != nil {
					return
				}
				m.stats.Reconnects.Add(1)
				m.Logger.Error("Bybit book shard disconnected", "shard", index, "targets", len(targets), "error", err)
				if !exchange.Wait(ctx, m.ReconnectJitter(delay)) {
					return
				}
				delay = min(delay*2, m.ReconnectMax)
			}
		}()
	}
	workers.Wait()
	return nil
}

// Validate checks final local configuration, including an endpoint override,
// without opening connections or starting workers.
func (m *BookManager) Validate() error {
	if m == nil || m.ConnectGate == nil || m.Dialer == nil || m.Logger == nil || m.ReconnectJitter == nil {
		return fmt.Errorf("bybit book manager missing dependencies")
	}
	if m.ReconnectBase <= 0 || m.ReconnectMax < m.ReconnectBase || m.QueueCapacity < 0 || m.PreAckCapacity < 0 || m.ControlInterval < 0 || m.SubscribeTimeout < 0 || m.PingInterval < 0 || m.SilenceTimeout < 0 {
		return fmt.Errorf("bybit book manager invalid timing/capacity")
	}
	return validateWSEndpoint(m.WSEndpoint)
}

func validateWSEndpoint(endpoint string) error {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Hostname() == "" || (parsed.Scheme != "ws" && parsed.Scheme != "wss") || parsed.Fragment != "" {
		return fmt.Errorf("bybit websocket endpoint must be an absolute ws/wss URL")
	}
	return nil
}

func (m *BookManager) runConnection(ctx context.Context, targets []*wsstream.Target) error {
	return wsstream.RunConnection(ctx, wsstream.Config{Endpoint: m.WSEndpoint, Dialer: m.Dialer, ConnectGate: m.ConnectGate, Protocol: streamProtocol{}, Targets: targets, BatchSize: 20, QueueCapacity: m.QueueCapacity, PreAckCapacity: m.PreAckCapacity, RequireAckID: true, Resync: true, ControlInterval: m.ControlInterval, PingInterval: m.PingInterval, SilenceTimeout: m.SilenceTimeout, SubscribeTimeout: m.SubscribeTimeout, Stats: &m.stats})
}

func (m *BookManager) HealthSnapshot() BookHealthSnapshot {
	h := BookHealthSnapshot{ExpectedInstruments: len(m.instruments), ConnectedShards: int(m.stats.Connected.Load()), Reconnects: m.stats.Reconnects.Load(), Resyncs: m.stats.Resyncs.Load(), QueuePeak: m.stats.QueuePeak.Load(), QueueOverflows: m.stats.QueueOverflows.Load()}
	for _, instrument := range m.instruments {
		view := m.books[instrument.ID].View()
		switch view.State {
		case orderbook.StateValid:
			h.ReadyInstruments++
			if h.OldestSourceTime.IsZero() || view.SourceTime.Before(h.OldestSourceTime) {
				h.OldestSourceTime = view.SourceTime
			}
		case orderbook.StateResyncing, orderbook.StateConnecting:
			h.ResyncingInstruments++
		default:
			h.InvalidInstruments++
		}
	}
	return h
}

type streamProtocol struct{}

func (p streamProtocol) Decode(payload []byte) (wsstream.Message, error) {
	var envelope struct {
		Topic string `json:"topic"`
		websocketControl
		Data struct {
			Symbol string `json:"s"`
		} `json:"data"`
	}
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return wsstream.Message{}, fmt.Errorf("Bybit websocket JSON: %w", err)
	}
	if envelope.Topic != "" {
		if envelope.Topic != fmt.Sprintf("orderbook.%d.%s", orderBookDepth, envelope.Data.Symbol) {
			return wsstream.Message{}, fmt.Errorf("Bybit websocket topic/symbol identity mismatch")
		}
		return wsstream.Message{Topic: envelope.Topic}, nil
	}
	if envelope.Success == nil || !*envelope.Success {
		return wsstream.Message{}, fmt.Errorf("Bybit websocket control op=%q failed: %s", envelope.Op, envelope.RetMsg)
	}
	if envelope.Op == "ping" && envelope.RetMsg == "pong" {
		return wsstream.Message{Pong: true}, nil
	}
	if (envelope.Op != "subscribe" && envelope.Op != "unsubscribe") || envelope.ReqID == "" {
		return wsstream.Message{}, fmt.Errorf("Bybit unsupported websocket acknowledgement")
	}
	return wsstream.Message{Acknowledgement: true, Operation: envelope.Op, RequestID: envelope.ReqID}, nil
}
func (p streamProtocol) Subscription(id, operation string, topics []string) any {
	return map[string]any{"req_id": id, "op": operation, "args": topics}
}
func (p streamProtocol) Ping() (int, []byte) { return websocket.TextMessage, []byte(`{"op":"ping"}`) }
