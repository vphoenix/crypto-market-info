package app

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"syscall"

	"github.com/vphoenix/crypto-market-info/internal/config"
	"github.com/vphoenix/crypto-market-info/internal/exchange/binance"
	"github.com/vphoenix/crypto-market-info/internal/exchange/bybit"
	"github.com/vphoenix/crypto-market-info/internal/exchange/okx"
	"github.com/vphoenix/crypto-market-info/internal/funding"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/orderbook"
	"github.com/vphoenix/crypto-market-info/internal/universe"
)

type VenueCapacity struct {
	Venue              string `json:"venue"`
	Instruments        int    `json:"instruments"`
	BookConnections    int    `json:"book_connections"`
	FundingConnections int    `json:"funding_connections"`
	BufferedEvents     int    `json:"buffered_events"`
}
type CapacityPlan struct {
	Venues                        []VenueCapacity `json:"venues"`
	TotalInstruments              int             `json:"total_instruments"`
	SampleSources                 int             `json:"sample_sources"`
	WSConnections                 int             `json:"ws_connections"`
	BufferedEvents                int             `json:"buffered_events"`
	EstimatedEventBytes           uint64          `json:"estimated_event_bytes"`
	PayloadMemoryIncluded         bool            `json:"payload_memory_included"`
	FileDescriptorLimit           uint64          `json:"file_descriptor_limit"`
	MinuteQueueCapacity           int             `json:"minute_queue_capacity"`
	BinanceSnapshotMinimumSeconds int             `json:"binance_snapshot_minimum_seconds"`
}
type PerpetualDiscovery struct {
	universe.Result
	SelectionRevision string          `json:"selection_revision"`
	SelectionConfig   json.RawMessage `json:"selection_config"`
	Capacity          CapacityPlan    `json:"capacity"`
	aliases           *universe.Aliases
}
type perpetualVenueSpec struct {
	selection    universe.VenueSelectionConfig
	fetch        func(context.Context, model.MarketType) ([]model.Instrument, error)
	buildBook    func([]model.Instrument, map[uint32]*orderbook.Book, int, *slog.Logger) (bookRuntime, error)
	provider     funding.ActualProvider
	buildFunding func([]model.Instrument, *funding.EstimateStore, funding.ConfirmationScheduler, *slog.Logger) (bookRuntime, error)
}

type bookRuntime struct {
	run    func(context.Context) error
	health func() any
}
type venueClients struct {
	binance *binance.Client
	okx     *okx.Client
	bybit   *bybit.Client
}

func newVenueClients(cfg config.Config, logger *slog.Logger) venueClients {
	b := binance.NewClient()
	b.SpotBaseURL = cfg.BinanceSpotREST
	b.FuturesBaseURL = cfg.BinanceFuturesREST
	o := okx.NewClient()
	o.BaseURL = cfg.OKXREST
	y := bybit.NewClient()
	y.BaseURL = cfg.BybitREST
	y.Logger = logger
	return venueClients{b, o, y}
}
func venueSpecs(cfg config.Config, clients venueClients) ([]perpetualVenueSpec, error) {
	fetchers := map[string]func(context.Context, model.MarketType) ([]model.Instrument, error){"Binance": clients.binance.Instruments, "OKX": clients.okx.Instruments, "Bybit": clients.bybit.Instruments}
	out := make([]perpetualVenueSpec, 0, len(cfg.PerpetualSelection.Venues))
	for _, v := range cfg.PerpetualSelection.Venues {
		f, ok := fetchers[v.Venue]
		if !ok {
			return nil, fmt.Errorf("unregistered perpetual venue %s", v.Venue)
		}
		builders := map[string]func([]model.Instrument, map[uint32]*orderbook.Book, int, *slog.Logger) (bookRuntime, error){
			"Binance": func(i []model.Instrument, b map[uint32]*orderbook.Book, n int, l *slog.Logger) (bookRuntime, error) {
				m, e := binance.NewBookManager(clients.binance, i, b, n, l)
				if e != nil {
					return bookRuntime{}, e
				}
				m.WSEndpoint = cfg.BinanceFuturesWS
				if e = m.Validate(); e != nil {
					return bookRuntime{}, e
				}
				return bookRuntime{m.Run, func() any { return m.HealthSnapshot() }}, nil
			},
			"OKX": func(i []model.Instrument, b map[uint32]*orderbook.Book, n int, l *slog.Logger) (bookRuntime, error) {
				m, e := okx.NewBookManager(clients.okx, i, b, n, l)
				if e != nil {
					return bookRuntime{}, e
				}
				m.WSEndpoint = cfg.OKXWS
				if e = m.Validate(); e != nil {
					return bookRuntime{}, e
				}
				return bookRuntime{m.Run, func() any { return m.HealthSnapshot() }}, nil
			},
			"Bybit": func(i []model.Instrument, b map[uint32]*orderbook.Book, n int, l *slog.Logger) (bookRuntime, error) {
				m, e := bybit.NewBookManager(clients.bybit, i, b, n, l)
				if e != nil {
					return bookRuntime{}, e
				}
				m.WSEndpoint = cfg.BybitWS
				if e = m.Validate(); e != nil {
					return bookRuntime{}, e
				}
				return bookRuntime{m.Run, func() any { return m.HealthSnapshot() }}, nil
			},
		}
		providers := map[string]funding.ActualProvider{"Binance": clients.binance, "OKX": clients.okx, "Bybit": clients.bybit}
		fundingBuilders := map[string]func([]model.Instrument, *funding.EstimateStore, funding.ConfirmationScheduler, *slog.Logger) (bookRuntime, error){
			"Binance": func(i []model.Instrument, e *funding.EstimateStore, c funding.ConfirmationScheduler, l *slog.Logger) (bookRuntime, error) {
				r := &binance.FundingRuntime{Client: clients.binance, Instruments: i, Estimates: e, Confirmations: c, WSEndpoint: cfg.BinanceMarketWS, Logger: l}
				if err := r.Validate(); err != nil {
					return bookRuntime{}, err
				}
				return bookRuntime{r.Run, func() any { return r.HealthSnapshot() }}, nil
			},
			"OKX": func(i []model.Instrument, e *funding.EstimateStore, c funding.ConfirmationScheduler, l *slog.Logger) (bookRuntime, error) {
				r := &okx.FundingRuntime{Instruments: i, Estimates: e, Confirmations: c, WSEndpoint: cfg.OKXWS, ConnectGate: clients.okx.WebsocketConnectGate(), Logger: l}
				if err := r.Validate(); err != nil {
					return bookRuntime{}, err
				}
				return bookRuntime{r.Run, func() any { return r.HealthSnapshot() }}, nil
			},
			"Bybit": func(i []model.Instrument, e *funding.EstimateStore, c funding.ConfirmationScheduler, l *slog.Logger) (bookRuntime, error) {
				r := &bybit.FundingRuntime{Instruments: i, Estimates: e, Confirmations: c, WSEndpoint: cfg.BybitWS, ConnectGate: clients.bybit.WebsocketConnectGate(), Logger: l}
				if err := r.Validate(); err != nil {
					return bookRuntime{}, err
				}
				return bookRuntime{r.Run, func() any { return r.HealthSnapshot() }}, nil
			},
		}
		out = append(out, perpetualVenueSpec{selection: v, fetch: f, buildBook: builders[v.Venue], provider: providers[v.Venue], buildFunding: fundingBuilders[v.Venue]})
	}
	return out, nil
}

func discoverPerpetual(ctx context.Context, cfg config.Config, clients venueClients) (PerpetualDiscovery, error) {
	var d PerpetualDiscovery
	a, err := universe.LoadAliases(cfg.PerpAssetAliasesFile)
	if err != nil {
		return d, err
	}
	d.aliases = a
	specs, err := venueSpecs(cfg, clients)
	if err != nil {
		return d, err
	}
	var catalogs []universe.VenueCatalog
	for _, v := range specs {
		if v.selection.Mode == universe.Disabled {
			continue
		}
		items, err := v.fetch(ctx, model.MarketPerpetual)
		if err != nil {
			return d, fmt.Errorf("%s complete catalog: %w", v.selection.Venue, err)
		}
		catalogs = append(catalogs, universe.VenueCatalog{Venue: v.selection.Venue, Instruments: items})
	}
	d.Result, err = universe.BuildCommonPerpetualUniverse(catalogs, cfg.PerpetualSelection, a)
	if err != nil {
		return d, err
	}
	d.Capacity, err = perpetualCapacity(cfg, d.Result)
	if err != nil {
		return d, err
	}
	limits := struct {
		TotalInstruments, WSConnections, BufferedEvents, SampleSources, MinuteQueueCapacity int
		FundingEnabled                                                                      bool
	}{cfg.PerpMaxTotalInstruments, cfg.PerpMaxTotalWSConnections, cfg.PerpMaxBufferedEvents, cfg.MaxSampleSources, d.Capacity.MinuteQueueCapacity, cfg.FundingEnabled}
	var encoded string
	d.SelectionRevision, encoded, err = universe.SelectionRevision(cfg.PerpetualSelection, a.Revision, d.Selected, limits)
	d.SelectionConfig = json.RawMessage(encoded)
	return d, err
}

// PrintPerpetualCatalogs supplies exact source identities for alias maintenance.
func PrintPerpetualCatalogs(ctx context.Context, cfg config.Config, w io.Writer, logger *slog.Logger) error {
	specs, err := venueSpecs(cfg, newVenueClients(cfg, logger))
	if err != nil {
		return err
	}
	catalogs := []universe.VenueCatalog{}
	for _, v := range specs {
		if v.selection.Mode == universe.Disabled {
			continue
		}
		items, err := v.fetch(ctx, model.MarketPerpetual)
		if err != nil {
			return err
		}
		catalogs = append(catalogs, universe.VenueCatalog{Venue: v.selection.Venue, Instruments: items})
	}
	e := json.NewEncoder(w)
	e.SetIndent("", "  ")
	return e.Encode(catalogs)
}

func PrintPerpetualUniverse(ctx context.Context, cfg config.Config, w io.Writer, logger *slog.Logger) error {
	d, err := discoverPerpetual(ctx, cfg, newVenueClients(cfg, logger))
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	return encoder.Encode(d)
}

func perpetualCapacity(cfg config.Config, r universe.Result) (CapacityPlan, error) {
	p := CapacityPlan{Venues: []VenueCapacity{}, TotalInstruments: len(r.Selected), SampleSources: len(r.Selected) + len(cfg.BinanceSpotSymbols) + len(cfg.OKXSpotSymbols), WSConnections: len(cfg.BinanceSpotSymbols) + len(cfg.OKXSpotSymbols)}
	var fd syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &fd); err != nil {
		return p, err
	}
	p.FileDescriptorLimit = fd.Cur
	counts := map[string]int{}
	for _, s := range r.Selected {
		counts[s.Instrument.Exchange]++
	}
	var failures []string
	for _, v := range cfg.PerpetualSelection.Venues {
		n := counts[v.Venue]
		if v.TopicsPerConnection <= 0 {
			return p, fmt.Errorf("%s topics per connection must be positive", v.Venue)
		}
		b := VenueCapacity{Venue: v.Venue, Instruments: n, BookConnections: (n + v.TopicsPerConnection - 1) / v.TopicsPerConnection}
		if n > 0 && cfg.FundingEnabled {
			b.FundingConnections = 1
		}
		budgets := map[string]struct {
			book    func(int, int) int
			funding func(int) int
			bytes   func() uintptr
		}{
			"Binance": {binance.BookBufferedEventSlots, binance.FundingBufferedEventSlots, binance.BufferedEventSlotBytes},
			"OKX":     {okx.BookBufferedEventSlots, okx.FundingBufferedEventSlots, okx.BufferedEventSlotBytes},
			"Bybit":   {bybit.BookBufferedEventSlots, bybit.FundingBufferedEventSlots, bybit.BufferedEventSlotBytes},
		}
		budget, ok := budgets[v.Venue]
		if !ok {
			return p, fmt.Errorf("missing capacity adapter for %s", v.Venue)
		}
		b.BufferedEvents = budget.book(n, v.TopicsPerConnection)
		if v.Venue == "Binance" && n > 0 {
			p.BinanceSnapshotMinimumSeconds = n
		}
		if b.FundingConnections > 0 {
			b.BufferedEvents += budget.funding(n)
		}
		p.EstimatedEventBytes += uint64(b.BufferedEvents) * uint64(budget.bytes())
		p.WSConnections += b.BookConnections + b.FundingConnections
		p.BufferedEvents += b.BufferedEvents
		p.Venues = append(p.Venues, b)
		if n > v.MaxInstruments {
			failures = append(failures, fmt.Sprintf("%s instruments %d > %d", v.Venue, n, v.MaxInstruments))
		}
		if v.Venue == "Bybit" && b.BookConnections+b.FundingConnections > 1000 {
			failures = append(failures, "Bybit market-data connections exceed 1000")
		}
	}
	// Actual sizeof queue envelopes is deterministic; variable JSON/level backing
	// storage and orderbook maps must additionally be measured during the soak.
	p.MinuteQueueCapacity = cfg.MinuteQueueCapacity
	if p.MinuteQueueCapacity == 0 {
		p.MinuteQueueCapacity = max(512, 2*p.SampleSources)
	}
	if p.TotalInstruments > cfg.PerpMaxTotalInstruments {
		failures = append(failures, fmt.Sprintf("total perpetual instruments %d > %d", p.TotalInstruments, cfg.PerpMaxTotalInstruments))
	}
	if p.SampleSources > cfg.MaxSampleSources {
		failures = append(failures, fmt.Sprintf("sample sources %d > %d", p.SampleSources, cfg.MaxSampleSources))
	}
	if p.WSConnections > cfg.PerpMaxTotalWSConnections {
		failures = append(failures, fmt.Sprintf("websocket connections %d > %d", p.WSConnections, cfg.PerpMaxTotalWSConnections))
	}
	if p.BufferedEvents > cfg.PerpMaxBufferedEvents {
		failures = append(failures, fmt.Sprintf("buffered events %d > %d", p.BufferedEvents, cfg.PerpMaxBufferedEvents))
	}
	if uint64(p.WSConnections+32) > fd.Cur-fd.Cur/5 {
		failures = append(failures, "insufficient file descriptors with 20% reserve")
	}
	if p.MinuteQueueCapacity < 2*p.SampleSources {
		failures = append(failures, fmt.Sprintf("minute queue %d < two source minutes %d", p.MinuteQueueCapacity, 2*p.SampleSources))
	}
	if len(failures) > 0 {
		return p, fmt.Errorf("perpetual capacity exceeded: %v; plan=%+v", failures, p)
	}
	return p, nil
}
