package app

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/config"
	"github.com/vphoenix/crypto-market-info/internal/exchange/binance"
	"github.com/vphoenix/crypto-market-info/internal/exchange/okx"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/orderbook"
	"github.com/vphoenix/crypto-market-info/internal/sampler"
	chstore "github.com/vphoenix/crypto-market-info/internal/storage/clickhouse"
	"github.com/vphoenix/crypto-market-info/internal/universe"
)

type preparedMarkets struct {
	components         []component
	fundingInstruments []model.Instrument
	byVenue            map[string][]model.Instrument
	run                model.PerpetualUniverseRun
	members            []model.PerpetualUniverseMember
	health             map[string]func() any
}

func prepareMarkets(ctx context.Context, cfg config.Config, store *chstore.Client, clients venueClients, d PerpetualDiscovery, logger *slog.Logger) (preparedMarkets, error) {
	m := preparedMarkets{byVenue: make(map[string][]model.Instrument)}
	definitions := []model.Instrument{}
	for _, v := range []struct {
		venue   string
		symbols []string
		fetch   func(context.Context, model.MarketType) ([]model.Instrument, error)
	}{
		{"Binance", cfg.BinanceSpotSymbols, clients.binance.Instruments}, {"OKX", cfg.OKXSpotSymbols, clients.okx.Instruments},
	} {
		if len(v.symbols) == 0 {
			continue
		}
		items, err := v.fetch(ctx, model.MarketSpot)
		if err != nil {
			return m, err
		}
		items, err = selectSymbols(v.venue, items, v.symbols)
		if err != nil {
			return m, err
		}
		definitions = append(definitions, items...)
	}
	for _, s := range d.Selected {
		definitions = append(definitions, s.Instrument)
	}
	if len(definitions) == 0 && !yieldEnabled(cfg) && !cfg.Options.Enabled {
		return m, fmt.Errorf("no instruments or yield collectors configured")
	}
	registered, err := store.RegisterInstruments(ctx, definitions)
	if err != nil {
		return m, err
	}
	all, err := store.Instruments(ctx)
	if err != nil {
		return m, err
	}
	at := time.Now().UTC().Truncate(time.Millisecond)
	mappings := []model.CanonicalMapping{}
	for _, i := range all {
		if !universe.Eligible(i) {
			continue
		}
		s, err := d.aliases.Resolve(i)
		if err != nil {
			return m, err
		}
		mappings = append(mappings, model.CanonicalMapping{InstrumentID: i.ID, MappingRevision: d.MappingRevision, CanonicalMarketKey: s.MarketKey.String(), CanonicalBaseAsset: s.MarketKey.BaseAsset, CanonicalQuoteAsset: "USDT", CanonicalSettleAsset: "USDT", CanonicalBaseUnitsPerVenueBaseUnit: s.CanonicalBaseUnitsPerVenueBaseUnit, MappingKind: s.MappingKind, RecordedAt: at})
	}
	if err = store.WriteCanonicalMappings(ctx, mappings); err != nil {
		return m, err
	}
	m.run = model.PerpetualUniverseRun{RunID: uuid.New(), SelectionRevision: d.SelectionRevision, MappingRevision: d.MappingRevision, StartedAt: at, SelectionConfigJSON: string(d.SelectionConfig), CanonicalInclude: cfg.PerpetualSelection.Include, CanonicalExclude: cfg.PerpetualSelection.Exclude, CanonicalGroupCount: uint32(d.GroupCount), InstrumentCount: uint32(len(d.Selected)), EnabledVenues: []string{}}
	for _, v := range cfg.PerpetualSelection.Venues {
		if v.Mode != universe.Disabled {
			m.run.EnabledVenues = append(m.run.EnabledVenues, v.Venue)
		}
	}
	books := map[uint32]*orderbook.Book{}
	sources := []sampler.Source{}
	perpetualByVenue := map[string][]model.Instrument{}
	for _, i := range registered {
		retained := 400
		if i.Exchange == "Binance" || i.Exchange == "Bybit" {
			retained = 1000
		}
		book, err := orderbook.New(i.ID, retained)
		if err != nil {
			return m, err
		}
		books[i.ID] = book
		sources = append(sources, sampler.Source{InstrumentID: i.ID, Book: book})
		if i.MarketType == model.MarketPerpetual {
			perpetualByVenue[i.Exchange] = append(perpetualByVenue[i.Exchange], i)
			s, err := d.aliases.Resolve(i)
			if err != nil {
				return m, err
			}
			m.members = append(m.members, model.PerpetualUniverseMember{RunID: m.run.RunID, InstrumentID: i.ID, CanonicalMarketKey: s.MarketKey.String()})
			if cfg.FundingEnabled {
				m.fundingInstruments = append(m.fundingInstruments, i)
				m.byVenue[i.Exchange] = append(m.byVenue[i.Exchange], i)
			}
			continue
		}
		switch i.Exchange {
		case "Binance":
			r := &binance.Runtime{Instrument: i, Book: book, Client: clients.binance, WSEndpoint: cfg.BinanceSpotWS, Logger: logger}
			m.components = append(m.components, component{name: "Binance spot " + i.ExchangeSymbol, run: r.Run})
		case "OKX":
			r := &okx.Runtime{Instrument: i, Book: book, WSEndpoint: cfg.OKXWS, ConnectGate: clients.okx.WebsocketConnectGate(), Logger: logger}
			m.components = append(m.components, component{name: "OKX spot " + i.ExchangeSymbol, run: r.Run})
		default:
			return m, fmt.Errorf("unsupported spot venue %s", i.Exchange)
		}
	}
	health := map[string]func() any{}
	m.health = health
	specs, err := venueSpecs(cfg, clients)
	if err != nil {
		return m, err
	}
	for _, v := range specs {
		instruments := perpetualByVenue[v.selection.Venue]
		if len(instruments) == 0 {
			continue
		}
		r, err := v.buildBook(instruments, books, v.selection.TopicsPerConnection, logger)
		if err != nil {
			return m, err
		}
		m.components = append(m.components, component{name: v.selection.Venue + " perpetual book manager", run: r.run})
		health[v.selection.Venue] = r.health
	}
	if len(sources) > 0 {
		engine, err := sampler.NewEngine(sources, store, d.Capacity.MinuteQueueCapacity, logger)
		if err != nil {
			return m, err
		}
		m.components = append(m.components, component{name: "second sampler", run: engine.Run})
		health["sampler"] = func() any { return engine.HealthSnapshot() }
	}
	if len(health) > 0 {
		m.components = append(m.components, component{name: "perpetual health", run: func(ctx context.Context) error {
			ticker := time.NewTicker(time.Minute)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return nil
				case <-ticker.C:
					states := map[string]any{}
					for venue, snapshot := range health {
						states[venue] = snapshot()
					}
					logger.Info("perpetual_runtime_health", "run_id", m.run.RunID, "expected_instruments", m.run.InstrumentCount, "catalog_built_at", m.run.StartedAt, "venues", states)
				}
			}
		}})
	}
	return m, nil
}

func commitPerpetualRun(ctx context.Context, store *chstore.Client, d PerpetualDiscovery, m preparedMarkets, logger *slog.Logger) error {
	if err := store.WritePerpetualUniverseRun(ctx, m.run, m.members); err != nil {
		return err
	}
	logger.Info("perpetual_universe_started", "run_id", m.run.RunID, "mapping_revision", d.MappingRevision, "selection_revision", d.SelectionRevision, "canonical_groups", d.GroupCount, "venues", d.Venues, "capacity", d.Capacity, "alias_matches", d.AliasCount, "identity_matches", d.IdentityCount)
	for _, warning := range d.Warnings {
		logger.Warn("perpetual_universe_warning", "warning", warning)
	}
	return nil
}
