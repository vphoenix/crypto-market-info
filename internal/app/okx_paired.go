package app

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/vphoenix/crypto-market-info/internal/config"
	"github.com/vphoenix/crypto-market-info/internal/exchange"
	"github.com/vphoenix/crypto-market-info/internal/exchange/okx"
	"github.com/vphoenix/crypto-market-info/internal/funding"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/orderbook"
	"github.com/vphoenix/crypto-market-info/internal/sampler"
	chstore "github.com/vphoenix/crypto-market-info/internal/storage/clickhouse"
	"io"
	"log/slog"
	"sync"
	"syscall"
	"time"
)

type pairCapacity struct{ Pairs, AdditionalSources, PeakWSConnections, PeakBufferedEvents int }

func pairedCapacity(cfg config.Config, legacy CapacityPlan, pairs int, extra []model.Instrument, previousSources, previousPerps int) (pairCapacity, error) {
	p := pairCapacity{Pairs: pairs, AdditionalSources: len(extra)}
	if pairs < 1 || pairs > cfg.OKXPairedMaxPairs {
		return p, fmt.Errorf("pair count outside configured maximum")
	}
	perps := 0
	for _, i := range extra {
		if i.MarketType == model.MarketPerpetual {
			perps++
		}
	}
	ownedPerps := 0
	for _, venue := range legacy.Venues {
		if venue.Venue == "OKX" {
			ownedPerps = venue.Instruments
		}
	}
	for _, venue := range cfg.PerpetualSelection.Venues {
		if venue.Venue == "OKX" && venue.MaxInstruments > 0 && ownedPerps+perps > venue.MaxInstruments {
			return p, fmt.Errorf("OKX paired perpetuals plus existing streams exceed venue instrument limit")
		}
	}
	connections := (len(extra) + okx.MaxBookTopicsPerConnection - 1) / okx.MaxBookTopicsPerConnection
	buffers := okx.BookBufferedEventSlots(len(extra), okx.MaxBookTopicsPerConnection)
	if cfg.FundingEnabled && perps > 0 {
		connections++
		buffers += okx.FundingBufferedEventSlots(perps)
	}
	oldConnections := (previousSources + okx.MaxBookTopicsPerConnection - 1) / okx.MaxBookTopicsPerConnection
	oldBuffers := okx.BookBufferedEventSlots(previousSources, okx.MaxBookTopicsPerConnection)
	if cfg.FundingEnabled && previousPerps > 0 {
		oldConnections++
		oldBuffers += okx.FundingBufferedEventSlots(previousPerps)
	}
	// Keep the conservative two-generation reservation and account for a larger
	// retiring generation when the next complete universe becomes smaller.
	p.PeakWSConnections = legacy.WSConnections + connections + max(connections, oldConnections)
	p.PeakBufferedEvents = legacy.BufferedEvents + buffers + max(buffers, oldBuffers)
	if legacy.SampleSources+len(extra) > cfg.MaxSampleSources || legacy.TotalInstruments+perps > cfg.PerpMaxTotalInstruments || p.PeakWSConnections > cfg.PerpMaxTotalWSConnections || p.PeakBufferedEvents > cfg.PerpMaxBufferedEvents {
		return p, fmt.Errorf("paired warm-up capacity exceeds configured limits: %+v", p)
	}
	var fd syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &fd); err != nil {
		return p, err
	}
	if uint64(p.PeakWSConnections+32) > fd.Cur-fd.Cur/5 {
		return p, fmt.Errorf("paired warm-up exceeds descriptor reserve")
	}
	if cfg.MinuteQueueCapacity > 0 && cfg.MinuteQueueCapacity < 2*len(extra) {
		return p, fmt.Errorf("paired minute queue cannot hold two complete minutes")
	}
	return p, nil
}
func extraPairDefinitions(pairs []model.OKXPairMember, owned []model.Instrument) []model.Instrument {
	var extra []model.Instrument
	for _, p := range pairs {
		for _, i := range []model.Instrument{p.Spot, p.Perpetual} {
			exists := false
			for _, old := range owned {
				if old.SameDefinition(i) {
					exists = true
					break
				}
			}
			if !exists {
				extra = append(extra, i)
			}
		}
	}
	return extra
}
func registerPairCatalog(ctx context.Context, store *chstore.Client, catalog *okx.PairedCatalog) error {
	var defs []model.Instrument
	for _, p := range catalog.Observation.Pairs {
		defs = append(defs, p.Spot, p.Perpetual)
	}
	registered, err := store.RegisterInstruments(ctx, defs)
	if err != nil {
		return err
	}
	for n := range catalog.Observation.Pairs {
		catalog.Observation.Pairs[n].Spot = registered[2*n]
		catalog.Observation.Pairs[n].Perpetual = registered[2*n+1]
	}
	return store.WriteOKXLoanPolicy(ctx, catalog.Policy)
}

type pairedGeneration struct {
	cancel     context.CancelFunc
	wait       sync.WaitGroup
	manager    *okx.BookManager
	sources    []sampler.Source
	estimates  *funding.EstimateStore
	perpetuals []model.Instrument
	errors     chan error
	ctx        context.Context
}

func newPairedGeneration(ctx context.Context, cfg config.Config, client *okx.Client, extra []model.Instrument, worker *funding.ConfirmationWorker, logger *slog.Logger) (*pairedGeneration, error) {
	ctx, cancel := context.WithCancel(ctx)
	g := &pairedGeneration{ctx: ctx, cancel: cancel, estimates: funding.NewEstimateStore(), errors: make(chan error, 4)}
	books := map[uint32]*orderbook.Book{}
	for _, i := range extra {
		b, err := orderbook.New(i.ID, 400)
		if err != nil {
			cancel()
			return nil, err
		}
		books[i.ID] = b
		g.sources = append(g.sources, sampler.Source{InstrumentID: i.ID, Book: b, StoredDepth: cfg.MarketBookDepth})
		if i.MarketType == model.MarketPerpetual {
			g.perpetuals = append(g.perpetuals, i)
		}
	}
	if len(extra) > 0 {
		m, err := okx.NewBookManager(client, extra, books, okx.MaxBookTopicsPerConnection, logger)
		if err != nil {
			cancel()
			return nil, err
		}
		m.WSEndpoint = cfg.OKXWS
		if err = m.Validate(); err != nil {
			cancel()
			return nil, err
		}
		g.manager = m
	}
	var fund *okx.FundingRuntime
	if cfg.FundingEnabled && len(g.perpetuals) > 0 {
		fund = &okx.FundingRuntime{Instruments: g.perpetuals, Estimates: g.estimates, Confirmations: worker, WSEndpoint: cfg.OKXWS, ConnectGate: client.WebsocketConnectGate(), Logger: logger}
		if err := fund.Validate(); err != nil {
			cancel()
			return nil, err
		}
	}
	if g.manager != nil {
		g.start(g.manager.Run)
	}
	if fund != nil {
		g.start(fund.Run)
	}
	return g, nil
}
func (g *pairedGeneration) start(run func(context.Context) error) {
	g.wait.Add(1)
	go func() {
		defer g.wait.Done()
		err := run(g.ctx)
		if g.ctx.Err() == nil {
			if err == nil {
				err = fmt.Errorf("paired generation worker exited unexpectedly")
			}
			select {
			case g.errors <- err:
			default:
			}
		}
	}()
}
func (g *pairedGeneration) stop() { g.cancel(); g.wait.Wait() }
func (g *pairedGeneration) warm(ctx context.Context) error {
	deadline := time.NewTimer(60 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		if g.manager == nil || g.manager.HealthSnapshot().ReadyInstruments == len(g.sources) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-g.errors:
			return err
		case <-deadline.C:
			return fmt.Errorf("paired books did not all warm within 60s")
		case <-ticker.C:
		}
	}
}
func runOKXPaired(ctx context.Context, cfg config.Config, store *chstore.Client, client *okx.Client, owned []model.Instrument, legacy CapacityPlan, logger *slog.Logger) error {
	return runIndependentSources(ctx, logger, 10*time.Second, component{name: "OKX paired markets", run: func(ctx context.Context) error {
		return collectOKXPaired(ctx, cfg, store, client, owned, legacy, logger)
	}})
}
func collectOKXPaired(ctx context.Context, cfg config.Config, store *chstore.Client, client *okx.Client, owned []model.Instrument, legacy CapacityPlan, logger *slog.Logger) error {
	if err := store.InitOKXPairSchema(ctx); err != nil {
		return err
	}
	parent := ctx
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	worker := &funding.ConfirmationWorker{Exchange: "OKX", Provider: client, Sink: store, QueueCapacity: 8192, Logger: logger}
	var background sync.WaitGroup
	hourly := &pairedFundingScheduler{sink: store, logger: logger}
	errors := make(chan error, 4)
	runBackground := func(run func(context.Context) error) {
		background.Add(1)
		go func() {
			defer background.Done()
			if err := run(ctx); err != nil && ctx.Err() == nil {
				select {
				case errors <- err:
				default:
				}
				cancel()
			}
		}()
	}
	defer func() { cancel(); background.Wait() }()
	if cfg.FundingEnabled {
		runBackground(worker.Run)
		runBackground(hourly.Run)
	}
	// Failures produce no fresh policy row; collection continues independently.
	runBackground(func(ctx context.Context) error {
		for ctx.Err() == nil {
			policy, err := client.LoanPolicy(ctx)
			if err == nil {
				err = store.WriteOKXLoanPolicy(ctx, policy)
			}
			if err != nil {
				logger.Error("OKX public loan policy unavailable", "error", err)
			}
			if !exchange.Wait(ctx, time.Minute) {
				break
			}
		}
		return nil
	})
	var engine *sampler.Engine
	var active *pairedGeneration
	defer func() {
		if active != nil {
			active.stop()
		}
	}()
	var fingerprint string
	refresh := time.NewTimer(0)
	defer refresh.Stop()
	for {
		select {
		case <-ctx.Done():
			if parent.Err() != nil {
				return nil
			}
			select {
			case err := <-errors:
				return err
			default:
				return fmt.Errorf("paired background stopped")
			}
		case err := <-errors:
			return err
		case <-refresh.C:
			attempt, stopAttempt := context.WithTimeout(ctx, 45*time.Second)
			catalog, err := client.PairedCatalog(attempt, cfg.OKXPairedMaxPairs)
			stopAttempt()
			if err != nil {
				logger.Error("OKX paired catalog unavailable; retaining existing streams", "error", err)
				refresh.Reset(time.Minute)
				continue
			}
			extra := extraPairDefinitions(catalog.Observation.Pairs, owned)
			previousSources, previousPerps := 0, 0
			if active != nil {
				previousSources = len(active.sources)
				previousPerps = len(active.perpetuals)
			}
			capacity, err := pairedCapacity(cfg, legacy, len(catalog.Observation.Pairs), extra, previousSources, previousPerps)
			if err != nil {
				logger.Error("OKX paired capacity rejected", "error", err)
				refresh.Reset(time.Minute)
				continue
			}
			if err = registerPairCatalog(ctx, store, &catalog); err != nil {
				return err
			}
			extra = extraPairDefinitions(catalog.Observation.Pairs, owned)
			key := model.ReferenceHash(catalog.Observation.Pairs)
			if !model.ValidDigest(key) {
				return fmt.Errorf("cannot hash paired instrument definitions")
			}
			if key == fingerprint {
				catalog.Observation.EffectiveMinute = time.Now().UTC().Truncate(time.Minute).Add(time.Minute)
				if err = store.WriteOKXPairCatalog(ctx, catalog.Observation); err != nil {
					return err
				}
				refresh.Reset(cfg.OKXPairedRefresh)
				continue
			}
			staged, err := newPairedGeneration(ctx, cfg, client, extra, worker, logger)
			if err != nil {
				return err
			}
			runBackground(func(ctx context.Context) error {
				select {
				case err := <-staged.errors:
					return err
				case <-staged.ctx.Done():
					return nil
				case <-ctx.Done():
					return nil
				}
			})
			if err = staged.warm(ctx); err != nil {
				staged.stop()
				logger.Error("OKX paired warm-up failed; retaining old generation", "error", err)
				refresh.Reset(time.Minute)
				continue
			}
			effective := time.Now().UTC().Truncate(time.Minute).Add(2 * time.Minute)
			catalog.Observation.EffectiveMinute = effective
			if err = store.WriteOKXPairCatalog(ctx, catalog.Observation); err != nil {
				staged.stop()
				return err
			}
			if engine == nil && len(staged.sources) > 0 {
				queue := cfg.MinuteQueueCapacity
				if queue == 0 {
					queue = max(512, 4*cfg.OKXPairedMaxPairs)
				}
				engine, err = sampler.NewEngine(staged.sources, store, queue, logger)
				if err != nil {
					staged.stop()
					return err
				}
				// Start sampling at the same committed boundary as the catalog, not during
				// warm-up. No sampler exists yet, so no completed minute can be lost.
				e := engine
				runBackground(func(ctx context.Context) error { return e.RunAt(ctx, effective) })
			} else if engine != nil {
				if err = engine.ScheduleSources(staged.sources, effective); err != nil {
					staged.stop()
					return err
				}
			}
			staged.start(func(ctx context.Context) error {
				return runDiscoveryFacts(ctx, client, store, catalog.Observation.Pairs, logger)
			})
			if cfg.FundingEnabled && len(staged.perpetuals) > 0 {
				now := time.Now().UTC().Truncate(time.Millisecond)
				pending, err := store.LoadPendingFundingConfirmations(ctx, now.Add(-funding.StartupBackfillWindow), now)
				if err != nil {
					staged.stop()
					return err
				}
				all, err := store.Instruments(ctx)
				if err != nil {
					staged.stop()
					return err
				}
				if err = funding.ScheduleStartupBackfill(ctx, pending, startupBackfillInstruments(staged.perpetuals, all), map[string]funding.ConfirmationScheduler{"OKX": worker}); err != nil {
					staged.stop()
					return err
				}
			}
			if cfg.FundingEnabled {
				hourly.schedule(staged.perpetuals, staged.estimates, effective)
			}
			logger.Info("OKX paired catalog scheduled", "pairs", len(catalog.Observation.Pairs), "additional_sources", len(extra), "effective_minute", effective, "capacity", capacity)
			if !exchange.Wait(ctx, time.Until(effective.Add(time.Second))) {
				staged.stop()
				if parent.Err() != nil {
					return nil
				}
				select {
				case err := <-errors:
					return err
				default:
					return fmt.Errorf("paired activation interrupted")
				}
			}
			if active != nil {
				active.stop()
			}
			active = staged
			fingerprint = key
			refresh.Reset(cfg.OKXPairedRefresh)
		}
	}
}
func PrintOKXPairedCatalog(ctx context.Context, cfg config.Config, w io.Writer) error {
	c := okx.NewClient()
	c.BaseURL = cfg.OKXREST
	catalog, err := c.PairedCatalog(ctx, cfg.OKXPairedMaxPairs)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	return encoder.Encode(catalog)
}
