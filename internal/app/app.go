package app

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/config"
	"github.com/vphoenix/crypto-market-info/internal/funding"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/optionslive"
	chstore "github.com/vphoenix/crypto-market-info/internal/storage/clickhouse"
	marketyield "github.com/vphoenix/crypto-market-info/internal/yield"
	"github.com/vphoenix/crypto-market-info/internal/yield/aave"
	"github.com/vphoenix/crypto-market-info/internal/yield/ankr"
	"github.com/vphoenix/crypto-market-info/internal/yield/avalanche"
	"github.com/vphoenix/crypto-market-info/internal/yield/benqi"
	"github.com/vphoenix/crypto-market-info/internal/yield/jito"
	"github.com/vphoenix/crypto-market-info/internal/yield/justlend"
	"github.com/vphoenix/crypto-market-info/internal/yield/kamino"
	"github.com/vphoenix/crypto-market-info/internal/yield/marinade"
	"github.com/vphoenix/crypto-market-info/internal/yield/okxearn"
	"github.com/vphoenix/crypto-market-info/internal/yield/save"
	"github.com/vphoenix/crypto-market-info/internal/yield/solana"
	"github.com/vphoenix/crypto-market-info/internal/yield/solvalidator"
	"github.com/vphoenix/crypto-market-info/internal/yield/tron"
)

type component struct {
	name string
	run  func(context.Context) error
}

type yieldCollectorSpec struct {
	name, source string
	collector    marketyield.Collector
}

func Run(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	if logger == nil {
		logger = slog.Default()
	}
	if cfg.DEXEnabled {
		return runIndependentSources(ctx, logger, 10*time.Second,
			component{name: "Ethereum DEX", run: func(ctx context.Context) error {
				store, err := chstore.Open(ctx, cfg.ClickHouse)
				if err != nil {
					return err
				}
				defer store.Close()
				return runDEX(ctx, cfg, store, logger)
			}},
			component{name: "CEX and other sources", run: func(ctx context.Context) error {
				return runMarketSources(ctx, cfg, logger)
			}},
		)
	}
	return runMarketSources(ctx, cfg, logger)
}

// Each source group owns its connection and startup lifecycle. A CEX catalog
// timeout must not cancel DEX live sampling or force a process-wide restart.
func runIndependentSources(ctx context.Context, logger *slog.Logger, retry time.Duration, groups ...component) error {
	var wait sync.WaitGroup
	for _, group := range groups {
		group := group
		wait.Add(1)
		go func() {
			defer wait.Done()
			for ctx.Err() == nil {
				err := group.run(ctx)
				if ctx.Err() != nil {
					return
				}
				logger.Error("collector source group retrying", "group", group.name, "error", err)
				if !dexDelay(ctx, retry) {
					return
				}
			}
		}()
	}
	<-ctx.Done()
	wait.Wait()
	return nil
}

func runMarketSources(ctx context.Context, cfg config.Config, logger *slog.Logger) error {
	clients := newVenueClients(cfg, logger)
	discovery, err := discoverPerpetual(ctx, cfg, clients)
	if err != nil {
		return err
	}
	store, err := chstore.Open(ctx, cfg.ClickHouse)
	if err != nil {
		return err
	}
	defer store.Close()
	if err = store.InitSchema(ctx); err != nil {
		return err
	}
	markets, err := prepareMarkets(ctx, cfg, store, clients, discovery, logger)
	if err != nil {
		return err
	}
	components := markets.components
	if cfg.Options.Enabled {
		components = append(components, component{name: "Deribit options", run: func(ctx context.Context) error {
			return optionslive.Run(ctx, cfg.Options, store, logger)
		}})
	}
	fundingInstruments := markets.fundingInstruments
	if len(fundingInstruments) > 0 {
		estimates := funding.NewEstimateStore()
		scheduler := &funding.Scheduler{Instruments: fundingInstruments, Estimates: estimates, Sink: store, Logger: logger}
		components = append(components, component{name: "funding scheduler", run: scheduler.Run})
		now := time.UnixMilli(time.Now().UTC().UnixMilli()).UTC()
		pending, loadErr := store.LoadPendingFundingConfirmations(ctx, now.Add(-funding.StartupBackfillWindow), now)
		if loadErr != nil {
			return fmt.Errorf("load pending funding confirmations: %w", loadErr)
		}
		queueCapacity := max(4096, len(pending)+1)
		confirmationWorkers := make(map[string]funding.ConfirmationScheduler, len(cfg.PerpetualSelection.Venues))
		specs, err := venueSpecs(cfg, clients)
		if err != nil {
			return err
		}
		for _, spec := range specs {
			instruments := markets.byVenue[spec.selection.Venue]
			if len(instruments) == 0 {
				continue
			}
			worker := &funding.ConfirmationWorker{Exchange: spec.selection.Venue, Provider: spec.provider, Sink: store, QueueCapacity: queueCapacity, Logger: logger}
			confirmationWorkers[spec.selection.Venue] = worker
			runtime, err := spec.buildFunding(instruments, estimates, worker, logger)
			if err != nil {
				return err
			}
			components = append(components, component{name: spec.selection.Venue + " funding confirmation", run: worker.Run}, component{name: spec.selection.Venue + " funding websocket", run: runtime.run})
			markets.health[spec.selection.Venue+" funding"] = runtime.health
		}
		storedInstruments, loadErr := store.Instruments(ctx)
		if loadErr != nil {
			return fmt.Errorf("load instruments for funding backfill: %w", loadErr)
		}
		backfillInstruments := startupBackfillInstruments(fundingInstruments, storedInstruments)
		if err = funding.ScheduleStartupBackfill(ctx, pending, backfillInstruments, confirmationWorkers); err != nil {
			return err
		}
	}
	if yieldEnabled(cfg) {
		if err = store.InitYieldRegistry(ctx); err != nil {
			return err
		}
	}
	if cfg.JustLendYieldEnabled {
		collector := &justlend.Collector{Client: justlend.NewClient(cfg.JustLendBaseURL)}
		runner := &marketyield.Runner{Source: "justlend", Collector: collector, Sink: store, Interval: time.Hour, RetryInterval: 10 * time.Minute, Logger: logger}
		components = append(components, component{name: "JustLend yield", run: runner.Run})
	}
	if cfg.TRONStakingYieldEnabled {
		collector := &tron.Collector{Client: tron.NewClient(cfg.TRONHTTPURL)}
		runner := &marketyield.Runner{Source: "tron-native-staking", Collector: collector, Sink: store, Interval: 6 * time.Hour, RetryInterval: 10 * time.Minute, Logger: logger}
		components = append(components, component{name: "TRON staking yield", run: runner.Run})
	}
	if cfg.SOLYieldEnabled {
		rpcClient := solana.NewClient(cfg.SolanaRPCURL)
		poolReader := &solana.Reader{Client: rpcClient}
		for _, item := range solYieldCollectors(cfg, rpcClient, poolReader) {
			runner := &marketyield.Runner{Source: item.source, Collector: item.collector, Sink: store, Interval: 6 * time.Hour, RetryInterval: 10 * time.Minute, Logger: logger}
			components = append(components, component{name: item.name, run: runner.Run})
		}
		for _, voteAccount := range cfg.SOLValidatorVoteAccounts {
			collector, collectorErr := solvalidator.NewCollector(cfg.MarinadeValidatorsBaseURL, voteAccount)
			if collectorErr != nil {
				return collectorErr
			}
			runner := &marketyield.Runner{Source: "solana-validator:" + voteAccount, Collector: collector, Sink: store, Interval: 6 * time.Hour, RetryInterval: 10 * time.Minute, Logger: logger}
			components = append(components, component{name: "SOL validator " + voteAccount, run: runner.Run})
		}
	}
	if cfg.AVAXYieldEnabled {
		for _, item := range avaxYieldCollectors(cfg) {
			runner := &marketyield.Runner{Source: item.source, Collector: item.collector, Sink: store, Interval: time.Hour, RetryInterval: 10 * time.Minute, Logger: logger}
			components = append(components, component{name: item.name, run: runner.Run})
		}
	}
	if err = commitPerpetualRun(ctx, store, discovery, markets, logger); err != nil {
		return err
	}
	return runComponents(ctx, components)
}

func yieldEnabled(cfg config.Config) bool {
	return cfg.JustLendYieldEnabled || cfg.TRONStakingYieldEnabled || cfg.SOLYieldEnabled || cfg.AVAXYieldEnabled
}

func avaxYieldCollectors(cfg config.Config) []yieldCollectorSpec {
	rpc := avalanche.NewClient(cfg.AvalancheRPCURL)
	return []yieldCollectorSpec{
		{name: "OKX AVAX earn yield", source: "okx-avax-flexible", collector: okxearn.NewCollector(cfg.OKXREST)},
		{name: "Aave V3 AVAX yield", source: "aave-v3-avax", collector: aave.NewV3Collector("")},
		{name: "Aave V4 AVAX yield", source: "aave-v4-avax", collector: aave.NewV4Collector("")},
		{name: "BENQI sAVAX yield", source: "benqi-savax", collector: benqi.NewStakingCollector(rpc)},
		{name: "Ankr ankrAVAX yield", source: "ankr-ankravax", collector: ankr.NewCollector(rpc)},
		{name: "BENQI AVAX lending yield", source: "benqi-avax-lending", collector: benqi.NewLendingCollector(rpc)},
	}
}

func solYieldCollectors(cfg config.Config, rpcClient *solana.Client, poolReader *solana.Reader) []yieldCollectorSpec {
	return []yieldCollectorSpec{
		{name: "bSOL yield", source: "solana-stakepool-bsol", collector: &solana.BSOLCollector{Reader: poolReader}},
		{name: "laineSOL yield", source: "solana-stakepool-lainesol", collector: &solana.StakePoolCollector{Reader: poolReader, Product: solana.LaineSOLProduct}},
		{name: "JupSOL yield", source: "solana-stakepool-jupsol", collector: &solana.StakePoolCollector{Reader: poolReader, Product: solana.JupSOLProduct}},
		{name: "hSOL yield", source: "solana-stakepool-hsol", collector: &solana.StakePoolCollector{Reader: poolReader, Product: solana.HSOLProduct}},
		{name: "JitoSOL yield", source: "jitosol", collector: jito.NewCollector(cfg.JitoSOLBaseURL, poolReader)},
		{name: "mSOL yield", source: "marinade-msol", collector: marinade.NewMSOLCollector(cfg.MarinadeAPYBaseURL, rpcClient)},
		{name: "Marinade Native yield", source: "marinade-native", collector: marinade.NewNativeCollector(cfg.MarinadeAPYBaseURL)},
		{name: "Kamino Main SOL yield", source: "kamino-main-sol", collector: kamino.NewCollector(cfg.KaminoBaseURL, rpcClient)},
		{name: "Save Main SOL yield", source: "save-main-sol", collector: save.NewCollector(cfg.SaveBaseURL, rpcClient)},
	}
}

func runComponents(ctx context.Context, components []component) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errors := make(chan error, len(components))
	var wait sync.WaitGroup
	for _, item := range components {
		item := item
		wait.Add(1)
		go func() {
			defer wait.Done()
			if runErr := item.run(runCtx); runErr != nil {
				errors <- fmt.Errorf("%s: %w", item.name, runErr)
			}
		}()
	}
	select {
	case <-ctx.Done():
		cancel()
		wait.Wait()
		return nil
	case runErr := <-errors:
		cancel()
		wait.Wait()
		return runErr
	}
}

func selectSymbols(exchange string, available []model.Instrument, symbols []string) ([]model.Instrument, error) {
	bySymbol := make(map[string]model.Instrument, len(available))
	for _, item := range available {
		bySymbol[item.ExchangeSymbol] = item
	}
	out := make([]model.Instrument, 0, len(symbols))
	seen := make(map[string]struct{}, len(symbols))
	for _, symbol := range symbols {
		if _, ok := seen[symbol]; ok {
			return nil, fmt.Errorf("duplicate %s symbol %q", exchange, symbol)
		}
		seen[symbol] = struct{}{}
		item, ok := bySymbol[symbol]
		if !ok {
			return nil, fmt.Errorf("configured %s symbol %q is not a supported live instrument", exchange, symbol)
		}
		out = append(out, item)
	}
	return out, nil
}

type fundingRoute struct {
	exchange string
	market   model.MarketType
	symbol   string
}

func startupBackfillInstruments(current, stored []model.Instrument) []model.Instrument {
	currentRoutes := make(map[fundingRoute]model.Instrument, len(current))
	result := make([]model.Instrument, 0, len(current)+len(stored))
	seenIDs := make(map[uint32]struct{}, len(current)+len(stored))
	for _, instrument := range current {
		if instrument.ID == 0 || instrument.MarketType != model.MarketPerpetual {
			continue
		}
		route := fundingRoute{exchange: instrument.Exchange, market: instrument.MarketType, symbol: instrument.ExchangeSymbol}
		currentRoutes[route] = instrument
		result = append(result, instrument)
		seenIDs[instrument.ID] = struct{}{}
	}
	for _, instrument := range stored {
		if instrument.ID == 0 || instrument.MarketType != model.MarketPerpetual {
			continue
		}
		if _, exists := seenIDs[instrument.ID]; exists {
			continue
		}
		route := fundingRoute{exchange: instrument.Exchange, market: instrument.MarketType, symbol: instrument.ExchangeSymbol}
		active, exists := currentRoutes[route]
		if !exists || !sameFundingAssets(active, instrument) {
			continue
		}
		result = append(result, instrument)
		seenIDs[instrument.ID] = struct{}{}
	}
	return result
}

func sameFundingAssets(left, right model.Instrument) bool {
	if left.BaseAsset != right.BaseAsset || left.QuoteAsset != right.QuoteAsset {
		return false
	}
	if left.SettleAsset == nil || right.SettleAsset == nil {
		return left.SettleAsset == nil && right.SettleAsset == nil
	}
	return *left.SettleAsset == *right.SettleAsset
}
