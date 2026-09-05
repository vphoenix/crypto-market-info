package app

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/config"
	"github.com/vphoenix/crypto-market-info/internal/exchange"
	"github.com/vphoenix/crypto-market-info/internal/exchange/binance"
	"github.com/vphoenix/crypto-market-info/internal/exchange/bybit"
	"github.com/vphoenix/crypto-market-info/internal/exchange/okx"
	"github.com/vphoenix/crypto-market-info/internal/funding"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/orderbook"
	"github.com/vphoenix/crypto-market-info/internal/sampler"
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
	store, err := chstore.Open(ctx, cfg.ClickHouse)
	if err != nil {
		return err
	}
	defer store.Close()
	if err = store.InitSchema(ctx); err != nil {
		return err
	}
	binanceClient := binance.NewClient()
	binanceClient.SpotBaseURL = cfg.BinanceSpotREST
	binanceClient.FuturesBaseURL = cfg.BinanceFuturesREST
	okxClient := okx.NewClient()
	okxClient.BaseURL = cfg.OKXREST
	bybitClient := bybit.NewClient()
	bybitClient.BaseURL = cfg.BybitREST
	bybitClient.Logger = logger
	// OKX counts connection attempts across public websocket channels. Sharing one
	// gate prevents book and funding runtimes from creating a reconnect burst.
	okxConnectGate := exchange.NewRequestGate(500 * time.Millisecond)
	bybitConnectGate := exchange.NewRequestGate(time.Second)
	if connections := bybitWebsocketConnections(cfg.BybitPerpSymbols, cfg.FundingEnabled); connections > 1000 {
		return fmt.Errorf("Bybit linear websocket connection budget exceeded: %d > 1000", connections)
	}
	type target struct {
		definition model.Instrument
		ws         string
	}
	var targets []target
	load := func(exchangeName string, market model.MarketType, symbols []string, ws string, fetch func(context.Context, model.MarketType) ([]model.Instrument, error)) error {
		if len(symbols) == 0 {
			return nil
		}
		available, fetchErr := fetch(ctx, market)
		if fetchErr != nil {
			return fetchErr
		}
		selected, selectErr := selectSymbols(exchangeName, available, symbols)
		if selectErr != nil {
			return selectErr
		}
		for _, item := range selected {
			targets = append(targets, target{definition: item, ws: ws})
		}
		return nil
	}
	if err = load("Binance", model.MarketSpot, cfg.BinanceSpotSymbols, cfg.BinanceSpotWS, binanceClient.Instruments); err != nil {
		return err
	}
	if err = load("Binance", model.MarketPerpetual, cfg.BinancePerpSymbols, cfg.BinanceFuturesWS, binanceClient.Instruments); err != nil {
		return err
	}
	if err = load("OKX", model.MarketSpot, cfg.OKXSpotSymbols, cfg.OKXWS, okxClient.Instruments); err != nil {
		return err
	}
	if err = load("OKX", model.MarketPerpetual, cfg.OKXPerpSymbols, cfg.OKXWS, okxClient.Instruments); err != nil {
		return err
	}
	if err = load("Bybit", model.MarketPerpetual, cfg.BybitPerpSymbols, cfg.BybitWS, bybitClient.Instruments); err != nil {
		return err
	}
	if len(targets) == 0 && !yieldEnabled(cfg) {
		return fmt.Errorf("no instruments are configured")
	}
	definitions := make([]model.Instrument, len(targets))
	for index := range targets {
		definitions[index] = targets[index].definition
	}
	registered, err := store.RegisterInstruments(ctx, definitions)
	if err != nil {
		return err
	}
	var components []component
	sampleSources := make([]sampler.Source, 0, len(targets))
	fundingInstruments := make([]model.Instrument, 0, len(targets))
	binanceFundingInstruments := make([]model.Instrument, 0, len(targets))
	okxFundingInstruments := make([]model.Instrument, 0, len(targets))
	bybitFundingInstruments := make([]model.Instrument, 0, len(targets))
	for index, target := range targets {
		instrument := registered[index]
		retained := 400
		if instrument.Exchange == "Binance" || instrument.Exchange == "Bybit" {
			retained = 1000
		}
		book, bookErr := orderbook.New(instrument.ID, retained)
		if bookErr != nil {
			return bookErr
		}
		sampleSources = append(sampleSources, sampler.Source{InstrumentID: instrument.ID, Book: book})
		switch instrument.Exchange {
		case "Binance":
			runtime := &binance.Runtime{Instrument: instrument, Book: book, Client: binanceClient, WSEndpoint: target.ws, Logger: logger}
			components = append(components, component{name: "binance " + instrument.ExchangeSymbol, run: runtime.Run})
			if cfg.FundingEnabled && instrument.MarketType == model.MarketPerpetual {
				fundingInstruments = append(fundingInstruments, instrument)
				binanceFundingInstruments = append(binanceFundingInstruments, instrument)
			}
		case "OKX":
			runtime := &okx.Runtime{Instrument: instrument, Book: book, WSEndpoint: target.ws, ConnectGate: okxConnectGate, Logger: logger}
			components = append(components, component{name: "okx " + instrument.ExchangeSymbol, run: runtime.Run})
			if cfg.FundingEnabled && instrument.MarketType == model.MarketPerpetual {
				fundingInstruments = append(fundingInstruments, instrument)
				okxFundingInstruments = append(okxFundingInstruments, instrument)
			}
		case "Bybit":
			runtime := &bybit.Runtime{Instrument: instrument, Book: book, WSEndpoint: target.ws, ConnectGate: bybitConnectGate, Logger: logger}
			components = append(components, component{name: "bybit " + instrument.ExchangeSymbol, run: runtime.Run})
			if cfg.FundingEnabled && instrument.MarketType == model.MarketPerpetual {
				fundingInstruments = append(fundingInstruments, instrument)
				bybitFundingInstruments = append(bybitFundingInstruments, instrument)
			}
		default:
			return fmt.Errorf("unsupported registered exchange %q", instrument.Exchange)
		}
	}
	if len(sampleSources) > 0 {
		sampleEngine, sampleErr := sampler.NewEngine(sampleSources, store, cfg.MinuteQueueCapacity, logger)
		if sampleErr != nil {
			return sampleErr
		}
		components = append(components, component{name: "second sampler", run: sampleEngine.Run})
	}
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
		confirmationWorkers := make(map[string]funding.ConfirmationScheduler, 3)
		if len(binanceFundingInstruments) > 0 {
			worker := &funding.ConfirmationWorker{Exchange: "Binance", Provider: binanceClient, Sink: store, QueueCapacity: queueCapacity, Logger: logger}
			confirmationWorkers["Binance"] = worker
			runtime := &binance.FundingRuntime{Instruments: binanceFundingInstruments, Estimates: estimates, Confirmations: worker, WSEndpoint: cfg.BinanceMarketWS, Logger: logger}
			components = append(components,
				component{name: "Binance funding confirmation", run: worker.Run},
				component{name: "Binance funding websocket", run: runtime.Run},
			)
		}
		if len(okxFundingInstruments) > 0 {
			worker := &funding.ConfirmationWorker{Exchange: "OKX", Provider: okxClient, Sink: store, QueueCapacity: queueCapacity, Logger: logger}
			confirmationWorkers["OKX"] = worker
			runtime := &okx.FundingRuntime{Instruments: okxFundingInstruments, Estimates: estimates, Confirmations: worker, WSEndpoint: cfg.OKXWS, ConnectGate: okxConnectGate, Logger: logger}
			components = append(components,
				component{name: "OKX funding confirmation", run: worker.Run},
				component{name: "OKX funding websocket", run: runtime.Run},
			)
		}
		if len(bybitFundingInstruments) > 0 {
			worker := &funding.ConfirmationWorker{Exchange: "Bybit", Provider: bybitClient, Sink: store, QueueCapacity: queueCapacity, Logger: logger}
			confirmationWorkers["Bybit"] = worker
			runtime := &bybit.FundingRuntime{Instruments: bybitFundingInstruments, Estimates: estimates, Confirmations: worker, WSEndpoint: cfg.BybitWS, ConnectGate: bybitConnectGate, Logger: logger}
			components = append(components,
				component{name: "Bybit funding confirmation", run: worker.Run},
				component{name: "Bybit funding websocket", run: runtime.Run},
			)
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

func bybitWebsocketConnections(symbols []string, fundingEnabled bool) int {
	connections := len(symbols)
	if fundingEnabled && len(symbols) > 0 {
		connections++
	}
	return connections
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
