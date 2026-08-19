package app

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/config"
	"github.com/vphoenix/crypto-market-info/internal/exchange/binance"
	"github.com/vphoenix/crypto-market-info/internal/exchange/okx"
	"github.com/vphoenix/crypto-market-info/internal/funding"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/orderbook"
	"github.com/vphoenix/crypto-market-info/internal/sampler"
	chstore "github.com/vphoenix/crypto-market-info/internal/storage/clickhouse"
)

type component struct {
	name string
	run  func(context.Context) error
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
	if len(targets) == 0 {
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
	for index, target := range targets {
		instrument := registered[index]
		retained := 400
		if instrument.Exchange == "Binance" {
			retained = 1000
		}
		book, bookErr := orderbook.New(instrument.ID, retained)
		if bookErr != nil {
			return bookErr
		}
		sampleSources = append(sampleSources, sampler.Source{InstrumentID: instrument.ID, Book: book})
		if instrument.Exchange == "Binance" {
			runtime := &binance.Runtime{Instrument: instrument, Book: book, Client: binanceClient, WSEndpoint: target.ws, Logger: logger}
			components = append(components, component{name: "binance " + instrument.ExchangeSymbol, run: runtime.Run})
			if cfg.FundingEnabled && instrument.MarketType == model.MarketPerpetual {
				fundingInstruments = append(fundingInstruments, instrument)
				binanceFundingInstruments = append(binanceFundingInstruments, instrument)
			}
		} else {
			runtime := &okx.Runtime{Instrument: instrument, Book: book, WSEndpoint: target.ws, Logger: logger}
			components = append(components, component{name: "okx " + instrument.ExchangeSymbol, run: runtime.Run})
			if cfg.FundingEnabled && instrument.MarketType == model.MarketPerpetual {
				fundingInstruments = append(fundingInstruments, instrument)
				okxFundingInstruments = append(okxFundingInstruments, instrument)
			}
		}
	}
	sampleEngine, err := sampler.NewEngine(sampleSources, store, cfg.MinuteQueueCapacity, logger)
	if err != nil {
		return err
	}
	components = append(components, component{name: "second sampler", run: sampleEngine.Run})
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
		confirmationWorkers := make(map[string]funding.ConfirmationScheduler, 2)
		if len(binanceFundingInstruments) > 0 {
			worker := &funding.ConfirmationWorker{Exchange: "Binance", Provider: binanceClient, Sink: store, QueueCapacity: queueCapacity, Logger: logger}
			confirmationWorkers["Binance"] = worker
			runtime := &binance.FundingRuntime{Instruments: binanceFundingInstruments, Estimates: estimates, Confirmations: worker, WSEndpoint: cfg.BinanceFuturesWS, Logger: logger}
			components = append(components,
				component{name: "Binance funding confirmation", run: worker.Run},
				component{name: "Binance funding websocket", run: runtime.Run},
			)
		}
		if len(okxFundingInstruments) > 0 {
			worker := &funding.ConfirmationWorker{Exchange: "OKX", Provider: okxClient, Sink: store, QueueCapacity: queueCapacity, Logger: logger}
			confirmationWorkers["OKX"] = worker
			runtime := &okx.FundingRuntime{Instruments: okxFundingInstruments, Estimates: estimates, Confirmations: worker, WSEndpoint: cfg.OKXWS, Logger: logger}
			components = append(components,
				component{name: "OKX funding confirmation", run: worker.Run},
				component{name: "OKX funding websocket", run: runtime.Run},
			)
		}
		if err = funding.ScheduleStartupBackfill(ctx, pending, registered, confirmationWorkers); err != nil {
			return err
		}
	}
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
