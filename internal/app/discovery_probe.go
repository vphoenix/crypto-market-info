package app

import (
	"context"
	"fmt"
	"github.com/vphoenix/crypto-market-info/internal/config"
	"github.com/vphoenix/crypto-market-info/internal/exchange"
	"github.com/vphoenix/crypto-market-info/internal/exchange/okx"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/sampler"
	chstore "github.com/vphoenix/crypto-market-info/internal/storage/clickhouse"
	"log/slog"
	"strings"
	"time"
)

// Public integration capture writes only an isolated operator-named database.
// It never reconstructs historical commit metadata or touches an account.
func RunDiscoveryProbe(ctx context.Context, cfg config.Config, assets []string, logger *slog.Logger) error {
	if !strings.HasPrefix(cfg.ClickHouse.Database, "discovery_validation_") {
		return fmt.Errorf("probe requires discovery_validation_ database prefix")
	}
	client := okx.NewClient()
	client.BaseURL = cfg.OKXREST
	catalog, err := client.PairedCatalog(ctx, 500)
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
	if err = store.InitOKXPairSchema(ctx); err != nil {
		return err
	}
	if err = registerPairCatalog(ctx, store, &catalog); err != nil {
		return err
	}
	wanted := map[string]bool{}
	for _, a := range assets {
		if a == "" || wanted[a] {
			return fmt.Errorf("invalid probe asset")
		}
		wanted[a] = true
	}
	chosen := []model.OKXPairMember{}
	extra := []model.Instrument{}
	for _, p := range catalog.Observation.Pairs {
		if wanted[p.Base] {
			chosen = append(chosen, p)
			extra = append(extra, p.Spot, p.Perpetual)
			delete(wanted, p.Base)
		}
	}
	if len(wanted) > 0 || len(chosen) == 0 {
		return fmt.Errorf("probe assets are not eligible catalog pairs")
	}
	cfg.FundingEnabled = false
	cfg.MarketBookDepth = 5
	generation, err := newPairedGeneration(ctx, cfg, client, extra, nil, logger)
	if err != nil {
		return err
	}
	defer generation.stop()
	if err = generation.warm(ctx); err != nil {
		return err
	}
	effective := time.Now().UTC().Truncate(time.Minute).Add(time.Minute)
	catalog.Observation.EffectiveMinute = effective
	if err = store.WriteOKXPairCatalog(ctx, catalog.Observation); err != nil {
		return err
	}
	engine, err := sampler.NewEngine(generation.sources, store, 64, logger)
	if err != nil {
		return err
	}
	capture, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- engine.RunAt(capture, effective) }()
	generation.start(func(ctx context.Context) error { return runDiscoveryFacts(ctx, client, store, chosen, logger) })
	logger.Info("isolated public discovery capture started", "database", cfg.ClickHouse.Database, "pairs", len(chosen), "minute", effective)
	if !exchange.Wait(ctx, time.Until(effective.Add(time.Minute+3*time.Second))) {
		cancel()
		<-done
		return ctx.Err()
	}
	cancel()
	if err = <-done; err != nil {
		return err
	}
	generation.stop()
	policy, err := client.LoanPolicy(ctx)
	if err != nil {
		return err
	}
	if err = store.WriteOKXLoanPolicy(ctx, policy); err != nil {
		return err
	}
	// Observe the now-completed minute only after the boundary; do not backdate
	// a candle from the preceding minute to make the final scan appear complete.
	for _, pair := range chosen {
		state := discoveryPairState{pair: pair}
		if err = collectDiscoveryPair(ctx, client, store, &state); err != nil {
			return err
		}
	}
	logger.Info("isolated public discovery capture completed", "health", engine.HealthSnapshot(), "orders_sent", 0)
	return nil
}
