package app

import (
	"context"
	"fmt"
	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/exchange"
	"github.com/vphoenix/crypto-market-info/internal/exchange/okx"
	"github.com/vphoenix/crypto-market-info/internal/model"
	chstore "github.com/vphoenix/crypto-market-info/internal/storage/clickhouse"
	"log/slog"
	"sync"
	"time"
)

// This digest identifies a reviewed rule family, not a permission to trade.
// The trading registry separately pins the calculator and review evidence.
const discoveryCapitalHash = "c9c2e0332ef50ce53f3fbe193dc865a0f34e32289ddd90d79ab80370cd1f0787"

type discoveryPairState struct {
	pair             model.OKXPairMember
	capitalAt, feeAt time.Time
	positionLimit    decimal.Decimal
}

func collectDiscoveryPair(ctx context.Context, client *okx.Client, store *chstore.Client, state *discoveryPairState) error {
	now := time.Now().UTC()
	// Failure never advances the schedule or creates a fresh row/commit.
	if state.capitalAt.IsZero() || now.Sub(state.capitalAt) >= 30*time.Minute {
		parameters, err := client.DiscoveryCapital(ctx, state.pair.Perpetual, discoveryCapitalHash, decimal.NewFromInt(5))
		if err != nil {
			return fmt.Errorf("capital: %w", err)
		}
		limit := decimal.Zero
		for _, tier := range parameters.PerpTiers {
			if tier.MaximumLeverage.GreaterThanOrEqual(decimal.NewFromInt(5)) {
				limit = tier.Maximum
			}
		}
		if !limit.IsPositive() {
			return fmt.Errorf("perpetual has no 5x risk tier")
		}
		if err = store.WriteDiscoveryCapital(ctx, parameters); err != nil {
			return err
		}
		state.positionLimit = limit
		state.capitalAt = now
	}
	if state.feeAt.IsZero() || now.Sub(state.feeAt) >= 6*time.Hour {
		fee, err := client.DiscoveryReferenceFees(ctx, state.pair, state.positionLimit)
		if err != nil {
			return fmt.Errorf("reference fee: %w", err)
		}
		if err = store.WriteDiscoveryFact(ctx, fee); err != nil {
			return err
		}
		state.feeAt = now
	}
	forecast, bar, err := client.DiscoveryMarketFacts(ctx, state.pair.Perpetual)
	if err != nil {
		return err
	}
	if err = store.WriteDiscoveryFact(ctx, forecast); err != nil {
		return err
	}
	return store.WriteDiscoveryFact(ctx, bar)
}

// Eight workers share the client's public request gate. Each owns disjoint
// pair state, bounds memory and cancellation, and isolates a bad coin. Only
// registered, published catalog pairs are observed; no private client exists.
func runDiscoveryFacts(ctx context.Context, client *okx.Client, store *chstore.Client, pairs []model.OKXPairMember, logger *slog.Logger) error {
	workers := min(8, len(pairs))
	var wait sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		states := []discoveryPairState{}
		for n := worker; n < len(pairs); n += workers {
			states = append(states, discoveryPairState{pair: pairs[n]})
		}
		wait.Add(1)
		go func(states []discoveryPairState) {
			defer wait.Done()
			for ctx.Err() == nil {
				start := time.Now()
				for n := range states {
					if ctx.Err() != nil {
						return
					}
					attempt, cancel := context.WithTimeout(ctx, 60*time.Second)
					err := collectDiscoveryPair(attempt, client, store, &states[n])
					cancel()
					if err != nil && ctx.Err() == nil {
						logger.Warn("OKX discovery observation rejected", "base", states[n].pair.Base, "error", err)
					}
				}
				if !exchange.Wait(ctx, max(0, 20*time.Second-time.Since(start))) {
					return
				}
			}
		}(states)
	}
	<-ctx.Done()
	wait.Wait()
	return nil
}
