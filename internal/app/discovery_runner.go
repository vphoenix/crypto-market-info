package app

import (
	"context"
	"errors"
	"fmt"
	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/exchange"
	"github.com/vphoenix/crypto-market-info/internal/exchange/okx"
	"github.com/vphoenix/crypto-market-info/internal/model"
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
	pendingCapital   *model.CapitalParameters
}

type discoveryPairClient interface {
	DiscoveryMarketFacts(context.Context, model.Instrument) (model.FundingPrediction, model.TradeBar, error)
	DiscoveryCapital(context.Context, model.Instrument, string, decimal.Decimal) (model.CapitalParameters, error)
	DiscoveryReferenceFees(context.Context, model.OKXPairMember, decimal.Decimal) (model.ReferenceFee, error)
}

type discoveryPairStore interface {
	WriteDiscoveryFact(context.Context, model.DiscoveryFact) error
	WriteDiscoveryCapital(context.Context, model.CapitalParameters) error
}

func collectDiscoveryPair(ctx context.Context, client discoveryPairClient, store discoveryPairStore, state *discoveryPairState) error {
	// Market observations do not depend on a supported reference fee profile.
	// Rules retain their own failure/schedule; neither side fabricates the other.
	marketErr := collectDiscoveryMarket(ctx, client, store, state.pair.Perpetual)
	ruleErr := collectDiscoveryRules(ctx, client, store, state)
	return errors.Join(marketErr, ruleErr)
}

func collectDiscoveryMarket(ctx context.Context, client discoveryPairClient, store discoveryPairStore, instrument model.Instrument) error {
	forecast, bar, err := client.DiscoveryMarketFacts(ctx, instrument)
	// Each returned observation can be complete while the other source failed.
	// Source times/hashes are already fixed; preserve each independent fact.
	failures := []error{err}
	facts := make([]model.DiscoveryFact, 0, 2)
	if err == nil || forecast.ID != "" {
		facts = append(facts, forecast)
	}
	if err == nil || bar.ID != "" {
		facts = append(facts, bar)
	}
	if batchStore, ok := store.(interface {
		WriteDiscoveryFacts(context.Context, []model.DiscoveryFact) error
	}); ok && len(facts) > 1 {
		if batchStore.WriteDiscoveryFacts(ctx, facts) == nil {
			return errors.Join(failures...)
		}
	}
	// A failed group must not suppress its independently valid sibling. Retry
	// the exact observations; a successful/unknown prior commit stays idempotent.
	for _, fact := range facts {
		if writeErr := store.WriteDiscoveryFact(ctx, fact); writeErr != nil {
			failures = append(failures, writeErr)
		}
	}
	return errors.Join(failures...)
}

func collectDiscoveryRules(ctx context.Context, client discoveryPairClient, store discoveryPairStore, state *discoveryPairState) error {
	now := time.Now().UTC()
	// Failure never advances the schedule or creates a fresh row/commit.
	if state.pendingCapital != nil || state.capitalAt.IsZero() || now.Sub(state.capitalAt) >= 30*time.Minute {
		if state.pendingCapital == nil {
			parameters, err := client.DiscoveryCapital(ctx, state.pair.Perpetual, discoveryCapitalHash, decimal.NewFromInt(5))
			if err != nil {
				return fmt.Errorf("capital: %w", err)
			}
			// Reject an unrepresentable source before freezing a retry. Once a
			// writer has seen it, an unknown commit must retain its original ID.
			if err := parameters.Validate(); err != nil {
				return fmt.Errorf("capital: %w", err)
			}
			state.pendingCapital = &parameters
		}
		parameters := *state.pendingCapital
		limit := decimal.Zero
		for _, tier := range parameters.PerpTiers {
			if tier.MaximumLeverage.GreaterThanOrEqual(decimal.NewFromInt(5)) {
				limit = tier.Maximum
			}
		}
		if !limit.IsPositive() {
			state.pendingCapital = nil
			return fmt.Errorf("perpetual has no 5x risk tier")
		}
		if err := store.WriteDiscoveryCapital(ctx, parameters); err != nil {
			return err
		}
		state.positionLimit = limit
		state.capitalAt = time.UnixMilli(parameters.ObservedMS).UTC()
		state.pendingCapital = nil
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
	return nil
}

// Eight workers share the client's public request gate. Each owns disjoint
// pair state, bounds memory and cancellation, and isolates a bad coin. Only
// registered, published catalog pairs are observed; no private client exists.
func runDiscoveryFacts(ctx context.Context, client *okx.Client, store discoveryPairStore, pairs []model.OKXPairMember, logger *slog.Logger) error {
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
