package app

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/model"
)

type factClient struct {
	forecast                      model.FundingPrediction
	bar                           model.TradeBar
	marketErr, feeErr, capitalErr error
}

func (c *factClient) DiscoveryMarketFacts(context.Context, model.Instrument) (model.FundingPrediction, model.TradeBar, error) {
	return c.forecast, c.bar, c.marketErr
}
func (c *factClient) DiscoveryCapital(context.Context, model.Instrument, string, decimal.Decimal) (model.CapitalParameters, error) {
	return model.CapitalParameters{}, c.capitalErr
}
func (c *factClient) DiscoveryReferenceFees(context.Context, model.OKXPairMember, decimal.Decimal) (model.ReferenceFee, error) {
	return model.ReferenceFee{}, c.feeErr
}

type factStore struct {
	facts   []model.DiscoveryFact
	failure error
}

func (s *factStore) WriteDiscoveryFact(_ context.Context, f model.DiscoveryFact) error {
	if s.failure != nil {
		return s.failure
	}
	if err := f.Validate(); err != nil {
		return err
	}
	s.facts = append(s.facts, f)
	return nil
}
func (s *factStore) WriteDiscoveryCapital(context.Context, model.CapitalParameters) error {
	return errors.New("unexpected capital write")
}

func marketFacts() (model.FundingPrediction, model.TradeBar) {
	ms := time.Date(2026, 10, 9, 8, 0, 5, 0, time.UTC).UnixMilli()
	sources := []model.PublicSource{{URL: "https://example.test/public", PayloadHash: model.DiscoveryHash("source"), SourceMS: ms, ObservedMS: ms, TimeBasis: "exchange_ts"}}
	f := model.FundingPrediction{ID: "prediction", InstrumentID: 1, ObservedMS: ms, SourceMS: ms, FundingMS: ms + 3600000,
		Rate: decimal.RequireFromString("-0.001"), Mark: decimal.NewFromInt(100), MarkMS: ms, BaseIndex: decimal.NewFromInt(100), BaseUSDIndex: decimal.NewFromInt(100), QuoteUSDIndex: decimal.NewFromInt(1), BaseIndexMS: ms, QuoteIndexMS: ms, Sources: sources}
	b := model.TradeBar{ID: "bar", InstrumentID: 1, ObservedMS: ms, SourceMS: ms, MinuteMS: (ms/60000 - 1) * 60000, BaseVolume: decimal.NewFromInt(10), Final: true, ConversionHash: model.DiscoveryHash("conversion"), Sources: sources}
	return f, b
}

func TestIndependentMarketFactsSurviveUnconfirmedBarAndUnknownRules(t *testing.T) {
	f, b := marketFacts()
	barPending := errors.New("previous complete minute unavailable")
	ruleUnknown := errors.New("public Standard fee group unsupported")
	for _, fault := range []string{"bar_pending", "fee_unknown", "capital_unknown", "forecast_absent", "forecast_failed_volume_valid", "invalid_forecast", "storage_failure"} {
		t.Run(fault, func(t *testing.T) {
			client := &factClient{forecast: f, bar: b}
			store := &factStore{}
			state := discoveryPairState{capitalAt: time.Now(), feeAt: time.Now(), positionLimit: decimal.NewFromInt(1)}
			want := 2
			switch fault {
			case "bar_pending":
				client.bar = model.TradeBar{}
				client.marketErr = barPending
				want = 1
			case "fee_unknown":
				state.feeAt = time.Time{}
				client.feeErr = ruleUnknown
			case "capital_unknown":
				state.capitalAt = time.Time{}
				client.capitalErr = ruleUnknown
			case "forecast_absent":
				client.forecast = model.FundingPrediction{}
				client.bar = model.TradeBar{}
				client.marketErr = errors.New("funding RPC failed")
				want = 0
			case "invalid_forecast":
				client.forecast.Mark = decimal.Zero
				client.bar = model.TradeBar{}
				client.marketErr = barPending
				want = 0
			case "forecast_failed_volume_valid":
				client.forecast = model.FundingPrediction{}
				client.marketErr = errors.New("funding RPC failed")
				want = 1
			case "storage_failure":
				store.failure = errors.New("storage failed")
				want = 0
			}
			err := collectDiscoveryPair(context.Background(), client, store, &state)
			if err == nil || len(store.facts) != want {
				t.Fatalf("fault=%s err=%v saved=%v", fault, err, store.facts)
			}
			first := model.DiscoveryFact(f)
			if fault == "forecast_failed_volume_valid" {
				first = b
			}
			if want > 0 && !reflect.DeepEqual(store.facts[0], first) {
				t.Fatal("independent forecast identity/time/source changed")
			}
			if want == 2 && !reflect.DeepEqual(store.facts[1], b) {
				t.Fatal("complete volume lost or changed")
			}
			if fault == "bar_pending" {
				client.marketErr = nil
				client.bar = b
				client.forecast.ID = "next-prediction"
				if err = collectDiscoveryPair(context.Background(), client, store, &state); err != nil || len(store.facts) != 3 {
					t.Fatal("bar did not recover on next observation", err)
				}
				if !reflect.DeepEqual(store.facts[0], f) {
					t.Fatal("old prediction rewritten during recovery")
				}
			}
			if fault == "fee_unknown" && !state.feeAt.IsZero() || fault == "capital_unknown" && !state.capitalAt.IsZero() {
				t.Fatal("unknown rule advanced its retry schedule")
			}
		})
	}
}

type batchFactStore struct {
	factStore
	batches      [][]model.DiscoveryFact
	batchFailure error
	singles      int
}

func (s *batchFactStore) WriteDiscoveryFacts(_ context.Context, facts []model.DiscoveryFact) error {
	s.batches = append(s.batches, append([]model.DiscoveryFact(nil), facts...))
	if s.batchFailure != nil {
		return s.batchFailure
	}
	for _, f := range facts {
		if err := f.Validate(); err != nil {
			return err
		}
	}
	s.facts = append(s.facts, facts...)
	return nil
}
func (s *batchFactStore) WriteDiscoveryFact(ctx context.Context, f model.DiscoveryFact) error {
	s.singles++
	return s.factStore.WriteDiscoveryFact(ctx, f)
}
func TestDiscoveryMarketGroupPreservesIndependentObservations(t *testing.T) {
	f, b := marketFacts()
	for _, fault := range []string{"none", "group_failure", "invalid_forecast", "bar_absent"} {
		t.Run(fault, func(t *testing.T) {
			client := &factClient{forecast: f, bar: b}
			store := &batchFactStore{}
			batches, singles, want := 1, 0, 2
			switch fault {
			case "group_failure":
				store.batchFailure = errors.New("unknown commit")
				singles = 2
			case "invalid_forecast":
				client.forecast.Mark = decimal.Zero
				singles = 2
				want = 1
			case "bar_absent":
				client.bar = model.TradeBar{}
				client.marketErr = errors.New("bar absent")
				batches = 0
				singles = 1
				want = 1
			}
			err := collectDiscoveryMarket(context.Background(), client, store, model.Instrument{})
			if (err != nil) != (fault == "invalid_forecast" || fault == "bar_absent") || len(store.batches) != batches || store.singles != singles || len(store.facts) != want {
				t.Fatalf("err=%v batch=%d singles=%d saved=%d", err, len(store.batches), store.singles, len(store.facts))
			}
			expected := []model.DiscoveryFact{f, b}
			if fault == "invalid_forecast" {
				expected = expected[1:]
			} else if fault == "bar_absent" {
				expected = expected[:1]
			}
			if !reflect.DeepEqual(store.facts, expected) {
				t.Fatal("observation identity/time/source changed")
			}
		})
	}
}
