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

type capitalRetryClient struct {
	factClient
	calls   int
	capital model.CapitalParameters
}

func (c *capitalRetryClient) DiscoveryCapital(context.Context, model.Instrument, string, decimal.Decimal) (model.CapitalParameters, error) {
	c.calls++
	return c.capital, nil
}

type capitalRetryStore struct {
	factStore
	writes []model.CapitalParameters
	fail   bool
}

func (s *capitalRetryStore) WriteDiscoveryCapital(_ context.Context, p model.CapitalParameters) error {
	s.writes = append(s.writes, p)
	if s.fail {
		return errors.New("ambiguous storage commit")
	}
	return nil
}

func retryCapitalFixture(now time.Time) model.CapitalParameters {
	d := decimal.RequireFromString
	return model.CapitalParameters{
		ID: "frozen-rule-observation", InstrumentID: 17, Base: "BTC", ModelHash: model.ReferenceHash("model"),
		ObservedMS: now.UnixMilli(), SourceMS: now.Add(-time.Millisecond).UnixMilli(), EffectiveMS: now.Add(-time.Millisecond).UnixMilli(), ValidUntilMS: now.Add(time.Hour).UnixMilli(), BorrowLeverage: d("5"),
		QuoteDiscount: []model.DiscountTier{{Maximum: d("1000000"), Rate: d("1"), Penalty: d("0.01")}},
		BaseDiscount:  []model.DiscountTier{{Maximum: d("10000"), Rate: d("0.9"), Penalty: d("0.02")}},
		PerpTiers:     []model.RiskTier{{Maximum: d("100"), Initial: d("0.1"), Maintenance: d("0.05"), MaximumLeverage: d("10")}},
		BorrowTiers:   []model.RiskTier{{Maximum: d("50"), Initial: d("0.2"), Maintenance: d("0.1"), MaximumLeverage: d("5")}},
		Sources:       []model.PublicSource{{URL: "https://example.test/public", PayloadHash: model.ReferenceHash("payload"), TimeBasis: "request_without_source_clock", SourceMS: now.Add(-time.Millisecond).UnixMilli(), ObservedMS: now.UnixMilli()}},
	}
}

func TestCapitalRetryPreservesObservationWhileMarketContinues(t *testing.T) {
	now := time.Now().UTC().Add(-time.Second)
	f, b := marketFacts()
	client := &capitalRetryClient{factClient: factClient{forecast: f, bar: b}, capital: retryCapitalFixture(now)}
	state := discoveryPairState{feeAt: time.Now()}
	store := &capitalRetryStore{fail: true}
	if err := collectDiscoveryPair(context.Background(), client, store, &state); err == nil || state.pendingCapital == nil || !state.capitalAt.IsZero() {
		t.Fatal("failed rule write advanced schedule")
	}
	client.capital.ID = "must-not-replace-pending"
	if err := collectDiscoveryPair(context.Background(), client, store, &state); err == nil {
		t.Fatal("ambiguous write accepted")
	}
	if client.calls != 1 || len(store.writes) != 2 || !reflect.DeepEqual(store.writes[0], store.writes[1]) || len(store.facts) != 4 {
		t.Fatal("pending recreated or market facts blocked")
	}
	store.fail = false
	if err := collectDiscoveryPair(context.Background(), client, store, &state); err != nil || state.pendingCapital != nil || !state.capitalAt.Equal(now.Truncate(time.Millisecond)) {
		t.Fatal("original rule did not commit", err)
	}
	if client.calls != 1 || !reflect.DeepEqual(store.writes[0], store.writes[2]) {
		t.Fatal("retry identity changed")
	}
}
func TestUnsupportedCapitalDoesNotBecomeUnretryablePending(t *testing.T) {
	client := &capitalRetryClient{capital: retryCapitalFixture(time.Now())}
	client.capital.PerpTiers[0].MaximumLeverage = decimal.NewFromInt(4)
	state := discoveryPairState{}
	store := &capitalRetryStore{}
	for range 2 {
		if err := collectDiscoveryRules(context.Background(), client, store, &state); err == nil || state.pendingCapital != nil {
			t.Fatal("invalid source retained as pending")
		}
	}
	if client.calls != 2 || len(store.writes) != 0 {
		t.Fatal("invalid source was not retried")
	}
}

func TestUnstorableCapitalCanRecoverOnNextSourceObservation(t *testing.T) {
	for _, scope := range []string{"quote", "base", "perp", "borrow", "leverage", "range"} {
		t.Run(scope, func(t *testing.T) {
			now := time.Now().UTC()
			client := &capitalRetryClient{capital: retryCapitalFixture(now)}
			bad := decimal.RequireFromString("0.1234567890123456789")
			switch scope {
			case "quote":
				client.capital.QuoteDiscount[0].Rate = bad
			case "base":
				client.capital.BaseDiscount[0].Rate = bad
			case "perp":
				client.capital.PerpTiers[0].Initial = bad
			case "borrow":
				client.capital.BorrowTiers[0].Initial = bad
			case "leverage":
				client.capital.BorrowLeverage = bad
			case "range":
				client.capital.PerpTiers[0].Maximum = decimal.RequireFromString("100000000000000000000")
			}
			state := discoveryPairState{feeAt: now}
			store := &capitalRetryStore{}
			if err := collectDiscoveryRules(context.Background(), client, store, &state); err == nil || state.pendingCapital != nil || len(store.writes) != 0 {
				t.Fatal("unrepresentable source reached pending/writer", err)
			}
			client.capital = retryCapitalFixture(now.Add(time.Millisecond))
			if err := collectDiscoveryRules(context.Background(), client, store, &state); err != nil || client.calls != 2 || len(store.writes) != 1 || state.pendingCapital != nil {
				t.Fatal("corrected source did not recover", err)
			}
		})
	}
}
