package funding

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/model"
)

type fakeSink struct {
	mu    sync.Mutex
	rates []model.FundingRate
}

func (f *fakeSink) UpsertFundingRate(_ context.Context, rate model.FundingRate) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rates = append(f.rates, rate)
	return nil
}

func (f *fakeSink) snapshot() []model.FundingRate {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]model.FundingRate(nil), f.rates...)
}

func fundingInstrument(id uint32, exchange string) model.Instrument {
	settle := "USDT"
	return model.Instrument{
		ID:                 id,
		Exchange:           exchange,
		MarketType:         model.MarketPerpetual,
		ExchangeSymbol:     "BTC-USDT-SWAP",
		BaseAsset:          "BTC",
		QuoteAsset:         "USDT",
		SettleAsset:        &settle,
		ContractMultiplier: decimal.NewFromInt(1),
		PriceTickSize:      decimal.RequireFromString("0.1"),
		QuantityStepSize:   decimal.RequireFromString("0.001"),
	}
}

func TestEstimateStoreUsesPreSettlementValueAndRejectsStaleData(t *testing.T) {
	store := NewEstimateStore()
	hour := time.Date(2026, 8, 19, 8, 0, 0, 0, time.UTC)
	preSettlement := model.FundingEstimate{InstrumentID: 1, FundingTime: hour.Add(123 * time.Millisecond), Rate: decimal.RequireFromString("0.1"), SourceTime: hour.Add(-time.Second)}
	postSettlement := model.FundingEstimate{InstrumentID: 1, FundingTime: hour.Add(8*time.Hour + 456*time.Millisecond), Rate: decimal.RequireFromString("0.2"), SourceTime: hour.Add(time.Millisecond)}
	if err := store.Put(preSettlement); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(postSettlement); err != nil {
		t.Fatal(err)
	}
	got, found := store.At(1, hour, 2*time.Minute)
	if !found || !got.Rate.Equal(preSettlement.Rate) || got.FundingTime.UnixMilli() != preSettlement.FundingTime.UnixMilli() {
		t.Fatalf("settlement estimate=%+v found=%v", got, found)
	}
	if _, found = store.At(1, hour.Add(time.Hour), 2*time.Minute); found {
		t.Fatal("stale estimate was returned")
	}
	store.MarkUnavailable([]uint32{1})
	if _, found = store.At(1, hour, 2*time.Minute); found {
		t.Fatal("estimate from an unavailable websocket was returned")
	}
}

func TestEstimateStoreIgnoresOutOfOrderUpdateForSameTarget(t *testing.T) {
	store := NewEstimateStore()
	target := time.Date(2026, 8, 19, 16, 0, 0, 123000000, time.UTC)
	newer := model.FundingEstimate{InstrumentID: 1, FundingTime: target, Rate: decimal.RequireFromString("0.2"), SourceTime: target.Add(-time.Second)}
	older := newer
	older.Rate = decimal.RequireFromString("0.1")
	older.SourceTime = newer.SourceTime.Add(-time.Second)
	if err := store.Put(newer); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(older); err != nil {
		t.Fatal(err)
	}
	got, found := store.At(1, target, 2*time.Minute)
	if !found || !got.Rate.Equal(newer.Rate) {
		t.Fatalf("out-of-order update replaced latest: %+v", got)
	}
}

func TestEstimateStoreDoesNotSubstituteNextPeriodAtSettlement(t *testing.T) {
	store := NewEstimateStore()
	hour := time.Date(2026, 8, 19, 8, 0, 0, 0, time.UTC)
	staleSettlement := model.FundingEstimate{InstrumentID: 1, FundingTime: hour.Add(123 * time.Millisecond), Rate: decimal.RequireFromString("0.1"), SourceTime: hour.Add(-3 * time.Minute)}
	postSettlement := model.FundingEstimate{InstrumentID: 1, FundingTime: hour.Add(8*time.Hour + 456*time.Millisecond), Rate: decimal.RequireFromString("0.2"), SourceTime: hour}
	if err := store.Put(staleSettlement); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(postSettlement); err != nil {
		t.Fatal(err)
	}
	if got, found := store.At(1, hour, 2*time.Minute); found {
		t.Fatalf("next-period estimate was substituted at settlement: %+v", got)
	}
}

func TestSchedulerWritesOnlyFreshWebSocketEstimate(t *testing.T) {
	hour := time.Date(2026, 8, 19, 9, 0, 0, 0, time.UTC)
	store := NewEstimateStore()
	fresh := model.FundingEstimate{InstrumentID: 1, FundingTime: hour.Add(7*time.Hour + 321*time.Millisecond), Rate: decimal.RequireFromString("0.1"), SourceTime: hour.Add(-30 * time.Second)}
	stale := model.FundingEstimate{InstrumentID: 2, FundingTime: hour.Add(7 * time.Hour), Rate: decimal.RequireFromString("0.2"), SourceTime: hour.Add(-3 * time.Minute)}
	if err := store.Put(fresh); err != nil {
		t.Fatal(err)
	}
	if err := store.Put(stale); err != nil {
		t.Fatal(err)
	}
	sink := &fakeSink{}
	scheduler := Scheduler{Instruments: []model.Instrument{fundingInstrument(1, "Binance"), fundingInstrument(2, "OKX")}, Estimates: store, Sink: sink, MaxEstimateAge: 2 * time.Minute}
	scheduler.CollectHour(context.Background(), hour)
	rates := sink.snapshot()
	if len(rates) != 1 || rates[0].InstrumentID != 1 || rates[0].IsActual || rates[0].FundingTime.UnixMilli() != fresh.FundingTime.UnixMilli() {
		t.Fatalf("rates=%+v", rates)
	}
}
