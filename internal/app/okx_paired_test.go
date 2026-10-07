package app

import (
	"context"
	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/config"
	"github.com/vphoenix/crypto-market-info/internal/funding"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"testing"
	"time"
)

func TestPairCapacityIncludesTwoGenerationsAndExistingOwnedStreams(t *testing.T) {
	cfg, err := config.Load()
	if err != nil {
		t.Fatal(err)
	}
	extra := make([]model.Instrument, 332)
	for n := range extra {
		extra[n].MarketType = model.MarketSpot
		if n%2 == 1 {
			extra[n].MarketType = model.MarketPerpetual
		}
	}
	plan, err := pairedCapacity(cfg, CapacityPlan{WSConnections: 8, SampleSources: 5, TotalInstruments: 3, BufferedEvents: 60000}, 167, extra, 0, 0)
	if err != nil || plan.AdditionalSources != 332 || plan.PeakWSConnections != 44 {
		t.Fatalf("incorrect full scope or peak capacity: %+v %v", plan, err)
	}
	cfg.MaxSampleSources = 336
	if _, err = pairedCapacity(cfg, CapacityPlan{SampleSources: 5}, 167, extra, 0, 0); err == nil {
		t.Fatal("partial capacity accepted")
	}
}

func TestExtraPairDefinitionsNeverDuplicateManualFeed(t *testing.T) {
	usd := "USDT"
	spot := model.Instrument{ID: 3, Exchange: "OKX", MarketType: model.MarketSpot, ExchangeSymbol: "BTC-USDT", BaseAsset: "BTC", QuoteAsset: "USDT", ContractMultiplier: decimal.NewFromInt(1), PriceTickSize: decimal.RequireFromString("0.1"), QuantityStepSize: decimal.RequireFromString("0.0001")}
	perp := spot
	perp.ID = 6
	perp.MarketType = model.MarketPerpetual
	perp.ExchangeSymbol = "BTC-USDT-SWAP"
	perp.SettleAsset = &usd
	perp.VenueContractVersion = "1"
	other := spot
	other.ID = 10
	other.BaseAsset = "ETH"
	other.ExchangeSymbol = "ETH-USDT"
	pair := []model.OKXPairMember{{Base: "BTC", Spot: spot, Perpetual: perp}, {Base: "ETH", Spot: other, Perpetual: perp}}
	extra := extraPairDefinitions(pair, []model.Instrument{spot, perp})
	if len(extra) != 1 || extra[0].ID != 10 {
		t.Fatalf("duplicate owner created: %+v", extra)
	}
}

// The sink retires the old connection on the first (slow) write. All quotes
// must already be frozen, and the parent scheduler must finish that hour.
type retireFundingSink struct {
	rates  []model.FundingRate
	retire func()
}

func (s *retireFundingSink) UpsertFundingRate(ctx context.Context, r model.FundingRate) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.rates = append(s.rates, r)
	if len(s.rates) == 1 && s.retire != nil {
		s.retire()
	}
	return nil
}
func TestPairedFundingBoundarySwitchAndRetiredConnection(t *testing.T) {
	hour := time.Date(2026, 10, 7, 8, 0, 0, 0, time.UTC)
	old, next := funding.NewEstimateStore(), funding.NewEstimateStore()
	for _, id := range []uint32{1, 2} {
		for _, x := range []struct {
			store *funding.EstimateStore
			rate  string
		}{{old, "0.0001"}, {next, "0.0002"}} {
			for _, h := range []time.Time{hour.Add(-time.Hour), hour} {
				if err := x.store.Put(model.FundingEstimate{InstrumentID: id, FundingTime: h, SourceTime: h.Add(-time.Second), Rate: decimal.RequireFromString(x.rate)}); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	sink := &retireFundingSink{retire: func() { old.MarkUnavailable([]uint32{1, 2}) }}
	s := &pairedFundingScheduler{sink: sink}
	instruments := []model.Instrument{{ID: 1, Exchange: "OKX"}, {ID: 2, Exchange: "OKX"}}
	s.schedule(instruments, old, hour.Add(-2*time.Hour))
	s.collectHour(context.Background(), hour.Add(-time.Hour))
	if len(sink.rates) != 2 {
		t.Fatal("retiring connection interrupted hourly persistence", sink.rates)
	}
	s.schedule(instruments, next, hour) // Exact hour is owned by the new generation.
	s.collectHour(context.Background(), hour)
	if len(sink.rates) != 4 || !sink.rates[2].Rate.Equal(decimal.RequireFromString("0.0002")) || !sink.rates[3].Rate.Equal(decimal.RequireFromString("0.0002")) {
		t.Fatal("exact boundary skipped or duplicated", sink.rates)
	}
	s.schedule(nil, funding.NewEstimateStore(), hour.Add(time.Minute))
	s.collectHour(context.Background(), hour.Add(time.Hour))
	if len(sink.rates) != 4 {
		t.Fatal("removed universe kept hourly ownership")
	}
}

func TestPairedFundingSecondPendingAcrossHourKeepsEffectiveOwner(t *testing.T) {
	hour := time.Date(2026, 10, 7, 5, 0, 0, 0, time.UTC)
	now := hour.Add(-40 * time.Minute)
	sink := &retireFundingSink{}
	s := &pairedFundingScheduler{sink: sink, now: func() time.Time { return now }}
	a, b := funding.NewEstimateStore(), funding.NewEstimateStore()
	for _, x := range []struct {
		store *funding.EstimateStore
		at    time.Time
		rate  string
	}{{a, hour, "0.0001"}, {b, hour.Add(time.Hour), "0.0002"}} {
		if err := x.store.Put(model.FundingEstimate{InstrumentID: 1, FundingTime: x.at, SourceTime: x.at.Add(-time.Second), Rate: decimal.RequireFromString(x.rate)}); err != nil {
			t.Fatal(err)
		}
	}
	s.schedule([]model.Instrument{{ID: 1, Exchange: "OKX"}}, a, hour.Add(-38*time.Minute)) // 04:22
	now = hour.Add(-time.Minute)
	s.schedule([]model.Instrument{{ID: 1, Exchange: "OKX"}}, b, hour.Add(time.Minute)) // 05:01
	s.collectHour(context.Background(), hour)
	s.collectHour(context.Background(), hour.Add(time.Hour))
	if len(sink.rates) != 2 || sink.rates[0].Rate.String() != "0.0001" || sink.rates[1].Rate.String() != "0.0002" {
		t.Fatal("effective generation lost to future pending generation", sink.rates)
	}
}

func TestPairedCapacityIncludesLargerRetiringGeneration(t *testing.T) {
	cfg, err := config.Load()
	if err != nil {
		t.Fatal(err)
	}
	extra := []model.Instrument{{MarketType: model.MarketSpot}, {MarketType: model.MarketPerpetual}}
	cap, err := pairedCapacity(cfg, CapacityPlan{}, 1, extra, 334, 167)
	if err != nil || cap.PeakWSConnections != 20 {
		t.Fatalf("retiring generation understated: %+v %v", cap, err)
	}
	cfg.PerpMaxTotalWSConnections = 19
	if _, err = pairedCapacity(cfg, CapacityPlan{}, 1, extra, 334, 167); err == nil {
		t.Fatal("shrinking generation escaped peak capacity limit")
	}
}

func TestPairedCapacityIncludesExistingOKXVenueLimit(t *testing.T) {
	cfg, err := config.Load()
	if err != nil {
		t.Fatal(err)
	}
	for n := range cfg.PerpetualSelection.Venues {
		if cfg.PerpetualSelection.Venues[n].Venue == "OKX" {
			cfg.PerpetualSelection.Venues[n].MaxInstruments = 2
		}
	}
	extra := []model.Instrument{{MarketType: model.MarketSpot}, {MarketType: model.MarketPerpetual}}
	if _, err = pairedCapacity(cfg, CapacityPlan{Venues: []VenueCapacity{{Venue: "OKX", Instruments: 2}}}, 1, extra, 0, 0); err == nil {
		t.Fatal("per-venue budget ignored existing streams")
	}
}
