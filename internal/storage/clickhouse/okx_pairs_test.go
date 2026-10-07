package clickhouse

import (
	"context"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/replay"
	"reflect"
	"testing"
	"time"
)

func TestOKXPairReferenceRoundTripAndCommittedParent(t *testing.T) {
	c := commonUniverseTestClient(t)
	ctx := context.Background()
	if err := c.InitOKXPairSchema(ctx); err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC)
	coefficient := decimal.NewFromInt(5)
	p := model.OKXLoanPolicy{ID: uuid.New(), RequestedAt: at, ObservedAt: at.Add(time.Millisecond), SourceURL: "https://www.okx.com/api/v5/public/interest-rate-loan-quota", PayloadHash: model.ReferenceHash("source response"), Basic: []model.OKXBasicLoan{{Currency: "BTC", DailyRate: decimal.RequireFromString("0.00001392"), PublicQuota: decimal.NewFromInt(175)}}, Levels: []model.OKXLoanLevel{{Family: "vip", Level: "VIP 1", QuotaCoefficient: &coefficient}}}
	spot := model.Instrument{Exchange: "OKX", MarketType: model.MarketSpot, ExchangeSymbol: "BTC-USDT", BaseAsset: "BTC", QuoteAsset: "USDT", ContractMultiplier: decimal.NewFromInt(1), PriceTickSize: decimal.RequireFromString("0.1"), QuantityStepSize: decimal.RequireFromString("0.0001")}
	perp := spot
	perp.MarketType = model.MarketPerpetual
	perp.ExchangeSymbol = "BTC-USDT-SWAP"
	perp.VenueContractVersion = "1597026383085"
	usd := "USDT"
	perp.SettleAsset = &usd
	perp.ContractMultiplier = decimal.RequireFromString("0.01")
	perp.QuantityStepSize = decimal.NewFromInt(1)
	registered, err := c.RegisterInstruments(ctx, []model.Instrument{spot, perp})
	if err != nil {
		t.Fatal(err)
	}
	o := model.OKXPairObservation{ID: uuid.New(), ObservedAt: at.Add(2 * time.Millisecond), EffectiveMinute: at.Add(time.Minute), LoanPolicyID: p.ID, Pairs: []model.OKXPairMember{{Base: "BTC", Spot: registered[0], Perpetual: registered[1], SpotMinSize: decimal.RequireFromString("0.001"), PerpetualMinSize: decimal.NewFromInt(1)}}}
	for n := 0; n < 4; n++ {
		o.SourceURLs = append(o.SourceURLs, "https://www.okx.com/public")
		o.SourceHashes = append(o.SourceHashes, p.PayloadHash)
		o.RequestedAt = append(o.RequestedAt, at)
		o.ReceivedAt = append(o.ReceivedAt, at.Add(time.Millisecond))
		o.RawCounts = append(o.RawCounts, 1)
	}
	if err = c.WriteOKXPairCatalog(ctx, o); err == nil {
		t.Fatal("catalog committed without loan parent")
	}
	if err = c.WriteOKXLoanPolicy(ctx, p); err != nil {
		t.Fatal(err)
	}
	if err = c.WriteOKXLoanPolicy(ctx, p); err != nil {
		t.Fatal(err)
	}
	got, err := c.LatestOKXLoanPolicy(ctx, at.Add(time.Second))
	if err != nil || model.ReferenceHash(got) != model.ReferenceHash(p) {
		t.Fatalf("policy altered: %+v %v", got, err)
	}
	if got.Levels[0].InterestDiscount != nil || got.Levels[0].Quota != nil {
		t.Fatal("unknown loan term became zero")
	}
	o.SourceURLs[3] = p.SourceURL
	wrong := o
	wrong.SourceHashes = append([]string(nil), o.SourceHashes...)
	wrong.SourceHashes[3] = model.ReferenceHash("different response")
	if err = c.WriteOKXPairCatalog(ctx, wrong); err == nil {
		t.Fatal("catalog committed against different loan response")
	}
	if err = c.WriteOKXPairCatalog(ctx, o); err != nil {
		t.Fatal(err)
	}
	if _, err = c.LatestOKXPairCatalog(ctx, at.Add(time.Second)); err == nil {
		t.Fatal("future paired catalog visible early")
	}
	current, err := c.LatestOKXPairCatalog(ctx, at.Add(time.Minute))
	if err != nil || model.ReferenceHash(current) != model.ReferenceHash(o) {
		t.Fatalf("catalog altered: %v", err)
	}
	var rows uint64
	if err = c.conn.QueryRow(ctx, "SELECT count() FROM "+c.table("okx_public_loan_policy")+" FINAL").Scan(&rows); err != nil || rows != 1 {
		t.Fatal("retry changed observation identity")
	}
}

func TestClickHouseMixedFiveTenFiftyMinuteReplay(t *testing.T) {
	c := commonUniverseTestClient(t)
	ctx := context.Background()
	at := time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC)
	for n, depth := range []uint8{50, 10, 5} {
		b := measuredBatch(uint32(n+1), at.Add(time.Duration(n)*time.Minute), false)
		b.Minute.StoredDepth = depth
		b.Minute.ValidBitmap = model.MinuteMask
		for i := 0; i < int(depth); i++ {
			b.Minute.Bids[i] = model.Level{PriceTick: int64(1000 - i), QtyLot: uint64(i + 1)}
			b.Minute.Asks[i] = model.Level{PriceTick: int64(1001 + i), QtyLot: uint64(i + 2)}
		}
		b.Deltas = []model.BookDelta{{MinuteID: b.Minute.ID, SecondOffset: 1, BidChangePrice: []int64{1000, 1000 - int64(depth)}, BidChangeQty: []uint64{0, 42}, AskChangePrice: []int64{1001, 1001 + int64(depth)}, AskChangeQty: []uint64{0, 43}}}
		if err := c.WriteMinute(ctx, b); err != nil {
			t.Fatal(err)
		}
		loaded, err := c.LoadMinute(ctx, b.Minute.InstrumentID, b.Minute.MinuteTime)
		if err != nil {
			t.Fatal(err)
		}
		deltas, err := c.LoadDeltas(ctx, loaded.ID, 59)
		if err != nil {
			t.Fatal(err)
		}
		for second := uint8(0); second < 60; second++ {
			expected, valid, err := replay.AtSecond(b.Minute, b.Deltas, second)
			if err != nil || !valid {
				t.Fatal(err)
			}
			got, valid, err := replay.AtSecond(loaded, deltas, second)
			if err != nil || !valid || !reflect.DeepEqual(got, expected) {
				t.Fatalf("depth %d second %d changed: %v", depth, second, err)
			}
		}
	}
}

func TestOptionsLiveFiveLevelDeliveryAndLegacyTen(t *testing.T) {
	c := derivativeIntegrationClient(t)
	ctx := context.Background()
	run, old := liveDBFixture(t, c)
	if err := c.WriteOptionsMinute(ctx, old); err != nil {
		t.Fatal("old ten-level commit", err)
	}
	instruments, err := c.Instruments(ctx)
	if err != nil {
		t.Fatal(err)
	}
	delivery := map[uint32]bool{}
	for _, i := range instruments {
		delivery[i.ID] = i.MarketType == model.MarketDelivery
	}
	next := old.Clone()
	next.MinuteTime = old.MinuteTime.Add(time.Minute)
	next.PreparedAt = next.MinuteTime.Add(time.Minute)
	hasFuture := false
	for n, b := range old.Books {
		depth := 10
		if delivery[b.InstrumentID] {
			depth = 5
			hasFuture = true
		}
		rebuilt := derivativeSyntheticMinuteDepth(t, b.InstrumentID, next.MinuteTime, false, true, true, depth)
		for sec := range rebuilt.Quality {
			rebuilt.Quality[sec].TradingRuleID = b.Quality[sec].TradingRuleID
			rebuilt.Quality[sec].RulePublishedAt = b.Quality[sec].RulePublishedAt
			rebuilt.Quality[sec].MarketStateAt = b.Quality[sec].MarketStateAt
		}
		next.Books[n] = rebuilt
	}
	if !hasFuture {
		t.Fatal("fixture lacks delivery")
	}
	if err = c.WriteOptionsMinute(ctx, next); err != nil {
		t.Fatal("mixed five/ten commit", err)
	}
	for _, at := range []time.Time{old.MinuteTime, next.MinuteTime} {
		loaded, err := c.LoadOptionsMinute(ctx, run.ID, at)
		if err != nil {
			t.Fatal(err)
		}
		for _, b := range loaded.Books {
			depth := 10
			if at.Equal(next.MinuteTime) && delivery[b.InstrumentID] {
				depth = 5
			}
			for sec := uint8(0); sec < 60; sec++ {
				got, e := replay.ReplayDerivative(b, sec)
				if e != nil || !got.Quality.ReplayValid || int(got.StoredDepth) != depth || len(got.Snapshot.Bids) != depth {
					t.Fatalf("depth %d replay failed: %v", depth, e)
				}
			}
		}
	}
	bad := old.Clone()
	for n, b := range bad.Books {
		if !delivery[b.InstrumentID] {
			bad.Books[n] = derivativeSyntheticMinuteDepth(t, b.InstrumentID, b.MinuteTime, false, true, true, 5)
			for sec := range bad.Books[n].Quality {
				bad.Books[n].Quality[sec].TradingRuleID = b.Quality[sec].TradingRuleID
				bad.Books[n].Quality[sec].RulePublishedAt = b.Quality[sec].RulePublishedAt
			}
			break
		}
	}
	if err = c.WriteOptionsMinute(ctx, bad); err == nil {
		t.Fatal("option allowed to lose half its depth")
	}
}
