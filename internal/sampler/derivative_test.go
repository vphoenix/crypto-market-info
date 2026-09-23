package sampler_test

import (
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/orderbook"
	"github.com/vphoenix/crypto-market-info/internal/replay"
	"github.com/vphoenix/crypto-market-info/internal/sampler"
)

func TestDerivativeEverySecondSignedEmptyRecoveryAndDeepPromotion(t *testing.T) {
	at := time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC)
	book, err := orderbook.NewDerivative(1, true, 100)
	if err != nil {
		t.Fatal(err)
	}
	epoch := uuid.New()
	if err = book.Reset(epoch); err != nil {
		t.Fatal(err)
	}
	buf, _ := sampler.NewDerivativeMinuteBuffer(1, at, true)
	var expected [60]model.DerivativeSnapshot
	var wantValid [60]bool
	seq := uint64(10)
	apply := func(second int, snapshot bool, bids, asks []orderbook.DerivativeChangeLevel) {
		t.Helper()
		u := orderbook.DerivativeUpdate{InstrumentID: 1, Snapshot: snapshot, Epoch: epoch, PrevChangeID: seq, ChangeID: seq + 3, SourceTime: at.Add(time.Duration(second)*time.Second - time.Millisecond), ReceivedAt: at.Add(time.Duration(second) * time.Second), Bids: bids, Asks: asks}
		seq += 3
		if err := book.Apply(u); err != nil {
			t.Fatal(err)
		}
	}
	newLevel := func(p int64, q uint64) orderbook.DerivativeChangeLevel {
		return orderbook.DerivativeChangeLevel{Action: orderbook.DerivativeNew, Level: model.Level{PriceTick: p, QtyLot: q}}
	}
	var levels []orderbook.DerivativeChangeLevel
	for p := int64(0); p >= -10; p-- {
		levels = append(levels, newLevel(p, 1))
	}
	apply(0, true, levels, []orderbook.DerivativeChangeLevel{newLevel(1, 3)})
	for second := 0; second < 60; second++ {
		switch second {
		case 1:
			apply(second, false, []orderbook.DerivativeChangeLevel{{Action: orderbook.DerivativeDelete, Level: model.Level{PriceTick: 0}}}, nil)
		case 2:
			// Multiple source updates in one second must collapse to the last qty.
			for _, q := range []uint64{5, 8, 12} {
				apply(second, false, []orderbook.DerivativeChangeLevel{{Action: orderbook.DerivativeChange, Level: model.Level{PriceTick: -1, QtyLot: q}}}, nil)
			}
		case 4:
			book.Invalidate(model.DerivativeSequenceGap)
		case 7:
			epoch = uuid.New()
			if err := book.Reset(epoch); err != nil {
				t.Fatal(err)
			}
			seq = 1
			apply(second, true, nil, nil) // Known empty is a valid recovered snapshot.
		case 8:
			apply(second, false, []orderbook.DerivativeChangeLevel{newLevel(-2, 9)}, nil)
		}
		s, q := book.Current()
		q.Sampled = true
		q.CapturedAt = at.Add(time.Duration(second) * time.Second)
		q.MarketKnown = true
		q.MarketOpen = true
		q.MarketStateAt = at.Add(-time.Second)
		q.MarketStateBasis = 1
		if err := buf.Sample(at.Add(time.Duration(second)*time.Second), s, q); err != nil {
			t.Fatal(err)
		}
		expected[second] = s
		wantValid[second] = q.StreamValid
	}
	b, err := buf.Complete()
	if err != nil {
		t.Fatal(err)
	}
	if err = replay.ValidateDerivativeBatch(b); err != nil {
		t.Fatal(err)
	}
	for second := uint8(0); second < 60; second++ {
		r, err := replay.ReplayDerivative(b, second)
		if err != nil {
			t.Fatal(err)
		}
		if r.Quality.ReplayValid != wantValid[second] {
			t.Fatalf("validity at %d", second)
		}
		if wantValid[second] && (!slices.Equal(r.Snapshot.Bids, expected[second].Bids) || !slices.Equal(r.Snapshot.Asks, expected[second].Asks) || !r.Snapshot.SourceTime.Equal(expected[second].SourceTime)) {
			t.Fatalf("second %d: %+v != %+v", second, r.Snapshot, expected[second])
		}
	}
	if b.Minute.StoredDepth != 10 || len(b.Minute.Bids) != 10 || b.Deltas[0].BidChangePrice[0] != 0 {
		t.Fatalf("depth/zero deletion %+v", b)
	}
	if b.Quality[7].BidLevels != 0 || !b.Quality[7].ReplayValid {
		t.Fatal("empty recovery lost")
	}
	broken := model.CloneDerivativeBatch(b)
	broken.Deltas = broken.Deltas[1:]
	if err := replay.ValidateDerivativeBatch(broken); err == nil {
		t.Fatal("missing delta treated as unchanged")
	}
	// Returned batches own their arrays.
	b.Minute.Bids[0].QtyLot = 999
	b.Deltas[0].BidChangeQty[0] = 999
	again, _ := buf.Complete()
	if again.Minute.Bids[0].QtyLot == 999 || again.Deltas[0].BidChangeQty[0] == 999 {
		t.Fatal("batch aliases buffer")
	}
}

func TestDerivativeNoAnchorAndCutoff(t *testing.T) {
	at := time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC)
	buf, _ := sampler.NewDerivativeMinuteBuffer(1, at, false)
	epoch := uuid.New()
	for second := 0; second < 60; second++ {
		tm := at.Add(time.Duration(second) * time.Second)
		s := model.DerivativeSnapshot{InstrumentID: 1, SourceTime: tm.Add(-time.Millisecond), ReceivedAt: tm, Epoch: epoch}
		q := model.DerivativeQuality{Sampled: true, StreamValid: true, MarketKnown: true, MarketOpen: true, CapturedAt: tm, SourceTime: s.SourceTime, ReceivedAt: tm, Epoch: epoch, LastSnapshotAt: tm, MarketStateAt: at, MarketStateBasis: 1}
		if second == 0 {
			q.StreamValid = false
			q.Reason = model.DerivativeNotReady
			s = model.DerivativeSnapshot{}
		}
		if second == 1 {
			future := q
			future.RulePublishedAt = tm.Add(time.Microsecond)
			future.TradingRuleID = "a"
			if err := buf.Sample(tm, s, future); err == nil {
				t.Fatal("accepted future control state")
			}
			late := q
			late.CapturedAt = tm.Add(251 * time.Millisecond)
			if err := buf.Sample(tm, s, late); err == nil {
				t.Fatal("accepted late capture")
			}
		}
		if err := buf.Sample(tm, s, q); err != nil {
			t.Fatal(err)
		}
	}
	b, err := buf.Complete()
	if err != nil {
		t.Fatal(err)
	}
	if b.Minute != nil || b.Quality[59].ReplayValid || b.Quality[59].Reason != model.DerivativeAnchorMissing {
		t.Fatal("synthesized missing anchor")
	}
	if err = replay.ValidateDerivativeBatch(b); err != nil {
		t.Fatal(err)
	}
}
