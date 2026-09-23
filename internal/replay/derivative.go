package replay

import (
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/model"
)

var ErrIncompleteDerivativeBatch = errors.New("incomplete derivative batch")

type DerivativeResult struct {
	SampleTime  time.Time
	StoredDepth uint8
	Snapshot    model.DerivativeSnapshot
	Quality     model.DerivativeQuality
}

func ValidateDerivativeBatch(b model.DerivativeMinuteBatch) error {
	id, err := model.MinuteID(b.InstrumentID, b.MinuteTime)
	if err != nil || b.MinuteTime.IsZero() || !b.MinuteTime.Equal(b.MinuteTime.Truncate(time.Minute)) {
		return fmt.Errorf("invalid derivative minute identity")
	}
	var valid uint64
	for i, q := range b.Quality {
		at := b.MinuteTime.Add(time.Duration(i) * time.Second)
		if q.Reason > model.DerivativeMarketClosed || q.BidLevels > model.BookDepth || q.AskLevels > model.BookDepth {
			return fmt.Errorf("invalid quality domain at second %d", i)
		}
		for _, t := range []time.Time{q.SourceTime, q.ReceivedAt, q.CapturedAt, q.LastSnapshotAt, q.ConnectionConfirmedAt, q.RulePublishedAt, q.MarketStateAt} {
			if !t.IsZero() && !t.Equal(t.Truncate(time.Microsecond)) {
				return fmt.Errorf("quality timestamp exceeds storage precision")
			}
		}
		if q.Sampled {
			if q.CapturedAt.Before(at) || q.CapturedAt.After(at.Add(250*time.Millisecond)) {
				return fmt.Errorf("invalid capture deadline at second %d", i)
			}
		} else if q.StreamValid || q.ReplayValid {
			return fmt.Errorf("unsampled second marked valid")
		}
		for _, t := range []time.Time{q.ReceivedAt, q.LastSnapshotAt, q.ConnectionConfirmedAt, q.RulePublishedAt, q.MarketStateAt} {
			if t.After(at) {
				return fmt.Errorf("future quality at second %d", i)
			}
		}
		if q.StreamValid && (q.SourceTime.IsZero() || q.ReceivedAt.IsZero() || q.Epoch == uuid.Nil || q.LastSnapshotAt.IsZero()) {
			return fmt.Errorf("stream validity without provenance")
		}
		if q.MarketOpen && !q.MarketKnown {
			return fmt.Errorf("open market with unknown state")
		}
		if q.MarketKnown && (q.MarketStateAt.IsZero() || q.MarketStateBasis < 1 || q.MarketStateBasis > 2) {
			return fmt.Errorf("market state lacks evidence")
		}
		if !q.MarketKnown && q.MarketStateBasis != 0 {
			return fmt.Errorf("unknown market state has known basis")
		}
		if (q.TradingRuleID == "") != q.RulePublishedAt.IsZero() || (q.TradingRuleID != "" && !model.ValidDigest(q.TradingRuleID)) {
			return fmt.Errorf("invalid trading rule reference")
		}
		wantValid := q.Sampled && q.StreamValid && q.MarketKnown && b.Minute != nil && q.Reason == model.DerivativeOK
		if q.ReplayValid != wantValid {
			return fmt.Errorf("quality validity mismatch at second %d", i)
		}
		if !q.ReplayValid && q.Reason == model.DerivativeOK {
			return fmt.Errorf("missing invalid reason at second %d", i)
		}
		if b.Minute == nil && q.StreamValid && q.MarketKnown && q.Reason != model.DerivativeAnchorMissing {
			return fmt.Errorf("unanchored valid stream lacks anchor_missing")
		}
		if q.ReplayValid {
			valid |= 1 << i
		}
	}
	if b.Minute == nil {
		if len(b.Deltas) != 0 || valid != 0 {
			return fmt.Errorf("unanchored minute contains replay data")
		}
		return nil
	}
	m := b.Minute
	if m.ID != id || m.InstrumentID != b.InstrumentID || !m.MinuteTime.Equal(b.MinuteTime) || m.Signed != b.Signed || m.StoredDepth != model.BookDepth || m.EncodingVersion != model.DerivativeEncodingVersion {
		return fmt.Errorf("invalid derivative encoding/identity")
	}
	if m.ValidBitmap&1 == 0 || m.ValidBitmap != valid || m.ValidBitmap&^model.MinuteMask != 0 || m.DeltaBitmap&1 != 0 || m.DeltaBitmap&^m.ValidBitmap != 0 {
		return fmt.Errorf("invalid derivative minute bitmaps")
	}
	if err := model.ValidateDerivativeSides(m.Bids, m.Asks, model.BookDepth, m.Signed); err != nil {
		return err
	}
	bids, asks := derivativeMap(m.Bids), derivativeMap(m.Asks)
	if int(b.Quality[0].BidLevels) != len(bids) || int(b.Quality[0].AskLevels) != len(asks) {
		return fmt.Errorf("anchor and quality depth mismatch")
	}
	index := 0
	for second := 1; second < 60; second++ {
		if m.DeltaBitmap&(1<<second) != 0 {
			if index >= len(b.Deltas) || b.Deltas[index].SecondOffset != uint8(second) {
				return fmt.Errorf("%w: missing delta at %d", ErrIncompleteDerivativeBatch, second)
			}
			d := b.Deltas[index]
			index++
			if d.MinuteID != id || len(d.BidChangePrice)+len(d.AskChangePrice) == 0 {
				return fmt.Errorf("invalid delta identity/empty delta")
			}
			if err := applyDerivativeDelta(bids, d.BidChangePrice, d.BidChangeQty, true, m.Signed); err != nil {
				return err
			}
			if err := applyDerivativeDelta(asks, d.AskChangePrice, d.AskChangeQty, false, m.Signed); err != nil {
				return err
			}
		}
		if m.ValidBitmap&(1<<second) != 0 {
			if err := model.ValidateDerivativeSides(derivativeLevels(bids, true), derivativeLevels(asks, false), model.BookDepth, m.Signed); err != nil {
				return fmt.Errorf("second %d: %w", second, err)
			}
			if int(b.Quality[second].BidLevels) != len(bids) || int(b.Quality[second].AskLevels) != len(asks) {
				return fmt.Errorf("replay and quality depth mismatch")
			}
		}
	}
	if index != len(b.Deltas) {
		return fmt.Errorf("unexpected/duplicate delta row")
	}
	return nil
}

func ReplayDerivative(b model.DerivativeMinuteBatch, second uint8) (DerivativeResult, error) {
	if second > 59 {
		return DerivativeResult{}, fmt.Errorf("second outside minute")
	}
	if err := ValidateDerivativeBatch(b); err != nil {
		return DerivativeResult{}, err
	}
	r := DerivativeResult{SampleTime: b.MinuteTime.Add(time.Duration(second) * time.Second), StoredDepth: model.BookDepth, Quality: b.Quality[second]}
	if !r.Quality.ReplayValid {
		return r, nil
	}
	bids, asks := derivativeMap(b.Minute.Bids), derivativeMap(b.Minute.Asks)
	for _, d := range b.Deltas {
		if d.SecondOffset > second {
			break
		}
		_ = applyDerivativeDelta(bids, d.BidChangePrice, d.BidChangeQty, true, b.Signed)
		_ = applyDerivativeDelta(asks, d.AskChangePrice, d.AskChangeQty, false, b.Signed)
	}
	q := r.Quality
	r.Snapshot = model.DerivativeSnapshot{InstrumentID: b.InstrumentID, SourceTime: q.SourceTime, ReceivedAt: q.ReceivedAt, Epoch: q.Epoch, ChangeID: q.ChangeID, Bids: derivativeLevels(bids, true), Asks: derivativeLevels(asks, false)}
	return r, nil
}

func derivativeMap(levels []model.Level) map[int64]uint64 {
	m := make(map[int64]uint64, len(levels))
	for _, l := range levels {
		m[l.PriceTick] = l.QtyLot
	}
	return m
}
func derivativeLevels(m map[int64]uint64, bid bool) []model.Level {
	ls := make([]model.Level, 0, len(m))
	for p, q := range m {
		ls = append(ls, model.Level{PriceTick: p, QtyLot: q})
	}
	sort.Slice(ls, func(i, j int) bool {
		if bid {
			return ls[i].PriceTick > ls[j].PriceTick
		}
		return ls[i].PriceTick < ls[j].PriceTick
	})
	return ls
}
func applyDerivativeDelta(m map[int64]uint64, prices []int64, qtys []uint64, bid, signed bool) error {
	if len(prices) != len(qtys) || len(prices) > 2*model.BookDepth {
		return fmt.Errorf("invalid delta arrays")
	}
	for i, p := range prices {
		if (!signed && p <= 0) || (i > 0 && ((bid && prices[i-1] <= p) || (!bid && prices[i-1] >= p))) {
			return fmt.Errorf("invalid or unordered delta price")
		}
		old, exists := m[p]
		if qtys[i] == 0 {
			if !exists {
				return fmt.Errorf("delta deletes absent price")
			}
			delete(m, p)
		} else {
			if exists && old == qtys[i] {
				return fmt.Errorf("noncanonical no-op delta")
			}
			m[p] = qtys[i]
		}
	}
	return nil
}
