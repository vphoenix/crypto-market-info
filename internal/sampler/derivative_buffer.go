package sampler

import (
	"fmt"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/model"
)

// DerivativeMinuteBuffer consumes already-frozen, consecutive UTC seconds.
// It deliberately does not poll Current() or claim to implement live cutoffs.
type DerivativeMinuteBuffer struct {
	batch model.DerivativeMinuteBatch
	next  int
	last  model.DerivativeSnapshot
	depth int
}

func NewDerivativeMinuteBuffer(id uint32, minute time.Time, signed bool) (*DerivativeMinuteBuffer, error) {
	return NewDerivativeMinuteBufferWithDepth(id, minute, signed, model.BookDepth)
}

func NewDerivativeMinuteBufferWithDepth(id uint32, minute time.Time, signed bool, depth int) (*DerivativeMinuteBuffer, error) {
	if depth != 5 && depth != 10 {
		return nil, fmt.Errorf("derivative sampling depth must be 5 or 10")
	}
	if minute.IsZero() || !minute.Equal(minute.Truncate(time.Minute)) {
		return nil, fmt.Errorf("exact minute required")
	}
	if _, err := model.MinuteID(id, minute); err != nil {
		return nil, err
	}
	return &DerivativeMinuteBuffer{batch: model.DerivativeMinuteBatch{InstrumentID: id, MinuteTime: minute.UTC(), Signed: signed}, depth: depth}, nil
}

func (b *DerivativeMinuteBuffer) Sample(at time.Time, s model.DerivativeSnapshot, q model.DerivativeQuality) error {
	if b == nil || b.next >= 60 || !at.Equal(b.batch.MinuteTime.Add(time.Duration(b.next)*time.Second)) {
		return fmt.Errorf("samples must be exact consecutive seconds")
	}
	if q.ReplayValid {
		return fmt.Errorf("replay validity is derived by minute buffer")
	}
	if q.Sampled && (q.CapturedAt.Before(at) || q.CapturedAt.After(at.Add(250*time.Millisecond))) {
		return fmt.Errorf("sample outside cutoff completion deadline")
	}
	if !q.Sampled && q.StreamValid {
		return fmt.Errorf("unsampled state cannot be stream-valid")
	}
	for _, t := range []time.Time{q.ReceivedAt, q.LastSnapshotAt, q.ConnectionConfirmedAt, q.RulePublishedAt, q.MarketStateAt} {
		if t.After(at) {
			return fmt.Errorf("quality contains future state")
		}
	}
	if q.StreamValid {
		if s.InstrumentID != b.batch.InstrumentID {
			return fmt.Errorf("snapshot identity mismatch")
		}
		if err := s.Validate(b.depth, b.batch.Signed); err != nil {
			return err
		}
		if s.ReceivedAt.After(at) || !s.SourceTime.Equal(q.SourceTime) || !s.ReceivedAt.Equal(q.ReceivedAt) || s.Epoch != q.Epoch || s.ChangeID != q.ChangeID || int(q.BidLevels) != len(s.Bids) || int(q.AskLevels) != len(s.Asks) {
			return fmt.Errorf("snapshot and quality are different versions")
		}
	}
	valid := q.StreamValid && q.MarketKnown && q.Reason == model.DerivativeOK
	if q.StreamValid && !q.MarketKnown {
		q.Reason = model.DerivativeMetadataUncertain
	}
	if valid && b.next == 0 {
		id, _ := model.MinuteID(b.batch.InstrumentID, b.batch.MinuteTime)
		b.batch.Minute = &model.DerivativeMinute{ID: id, InstrumentID: b.batch.InstrumentID, MinuteTime: b.batch.MinuteTime,
			EncodingVersion: model.DerivativeEncodingVersion, StoredDepth: uint8(b.depth), Signed: b.batch.Signed,
			Bids: append([]model.Level(nil), s.Bids...), Asks: append([]model.Level(nil), s.Asks...)}
	}
	if valid && b.batch.Minute != nil {
		q.ReplayValid = true
		b.batch.Minute.ValidBitmap |= 1 << b.next
		if b.next > 0 {
			bp, bq := changes(b.last.Bids, s.Bids, true)
			ap, aq := changes(b.last.Asks, s.Asks, false)
			if len(bp)+len(ap) > 0 {
				b.batch.Deltas = append(b.batch.Deltas, model.BookDelta{MinuteID: b.batch.Minute.ID, SecondOffset: uint8(b.next), BidChangePrice: bp, BidChangeQty: bq, AskChangePrice: ap, AskChangeQty: aq})
				b.batch.Minute.DeltaBitmap |= 1 << b.next
			}
		}
		b.last = model.CloneDerivative(s)
	} else if valid {
		q.Reason = model.DerivativeAnchorMissing
	}
	b.batch.Quality[b.next] = q
	b.next++
	return nil
}

func (b *DerivativeMinuteBuffer) Complete() (model.DerivativeMinuteBatch, error) {
	if b == nil || b.next != 60 {
		return model.DerivativeMinuteBatch{}, fmt.Errorf("minute requires all 60 quality slots")
	}
	return model.CloneDerivativeBatch(b.batch), nil
}
