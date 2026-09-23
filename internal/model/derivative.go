package model

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

const DerivativeEncodingVersion uint8 = 1
const MinuteMask uint64 = 1<<60 - 1

// DerivativeSnapshot has no padding: a zero price can be a real combo level,
// and an empty side is a known absence of liquidity, not a missing snapshot.
type DerivativeSnapshot struct {
	InstrumentID uint32
	SourceTime   time.Time
	ReceivedAt   time.Time
	Epoch        uuid.UUID
	ChangeID     uint64
	Bids, Asks   []Level
}

func (s DerivativeSnapshot) Validate(depth int, signed bool) error {
	if s.InstrumentID == 0 || s.SourceTime.IsZero() || s.ReceivedAt.IsZero() || s.Epoch == uuid.Nil {
		return fmt.Errorf("derivative snapshot requires identity and provenance")
	}
	return ValidateDerivativeSides(s.Bids, s.Asks, depth, signed)
}

func ValidateDerivativeSides(bids, asks []Level, depth int, signed bool) error {
	if depth < 1 || len(bids) > depth || len(asks) > depth {
		return fmt.Errorf("derivative depth exceeded")
	}
	for side, levels := range [][]Level{bids, asks} {
		for i, l := range levels {
			if l.QtyLot == 0 || (!signed && l.PriceTick <= 0) {
				return fmt.Errorf("invalid derivative level")
			}
			if i > 0 && ((side == 0 && levels[i-1].PriceTick <= l.PriceTick) || (side == 1 && levels[i-1].PriceTick >= l.PriceTick)) {
				return fmt.Errorf("derivative side is not strictly ordered")
			}
		}
	}
	if len(bids) > 0 && len(asks) > 0 && bids[0].PriceTick >= asks[0].PriceTick {
		return fmt.Errorf("derivative book locked or crossed")
	}
	return nil
}

func CloneDerivative(s DerivativeSnapshot) DerivativeSnapshot {
	s.Bids = append([]Level(nil), s.Bids...)
	s.Asks = append([]Level(nil), s.Asks...)
	return s
}

type DerivativeReason uint8

const (
	DerivativeOK DerivativeReason = iota
	DerivativeNotReady
	DerivativeDisconnected
	DerivativeSequenceGap
	DerivativeParseError
	DerivativeMetadataUncertain
	DerivativeSamplingLag
	DerivativeAnchorMissing
	DerivativeResourceLimit
	DerivativeClockDiscontinuity
	DerivativeMarketClosed
)

// Zero times/IDs mean unknown and are stored as NULL, never as the sample time.
type DerivativeQuality struct {
	Sampled, StreamValid, ReplayValid     bool
	MarketKnown, MarketOpen               bool
	SourceTime, ReceivedAt, CapturedAt    time.Time
	Epoch                                 uuid.UUID
	ChangeID                              uint64
	LastSnapshotAt, ConnectionConfirmedAt time.Time
	TradingRuleID                         string
	RulePublishedAt                       time.Time
	MarketStateAt                         time.Time
	MarketStateBasis                      uint8 // 0 unknown, 1 lifecycle ingress, 2 catalog publication
	Reason                                DerivativeReason
	BidLevels, AskLevels                  uint8
}

type DerivativeMinute struct {
	ID                           uint64
	InstrumentID                 uint32
	MinuteTime                   time.Time
	EncodingVersion, StoredDepth uint8
	Signed                       bool
	ValidBitmap, DeltaBitmap     uint64
	Bids, Asks                   []Level
}

type DerivativeMinuteBatch struct {
	InstrumentID uint32
	MinuteTime   time.Time
	Signed       bool
	// nil means second zero was unavailable. Quality still contains 60 slots.
	Minute  *DerivativeMinute
	Deltas  []BookDelta
	Quality [60]DerivativeQuality
}

func CloneDerivativeBatch(b DerivativeMinuteBatch) DerivativeMinuteBatch {
	if b.Minute != nil {
		m := *b.Minute
		m.Bids = append([]Level(nil), m.Bids...)
		m.Asks = append([]Level(nil), m.Asks...)
		b.Minute = &m
	}
	b.Deltas = append([]BookDelta(nil), b.Deltas...)
	for i := range b.Deltas {
		d := &b.Deltas[i]
		d.BidChangePrice = append([]int64(nil), d.BidChangePrice...)
		d.BidChangeQty = append([]uint64(nil), d.BidChangeQty...)
		d.AskChangePrice = append([]int64(nil), d.AskChangePrice...)
		d.AskChangeQty = append([]uint64(nil), d.AskChangeQty...)
	}
	return b
}
