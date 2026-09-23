package orderbook

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/model"
)

type DerivativeAction uint8

const (
	DerivativeNew DerivativeAction = iota + 1
	DerivativeChange
	DerivativeDelete
)

type DerivativeChangeLevel struct {
	Action DerivativeAction
	model.Level
}
type DerivativeUpdate struct {
	InstrumentID           uint32
	Snapshot               bool
	Epoch                  uuid.UUID
	ChangeID, PrevChangeID uint64
	SourceTime, ReceivedAt time.Time
	Bids, Asks             []DerivativeChangeLevel
}

// DerivativeBook retains the COMPLETE source book. It never trims memory to
// stored depth. Resource or protocol errors invalidate it until a new epoch's
// snapshot establishes a new baseline.
type DerivativeBook struct {
	mu                           sync.RWMutex
	id                           uint32
	signed                       bool
	limit                        int
	epoch                        uuid.UUID
	valid                        bool
	reason                       model.DerivativeReason
	sequence                     uint64
	digest                       [32]byte
	source, received, snapshotAt time.Time
	bids, asks                   map[int64]uint64
}

func NewDerivative(id uint32, signed bool, maxLevels int) (*DerivativeBook, error) {
	if id == 0 || maxLevels < model.BookDepth || maxLevels > 20000 {
		return nil, fmt.Errorf("invalid derivative book identity/capacity")
	}
	return &DerivativeBook{id: id, signed: signed, limit: maxLevels, reason: model.DerivativeNotReady}, nil
}

func (b *DerivativeBook) Reset(epoch uuid.UUID) error {
	if epoch == uuid.Nil {
		return fmt.Errorf("empty connection epoch")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if epoch == b.epoch {
		return fmt.Errorf("recovery requires a new subscription epoch")
	}
	b.epoch, b.valid, b.reason = epoch, false, model.DerivativeNotReady
	b.bids, b.asks = nil, nil
	b.sequence, b.digest = 0, [32]byte{}
	b.source, b.received, b.snapshotAt = time.Time{}, time.Time{}, time.Time{}
	return nil
}

func (b *DerivativeBook) Invalidate(reason model.DerivativeReason) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if reason == model.DerivativeOK {
		reason = model.DerivativeParseError
	}
	b.valid, b.reason = false, reason
}

func (b *DerivativeBook) Apply(u DerivativeUpdate) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	// A queued message from a retired subscription must not poison its replacement.
	if u.Epoch != b.epoch || u.Epoch == uuid.Nil {
		return fmt.Errorf("stale subscription epoch")
	}
	fail := func(reason model.DerivativeReason, message string) error {
		b.valid, b.reason = false, reason
		return fmt.Errorf("%s", message)
	}
	if u.InstrumentID != b.id || u.SourceTime.IsZero() || u.ReceivedAt.IsZero() {
		return fail(model.DerivativeParseError, "invalid update identity/provenance")
	}
	digest := derivativeUpdateDigest(u)
	if b.valid && u.ChangeID == b.sequence {
		if digest == b.digest {
			return nil
		}
		return fail(model.DerivativeSequenceGap, "conflicting duplicate change ID")
	}
	if u.Snapshot {
		if b.valid || b.reason != model.DerivativeNotReady {
			return fail(model.DerivativeSequenceGap, "unexpected snapshot: reset subscription first")
		}
	} else if !b.valid || u.PrevChangeID != b.sequence || u.ChangeID <= b.sequence {
		return fail(model.DerivativeSequenceGap, "missing baseline or sequence predecessor")
	}
	if !b.received.IsZero() && u.ReceivedAt.Before(b.received) {
		return fail(model.DerivativeClockDiscontinuity, "receive time went backwards")
	}
	if len(u.Bids) > 2*b.limit || len(u.Asks) > 2*b.limit {
		return fail(model.DerivativeResourceLimit, "update exceeds level budget")
	}
	// Validate against temporary copies: a malformed final action cannot partially
	// update the last known state. The configured cap bounds this allocation.
	bids, asks := make(map[int64]uint64), make(map[int64]uint64)
	if !u.Snapshot {
		for p, q := range b.bids {
			bids[p] = q
		}
		for p, q := range b.asks {
			asks[p] = q
		}
	}
	for i, side := range []map[int64]uint64{bids, asks} {
		changes := u.Bids
		if i == 1 {
			changes = u.Asks
		}
		seen := make(map[int64]bool, len(changes))
		for _, l := range changes {
			if seen[l.PriceTick] || (!b.signed && l.PriceTick <= 0) {
				return fail(model.DerivativeParseError, "duplicate or invalid price")
			}
			seen[l.PriceTick] = true
			_, exists := side[l.PriceTick]
			if u.Snapshot && l.Action != DerivativeNew {
				return fail(model.DerivativeParseError, "snapshot action must be new")
			}
			switch l.Action {
			case DerivativeNew:
				if exists || l.QtyLot == 0 {
					return fail(model.DerivativeParseError, "invalid new level")
				}
				side[l.PriceTick] = l.QtyLot
			case DerivativeChange:
				if !exists || l.QtyLot == 0 {
					return fail(model.DerivativeParseError, "invalid change level")
				}
				side[l.PriceTick] = l.QtyLot
			case DerivativeDelete:
				if !exists || l.QtyLot != 0 {
					return fail(model.DerivativeParseError, "invalid deletion")
				}
				delete(side, l.PriceTick)
			default:
				return fail(model.DerivativeParseError, "unknown action")
			}
		}
		if len(side) > b.limit {
			return fail(model.DerivativeResourceLimit, "full book exceeds memory level cap")
		}
	}
	// sorted() uses a bounded heap, including correctly ordered negative prices.
	if err := model.ValidateDerivativeSides(sorted(bids, true, 1), sorted(asks, false, 1), 1, b.signed); err != nil {
		return fail(model.DerivativeParseError, err.Error())
	}
	b.bids, b.asks, b.sequence, b.digest = bids, asks, u.ChangeID, digest
	b.source, b.received = u.SourceTime.UTC(), u.ReceivedAt.UTC()
	if u.Snapshot {
		b.snapshotAt = b.received
	}
	b.valid, b.reason = true, model.DerivativeOK
	return nil
}

// Current is NOT an as-of sampler. A live runner must order ingress and cutoff
// barriers before reading it. The phase-one offline replay calls it serially.
func (b *DerivativeBook) Current() (model.DerivativeSnapshot, model.DerivativeQuality) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	q := model.DerivativeQuality{StreamValid: b.valid, Reason: b.reason}
	if !b.valid {
		return model.DerivativeSnapshot{}, q
	}
	s := model.DerivativeSnapshot{InstrumentID: b.id, SourceTime: b.source, ReceivedAt: b.received, Epoch: b.epoch, ChangeID: b.sequence,
		Bids: sorted(b.bids, true, model.BookDepth), Asks: sorted(b.asks, false, model.BookDepth)}
	q.SourceTime, q.ReceivedAt, q.Epoch, q.ChangeID, q.LastSnapshotAt = b.source, b.received, b.epoch, b.sequence, b.snapshotAt
	q.BidLevels, q.AskLevels = uint8(len(s.Bids)), uint8(len(s.Asks))
	return s, q
}

func derivativeUpdateDigest(u DerivativeUpdate) [32]byte {
	h := sha256.New()
	put := func(v uint64) { hashUint(h, v) }
	put(uint64(u.InstrumentID))
	if u.Snapshot {
		put(1)
	} else {
		put(0)
	}
	put(u.ChangeID)
	put(u.PrevChangeID)
	put(uint64(u.SourceTime.UnixMilli()))
	for _, side := range [][]DerivativeChangeLevel{u.Bids, u.Asks} {
		put(uint64(len(side)))
		for _, l := range side {
			put(uint64(l.Action))
			put(uint64(l.PriceTick))
			put(l.QtyLot)
		}
	}
	var result [32]byte
	copy(result[:], h.Sum(nil))
	return result
}
func hashUint(h hash.Hash, v uint64) {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], v)
	_, _ = h.Write(b[:])
}
