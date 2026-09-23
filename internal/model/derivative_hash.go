package model

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"hash"
	"time"
)

// DerivativeBatchHash is a deterministic binary encoding, not JSON, and does
// not depend on time zones, monotonic clock components, or nil-vs-empty slices.
func DerivativeBatchHash(b DerivativeMinuteBatch) string {
	h := &derivativeHasher{h: sha256.New()}
	h.text("derivative-book-batch-v1")
	h.u(uint64(b.InstrumentID))
	h.time(b.MinuteTime)
	h.flag(b.Signed)
	h.flag(b.Minute != nil)
	if m := b.Minute; m != nil {
		h.u(m.ID)
		h.u(uint64(m.InstrumentID))
		h.time(m.MinuteTime)
		h.u(uint64(m.EncodingVersion))
		h.u(uint64(m.StoredDepth))
		h.flag(m.Signed)
		h.u(m.ValidBitmap)
		h.u(m.DeltaBitmap)
		h.levels(m.Bids)
		h.levels(m.Asks)
	}
	h.u(uint64(len(b.Deltas)))
	for _, d := range b.Deltas {
		h.u(d.MinuteID)
		h.u(uint64(d.SecondOffset))
		h.ints(d.BidChangePrice)
		h.uints(d.BidChangeQty)
		h.ints(d.AskChangePrice)
		h.uints(d.AskChangeQty)
	}
	for _, q := range b.Quality {
		h.flag(q.Sampled)
		h.flag(q.StreamValid)
		h.flag(q.ReplayValid)
		h.flag(q.MarketKnown)
		h.flag(q.MarketOpen)
		h.time(q.SourceTime)
		h.time(q.ReceivedAt)
		h.time(q.CapturedAt)
		_, _ = h.h.Write(q.Epoch[:])
		h.u(q.ChangeID)
		h.time(q.LastSnapshotAt)
		h.time(q.ConnectionConfirmedAt)
		h.text(q.TradingRuleID)
		h.time(q.RulePublishedAt)
		h.time(q.MarketStateAt)
		h.u(uint64(q.MarketStateBasis))
		h.u(uint64(q.Reason))
		h.u(uint64(q.BidLevels))
		h.u(uint64(q.AskLevels))
	}
	return hex.EncodeToString(h.h.Sum(nil))
}

func ValidDigest(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}

type derivativeHasher struct{ h hash.Hash }

func (h *derivativeHasher) u(v uint64) {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], v)
	_, _ = h.h.Write(b[:])
}
func (h *derivativeHasher) text(s string) { h.u(uint64(len(s))); _, _ = h.h.Write([]byte(s)) }
func (h *derivativeHasher) flag(b bool) {
	if b {
		h.u(1)
	} else {
		h.u(0)
	}
}
func (h *derivativeHasher) time(t time.Time) {
	h.flag(!t.IsZero())
	if !t.IsZero() {
		h.u(uint64(t.Unix()))
		h.u(uint64(t.Nanosecond()))
	}
}
func (h *derivativeHasher) levels(ls []Level) {
	h.u(uint64(len(ls)))
	for _, l := range ls {
		h.u(uint64(l.PriceTick))
		h.u(l.QtyLot)
	}
}
func (h *derivativeHasher) ints(v []int64) {
	h.u(uint64(len(v)))
	for _, n := range v {
		h.u(uint64(n))
	}
}
func (h *derivativeHasher) uints(v []uint64) {
	h.u(uint64(len(v)))
	for _, n := range v {
		h.u(n)
	}
}
