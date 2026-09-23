package options

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/replay"
)

// BookEnvelope is explicitly an OFFLINE book-only foundation batch, not R3's
// full live OptionsCompletedMinute. The separate commit namespace prevents a
// fixture from claiming quote, analytics, catalog or lifecycle coverage.
type BookEnvelope struct {
	RunID                  uuid.UUID
	MinuteTime, PreparedAt time.Time
	Origin                 string // fixture or synthetic
	EvidenceHash           string
	Batches                []model.DerivativeMinuteBatch
}

func (e BookEnvelope) Validate() error {
	if e.RunID == uuid.Nil || e.MinuteTime.IsZero() || !e.MinuteTime.Equal(e.MinuteTime.Truncate(time.Minute)) || e.PreparedAt.IsZero() || !e.PreparedAt.Equal(e.PreparedAt.Truncate(time.Microsecond)) || !model.ValidDigest(e.EvidenceHash) || (e.Origin != "fixture" && e.Origin != "synthetic") {
		return fmt.Errorf("invalid offline book envelope")
	}
	return ValidateBookBatches(e.MinuteTime, e.Batches)
}

// ValidateBookBatches checks the shared encoding, independently of its source.
func ValidateBookBatches(minute time.Time, batches []model.DerivativeMinuteBatch) error {
	if len(batches) == 0 || len(batches) > 448 {
		return fmt.Errorf("book envelope outside member budget")
	}
	var estimated int
	for n, b := range batches {
		if !b.MinuteTime.Equal(minute) || (n > 0 && batches[n-1].InstrumentID >= b.InstrumentID) {
			return fmt.Errorf("book members must be sorted, unique and share a minute")
		}
		if err := replay.ValidateDerivativeBatch(b); err != nil {
			return fmt.Errorf("instrument %d: %w", b.InstrumentID, err)
		}
		estimated += 60*512 + 2048
		for _, d := range b.Deltas {
			estimated += (len(d.BidChangePrice)+len(d.AskChangePrice))*16 + 64
		}
	}
	if estimated > 128<<20 {
		return fmt.Errorf("book envelope exceeds byte budget")
	}
	return nil
}

func (e BookEnvelope) ID() string {
	h := sha256.New()
	_, _ = h.Write([]byte("offline-derivative-book-envelope-v1"))
	_, _ = h.Write(e.RunID[:])
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], uint64(e.MinuteTime.Unix()))
	_, _ = h.Write(b[:])
	for _, s := range []string{e.Origin, e.EvidenceHash, e.PreparedAt.UTC().Format(time.RFC3339Nano)} {
		binary.LittleEndian.PutUint64(b[:], uint64(len(s)))
		_, _ = h.Write(b[:])
		_, _ = h.Write([]byte(s))
	}
	for _, m := range e.Batches {
		_, _ = h.Write([]byte(model.DerivativeBatchHash(m)))
	}
	return hex.EncodeToString(h.Sum(nil))
}

func (e BookEnvelope) Clone() BookEnvelope {
	e.Batches = append([]model.DerivativeMinuteBatch(nil), e.Batches...)
	for i := range e.Batches {
		e.Batches[i] = model.CloneDerivativeBatch(e.Batches[i])
	}
	return e
}
