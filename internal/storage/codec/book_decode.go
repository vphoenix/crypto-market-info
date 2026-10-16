package codec

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
)

// bookReader decodes the existing wire format directly into integer slices.
// No per-price interface values or per-second maps coexist with the result.
type bookReader struct {
	raw    []byte
	budget int
}

func (r *bookReader) take(n int) ([]byte, error) {
	if n < 0 || n > len(r.raw) {
		return nil, io.ErrUnexpectedEOF
	}
	v := r.raw[:n]
	r.raw = r.raw[n:]
	return v, nil
}
func (r *bookReader) count(width int) (int, error) {
	raw, err := r.take(4)
	if err != nil {
		return 0, err
	}
	n := uint64(binary.BigEndian.Uint32(raw))
	if n > uint64(r.budget) {
		return 0, fmt.Errorf("array limit")
	}
	if n*uint64(width) > uint64(len(r.raw)) {
		return 0, io.ErrUnexpectedEOF
	}
	r.budget -= int(n)
	return int(n), nil
}
func readBookIntegers[T ~int64 | ~uint64](r *bookReader) ([]T, error) {
	n, err := r.count(8)
	if err != nil {
		return nil, err
	}
	raw, err := r.take(n * 8)
	if err != nil {
		return nil, err
	}
	values := make([]T, n)
	for i := range values {
		values[i] = T(binary.BigEndian.Uint64(raw[i*8:]))
	}
	return values, nil
}

func decodeBooksTyped(minute time.Time, payload []byte) (model.CompletedMinute, error) {
	out := model.CompletedMinute{MinuteTime: minute}
	err := walkBooks(minute, payload, func(batch model.MinuteBatch) error { out.Batches = append(out.Batches, batch); return nil })
	return out, err
}

// WalkBookDelivery validates and visits one book at a time without retaining the
// decoded batch. The original wire frame remains the durable source of truth.
func WalkBookDelivery(minute time.Time, payload []byte, version uint16, visit func(model.MinuteBatch, int64) error) error {
	known, raw, err := bookDeliveryFrame(minute, payload, version)
	if err != nil {
		return err
	}
	return walkBooks(minute, raw, func(batch model.MinuteBatch) error { return visit(batch, known) })
}

func walkBooks(minute time.Time, payload []byte, visit func(model.MinuteBatch) error) error {
	if minute.IsZero() || !minute.Equal(minute.UTC().Truncate(time.Minute)) {
		return fmt.Errorf("completed minute requires exact UTC minute")
	}
	if len(payload) < 17 || len(payload) > MaxBytes || payload[0] != Version || !bytes.Equal(payload[1:17], bookEnvelopeType.Tag[:]) {
		return fmt.Errorf("invalid codec header")
	}
	r := bookReader{raw: payload[17:], budget: 1 << 21}
	// Every valid book has four arrays of exactly 50 physical anchor values.
	// Validate that lower bound before allocating the result slice.
	n, err := r.count(8 + 4 + 8 + 8 + 1 + 4*(4+50*8) + 4)
	if err != nil {
		return err
	}
	var previous uint32
	for i := 0; i < n; i++ {
		b := new(model.MinuteBatch)
		raw, err := r.take(29)
		if err != nil {
			return err
		}
		b.Minute.ID = binary.BigEndian.Uint64(raw)
		b.Minute.InstrumentID = binary.BigEndian.Uint32(raw[8:])
		b.Minute.MinuteTime = time.Unix(int64(binary.BigEndian.Uint64(raw[12:])), 0).UTC()
		if b.Minute.MinuteTime.Year() < 1970 || b.Minute.MinuteTime.Year() > 9999 {
			return fmt.Errorf("timestamp range")
		}
		b.Minute.ValidBitmap = binary.BigEndian.Uint64(raw[20:])
		b.Minute.StoredDepth = raw[28]
		for _, side := range []*[50]model.Level{&b.Minute.Bids, &b.Minute.Asks} {
			for field := range 2 {
				n, err := r.count(8)
				if err != nil {
					return err
				}
				if n != 50 {
					return fmt.Errorf("anchor needs 50 physical levels")
				}
				raw, err := r.take(n * 8)
				if err != nil {
					return err
				}
				for level := range side {
					value := binary.BigEndian.Uint64(raw[level*8:])
					if field == 0 {
						side[level].PriceTick = int64(value)
					} else {
						side[level].QtyLot = value
					}
				}
			}
		}
		n, err := r.count(1 + 4*4)
		if err != nil {
			return err
		}
		if n > 59 {
			return fmt.Errorf("too many second deltas")
		}
		b.Deltas = make([]model.BookDelta, n)
		for j := range b.Deltas {
			d := &b.Deltas[j]
			offset, err := r.take(1)
			if err != nil {
				return err
			}
			d.MinuteID, d.SecondOffset = b.Minute.ID, offset[0]
			if d.BidChangePrice, err = readBookIntegers[int64](&r); err != nil {
				return err
			}
			if d.BidChangeQty, err = readBookIntegers[uint64](&r); err != nil {
				return err
			}
			if d.AskChangePrice, err = readBookIntegers[int64](&r); err != nil {
				return err
			}
			if d.AskChangeQty, err = readBookIntegers[uint64](&r); err != nil {
				return err
			}
		}
		if !b.Minute.MinuteTime.Equal(minute) || (i > 0 && previous >= b.Minute.InstrumentID) {
			return fmt.Errorf("completed batches must be sorted and share minute")
		}
		if err := contracts.ValidateMinuteBatch(*b); err != nil {
			return err
		}
		previous = b.Minute.InstrumentID
		if err := visit(*b); err != nil {
			return err
		}
	}
	if len(r.raw) != 0 {
		return fmt.Errorf("trailing bytes")
	}
	return nil
}
