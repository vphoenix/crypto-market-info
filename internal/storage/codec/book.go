package codec

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"reflect"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
)

var bookEnvelopeType = mustParse("Array(Tuple(id UInt64, instrument_id UInt32, minute_time DateTime('UTC'), valid_bitmap UInt64, stored_depth UInt8, bid_prices Array(Int64), bid_qtys Array(UInt64), ask_prices Array(Int64), ask_qtys Array(UInt64), deltas Array(Tuple(second_offset UInt8, bid_prices Array(Int64), bid_qtys Array(UInt64), ask_prices Array(Int64), ask_qtys Array(UInt64)))))")
var deltaType = mustParse("Tuple(minute_id UInt64, second_offset UInt8, bid_prices Array(Int64), bid_qtys Array(UInt64), ask_prices Array(Int64), ask_qtys Array(UInt64))")

func EncodeDelta(d model.BookDelta) ([]byte, error) {
	if d.SecondOffset == 0 || d.SecondOffset > 59 || len(d.BidChangePrice) != len(d.BidChangeQty) || len(d.AskChangePrice) != len(d.AskChangeQty) {
		return nil, fmt.Errorf("invalid delta")
	}
	return Encode(deltaType, map[string]any{"minute_id": d.MinuteID, "second_offset": d.SecondOffset, "bid_prices": d.BidChangePrice, "bid_qtys": d.BidChangeQty, "ask_prices": d.AskChangePrice, "ask_qtys": d.AskChangeQty})
}
func DecodeDelta(raw []byte) (model.BookDelta, error) {
	var d model.BookDelta
	v, e := Decode(deltaType, raw)
	if e != nil {
		return d, e
	}
	r := v.(map[string]any)
	d.MinuteID = r["minute_id"].(uint64)
	d.SecondOffset = r["second_offset"].(uint8)
	for _, x := range []struct {
		name string
		dest any
	}{{"bid_prices", &d.BidChangePrice}, {"bid_qtys", &d.BidChangeQty}, {"ask_prices", &d.AskChangePrice}, {"ask_qtys", &d.AskChangeQty}} {
		if e = Assign(reflect.ValueOf(x.dest).Elem(), r[x.name]); e != nil {
			return d, e
		}
	}
	_, e = EncodeDelta(d)
	return d, e
}

func mustParse(s string) *Type {
	t, e := Parse(s)
	if e != nil {
		panic(e)
	}
	return t
}
func ValidateCompletedMinute(c model.CompletedMinute) error {
	if c.MinuteTime.IsZero() || !c.MinuteTime.Equal(c.MinuteTime.UTC().Truncate(time.Minute)) {
		return fmt.Errorf("completed minute requires exact UTC minute")
	}
	for i, b := range c.Batches {
		if !b.Minute.MinuteTime.Equal(c.MinuteTime) || (i > 0 && c.Batches[i-1].Minute.InstrumentID >= b.Minute.InstrumentID) {
			return fmt.Errorf("completed batches must be sorted and share minute")
		}
		if e := contracts.ValidateMinuteBatch(b); e != nil {
			return e
		}
	}
	return nil
}

// EncodeBooks stores all 50 physical anchor levels, including old levels that
// can become visible after deleting an earlier price. No replay truncation occurs.
func EncodeBooks(c model.CompletedMinute) ([]byte, error) {
	if e := ValidateCompletedMinute(c); e != nil {
		return nil, e
	}
	rows := make([]any, len(c.Batches))
	for n, b := range c.Batches {
		bp, bq, ap, aq := make([]int64, 50), make([]uint64, 50), make([]int64, 50), make([]uint64, 50)
		for i := range 50 {
			bp[i] = b.Minute.Bids[i].PriceTick
			bq[i] = b.Minute.Bids[i].QtyLot
			ap[i] = b.Minute.Asks[i].PriceTick
			aq[i] = b.Minute.Asks[i].QtyLot
		}
		deltas := make([]any, len(b.Deltas))
		for i, d := range b.Deltas {
			deltas[i] = map[string]any{"second_offset": d.SecondOffset, "bid_prices": d.BidChangePrice, "bid_qtys": d.BidChangeQty, "ask_prices": d.AskChangePrice, "ask_qtys": d.AskChangeQty}
		}
		rows[n] = map[string]any{"id": b.Minute.ID, "instrument_id": b.Minute.InstrumentID, "minute_time": b.Minute.MinuteTime, "valid_bitmap": b.Minute.ValidBitmap, "stored_depth": b.Minute.StoredDepth, "bid_prices": bp, "bid_qtys": bq, "ask_prices": ap, "ask_qtys": aq, "deltas": deltas}
	}
	return encode(bookEnvelopeType, rows, 1<<21)
}
func DecodeBooks(minute time.Time, payload []byte) (model.CompletedMinute, error) {
	return decodeBooksTyped(minute, payload)
}

// Version 2 carries first knowledge inside the checksummed durable payload.
// Version 1 remains readable for already sealed Redis/spool deliveries.
var bookDeliveryType = mustParse("Tuple(known_from_ms Int64, books String)")

func EncodeBookDelivery(c model.CompletedMinute, version uint16) ([]byte, error) {
	raw, err := EncodeBooks(c)
	if err != nil {
		return nil, err
	}
	switch version {
	case 1:
		if c.KnownFromMS != 0 {
			return nil, fmt.Errorf("v1 cannot carry first knowledge")
		}
		return raw, nil
	case 2:
		if c.KnownFromMS < c.MinuteTime.Add(time.Minute).UnixMilli() || c.KnownFromMS > model.CeilMilliseconds(time.Now().UTC()) {
			return nil, fmt.Errorf("invalid minute first knowledge")
		}
		return Encode(bookDeliveryType, map[string]any{"known_from_ms": c.KnownFromMS, "books": string(raw)})
	default:
		return nil, fmt.Errorf("unsupported book delivery codec")
	}
}

func DecodeBookDelivery(minute time.Time, payload []byte, version uint16) (model.CompletedMinute, error) {
	known, raw, err := bookDeliveryFrame(minute, payload, version)
	if err != nil {
		return model.CompletedMinute{}, err
	}
	c, err := DecodeBooks(minute, raw)
	c.KnownFromMS = known
	return c, err
}

func bookDeliveryFrame(minute time.Time, payload []byte, version uint16) (int64, []byte, error) {
	if version == 1 {
		return 0, payload, nil
	}
	if version != 2 {
		return 0, nil, fmt.Errorf("unsupported book delivery codec")
	}
	// The v2 String field is a frame; avoid two payload-sized text copies.
	if len(payload) < 29 || len(payload) > MaxBytes || payload[0] != Version || !bytes.Equal(payload[1:17], bookDeliveryType.Tag[:]) {
		return 0, nil, fmt.Errorf("invalid book delivery header")
	}
	if uint64(binary.BigEndian.Uint32(payload[25:29])) != uint64(len(payload)-29) {
		return 0, nil, fmt.Errorf("book delivery frame length mismatch")
	}
	known := int64(binary.BigEndian.Uint64(payload[17:25]))
	if known < minute.Add(time.Minute).UnixMilli() || known > model.CeilMilliseconds(time.Now().UTC()) {
		return 0, nil, fmt.Errorf("invalid minute first knowledge")
	}
	return known, payload[29:], nil
}
