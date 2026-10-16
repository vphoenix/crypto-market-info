package codec

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/replay"
)

func rotatingBooks(t testing.TB, depths []int) model.CompletedMinute {
	t.Helper()
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	c := model.CompletedMinute{MinuteTime: at}
	for i, depth := range depths {
		id, err := model.MinuteID(uint32(i+1), at)
		if err != nil {
			t.Fatal(err)
		}
		b := model.MinuteBatch{Minute: model.MinuteBook{ID: id, InstrumentID: uint32(i + 1), MinuteTime: at, StoredDepth: uint8(depth), ValidBitmap: 1<<60 - 1}}
		for level := 0; level < depth; level++ {
			b.Minute.Bids[level] = model.Level{PriceTick: 100000 - int64(level), QtyLot: uint64(level + 1)}
			b.Minute.Asks[level] = model.Level{PriceTick: 100001 + int64(level), QtyLot: uint64(level + 1)}
		}
		for second := 1; second < 60; second++ {
			d := model.BookDelta{MinuteID: id, SecondOffset: uint8(second)}
			for level := 0; level < depth; level++ {
				old := int64((second - 1) * 1000)
				next := int64(second * 1000)
				d.BidChangePrice = append(d.BidChangePrice, 100000+old-int64(level), 100000+next-int64(level))
				d.AskChangePrice = append(d.AskChangePrice, 100001+old+int64(level), 100001+next+int64(level))
				d.BidChangeQty = append(d.BidChangeQty, 0, uint64(level+second+1))
				d.AskChangeQty = append(d.AskChangeQty, 0, uint64(level+second+1))
			}
			b.Deltas = append(b.Deltas, d)
		}
		c.Batches = append(c.Batches, b)
	}
	return c
}

func TestTypedBooksPreserveWireAndEverySecond(t *testing.T) {
	c := rotatingBooks(t, []int{5, 10, 50})
	for _, version := range []uint16{1, 2} {
		c.KnownFromMS = 0
		if version == 2 {
			c.KnownFromMS = c.MinuteTime.Add(65 * time.Second).UnixMilli()
		}
		raw, err := EncodeBookDelivery(c, version)
		if err != nil {
			t.Fatal(err)
		}
		got, err := DecodeBookDelivery(c.MinuteTime, raw, version)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(c, got) {
			t.Fatal("typed decoder changed model")
		}
		again, err := EncodeBookDelivery(got, version)
		if err != nil || !bytes.Equal(raw, again) {
			t.Fatal("wire changed", err)
		}
		for _, book := range got.Batches {
			for sec := uint8(0); sec < 60; sec++ {
				snapshot, valid, err := replay.AtSecond(book.Minute, book.Deltas, sec)
				if err != nil || !valid {
					t.Fatal(sec, valid, err)
				}
				if len(snapshot.Bids) != int(book.Minute.StoredDepth) || len(snapshot.Asks) != int(book.Minute.StoredDepth) {
					t.Fatal("depth lost")
				}
				for level := range snapshot.Bids {
					wantBid := model.Level{PriceTick: 100000 + int64(sec)*1000 - int64(level), QtyLot: uint64(level) + uint64(sec) + 1}
					wantAsk := model.Level{PriceTick: 100001 + int64(sec)*1000 + int64(level), QtyLot: wantBid.QtyLot}
					if snapshot.Bids[level] != wantBid || snapshot.Asks[level] != wantAsk {
						t.Fatal("deletion or replacement changed", sec, level)
					}
				}
			}
		}
	}
}

func TestTypedBooksRejectMalformedLengthsBeforeAllocation(t *testing.T) {
	c := rotatingBooks(t, []int{5})
	raw, err := EncodeBooks(c)
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func([]byte) []byte{
		"outer_count":  func(b []byte) []byte { binary.BigEndian.PutUint32(b[17:], 1<<21); return b },
		"anchor_count": func(b []byte) []byte { binary.BigEndian.PutUint32(b[50:], 1<<21); return b },
		"delta_count":  func(b []byte) []byte { binary.BigEndian.PutUint32(b[1666:], 60); return b },
		"truncated":    func(b []byte) []byte { return b[:len(b)-1] },
		"trailing":     func(b []byte) []byte { return append(b, 0) },
		"fingerprint":  func(b []byte) []byte { b[2] ^= 1; return b },
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeBooks(c.MinuteTime, mutate(bytes.Clone(raw))); err == nil {
				t.Fatal("accepted corrupt payload")
			}
		})
	}
}

func BenchmarkBookDeliveryDecodeExtreme(b *testing.B) {
	depths := make([]int, 64)
	for i := range depths {
		depths[i] = 50
	}
	c := rotatingBooks(b, depths)
	c.KnownFromMS = c.MinuteTime.Add(65 * time.Second).UnixMilli()
	raw, err := EncodeBookDelivery(c, 2)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(raw)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		got, err := DecodeBookDelivery(c.MinuteTime, raw, 2)
		if err != nil || len(got.Batches) != 64 {
			b.Fatal(err)
		}
	}
}
