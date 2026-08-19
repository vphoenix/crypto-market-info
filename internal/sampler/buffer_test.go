package sampler

import (
	"reflect"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/replay"
)

func TestMinuteBufferReplayValidNoChangeInvalidAndDelete(t *testing.T) {
	minute := time.Date(2026, 8, 19, 1, 2, 0, 0, time.UTC)
	buffer, err := NewMinuteBuffer(7, minute)
	if err != nil {
		t.Fatal(err)
	}
	s0 := sample(7, 1, []model.Level{{PriceTick: 100, QtyLot: 2}, {PriceTick: 99, QtyLot: 3}}, []model.Level{{PriceTick: 101, QtyLot: 4}, {PriceTick: 102, QtyLot: 5}})
	if err := buffer.Sample(minute, s0, true); err != nil {
		t.Fatal(err)
	}
	if err := buffer.Sample(minute.Add(time.Second), s0, true); err != nil {
		t.Fatal(err)
	}
	if err := buffer.Sample(minute.Add(2*time.Second), model.BookSnapshot{}, false); err != nil {
		t.Fatal(err)
	}
	s3 := sample(7, 4, []model.Level{{PriceTick: 100, QtyLot: 9}, {PriceTick: 98, QtyLot: 6}}, []model.Level{{PriceTick: 101, QtyLot: 4}, {PriceTick: 103, QtyLot: 7}})
	if err := buffer.Sample(minute.Add(3*time.Second), s3, true); err != nil {
		t.Fatal(err)
	}
	batch, ok := buffer.Batch()
	if !ok {
		t.Fatal("anchored minute was dropped")
	}
	if batch.Minute.ValidBitmap != 0b1011 || len(batch.Deltas) != 1 {
		t.Fatalf("bitmap=%b deltas=%+v", batch.Minute.ValidBitmap, batch.Deltas)
	}
	if got := batch.Deltas[0]; !reflect.DeepEqual(got.BidChangePrice, []int64{100, 99, 98}) || !reflect.DeepEqual(got.BidChangeQty, []uint64{9, 0, 6}) {
		t.Fatalf("unexpected bid delta %+v", got)
	}
	for _, second := range []uint8{0, 1, 3} {
		got, valid, err := replay.AtSecond(batch.Minute, batch.Deltas, second)
		if err != nil || !valid {
			t.Fatalf("second=%d valid=%v err=%v", second, valid, err)
		}
		want := s0
		if second == 3 {
			want = s3
		}
		if !reflect.DeepEqual(got.Bids, want.Bids) || !reflect.DeepEqual(got.Asks, want.Asks) {
			t.Fatalf("second=%d got=%+v want=%+v", second, got, want)
		}
	}
	if _, valid, _ := replay.AtSecond(batch.Minute, batch.Deltas, 2); valid {
		t.Fatal("invalid second replayed")
	}
}

func TestMinuteWithoutSecondZeroIsDropped(t *testing.T) {
	minute := time.Date(2026, 8, 19, 1, 2, 0, 0, time.UTC)
	buffer, _ := NewMinuteBuffer(1, minute)
	_ = buffer.Sample(minute, model.BookSnapshot{}, false)
	_ = buffer.Sample(minute.Add(time.Second), sample(1, 1, []model.Level{{PriceTick: 1, QtyLot: 1}}, []model.Level{{PriceTick: 2, QtyLot: 1}}), true)
	if _, ok := buffer.Batch(); ok {
		t.Fatal("minute without second-zero anchor was retained")
	}
}

func TestEverySecondRoundTripsAndSamplerSeesFinalSourceState(t *testing.T) {
	minute := time.Date(2026, 8, 19, 2, 0, 0, 0, time.UTC)
	buffer, _ := NewMinuteBuffer(9, minute)
	states := make([]model.BookSnapshot, 60)
	for second := 0; second < 60; second++ {
		states[second] = sample(9, int64(second+1), []model.Level{{PriceTick: 100, QtyLot: uint64(second + 1)}, {PriceTick: 99, QtyLot: 2}}, []model.Level{{PriceTick: 101, QtyLot: 3}, {PriceTick: 102, QtyLot: uint64(second + 4)}})
		if err := buffer.Sample(minute.Add(time.Duration(second)*time.Second), states[second], true); err != nil {
			t.Fatal(err)
		}
	}
	batch, ok := buffer.Batch()
	if !ok {
		t.Fatal("minute dropped")
	}
	if batch.Minute.ValidBitmap != (uint64(1)<<60)-1 || len(batch.Deltas) != 59 {
		t.Fatalf("bitmap=%b deltas=%d", batch.Minute.ValidBitmap, len(batch.Deltas))
	}
	for second := 0; second < 60; second++ {
		got, valid, err := replay.AtSecond(batch.Minute, batch.Deltas, uint8(second))
		if err != nil || !valid || !reflect.DeepEqual(got.Bids, states[second].Bids) || !reflect.DeepEqual(got.Asks, states[second].Asks) {
			t.Fatalf("second=%d got=%+v valid=%v err=%v", second, got, valid, err)
		}
	}
}

func sample(id uint32, sequence int64, bids, asks []model.Level) model.BookSnapshot {
	return model.BookSnapshot{InstrumentID: id, SourceTime: time.Unix(sequence, 0).UTC(), Sequence: sequence, Bids: bids, Asks: asks}
}
