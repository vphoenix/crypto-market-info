package sampler

import (
	"reflect"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/orderbook"
	"github.com/vphoenix/crypto-market-info/internal/replay"
)

func TestEngineTenLevelsRoundTripWithBoundaryChangesAndInvalidSeconds(t *testing.T) {
	start := time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)
	book, err := orderbook.New(1, 1000)
	if err != nil {
		t.Fatal(err)
	}
	initial := model.BookSnapshot{InstrumentID: 1, SourceTime: start, Sequence: 1}
	for level := range 60 {
		initial.Bids = append(initial.Bids, model.Level{PriceTick: int64(1000 - level), QtyLot: 1})
		initial.Asks = append(initial.Asks, model.Level{PriceTick: int64(1001 + level), QtyLot: 2})
	}
	if err = book.ApplySnapshot(initial); err != nil {
		t.Fatal(err)
	}
	engine, err := NewEngine([]Source{{InstrumentID: 1, Book: book}}, &fakeSink{}, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	expected := make([]model.BookSnapshot, 60)
	for second := range 60 {
		at := start.Add(time.Duration(second) * time.Second)
		switch second {
		case 2:
			// Original level 11 enters the saved top ten on both sides.
			err = book.ApplyChanges(1, orderbook.ChangeSet{SourceTime: at, Sequence: 2,
				Bids: []model.Level{{PriceTick: 1000}}, Asks: []model.Level{{PriceTick: 1001}}})
		case 3:
			err = book.ApplyChanges(2, orderbook.ChangeSet{SourceTime: at, Sequence: 3, Bids: []model.Level{{PriceTick: 999, QtyLot: 7}}})
			if err == nil {
				err = book.ApplyChanges(3, orderbook.ChangeSet{SourceTime: at.Add(time.Millisecond), Sequence: 4, Bids: []model.Level{{PriceTick: 999, QtyLot: 9}}})
			}
		case 4:
			// A deeper update must not generate a saved delta.
			err = book.ApplyChanges(4, orderbook.ChangeSet{SourceTime: at, Sequence: 5, Bids: []model.Level{{PriceTick: 970, QtyLot: 20}}})
		case 5:
			book.MarkInvalid("sequence gap")
		case 7:
			initial.Sequence, initial.SourceTime = 10, at
			err = book.ApplySnapshot(initial)
		}
		if err != nil {
			t.Fatal(err)
		}
		expected[second], _ = book.Snapshot(10)
		if err = engine.SampleAt(at); err != nil {
			t.Fatal(err)
		}
	}
	if err = engine.SampleAt(start.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	batch := (<-engine.queue).Batches[0]
	if batch.Minute.StoredDepth != 10 || len(batch.Deltas) != 3 {
		t.Fatalf("depth=%d deltas=%+v", batch.Minute.StoredDepth, batch.Deltas)
	}
	if batch.Minute.Bids[10] != (model.Level{}) || batch.Minute.Asks[10] != (model.Level{}) {
		t.Fatal("new anchor retained more than ten levels")
	}
	if expected[2].Bids[9].PriceTick != 990 || expected[2].Asks[9].PriceTick != 1011 || batch.Deltas[1].BidChangeQty[0] != 9 {
		t.Fatal("edge promotion or final same-second update was lost")
	}
	for second := range 60 {
		got, valid, err := replay.AtSecond(batch.Minute, batch.Deltas, uint8(second))
		if second == 5 || second == 6 {
			if err != nil || valid {
				t.Fatalf("invalid second %d became valid: %v", second, err)
			}
			continue
		}
		if err != nil || !valid || got.StoredDepth != 10 || !reflect.DeepEqual(got.Bids, expected[second].Bids) || !reflect.DeepEqual(got.Asks, expected[second].Asks) {
			t.Fatalf("second %d failed exact replay: got=%+v err=%v", second, got, err)
		}
	}
}
