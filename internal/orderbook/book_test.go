package orderbook

import (
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/model"
)

func TestBookFinalUpdateDeleteAndGapInvalidation(t *testing.T) {
	book, err := New(1, 1000)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(100, 0).UTC()
	if err := book.ApplySnapshot(model.BookSnapshot{InstrumentID: 1, SourceTime: now, Sequence: 10,
		Bids: []model.Level{{PriceTick: 100, QtyLot: 2}, {PriceTick: 99, QtyLot: 3}},
		Asks: []model.Level{{PriceTick: 101, QtyLot: 4}, {PriceTick: 102, QtyLot: 5}}}); err != nil {
		t.Fatal(err)
	}
	if err := book.ApplyChanges(10, ChangeSet{SourceTime: now.Add(time.Second), Sequence: 11,
		Bids: []model.Level{{PriceTick: 100, QtyLot: 7}, {PriceTick: 99, QtyLot: 0}}}); err != nil {
		t.Fatal(err)
	}
	snapshot, valid := book.Snapshot(50)
	if !valid || len(snapshot.Bids) != 1 || snapshot.Bids[0] != (model.Level{PriceTick: 100, QtyLot: 7}) {
		t.Fatalf("unexpected snapshot valid=%v bids=%+v", valid, snapshot.Bids)
	}
	book.MarkInvalid("sequence gap")
	if _, valid := book.Snapshot(50); valid {
		t.Fatal("invalid book produced a sample")
	}
}

func TestBookRejectsCrossedAtomicUpdate(t *testing.T) {
	book, _ := New(1, 50)
	now := time.Unix(100, 0).UTC()
	_ = book.ApplySnapshot(model.BookSnapshot{InstrumentID: 1, SourceTime: now, Sequence: 1,
		Bids: []model.Level{{PriceTick: 100, QtyLot: 1}}, Asks: []model.Level{{PriceTick: 101, QtyLot: 1}}})
	if err := book.ApplyChanges(1, ChangeSet{SourceTime: now.Add(time.Second), Sequence: 2,
		Bids: []model.Level{{PriceTick: 102, QtyLot: 1}}}); err == nil {
		t.Fatal("crossed update accepted")
	}
	snapshot, _ := book.Snapshot(1)
	if snapshot.Bids[0].PriceTick != 100 {
		t.Fatal("failed update was not rolled back")
	}
}

func TestMultipleUpdatesBeforeSampleExposeFinalQuantity(t *testing.T) {
	book, _ := New(3, 50)
	now := time.Unix(100, 0).UTC()
	_ = book.ApplySnapshot(model.BookSnapshot{InstrumentID: 3, SourceTime: now, Sequence: 1, Bids: []model.Level{{PriceTick: 100, QtyLot: 1}}, Asks: []model.Level{{PriceTick: 101, QtyLot: 1}}})
	_ = book.ApplyChanges(1, ChangeSet{SourceTime: now.Add(time.Millisecond), Sequence: 2, Bids: []model.Level{{PriceTick: 100, QtyLot: 2}}})
	_ = book.ApplyChanges(2, ChangeSet{SourceTime: now.Add(2 * time.Millisecond), Sequence: 3, Bids: []model.Level{{PriceTick: 100, QtyLot: 7}}})
	snapshot, valid := book.Snapshot(50)
	if !valid || snapshot.Bids[0].QtyLot != 7 {
		t.Fatalf("snapshot=%+v valid=%v", snapshot, valid)
	}
}
