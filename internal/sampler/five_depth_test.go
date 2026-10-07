package sampler

import (
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/orderbook"
	"github.com/vphoenix/crypto-market-info/internal/replay"
	"reflect"
	"testing"
	"time"
)

func TestFiveLevelSamplingPromotionsFinalUpdateAndRecovery(t *testing.T) {
	at := time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC)
	book, _ := orderbook.New(1, 400)
	initial := model.BookSnapshot{InstrumentID: 1, SourceTime: at, Sequence: 1}
	for n := 0; n < 12; n++ {
		initial.Bids = append(initial.Bids, model.Level{PriceTick: int64(100 - n), QtyLot: 1})
		initial.Asks = append(initial.Asks, model.Level{PriceTick: int64(101 + n), QtyLot: 2})
	}
	if err := book.ApplySnapshot(initial); err != nil {
		t.Fatal(err)
	}
	e, err := NewEngine([]Source{{InstrumentID: 1, Book: book, StoredDepth: 5}}, &fakeSink{}, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	var want [60]model.BookSnapshot
	for sec := 0; sec < 60; sec++ {
		switch sec {
		case 1:
			err = book.ApplyChanges(1, orderbook.ChangeSet{Sequence: 2, SourceTime: at.Add(time.Second), Bids: []model.Level{{PriceTick: 100}}, Asks: []model.Level{{PriceTick: 101}}})
		case 2:
			err = book.ApplyChanges(2, orderbook.ChangeSet{Sequence: 3, SourceTime: at.Add(2 * time.Second), Bids: []model.Level{{PriceTick: 99, QtyLot: 8}}})
			if err == nil {
				err = book.ApplyChanges(3, orderbook.ChangeSet{Sequence: 4, SourceTime: at.Add(2 * time.Second), Bids: []model.Level{{PriceTick: 99, QtyLot: 13}}})
			}
		case 3:
			err = book.ApplyChanges(4, orderbook.ChangeSet{Sequence: 5, SourceTime: at.Add(3 * time.Second), Bids: []model.Level{{PriceTick: 89, QtyLot: 42}}})
		case 4:
			book.MarkInvalid("sequence gap")
		case 6:
			initial.Sequence = 10
			initial.SourceTime = at.Add(6 * time.Second)
			err = book.ApplySnapshot(initial)
		}
		if err != nil {
			t.Fatal(err)
		}
		want[sec], _ = book.Snapshot(5)
		if err = e.SampleAt(at.Add(time.Duration(sec) * time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	if err = e.SampleAt(at.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	b := (<-e.queue).Batches[0]
	if b.Minute.StoredDepth != 5 || b.Minute.Bids[5] != (model.Level{}) {
		t.Fatal("incorrect five-level anchor")
	}
	if want[1].Bids[4].PriceTick != 95 || want[2].Bids[0].QtyLot != 13 {
		t.Fatal("promotion/final quantity lost")
	}
	for sec := 0; sec < 60; sec++ {
		got, valid, err := replay.AtSecond(b.Minute, b.Deltas, uint8(sec))
		if sec == 4 || sec == 5 {
			if err != nil || valid {
				t.Fatalf("invalid second %d replayed", sec)
			}
			continue
		}
		if err != nil || !valid || got.StoredDepth != 5 || !reflect.DeepEqual(got.Bids, want[sec].Bids) || !reflect.DeepEqual(got.Asks, want[sec].Asks) {
			t.Fatalf("second %d mismatch: %v", sec, err)
		}
	}
	for _, d := range b.Deltas {
		if d.SecondOffset == 3 {
			t.Fatal("deep-only update generated five-level delta")
		}
	}
}

func TestSourceSwitchFreezesOldMinuteAndAnchorsNewOwner(t *testing.T) {
	at := time.Date(2026, 10, 7, 0, 0, 0, 0, time.UTC)
	first := &fakeBook{snapshot: sample(1, 1, []model.Level{{PriceTick: 100, QtyLot: 1}}, []model.Level{{PriceTick: 101, QtyLot: 1}}), valid: true}
	second := &fakeBook{snapshot: sample(2, 1, []model.Level{{PriceTick: 200, QtyLot: 2}}, []model.Level{{PriceTick: 201, QtyLot: 2}}), valid: true}
	e, _ := NewEngine([]Source{{InstrumentID: 1, Book: first, StoredDepth: 10}}, &fakeSink{}, 4, nil)
	e.now = func() time.Time { return at }
	if err := e.ScheduleSources([]Source{{InstrumentID: 2, Book: second, StoredDepth: 5}}, at.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := e.ScheduleSources(nil, at.Add(2*time.Minute)); err == nil {
		t.Fatal("second pending replacement accepted")
	}
	for sec := 0; sec < 60; sec++ {
		if err := e.SampleAt(at.Add(time.Duration(sec) * time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.SampleAt(at.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	old := <-e.queue
	if len(old.Batches) != 1 || old.Batches[0].Minute.InstrumentID != 1 || old.Batches[0].Minute.StoredDepth != 10 || old.Batches[0].Minute.ValidBitmap != model.MinuteMask {
		t.Fatal("old minute was not completely frozen")
	}
	if err := e.SampleAt(at.Add(2 * time.Minute)); err != nil {
		t.Fatal(err)
	}
	next := <-e.queue
	if len(next.Batches) != 1 || next.Batches[0].Minute.InstrumentID != 2 || next.Batches[0].Minute.StoredDepth != 5 || next.Batches[0].Minute.ValidBitmap != 1 {
		t.Fatal("new minute missing its zero-second anchor")
	}
}
