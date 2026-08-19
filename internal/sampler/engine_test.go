package sampler

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/model"
)

type fakeBook struct {
	snapshot model.BookSnapshot
	valid    bool
}

func (f *fakeBook) Snapshot(int) (model.BookSnapshot, bool) { return f.snapshot, f.valid }

type fakeSink struct {
	mu      sync.Mutex
	batches []model.MinuteBatch
}

func (f *fakeSink) WriteMinute(_ context.Context, b model.MinuteBatch) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.batches = append(f.batches, b)
	return nil
}

func TestEngineWritesOnlyCompletedAnchoredMinute(t *testing.T) {
	start := time.Date(2026, 8, 19, 1, 2, 0, 0, time.UTC)
	book := &fakeBook{snapshot: sample(1, 1, []model.Level{{PriceTick: 1, QtyLot: 1}}, []model.Level{{PriceTick: 2, QtyLot: 1}}), valid: true}
	sink := &fakeSink{}
	engine, err := NewEngine([]Source{{InstrumentID: 1, Book: book}}, sink, 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = engine.SampleAt(start); err != nil {
		t.Fatal(err)
	}
	book.snapshot = sample(1, 2, []model.Level{{PriceTick: 1, QtyLot: 2}}, []model.Level{{PriceTick: 2, QtyLot: 1}})
	if err = engine.SampleAt(start.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err = engine.SampleAt(start.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	batch := <-engine.queue
	if batch.Minute.ValidBitmap != 3 || len(batch.Deltas) != 1 {
		t.Fatalf("batch=%+v", batch)
	}
	if len(sink.batches) != 0 {
		t.Fatal("SampleAt unexpectedly invoked writer goroutine")
	}
}
