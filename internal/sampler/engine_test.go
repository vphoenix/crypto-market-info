package sampler

import (
	"context"
	"errors"
	"strings"
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

func (f *fakeSink) WriteCompletedMinute(_ context.Context, b model.CompletedMinute) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.batches = append(f.batches, b.Batches...)
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
	completed := <-engine.queue
	if len(completed.Batches) != 1 || !completed.MinuteTime.Equal(start) {
		t.Fatalf("completed=%+v", completed)
	}
	batch := completed.Batches[0]
	if batch.Minute.ValidBitmap != 3 || len(batch.Deltas) != 1 {
		t.Fatalf("batch=%+v", batch)
	}
	if len(sink.batches) != 0 {
		t.Fatal("SampleAt unexpectedly invoked writer goroutine")
	}
}

func TestCompletedMinuteIncludesOnlyAnchorsInStableOrder(t *testing.T) {
	start := time.Date(2026, 8, 19, 1, 2, 0, 0, time.UTC)
	book := func(id uint32, valid bool) *fakeBook {
		return &fakeBook{snapshot: sample(id, 1, []model.Level{{PriceTick: 1, QtyLot: 1}}, []model.Level{{PriceTick: 2, QtyLot: 1}}), valid: valid}
	}
	b1, b2, b3 := book(1, true), book(2, false), book(3, true)
	e, err := NewEngine([]Source{{InstrumentID: 3, Book: b3}, {InstrumentID: 2, Book: b2}, {InstrumentID: 1, Book: b1}}, &fakeSink{}, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err = e.SampleAt(start); err != nil {
		t.Fatal(err)
	}
	b2.valid = true // A late recovery has no anchor and must not create a batch.
	if err = e.SampleAt(start.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err = e.SampleAt(start.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	envelope := <-e.queue
	if len(envelope.Batches) != 2 || envelope.Batches[0].Minute.InstrumentID != 1 || envelope.Batches[1].Minute.InstrumentID != 3 {
		t.Fatalf("wrong complete membership: %+v", envelope)
	}
	b1.snapshot.Bids[0].QtyLot = 99
	if envelope.Batches[0].Minute.Bids[0].QtyLot != 1 {
		t.Fatal("envelope changed after handoff")
	}
	if len(e.queue) != 0 {
		t.Fatal("minute emitted more than once")
	}
}

func TestCompletedMinuteEmitsEmptyEnvelope(t *testing.T) {
	start := time.Date(2026, 8, 19, 1, 2, 0, 0, time.UTC)
	e, err := NewEngine([]Source{{InstrumentID: 1, Book: &fakeBook{}}}, &fakeSink{}, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, at := range []time.Time{start, start.Add(time.Minute)} {
		if err = e.SampleAt(at); err != nil {
			t.Fatal(err)
		}
	}
	envelope := <-e.queue
	if len(envelope.Batches) != 0 || envelope.weight != 1 || !envelope.MinuteTime.Equal(start) {
		t.Fatalf("empty completion not preserved: %+v", envelope)
	}
}

func TestMinuteQueueCapacityIncludesWriterOwnedBatches(t *testing.T) {
	start := time.Date(2026, 8, 19, 1, 2, 0, 0, time.UTC)
	sources := []Source{{InstrumentID: 1, Book: &fakeBook{}}, {InstrumentID: 2, Book: &fakeBook{}}}
	if _, err := NewEngine(sources, &fakeSink{}, 3, nil); err == nil {
		t.Fatal("accepted capacity below two full minutes")
	}
	sources = []Source{{InstrumentID: 1, Book: &fakeBook{snapshot: sample(1, 1, []model.Level{{PriceTick: 1, QtyLot: 1}}, []model.Level{{PriceTick: 2, QtyLot: 1}}), valid: true}}}
	e, err := NewEngine(sources, &fakeSink{}, 2, nil)
	if err != nil {
		t.Fatal(err)
	}
	for minute := range 3 {
		if err = e.SampleAt(start.Add(time.Duration(minute) * time.Minute)); err != nil {
			t.Fatal(err)
		}
	}
	<-e.queue // The writer has received one but has not completed it.
	if err = e.SampleAt(start.Add(3 * time.Minute)); err == nil || !strings.Contains(err.Error(), "weighted queue full") {
		t.Fatalf("writer-owned capacity was lost: %v", err)
	}
	if err = e.flushIfCompleted(start.Add(4 * time.Minute)); err == nil {
		t.Fatal("failure allowed unfinished minute to flush")
	}
}

func TestMinuteChannelStillHasTwoEnvelopeLimit(t *testing.T) {
	start := time.Date(2026, 8, 19, 1, 2, 0, 0, time.UTC)
	e, _ := NewEngine([]Source{{InstrumentID: 1, Book: &fakeBook{}}}, &fakeSink{}, 512, nil)
	for minute := range 3 {
		if err := e.SampleAt(start.Add(time.Duration(minute) * time.Minute)); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.SampleAt(start.Add(3 * time.Minute)); err == nil || !strings.Contains(err.Error(), "two completed minutes") {
		t.Fatalf("missing envelope limit: %v", err)
	}
}

type bookFunc func(int) (model.BookSnapshot, bool)

func (f bookFunc) Snapshot(depth int) (model.BookSnapshot, bool) { return f(depth) }

func TestSamplingDeadlineMarksRemainderInvalidAndContinues(t *testing.T) {
	start := time.Date(2026, 8, 19, 1, 2, 0, 0, time.UTC)
	now := start
	calls := 0
	slow := bookFunc(func(int) (model.BookSnapshot, bool) {
		calls++
		if calls == 2 {
			now = now.Add(time.Second)
		}
		return sample(2, int64(calls), []model.Level{{PriceTick: 1, QtyLot: 1}}, []model.Level{{PriceTick: 2, QtyLot: 1}}), true
	})
	fast := &fakeBook{snapshot: sample(1, 1, []model.Level{{PriceTick: 1, QtyLot: 1}}, []model.Level{{PriceTick: 2, QtyLot: 1}}), valid: true}
	e, _ := NewEngine([]Source{{InstrumentID: 1, Book: fast}, {InstrumentID: 2, Book: slow}}, &fakeSink{}, 0, nil)
	e.now = func() time.Time { return now }
	if err := e.SampleAt(start); err != nil {
		t.Fatal(err)
	}
	now = start.Add(time.Second)
	if err := e.SampleAt(start.Add(time.Second)); err != nil {
		t.Fatalf("overrun terminated sampler: %v", err)
	}
	now = start.Add(2 * time.Second)
	if err := e.SampleAt(start.Add(2 * time.Second)); err != nil {
		t.Fatalf("sampler did not recover: %v", err)
	}
	now = start.Add(time.Minute)
	if err := e.SampleAt(start.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	completed := <-e.queue
	if len(completed.Batches) != 2 {
		t.Fatalf("completed batches=%d", len(completed.Batches))
	}
	if got := completed.Batches[0].Minute.ValidBitmap; got != 0b111 {
		t.Fatalf("fast valid bitmap=%b", got)
	}
	if got := completed.Batches[1].Minute.ValidBitmap; got != 0b101 {
		t.Fatalf("slow valid bitmap=%b", got)
	}
	h := e.HealthSnapshot()
	if h.SampleOverruns != 1 || h.MissedSamples != 1 || h.LastOverrunID != 2 || !h.LastOverrunAt.Equal(start.Add(time.Second)) {
		t.Fatalf("overrun health=%+v", h)
	}
}

func TestSamplingAlreadyPastDeadlineMarksWholeSecondInvalid(t *testing.T) {
	start := time.Date(2026, 8, 19, 1, 2, 0, 0, time.UTC)
	now := start.Add(2 * time.Second)
	e, _ := NewEngine([]Source{{InstrumentID: 1, Book: &fakeBook{snapshot: sample(1, 1, []model.Level{{PriceTick: 1, QtyLot: 1}}, []model.Level{{PriceTick: 2, QtyLot: 1}}), valid: true}}}, &fakeSink{}, 0, nil)
	e.now = func() time.Time { return now }
	if err := e.sampleBefore(start, start.Add(time.Second)); err != nil {
		t.Fatalf("late tick terminated sampler: %v", err)
	}
	if h := e.HealthSnapshot(); h.SampleOverruns != 1 || h.MissedSamples != 1 || h.LastOverrunID != 1 {
		t.Fatalf("late tick health=%+v", h)
	}
	if batch, ok := e.buffers[1].Batch(); ok || batch.Minute.ValidBitmap != 0 {
		t.Fatalf("late second became a valid anchor: %+v", batch)
	}
}

func TestBacklogTerminatesSampling(t *testing.T) {
	start := time.Date(2026, 8, 19, 1, 2, 0, 0, time.UTC)
	now := start
	e, _ := NewEngine([]Source{{InstrumentID: 1, Book: &fakeBook{}}}, &fakeSink{}, 0, nil)
	e.now = func() time.Time { return now }
	if err := e.SampleAt(start); err != nil {
		t.Fatal(err)
	}
	if err := e.SampleAt(start.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	now = now.Add(MaxMinuteBacklog)
	if err := e.SampleAt(start.Add(time.Minute + time.Second)); err == nil || !strings.Contains(err.Error(), "backlog") {
		t.Fatalf("expired backlog accepted: %v", err)
	}
}

type failSink struct{ err error }

func (s failSink) WriteCompletedMinute(context.Context, model.CompletedMinute) error { return s.err }

func TestWriterFailureTerminatesEngine(t *testing.T) {
	start := time.Now().UTC().Truncate(time.Minute)
	failure := errors.New("storage unavailable")
	e, _ := NewEngine([]Source{{InstrumentID: 1, Book: &fakeBook{}}}, failSink{failure}, 0, nil)
	if err := e.SampleAt(start.Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err := e.SampleAt(start); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := e.Run(ctx); !errors.Is(err, failure) {
		t.Fatalf("writer failure was swallowed: %v", err)
	}
}

func TestWriterReleasesWeightedCapacityAfterWholeEnvelope(t *testing.T) {
	start := time.Now().UTC().Truncate(time.Minute)
	e, _ := NewEngine([]Source{{InstrumentID: 1, Book: &fakeBook{}}}, &fakeSink{}, 0, nil)
	if err := e.SampleAt(start); err != nil {
		t.Fatal(err)
	}
	if err := e.SampleAt(start.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if h := e.HealthSnapshot(); h.PendingBatches != 1 || h.PeakPendingBatches != 1 || h.SampleCount != 2 {
		t.Fatalf("incorrect enqueue health: %+v", h)
	}
	close(e.queue)
	if err := e.writeCompleted(context.Background()); err != nil {
		t.Fatal(err)
	}
	if h := e.HealthSnapshot(); h.PendingBatches != 0 || !h.OldestPendingAt.IsZero() || h.WriteCount != 1 {
		t.Fatalf("capacity not released after completion: %+v", h)
	}
}

func BenchmarkEngine1100Sources(b *testing.B) {
	start := time.Now().UTC().Truncate(time.Minute)
	sources := make([]Source, 1100)
	for index := range sources {
		id := uint32(index + 1)
		book := &fakeBook{snapshot: model.BookSnapshot{InstrumentID: id, SourceTime: start}, valid: true}
		for level := range model.BookDepth {
			book.snapshot.Bids = append(book.snapshot.Bids, model.Level{PriceTick: int64(10000 - level), QtyLot: 1})
			book.snapshot.Asks = append(book.snapshot.Asks, model.Level{PriceTick: int64(10001 + level), QtyLot: 1})
		}
		sources[index] = Source{InstrumentID: id, Book: book}
	}
	e, _ := NewEngine(sources, &fakeSink{}, 0, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- e.writeCompleted(ctx) }()
	b.ResetTimer()
	for index := range b.N {
		if err := e.SampleAt(start.Add(time.Duration(index) * time.Second)); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	close(e.queue)
	if err := <-done; err != nil {
		b.Fatal(err)
	}
}
