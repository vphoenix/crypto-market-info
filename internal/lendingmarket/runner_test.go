package lendingmarket

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type fakeCollector struct {
	mu      sync.Mutex
	results []error
	calls   int
}

func (f *fakeCollector) Collect(context.Context) (Batch, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if len(f.results) > 0 {
		err := f.results[0]
		f.results = f.results[1:]
		if err != nil {
			return Batch{}, err
		}
	}
	b := testBatch()
	b.CollectedAt = time.UnixMilli(time.Now().UnixMilli()).UTC()
	for i := range b.Markets {
		b.Markets[i].ObservationTime = b.CollectedAt
	}
	return b, nil
}

type fakeSink struct {
	mu          sync.Mutex
	schemaErrs  []error
	writeErrs   []error
	attempts    []time.Time
	written     []Batch
	schemaCalls int
}

func (f *fakeSink) CheckJustLendMarketSchema(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.schemaCalls++
	if len(f.schemaErrs) > 0 {
		err := f.schemaErrs[0]
		f.schemaErrs = f.schemaErrs[1:]
		return err
	}
	return nil
}

func (f *fakeSink) WriteJustLendMarketBatch(_ context.Context, b Batch) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.attempts = append(f.attempts, b.CollectedAt)
	if len(f.writeErrs) > 0 {
		err := f.writeErrs[0]
		f.writeErrs = f.writeErrs[1:]
		return err
	}
	f.written = append(f.written, b)
	return nil
}

func runFor(t *testing.T, r *Runner, d time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	if err := r.Run(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestRunnerRetriesTheIdenticalBatchAfterWriteFailure(t *testing.T) {
	sink := &fakeSink{writeErrs: []error{errors.New("lock wait"), errors.New("lock wait")}}
	r := &Runner{Source: "test", Collector: &fakeCollector{}, Sink: sink, Interval: time.Second, RetryInterval: 10 * time.Millisecond}
	runFor(t, r, 200*time.Millisecond)
	if len(sink.attempts) != 3 || len(sink.written) != 1 {
		t.Fatalf("attempts %d written %d", len(sink.attempts), len(sink.written))
	}
	for _, attempt := range sink.attempts {
		if !attempt.Equal(sink.attempts[0]) {
			t.Fatal("retry changed the batch collection time")
		}
	}
}

func TestRunnerWritesNothingWhenCollectionFails(t *testing.T) {
	collector := &fakeCollector{results: []error{errors.New("HTTP 502"), errors.New("HTTP 502")}}
	sink := &fakeSink{}
	r := &Runner{Source: "test", Collector: collector, Sink: sink, Interval: time.Second, RetryInterval: 10 * time.Millisecond}
	runFor(t, r, 200*time.Millisecond)
	if collector.calls != 3 || len(sink.attempts) != 1 {
		t.Fatalf("collect calls %d write attempts %d", collector.calls, len(sink.attempts))
	}
}

func TestRunnerWaitsForSchemaWithoutCollecting(t *testing.T) {
	collector := &fakeCollector{}
	sink := &fakeSink{schemaErrs: []error{errors.New("not installed"), errors.New("not installed"), errors.New("not installed")}}
	r := &Runner{Source: "test", Collector: collector, Sink: sink, Interval: 30 * time.Millisecond}
	runFor(t, r, 75*time.Millisecond)
	if collector.calls != 0 || sink.schemaCalls < 2 {
		t.Fatalf("collect calls %d schema checks %d", collector.calls, sink.schemaCalls)
	}
}

func TestRunnerDropsAStaleUnwrittenBatch(t *testing.T) {
	sink := &fakeSink{writeErrs: []error{errors.New("down"), errors.New("down"), errors.New("down"), errors.New("down")}}
	collector := &fakeCollector{}
	r := &Runner{Source: "test", Collector: collector, Sink: sink, Interval: 40 * time.Millisecond, RetryInterval: 15 * time.Millisecond}
	runFor(t, r, 150*time.Millisecond)
	if collector.calls < 2 {
		t.Fatalf("stale batch kept for %d collections", collector.calls)
	}
	for _, b := range sink.written {
		if err := b.Validate(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestNextGridAlignsToInterval(t *testing.T) {
	at := time.Date(2026, 11, 2, 10, 3, 20, 0, time.UTC)
	if got := nextGrid(at, 5*time.Minute); got != 100*time.Second {
		t.Fatalf("next grid = %s", got)
	}
}
