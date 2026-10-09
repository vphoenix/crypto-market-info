package optionslive

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/exchange"
	"github.com/vphoenix/crypto-market-info/internal/options"
)

func pendingFixture(t *testing.T) options.CatalogEnvelope {
	t.Helper()
	e, at, out, _ := catalogFixture(t)
	for sec := 0; sec < 60; sec++ {
		catalogStep(t, e, at.Add(time.Duration(sec)*time.Second))
	}
	if len(*out) != 1 {
		t.Fatal("missing fixture minute")
	}
	return (*out)[0]
}

func TestPendingMinuteFreezesIdentityAndValues(t *testing.T) {
	v := pendingFixture(t)
	id := v.ID()
	budget := &exchange.BufferBudget{Limit: 4 << 20}
	p, err := freezeCatalogMinute(v, budget)
	if err != nil {
		t.Fatal(err)
	}
	defer p.release()
	// Mutating live arrays must not change an already queued batch or retries.
	v.Live.Books[0].Minute.Bids[0].QtyLot++
	v.Evidence[0].StateKinds[0] = 0
	got, err := p.thaw()
	if err != nil {
		t.Fatal(err)
	}
	if err := got.Validate(); err != nil {
		t.Fatal(err)
	}
	if got.ID() != id {
		t.Fatal("freeze changed immutable batch identity")
	}
	if budget.Used() != p.cost {
		t.Fatal("pending bytes not counted")
	}
}

type retryMinuteSink struct {
	*catalogMemorySink
	ids   []string
	calls int
}

func (s *retryMinuteSink) WriteOptionsCatalogMinute(ctx context.Context, v options.CatalogEnvelope) error {
	s.calls++
	s.ids = append(s.ids, v.ID())
	if _, ok := ctx.Deadline(); ok {
		return errors.New("writer imposed deadline before admission")
	}
	if s.calls == 1 {
		return context.DeadlineExceeded
	}
	return v.Validate()
}

func TestMinuteWriteTimeoutRetriesSameBatchWithoutStoppingSampling(t *testing.T) {
	v := pendingFixture(t)
	budget := &exchange.BufferBudget{Limit: 4 << 20}
	p, err := freezeCatalogMinute(v, budget)
	if err != nil {
		t.Fatal(err)
	}
	q := make(chan *catalogPendingMinute, 2)
	q <- p
	close(q)
	sink := &retryMinuteSink{catalogMemorySink: &catalogMemorySink{}}
	err = writeCatalogMinutes(context.Background(), q, sink, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil || sink.calls != 2 {
		t.Fatal("temporary write failure terminated writer", err, sink.calls)
	}
	if sink.ids[0] != sink.ids[1] || sink.ids[0] != v.ID() {
		t.Fatal("retry changed batch identity")
	}
	if budget.Used() != 0 {
		t.Fatal("pending budget leaked")
	}
}

type blockedMinuteSink struct {
	*catalogMemorySink
	started chan struct{}
}

func (s *blockedMinuteSink) WriteOptionsCatalogMinute(ctx context.Context, _ options.CatalogEnvelope) error {
	close(s.started)
	<-ctx.Done()
	return ctx.Err()
}

func TestCanceledMinuteWriterReleasesQueuedBytes(t *testing.T) {
	v := pendingFixture(t)
	budget := &exchange.BufferBudget{Limit: 4 << 20}
	q := make(chan *catalogPendingMinute, 2)
	for i := 0; i < 2; i++ {
		p, err := freezeCatalogMinute(v, budget)
		if err != nil {
			t.Fatal(err)
		}
		q <- p
	}
	close(q)
	sink := &blockedMinuteSink{catalogMemorySink: &catalogMemorySink{}, started: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- writeCatalogMinutes(ctx, q, sink, slog.New(slog.NewTextHandler(io.Discard, nil))) }()
	<-sink.started
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if budget.Used() != 0 {
		t.Fatal("canceled writer leaked queued bytes", budget.Used())
	}
}

func TestPendingMinuteByteLimitRejectsWithoutReservationLeak(t *testing.T) {
	v := pendingFixture(t)
	budget := &exchange.BufferBudget{Limit: 1}
	if _, err := freezeCatalogMinute(v, budget); err == nil || budget.Used() != 0 {
		t.Fatal("pending memory limit bypassed", err)
	}
}

func TestPendingMinutePreservesMissingAndNullableEvidence(t *testing.T) {
	v := pendingFixture(t)
	v.Live.Books[0].Minute = nil
	for i := range v.Live.Books[0].Quality {
		v.Live.Books[0].Quality[i].ReplayValid = false
	}
	v.Live.Indexes[0].Samples[0] = options.IndexSample{State: options.IndexMissing}
	at := v.Live.MinuteTime
	v.Evidence[0].LifecycleConfirmed[1] = nil
	v.Evidence[0].LifecycleConfirmed[2] = &at
	budget := &exchange.BufferBudget{Limit: 4 << 20}
	p, err := freezeCatalogMinute(v, budget)
	if err != nil {
		t.Fatal(err)
	}
	defer p.release()
	got, err := p.thaw()
	if err != nil || got.ID() != v.ID() || got.Live.Books[0].Minute != nil || got.Live.Indexes[0].Samples[0].Price != nil {
		t.Fatal("missing data or nullable provenance changed", err)
	}
	if got.Evidence[0].LifecycleConfirmed[1] != nil || !got.Evidence[0].LifecycleConfirmed[2].Equal(at) {
		t.Fatal("nullable evidence time changed")
	}
}

func TestCorruptMinuteWriterDoesNotWaitForProducerToCloseQueue(t *testing.T) {
	budget := &exchange.BufferBudget{Limit: 4 << 20}
	p, err := freezeCatalogMinute(pendingFixture(t), budget)
	if err != nil {
		t.Fatal(err)
	}
	p.raw[0] = 0
	q := make(chan *catalogPendingMinute, 2)
	q <- p
	done := make(chan error, 1)
	go func() {
		done <- writeCatalogMinutes(context.Background(), q, &catalogMemorySink{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	}()
	select {
	case err := <-done:
		if err == nil || budget.Used() != 0 {
			t.Fatal("corruption not reported or budget leaked", err, budget.Used())
		}
	case <-time.After(time.Second):
		t.Fatal("writer blocked waiting for failed worker to close its queue")
	}
	close(q)
}

func TestCapturedMinuteDoesNotEncodeOnSamplingThread(t *testing.T) {
	cache := newCatalogMinuteCache(4 << 20)
	for i := 0; i < cap(cache.encoders); i++ {
		cache.encoders <- struct{}{}
	}
	v := pendingFixture(t)
	p, err := captureCatalogMinute(v, cache)
	if err != nil || p.raw != nil || p.value == nil {
		t.Fatal("sampler encoded instead of transferring finished minute", err)
	}
	reserved := cache.budget.Used()
	q := make(chan *catalogPendingMinute, 2)
	q <- p
	close(q)
	done := make(chan error, 1)
	go func() {
		done <- writeCatalogMinutes(context.Background(), q, &catalogMemorySink{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	}()
	select {
	case err := <-done:
		t.Fatal("writer bypassed encoder admission", err)
	case <-time.After(30 * time.Millisecond):
	}
	if cache.budget.Used() != reserved {
		t.Fatal("expanded pending minute lost its byte reservation")
	}
	<-cache.encoders
	if err := <-done; err != nil || cache.budget.Used() != 0 {
		t.Fatal("background encoding changed content or leaked bytes", err, cache.budget.Used())
	}
}

func TestCanceledPendingEncodingReleasesExpandedBytes(t *testing.T) {
	cache := newCatalogMinuteCache(4 << 20)
	for i := 0; i < cap(cache.encoders); i++ {
		cache.encoders <- struct{}{}
	}
	p, err := captureCatalogMinute(pendingFixture(t), cache)
	if err != nil {
		t.Fatal(err)
	}
	q := make(chan *catalogPendingMinute, 2)
	q <- p
	close(q)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := writeCatalogMinutes(ctx, q, &catalogMemorySink{}, slog.New(slog.NewTextHandler(io.Discard, nil))); !errors.Is(err, context.Canceled) || cache.budget.Used() != 0 {
		t.Fatal("canceling encoder leaked pending bytes", err, cache.budget.Used())
	}
}

func TestNextMinuteSamplingCannotChangeCapturedPendingMinute(t *testing.T) {
	e, at, out, _ := catalogFixture(t)
	for sec := 0; sec < 60; sec++ {
		catalogStep(t, e, at.Add(time.Duration(sec)*time.Second))
	}
	id := (*out)[0].ID()
	cache := newCatalogMinuteCache(4 << 20)
	p, err := captureCatalogMinute((*out)[0], cache)
	if err != nil {
		t.Fatal(err)
	}
	defer p.release()
	// Delay the background encoder until the sampler has completed another
	// minute and rotated every buffer, index array and evidence array.
	for sec := 60; sec < 120; sec++ {
		catalogStep(t, e, at.Add(time.Duration(sec)*time.Second))
	}
	if err := p.compress(context.Background()); err != nil {
		t.Fatal(err)
	}
	v, err := p.thaw()
	if err != nil || v.ID() != id {
		t.Fatal("sampling mutated an earlier queued minute", err)
	}
	if err := v.Validate(); err != nil {
		t.Fatal(err)
	}
}
