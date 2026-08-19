package funding

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/model"
)

type providerCall struct {
	instrumentID uint32
	target       time.Time
	started      time.Time
}

type recordingProvider struct {
	mu         sync.Mutex
	calls      []providerCall
	active     int
	maxActive  int
	foundAfter int
}

func (p *recordingProvider) ActualFundingRate(_ context.Context, instrument model.Instrument, target time.Time) (model.FundingRate, bool, error) {
	p.mu.Lock()
	p.active++
	if p.active > p.maxActive {
		p.maxActive = p.active
	}
	p.calls = append(p.calls, providerCall{instrumentID: instrument.ID, target: target, started: time.Now()})
	callCount := len(p.calls)
	p.mu.Unlock()
	time.Sleep(3 * time.Millisecond)
	p.mu.Lock()
	p.active--
	p.mu.Unlock()
	found := p.foundAfter <= 1 || callCount >= p.foundAfter
	rate := model.FundingRate{InstrumentID: instrument.ID, HourTime: target.UTC().Truncate(time.Hour), FundingTime: target, Rate: decimal.RequireFromString("0.25"), IsActual: true}
	return rate, found, nil
}

func (p *recordingProvider) snapshot() ([]providerCall, int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]providerCall(nil), p.calls...), p.maxActive
}

func TestConfirmationWorkerSerializesAndRateLimitsInstruments(t *testing.T) {
	provider := &recordingProvider{foundAfter: 1}
	sink := &fakeSink{}
	worker := &ConfirmationWorker{Exchange: "Test", Provider: provider, Sink: sink, MinInterval: 30 * time.Millisecond, RetryDelays: []time.Duration{40 * time.Millisecond}}
	target := time.UnixMilli(time.Now().UnixMilli()).UTC()
	if err := worker.Schedule(context.Background(), fundingInstrument(1, "Test"), target); err != nil {
		t.Fatal(err)
	}
	second := fundingInstrument(2, "Test")
	second.ExchangeSymbol = "ETH-USDT-SWAP"
	second.BaseAsset = "ETH"
	if err := worker.Schedule(context.Background(), second, target); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	waitForRates(t, sink, 2)
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	calls, maxActive := provider.snapshot()
	if len(calls) != 2 || maxActive != 1 {
		t.Fatalf("calls=%+v maxActive=%d", calls, maxActive)
	}
	if calls[0].started.Before(target.Add(40 * time.Millisecond)) {
		t.Fatalf("first request started early: target=%s started=%s", target, calls[0].started)
	}
	if spacing := calls[1].started.Sub(calls[0].started); spacing < 28*time.Millisecond {
		t.Fatalf("requests were not rate limited: %s", spacing)
	}
}

func TestConfirmationWorkerRetriesAtConfiguredSettlementDelays(t *testing.T) {
	provider := &recordingProvider{foundAfter: 4}
	sink := &fakeSink{}
	delays := []time.Duration{20 * time.Millisecond, 45 * time.Millisecond, 70 * time.Millisecond, 95 * time.Millisecond}
	worker := &ConfirmationWorker{Exchange: "Test", Provider: provider, Sink: sink, MinInterval: time.Millisecond, RetryDelays: delays}
	target := time.UnixMilli(time.Now().UnixMilli()).UTC()
	instrument := fundingInstrument(1, "Test")
	for index := 0; index < 10; index++ {
		if err := worker.Schedule(context.Background(), instrument, target); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	waitForRates(t, sink, 1)
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	calls, maxActive := provider.snapshot()
	if len(calls) != 4 || maxActive != 1 {
		t.Fatalf("calls=%+v maxActive=%d", calls, maxActive)
	}
	for index, call := range calls {
		if call.started.Before(target.Add(delays[index])) {
			t.Fatalf("attempt %d started before configured delay: %s < %s", index+1, call.started, target.Add(delays[index]))
		}
		if call.target.UnixMilli() != target.UnixMilli() {
			t.Fatalf("attempt %d lost target milliseconds", index+1)
		}
	}
}

func TestConfirmationWorkerDefaultsMatchDesign(t *testing.T) {
	worker := &ConfirmationWorker{Exchange: "Test", Provider: &recordingProvider{}, Sink: &fakeSink{}}
	if err := worker.defaults(); err != nil {
		t.Fatal(err)
	}
	want := []time.Duration{2 * time.Minute, 5 * time.Minute, 15 * time.Minute, 60 * time.Minute}
	if worker.MinInterval != time.Second || len(worker.RetryDelays) != len(want) {
		t.Fatalf("interval=%s retries=%v", worker.MinInterval, worker.RetryDelays)
	}
	for index := range want {
		if worker.RetryDelays[index] != want[index] {
			t.Fatalf("retry delays=%v", worker.RetryDelays)
		}
	}
}

func TestInitialConfirmationAttemptCatchesUpOnceWithoutBurstingPastRetries(t *testing.T) {
	target := time.UnixMilli(1787097600123).UTC()
	delays := []time.Duration{2 * time.Minute, 5 * time.Minute, 15 * time.Minute, 60 * time.Minute}
	tests := []struct {
		name        string
		now         time.Time
		wantAttempt int
		wantDue     time.Time
	}{
		{name: "before first", now: target.Add(time.Minute), wantAttempt: 0, wantDue: target.Add(2 * time.Minute)},
		{name: "between retries", now: target.Add(10 * time.Minute), wantAttempt: 1, wantDue: target.Add(10 * time.Minute)},
		{name: "after all retries", now: target.Add(2 * time.Hour), wantAttempt: 3, wantDue: target.Add(2 * time.Hour)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			attempt, due := initialConfirmationAttempt(target, test.now, delays)
			if attempt != test.wantAttempt || !due.Equal(test.wantDue) {
				t.Fatalf("attempt=%d due=%s, want attempt=%d due=%s", attempt, due, test.wantAttempt, test.wantDue)
			}
		})
	}
}

func waitForRates(t *testing.T, sink *fakeSink, count int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(sink.snapshot()) >= count {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d funding writes; got %+v", count, sink.snapshot())
}
