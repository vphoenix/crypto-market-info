package yield

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

type runnerCollector struct {
	mu    sync.Mutex
	calls int
	at    time.Time
}

func (c *runnerCollector) Collect(context.Context) (Batch, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls++
	return validRunnerBatch(c.at), nil
}

type runnerSink struct {
	mu      sync.Mutex
	calls   int
	batches []Batch
	done    chan struct{}
}

func (s *runnerSink) WriteYieldBatch(_ context.Context, batch Batch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	s.batches = append(s.batches, batch)
	if s.calls == 1 {
		return errors.New("temporary")
	}
	if s.calls == 2 {
		close(s.done)
	}
	return nil
}

func TestRunnerRetriesSameValidatedBatchWithoutRecollecting(t *testing.T) {
	at := time.Now().UTC()
	collector := &runnerCollector{at: at}
	sink := &runnerSink{done: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	runner := &Runner{Source: "test", Collector: collector, Sink: sink, Interval: time.Hour, RetryInterval: time.Millisecond, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	finished := make(chan error, 1)
	go func() { finished <- runner.Run(ctx) }()
	select {
	case <-sink.done:
		cancel()
	case <-time.After(time.Second):
		t.Fatal("runner did not retry")
	}
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	collector.mu.Lock()
	calls := collector.calls
	collector.mu.Unlock()
	if calls != 1 {
		t.Fatalf("collector calls=%d", calls)
	}
	if len(sink.batches) != 2 || !sink.batches[0].CollectedAt.Equal(sink.batches[1].CollectedAt) {
		t.Fatal("runner regenerated pending batch")
	}
}

func validRunnerBatch(at time.Time) Batch {
	at = time.UnixMilli(at.UnixMilli()).UTC()
	rate := decimal.RequireFromString("0.1")
	return Batch{Source: "test", CollectedAt: at, Items: []CollectedYield{{Route: YieldRouteDefinition{ProviderType: "native", Provider: "TRON", ProductCode: "x", ProductName: "x", YieldType: "native_staking", DepositAssetKey: "a", PositionAssetKey: "a", RedeemAssetKey: "a", IncomeSource: "issuance", SourceURL: "x", CollectionEnabled: true}, Observation: YieldObservation{ObservationTime: at, CollectedAt: at, TierNo: 1, TierMinAmount: decimal.Zero, TierMode: "none", Rate: &rate, RateKind: "apr", RateOrigin: "derived", RateMode: "variable", RewardAssetKeys: []string{"a"}, RewardComponentRates: []*decimal.Decimal{&rate}, RulePrincipalLossMode: "none", RuleEligibility: "candidate", Availability: "available"}}}}
}
