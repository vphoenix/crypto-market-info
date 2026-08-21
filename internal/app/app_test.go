package app

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/model"
	marketyield "github.com/vphoenix/crypto-market-info/internal/yield"
)

type failingYieldCollector struct {
	mu    sync.Mutex
	calls int
}

func (c *failingYieldCollector) Collect(context.Context) (marketyield.Batch, error) {
	c.mu.Lock()
	c.calls++
	c.mu.Unlock()
	return marketyield.Batch{}, errors.New("source unavailable")
}

type unusedYieldSink struct{}

func (unusedYieldSink) WriteYieldBatch(context.Context, marketyield.Batch) error { return nil }

func TestSelectSymbolsIsExactOrderedAndRejectsMissing(t *testing.T) {
	available := []model.Instrument{{ExchangeSymbol: "A"}, {ExchangeSymbol: "B"}}
	selected, err := selectSymbols("X", available, []string{"B", "A"})
	if err != nil {
		t.Fatal(err)
	}
	if selected[0].ExchangeSymbol != "B" || selected[1].ExchangeSymbol != "A" {
		t.Fatalf("selected=%+v", selected)
	}
	if _, err = selectSymbols("X", available, []string{"a"}); err == nil {
		t.Fatal("case-folded symbol was accepted")
	}
}

func TestFailingYieldSourceDoesNotStopOtherComponent(t *testing.T) {
	collector := &failingYieldCollector{}
	runner := &marketyield.Runner{Source: "failing-yield", Collector: collector, Sink: unusedYieldSink{}, Interval: time.Hour, RetryInterval: time.Millisecond, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	progress := make(chan struct{}, 3)
	marketComponent := func(ctx context.Context) error {
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
				select {
				case progress <- struct{}{}:
				default:
				}
			}
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan error, 1)
	go func() {
		finished <- runComponents(ctx, []component{{name: "yield", run: runner.Run}, {name: "market", run: marketComponent}})
	}()
	for range 3 {
		select {
		case <-progress:
		case <-time.After(time.Second):
			t.Fatal("market component stopped while yield source was failing")
		}
	}
	deadline := time.Now().Add(time.Second)
	for {
		collector.mu.Lock()
		calls := collector.calls
		collector.mu.Unlock()
		if calls >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("yield runner did not retry continuous failures")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
}
