package optionslive

import (
	"context"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/exchange"
	"github.com/vphoenix/crypto-market-info/internal/options"
	"log/slog"
	"sync"
	"time"
)

type catalogWorker struct {
	failed    bool
	persisted bool
	id        uuid.UUID
	q         *ingress
	run       options.LiveRun
	specs     []options.ContractSpec
	cancel    context.CancelFunc
	done      chan struct{}
}

func startCatalogWorker(ctx context.Context, cfg Config, r options.LiveRun, specs []options.ContractSpec, sink CatalogSink, budget, levels *exchange.BufferBudget, recover func(uint32), resetIndex func(), logger *slog.Logger, failed chan<- uuid.UUID) (*catalogWorker, error) {
	ctx, cancel := context.WithCancel(ctx)
	w := &catalogWorker{id: uuid.New(), q: newIngress(time.Now()), run: r, specs: specs, cancel: cancel, done: make(chan struct{})}
	w.q.budget = budget
	batches := make(chan options.CatalogEnvelope, 2)
	errs := make(chan error, 1)
	fail := func(e error) {
		select {
		case errs <- e:
		default:
		}
	}
	e, err := newCatalogEngine(r, specs, time.Now(), cfg.MaxBookLevels, recover, resetIndex, func(v options.CatalogEnvelope) error {
		select {
		case batches <- v:
			return nil
		default:
			return fmt.Errorf("catalog minute queue full")
		}
	})
	if err != nil {
		cancel()
		return nil, err
	}
	e.catalog.levelBudget = levels
	e.futureDepth = cfg.FutureBookDepth
	writerCtx, writerCancel := context.WithCancel(context.WithoutCancel(ctx))
	written := make(chan struct{})
	go func() {
		defer close(written)
		for b := range batches {
			if time.Since(b.Live.PreparedAt) > 45*time.Second {
				fail(fmt.Errorf("catalog minute write expired"))
				return
			}
			attempt, stop := context.WithTimeout(writerCtx, 40*time.Second)
			err := sink.WriteOptionsCatalogMinute(attempt, b)
			stop()
			if err != nil {
				fail(err)
				return
			}
		}
	}()
	go func() {
		defer close(w.done)
		defer func() {
			cancel()
			w.q.stop()
			for _, n := range e.catalog.levels {
				levels.Release(n)
			}
			close(batches)
			timer := time.NewTimer(45 * time.Second)
			defer timer.Stop()
			select {
			case <-written:
			case <-timer.C:
				writerCancel()
				<-written
			}
			writerCancel()
		}()
		timer := time.NewTicker(10 * time.Millisecond)
		defer timer.Stop()
		var problem error
	loop:
		for {
			select {
			case <-ctx.Done():
				break loop
			case problem = <-errs:
				break loop
			case problem = <-w.q.failure:
				break loop
			case <-timer.C:
				if problem = w.q.pulse(); problem != nil {
					break loop
				}
			case v := <-w.q.queue:
				w.q.consumed(v)
				if problem = e.handle(v, time.Now()); problem != nil {
					break loop
				}
			}
		}
		if problem != nil && !errors.Is(problem, errShardRetired) {
			logger.Error("options shard failed", "run", e.run.ID, "error", problem)
			select {
			case failed <- w.id:
			case <-ctx.Done():
			}
		}
	}()
	return w, nil
}
func (q *ingress) stop() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.failed = true
	for {
		select {
		case v := <-q.queue:
			q.bytes -= len(v.Stream.Raw) + 512
			q.budget.Release(int64(len(v.Stream.Raw) + 512))
		default:
			return
		}
	}
}
func waitCatalogWorkers(workers []*catalogWorker) {
	var wg sync.WaitGroup
	for _, w := range workers {
		wg.Add(1)
		go func(w *catalogWorker) { defer wg.Done(); <-w.done }(w)
	}
	wg.Wait()
}
