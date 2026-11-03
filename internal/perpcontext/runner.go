package perpcontext

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// Runner polls one dex on a fixed wall-clock grid. A failed collection writes
// nothing and is retried later; a failed write retries the identical batch so
// the stored rows keep the original collection time and hash. It never
// returns an error after start, so one source cannot stop the collector.
type Runner struct {
	Source        string
	Collector     Collector
	Sink          Sink
	Interval      time.Duration
	RetryInterval time.Duration
	Logger        *slog.Logger
	now           func() time.Time
}

func (r *Runner) Run(ctx context.Context) error {
	if r.Collector == nil || r.Sink == nil || r.Interval <= 0 {
		return fmt.Errorf("perp context runner %q has invalid configuration", r.Source)
	}
	retry := r.RetryInterval
	if retry <= 0 || retry > r.Interval {
		retry = r.Interval
	}
	logger := r.Logger
	if logger == nil {
		logger = slog.Default()
	}
	now := r.now
	if now == nil {
		now = time.Now
	}
	schemaReady := false
	var pending *Batch
	wait := time.Duration(0)
	for {
		if !waitFor(ctx, wait) {
			return nil
		}
		wait = 0
		if !schemaReady {
			if err := r.Sink.CheckHyperliquidContextSchema(ctx); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				logger.Error("perp context collection failed", "source", r.Source, "stage", "schema", "error", err)
				wait = r.Interval
				continue
			}
			schemaReady = true
		}
		if pending == nil {
			started := now()
			batch, err := r.Collector.Collect(ctx)
			if err == nil {
				err = batch.Validate()
			}
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				logger.Error("perp context collection failed", "source", r.Source, "stage", "collect", "duration", now().Sub(started), "error", err)
				wait = retry
				continue
			}
			pending = &batch
		}
		started := now()
		if err := r.Sink.WriteHyperliquidContextBatch(ctx, *pending); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			logger.Error("perp context collection failed", "source", r.Source, "stage", "write", "duration", now().Sub(started), "assets", len(pending.Assets), "error", err)
			// A retry older than one interval would be a stale "current" row;
			// drop it and let the next grid point collect fresh state.
			if now().Sub(pending.CollectedAt) >= r.Interval {
				pending = nil
			}
			wait = retry
			continue
		}
		logger.Info("perp context collection written", "source", r.Source, "duration", now().Sub(started), "assets", len(pending.Assets))
		pending = nil
		wait = nextGrid(now(), r.Interval)
	}
}

// nextGrid waits until the next multiple of interval since the Unix epoch so
// restarts do not drift the sampling grid.
func nextGrid(at time.Time, interval time.Duration) time.Duration {
	next := at.Truncate(interval).Add(interval)
	return next.Sub(at)
}

func waitFor(ctx context.Context, delay time.Duration) bool {
	if delay <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
