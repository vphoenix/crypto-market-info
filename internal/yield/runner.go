package yield

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

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
		return fmt.Errorf("yield runner %q has invalid configuration", r.Source)
	}
	retry := r.RetryInterval
	if retry <= 0 {
		retry = 10 * time.Minute
	}
	logger := r.Logger
	if logger == nil {
		logger = slog.Default()
	}
	now := r.now
	if now == nil {
		now = time.Now
	}
	var pending *Batch
	wait := time.Duration(0)
	for {
		if !waitFor(ctx, wait) {
			return nil
		}
		wait = 0
		if pending == nil {
			started := now()
			batch, err := r.Collector.Collect(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				logger.Error("yield collection failed", "source", r.Source, "stage", "collect", "duration", now().Sub(started), "error", err)
				wait = retry
				continue
			}
			if err = batch.NormalizeAndValidateForLiveCollection(); err != nil {
				logger.Error("yield collection failed", "source", r.Source, "stage", "validate", "duration", now().Sub(started), "error", err)
				wait = retry
				continue
			}
			pending = &batch
		}
		started := now()
		if err := r.Sink.WriteYieldBatch(ctx, *pending); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			logger.Error("yield collection failed", "source", r.Source, "stage", "write", "duration", now().Sub(started), "routes", len(pending.Items), "error", err)
			wait = retry
			continue
		}
		logger.Info("yield collection written", "source", r.Source, "duration", now().Sub(started), "routes", len(pending.Items))
		age := now().Sub(pending.CollectedAt)
		pending = nil
		if age < r.Interval {
			wait = r.Interval - age
		}
	}
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
