package app

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/exchange"
)

// Only the pending metadata operation is retried. The current book generation
// and sampler keep running, and the caller retains the exact observation/IDs.
func retryPairedMetadata(ctx context.Context, logger *slog.Logger, operation string, call func(context.Context) error) error {
	return retryPairedMetadataWait(ctx, logger, operation, call, exchange.Wait)
}

func retryPairedMetadataWait(ctx context.Context, logger *slog.Logger, operation string, call func(context.Context) error, wait func(context.Context, time.Duration) bool) error {
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		bounded, cancel := context.WithTimeout(ctx, 40*time.Second)
		err := call(bounded)
		cancel()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var transient interface{ Temporary() bool }
		if !errors.Is(err, context.DeadlineExceeded) && !(errors.As(err, &transient) && transient.Temporary()) {
			return err
		}
		delay := min(5*time.Second<<min(attempt, 4), time.Minute)
		logger.Warn("OKX paired metadata retry; current sampling retained", "operation", operation, "retry_delay", delay, "error", err)
		if !wait(ctx, delay) {
			return ctx.Err()
		}
	}
}
