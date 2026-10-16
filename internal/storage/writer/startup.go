package writer

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// Startup diagnostics must identify a stalled phase before regular queue
// metrics exist. Each phase has a finite bound and preserves the original error.
func startupPhase(ctx context.Context, logger *slog.Logger, phase string, fn func(context.Context) error) error {
	started := time.Now()
	logger.Info("writer startup phase", "phase", phase, "state", "started")
	bounded, cancel := context.WithTimeout(ctx, 40*time.Second)
	defer cancel()
	if err := fn(bounded); err != nil {
		logger.Error("writer startup phase", "phase", phase, "state", "failed", "elapsed_ms", time.Since(started).Milliseconds(), "error", err)
		return fmt.Errorf("writer startup %s: %w", phase, err)
	}
	logger.Info("writer startup phase", "phase", phase, "state", "complete", "elapsed_ms", time.Since(started).Milliseconds())
	return nil
}
