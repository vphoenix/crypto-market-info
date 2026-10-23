package main

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestCommandExitCodeDistinguishesInternalCancellationFromSignal(t *testing.T) {
	signalCtx, stop := context.WithCancel(context.Background())
	stop()
	for _, tc := range []struct {
		name string
		ctx  context.Context
		err  error
		want int
	}{
		{"complete", context.Background(), nil, 0},
		{"internal_cancel", context.Background(), context.Canceled, 1},
		{"wrapped_database_cancel", context.Background(), fmt.Errorf("read captures: %w", context.Canceled), 1},
		{"deadline", context.Background(), context.DeadlineExceeded, 1},
		{"signal", signalCtx, context.Canceled, 0},
		{"signal_with_write_failure", signalCtx, errors.New("across_write_frozen_capture_failed"), 1},
		{"legacy_network_fault_is_retryable", context.Background(), errors.New("network_issue_stop: dns_not_found"), 1},
		{"network_fault_during_signal", signalCtx, errors.Join(context.Canceled, errors.New("network_issue_stop: dns_not_found")), 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := commandExitCode(tc.ctx, tc.err); got != tc.want {
				t.Fatalf("exit=%d, want=%d", got, tc.want)
			}
		})
	}
}
