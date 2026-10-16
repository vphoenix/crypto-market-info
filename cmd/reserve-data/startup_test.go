package main

import (
	"context"
	"errors"
	"testing"
)

func TestDatabaseOpenFailureReturnsOriginalError(t *testing.T) {
	for _, command := range []string{"init-schema", "watch", "backfill"} {
		t.Run(command, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			err := run(ctx, []string{command, "--clickhouse", "127.0.0.1:1", "--evidence", t.TempDir()})
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("database open error was lost: %v", err)
			}
		})
	}
}
