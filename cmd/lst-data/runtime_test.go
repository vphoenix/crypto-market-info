package main

import (
	"context"
	"strings"
	"testing"
)

func TestRuntimeFlagsRejectUnsafeEntrypointsBeforeDatabase(t *testing.T) {
	for _, args := range [][]string{{"runtime-handoff"}, {"runtime-rollback"}, {"runtime-convert", "--legacy-history-recovery"}, {"watch", "--runtime-state-dir", ""}, {"backfill", "--runtime-export", "/tmp/not-used"}} {
		if err := run(context.Background(), args); err == nil || !strings.Contains(err.Error(), "runtime") {
			t.Fatal(args, err)
		}
	}
}
