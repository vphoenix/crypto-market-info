package main

import (
	"context"
	"testing"
)

func TestRuntimeFlagsRejectUnsafeModesBeforeDatabaseAccess(t *testing.T) {
	for _, args := range [][]string{{"runtime-handoff"},
		{"runtime-rollback"},
		{"history", "--legacy-history-recovery", "--range=8453:1:2"},
		{"watch", "--runtime-state-dir="},
		{"runtime-convert", "--runtime-export=/tmp/not-used"},
	} {
		if err := run(context.Background(), args); err == nil {
			t.Fatal("accepted unsafe runtime command", args)
		}
	}
}
