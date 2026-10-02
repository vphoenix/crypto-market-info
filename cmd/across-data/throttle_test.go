package main

import (
	"context"
	"testing"
)

func TestNegativeRPCIntervalRejectedBeforeDatabaseAccess(t *testing.T) {
	for _, command := range []string{"backfill", "watch", "preflight", "init-schema", "report"} {
		t.Run(command, func(t *testing.T) {
			err := run(context.Background(), []string{command, "--rpc-min-interval=-1ns", "--clickhouse=invalid:database-address"})
			if err == nil || err.Error() != "rpc_min_interval_must_be_nonnegative" {
				t.Fatalf("negative interval reached later processing: %v", err)
			}
		})
	}
}
