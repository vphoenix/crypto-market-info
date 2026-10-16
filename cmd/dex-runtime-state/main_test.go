package main

import (
	"context"
	"strings"
	"testing"
)

func TestRuntimeHandoffRequiresExplicitPlanBeforeDatabase(t *testing.T) {
	t.Setenv("STORAGE_BACKEND", "mysql")
	t.Setenv("MYSQL_SOCKET", "/unused/runtime-test.sock")
	t.Setenv("MYSQL_USER", "test")
	t.Setenv("MYSQL_PASSWORD_FILE", "/unused/runtime-test-password")
	for _, args := range [][]string{{"handoff", "--epoch-file=/unused/epoch"}, {"verify", "--epoch-file=/unused/epoch", "--handoff-plan=/unused/plan"}} {
		err := run(context.Background(), args)
		if err == nil || !strings.Contains(err.Error(), "runtime_handoff_requires_explicit_plan") {
			t.Fatal(args, err)
		}
	}
}
