package main

import (
	"context"
	"strings"
	"testing"
)

func TestRuntimeOptionsFailBeforeSourceOrDatabase(t *testing.T) {
	for _, tc := range []struct {
		args          []string
		env, expected string
	}{
		{[]string{"runtime-handoff"}, "mysql", "runtime_handoff_requires_explicit_plan"},
		{[]string{"runtime-convert"}, "clickhouse", "runtime_conversion_requires_input_inventory"},
		{[]string{"watch"}, "mysql", "runtime_state_requires_mysql_and_epoch_file"},
		{[]string{"runtime-rollback"}, "mysql", "runtime_rollback_requires_current_state_export"},
		{[]string{"preflight", "--legacy-history-recovery"}, "mysql", "invalid_runtime_options"},
		{[]string{"watch"}, "unsupported", "unsupported storage backend"},
	} {
		t.Run(strings.Join(tc.args, "_"), func(t *testing.T) {
			t.Setenv("JUSTLEND_KEEPER_STORAGE_BACKEND", tc.env)
			t.Setenv("STORAGE_BACKEND", tc.env)
			t.Setenv("JUSTLEND_KEEPER_RUNTIME_EPOCH_FILE", "")
			t.Setenv("JUSTLEND_KEEPER_MYSQL_SOCKET", "/unused/runtime-test.sock")
			t.Setenv("JUSTLEND_KEEPER_MYSQL_USER", "test")
			t.Setenv("JUSTLEND_KEEPER_MYSQL_PASSWORD_FILE", "/unused/runtime-test-password")
			args := append(append([]string{}, tc.args...), "--config", "../../config/justlend-keeper-tron.json")
			err := run(context.Background(), args)
			if err == nil || !strings.Contains(err.Error(), tc.expected) {
				t.Fatal(err)
			}
		})
	}
}
