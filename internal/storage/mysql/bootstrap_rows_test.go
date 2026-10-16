package mysql

import (
	"context"
	"crypto/sha256"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestStartupSubsetVerificationCannotOpenHistory(t *testing.T) {
	if os.Getenv("MYSQL_INTEGRATION") != "1" {
		t.Skip("MYSQL_INTEGRATION=1 enables isolated control tests")
	}
	ctx := context.Background()
	db, err := Connect(ctx, Config{Socket: os.Getenv("CMI_MYSQL_TEST_SOCKET"), Username: os.Getenv("CMI_MYSQL_TEST_USER"), PasswordFile: os.Getenv("CMI_MYSQL_TEST_PASSWORD_FILE"), MaxConnections: 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	run := uuid.New()
	target := "cmi_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	t.Cleanup(func() {
		db.ExecContext(ctx, "DELETE FROM cmi_storage.migration_table WHERE run_id=?", run[:])
		db.ExecContext(ctx, "DELETE FROM cmi_storage.migration_run WHERE run_id=?", run[:])
	})
	if err = EnsureMigrationRun(ctx, db, run, sha256.Sum256([]byte("startup-state-test"))); err != nil {
		t.Fatal(err)
	}
	tab, _ := Mapping("crypto_market_info", "funding_rate_hourly")
	if err = PlanMigrationTable(ctx, db, tab, target, run, nil); err != nil {
		t.Fatal(err)
	}
	if err := MarkMigrationTableCopied(ctx, db, tab, target, run); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := MarkMigrationBootstrapRowsVerified(ctx, db, tab, target, run); err != nil {
			t.Fatal(err)
		}
	}
	_, _, state, err := LoadMigrationTablePlan(ctx, db, tab, target, run)
	if err != nil || state != MigrationRowsVerified {
		t.Fatal("startup subset claimed full verification", state, err)
	}
	if err = FinalizeMigration(ctx, db, run); err == nil {
		t.Fatal("partial startup run opened history")
	}
}
