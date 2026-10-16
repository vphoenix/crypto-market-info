package mysql

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestMySQLOperatorReadTimeoutPreservesContextCancellation(t *testing.T) {
	if os.Getenv("MYSQL_INTEGRATION") != "1" {
		t.Skip("MYSQL_INTEGRATION=1 enables read-only MySQL timeout test")
	}
	cfg := Config{Socket: os.Getenv("CMI_MYSQL_TEST_SOCKET"), Username: os.Getenv("CMI_MYSQL_TEST_USER"), PasswordFile: os.Getenv("CMI_MYSQL_TEST_PASSWORD_FILE"), MaxConnections: 1, ReadOnly: true, ReadTimeout: 2 * time.Minute}
	for _, timeout := range []time.Duration{-time.Second, 2*time.Minute + time.Millisecond} {
		invalid := cfg
		invalid.ReadTimeout = timeout
		if db, e := Connect(context.Background(), invalid); e == nil {
			db.Close()
			t.Fatal("unbounded operator timeout accepted")
		}
	}
	db, e := Connect(context.Background(), cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	var result int
	// The production default expires before this result arrives. The explicit
	// operator setting must allow it without disabling the context deadline.
	if e = db.QueryRowContext(ctx, "SELECT SLEEP(31)").Scan(&result); e != nil || result != 0 {
		t.Fatal("operator timeout did not permit a bounded slow read", result, e)
	}
	cancelled, stop := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer stop()
	if e = db.QueryRowContext(cancelled, "SELECT SLEEP(5)").Scan(&result); e == nil {
		t.Fatal("operator timeout disabled context cancellation")
	}
}
