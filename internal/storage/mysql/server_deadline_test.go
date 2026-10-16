package mysql

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"
)

func TestServerDeadlineAppliesToEveryConnectionAndAbandonedReadEnds(t *testing.T) {
	if os.Getenv("MYSQL_INTEGRATION") != "1" {
		t.Skip("isolated MySQL required")
	}
	cfg := Config{Socket: os.Getenv("CMI_MYSQL_TEST_SOCKET"), Username: os.Getenv("CMI_MYSQL_TEST_USER"), PasswordFile: os.Getenv("CMI_MYSQL_TEST_PASSWORD_FILE"), MaxConnections: 2, SelectTimeout: 200 * time.Millisecond, LockWaitTimeout: 5 * time.Second}
	db, err := Connect(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	first, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	for _, conn := range []*sql.Conn{first, second} {
		var selectMS, lockSeconds int
		if err = conn.QueryRowContext(context.Background(), "SELECT @@session.max_execution_time,@@session.innodb_lock_wait_timeout").Scan(&selectMS, &lockSeconds); err != nil || selectMS != 200 || lockSeconds != 5 {
			t.Fatal(selectMS, lockSeconds, err)
		}
	}
	var id uint64
	if err = first.QueryRowContext(context.Background(), "SELECT CONNECTION_ID()").Scan(&id); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	var result any
	err = first.QueryRowContext(ctx, "SELECT SUM(CRC32(CONCAT(a.COLUMN_NAME,b.COLUMN_NAME,c.COLUMN_NAME))) FROM information_schema.COLUMNS a CROSS JOIN information_schema.COLUMNS b CROSS JOIN information_schema.COLUMNS c").Scan(&result)
	if err == nil {
		t.Fatal("expensive cancelled read unexpectedly succeeded")
	}
	deadline := time.Now().Add(3 * time.Second)
	if _, err = second.ExecContext(context.Background(), "SET SESSION max_execution_time=5000"); err != nil {
		t.Fatal(err)
	}
	for {
		var active int
		err = second.QueryRowContext(context.Background(), "SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE ID=? AND COMMAND='Query'", id).Scan(&active)
		if err != nil {
			t.Fatal(err)
		}
		if active == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("abandoned server read survived execution deadline")
		}
		time.Sleep(50 * time.Millisecond)
	}
}
