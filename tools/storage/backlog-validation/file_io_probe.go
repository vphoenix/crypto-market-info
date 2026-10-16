package main

import (
	"context"
	"errors"
	"fmt"
	driver "github.com/go-sql-driver/mysql"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/storage/mysql"
)

// Classify cumulative file I/O by instrumentation type. No SQL statement,
// parameter, file contents, or row from any project's facts is returned.
func probeFileIO(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 12*time.Second)
	defer cancel()
	db, err := mysql.Connect(ctx, mysql.Config{Socket: "/home/ubuntu/crypto-market-info/var/storage/mysql/run/mysql.sock", Username: "root", PasswordFile: secretBase + "mysql-root-password", MaxConnections: 1, ReadOnly: true, SelectTimeout: 3 * time.Second})
	if err != nil {
		return err
	}
	defer db.Close()
	var id string
	if err = db.QueryRowContext(ctx, "SELECT @@server_uuid").Scan(&id); err != nil {
		return err
	}
	if id != "b820454a-c654-11f1-80fa-a04da03d4fd6" {
		return fmt.Errorf("file I/O probe server UUID changed")
	}
	out := map[string]any{"utc": time.Now().UTC(), "read_only": true, "server_uuid": id}
	started := time.Now()
	rows, err := auditRows(ctx, db, `SELECT EVENT_NAME,COUNT_READ AS read_ops,SUM_NUMBER_OF_BYTES_READ AS read_bytes,COUNT_WRITE AS write_ops,SUM_NUMBER_OF_BYTES_WRITE AS write_bytes,COUNT_MISC AS misc_ops,SUM_TIMER_WAIT AS wait_picoseconds FROM performance_schema.file_summary_by_event_name WHERE COUNT_STAR>0`)
	out["query_ms"] = time.Since(started).Milliseconds()
	if err != nil {
		out["error_type"] = fmt.Sprintf("%T", err)
		var serverError *driver.MySQLError
		if errors.As(err, &serverError) {
			out["error_code"] = serverError.Number
			out["message"] = serverError.Message
		}
	} else {
		out["file_classes"] = rows
	}
	return save(workspaceEvidence+"/file-io-probe-"+time.Now().UTC().Format("20060102T150405Z")+".json", out)
}
