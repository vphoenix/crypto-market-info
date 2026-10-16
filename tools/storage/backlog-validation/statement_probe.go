package main

import (
	"context"
	"fmt"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/storage/mysql"
)

func probeStatementCost(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	db, err := mysql.Connect(ctx, mysql.Config{Socket: workspaceEvidence + "/../mysql/run/mysql.sock", Username: "root", PasswordFile: secretBase + "mysql-root-password", MaxConnections: 1, ReadOnly: true, SelectTimeout: 3 * time.Second})
	if err != nil {
		return err
	}
	defer db.Close()
	var id string
	if err = db.QueryRowContext(ctx, "SELECT @@server_uuid").Scan(&id); err != nil {
		return err
	}
	if id != "b820454a-c654-11f1-80fa-a04da03d4fd6" {
		return fmt.Errorf("diagnostic server UUID changed")
	}
	out := map[string]any{"utc": time.Now().UTC(), "read_only": true, "server_uuid": id}
	queries := []struct{ name, sql string }{
		{"binlog_cache_status", "SHOW GLOBAL STATUS WHERE Variable_name IN ('Binlog_cache_use','Binlog_cache_disk_use','Binlog_stmt_cache_use','Binlog_stmt_cache_disk_use','Created_tmp_tables','Created_tmp_disk_tables')"},
		{"cache_settings", "SELECT @@binlog_cache_size AS binlog_cache_size,@@max_binlog_cache_size AS max_binlog_cache_size,@@tmp_table_size AS tmp_table_size,@@temptable_max_ram AS temptable_max_ram,@@temptable_max_mmap AS temptable_max_mmap"},
		{"statement_cost", "SELECT DIGEST,LEFT(DIGEST_TEXT,600) AS statement_shape,COUNT_STAR,SUM_TIMER_WAIT,SUM_CREATED_TMP_TABLES,SUM_CREATED_TMP_DISK_TABLES,SUM_SORT_ROWS FROM performance_schema.events_statements_summary_by_digest WHERE SCHEMA_NAME='crypto_market_info' ORDER BY SUM_TIMER_WAIT DESC LIMIT 100"},
	}
	for _, q := range queries {
		started := time.Now()
		r, e := auditRows(ctx, db, q.sql)
		out[q.name+"_ms"] = time.Since(started).Milliseconds()
		if e != nil {
			out[q.name+"_error"] = e.Error()
		} else {
			out[q.name] = r
		}
	}
	return save(workspaceEvidence+"/statement-cost-probe.json", out)
}

// Read only active live statements. Return table names and shape flags, never
// original SQL text or parameters. Cumulative performance-schema scans exceeded
// the bounded probe deadline on this host.
func probeCurrentWork(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 25*time.Second)
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
		return fmt.Errorf("diagnostic server UUID changed")
	}
	samples := []map[string]any{}
	defer func() {
		_ = save(workspaceEvidence+"/current-processlist-work-probe.json", map[string]any{"read_only": true, "samples": samples})
	}()
	for range 6 {
		rows, e := auditRows(ctx, db, `SELECT ID,COALESCE(DB,'') AS database_name,TIME,COALESCE(STATE,'') AS state,
LEFT(CONVERT(INFO USING latin1),6) AS operation,
REGEXP_SUBSTR(CONVERT(INFO USING latin1),'`+"`crypto_market_info[^`]*`[.]`[a-zA-Z0-9_]+`"+`') AS project_table,
LENGTH(INFO) AS statement_bytes,
LOCATE('producer_generation',INFO)>0 AS generation_fence,
LOCATE('log_coverage',SUBSTRING(INFO,LOCATE(' WHERE ',INFO)))>0 AS log_filter,
LOCATE('receipt_coverage',SUBSTRING(INFO,LOCATE(' WHERE ',INFO)))>0 AS receipt_filter,
LOCATE('finality',SUBSTRING(INFO,LOCATE(' WHERE ',INFO)))>0 AS finality_filter,
LOCATE('ORDER BY',INFO)>0 AS ordered,
LOCATE('LEAD(',INFO)>0 AS window_query
FROM information_schema.PROCESSLIST WHERE USER='cmi_live' AND COMMAND!='Sleep'`)
		sample := map[string]any{"utc": time.Now().UTC(), "statements": rows}
		if e != nil {
			sample["error"] = e.Error()
		}
		samples = append(samples, sample)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
	return save(workspaceEvidence+"/current-processlist-work-probe.json", map[string]any{"read_only": true, "samples": samples})
}
