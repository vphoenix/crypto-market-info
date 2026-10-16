package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	keeper "github.com/vphoenix/crypto-market-info/internal/justlendkeeper"
	"github.com/vphoenix/crypto-market-info/internal/storage/mysql"
)

// Administrative, read-only and finite. Ordinary collectors never receive the
// PROCESS permissions needed to see abandoned connections from other services.
func runtimeAudit(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()
	db, err := mysql.Connect(ctx, mysql.Config{Socket: "/home/ubuntu/crypto-market-info/var/storage/mysql/run/mysql.sock", Username: "root", PasswordFile: secretBase + "mysql-root-password", MaxConnections: 1, ReadOnly: true, SelectTimeout: 5 * time.Second})
	if err != nil {
		return err
	}
	defer db.Close()
	var id string
	if err = db.QueryRowContext(ctx, "SELECT @@server_uuid").Scan(&id); err != nil {
		return err
	}
	if id != "b820454a-c654-11f1-80fa-a04da03d4fd6" {
		return fmt.Errorf("runtime audit server UUID changed")
	}
	out := map[string]any{"utc": time.Now().UTC(), "server_uuid": id, "read_only": true,
		"orphan_classification": "connection ownership requires caller correlation; no automatic orphan claim or cancellation"}
	var observation struct {
		Pending map[string]struct {
			Path string `json:"checkpoint_path"`
		} `json:"low_frequency_pending"`
	}
	if raw, e := os.ReadFile(base + "/observation-latest.json"); e == nil && json.Unmarshal(raw, &observation) == nil {
		path := filepath.Clean(observation.Pending["justlend-keeper"].Path)
		if filepath.Base(path) == "state.gob" && (strings.HasPrefix(path, "/home/ubuntu/crypto-market-info/var/") || strings.HasPrefix(path, "/home/ubuntu/.local/share/crypto-market-info")) {
			if pending, e := keeper.ReadPendingStatus(path); e == nil {
				out["keeper_pending"] = pending
			} else {
				out["keeper_pending_error"] = e.Error()
			}
		}
	}
	queries := []struct{ name, sql string }{
		{"durability_and_buffer", `SELECT @@innodb_buffer_pool_size AS buffer_pool_bytes,@@binlog_cache_size AS binlog_cache_bytes,@@innodb_flush_log_at_trx_commit AS flush_at_commit,@@sync_binlog AS sync_binlog,@@innodb_doublewrite AS doublewrite`},
		{"buffer_resize_status", `SHOW GLOBAL STATUS LIKE 'Innodb_buffer_pool_resize_status'`},
		{"connections", `SELECT ID,USER,COALESCE(DB,'') AS database_name,COMMAND,TIME,COALESCE(STATE,'') AS state,
COALESCE(SHA2(INFO,256),'') AS statement_sha256,
CASE WHEN INFO LIKE 'SELECT%' THEN 'read' WHEN COMMAND='Sleep' THEN 'idle' ELSE 'other' END AS operation
FROM information_schema.PROCESSLIST WHERE USER IN ('cmi_live','cmi_query','cmi_migrate','cmi_ddl') ORDER BY ID`},
		{"transactions", `SELECT t.trx_mysql_thread_id,t.trx_state,t.trx_started,t.trx_rows_locked,t.trx_rows_modified
FROM information_schema.INNODB_TRX t JOIN information_schema.PROCESSLIST p ON p.ID=t.trx_mysql_thread_id
WHERE p.USER IN ('cmi_live','cmi_query','cmi_migrate','cmi_ddl')`},
		{"row_lock_waits", `SELECT COUNT(*) AS waits FROM performance_schema.data_lock_waits w
JOIN performance_schema.threads t ON t.THREAD_ID=w.REQUESTING_THREAD_ID WHERE t.PROCESSLIST_USER='cmi_live'`},
		{"engine_counters", `SHOW GLOBAL STATUS WHERE Variable_name IN ('Threads_running','Innodb_buffer_pool_reads','Innodb_buffer_pool_read_requests','Innodb_buffer_pool_pages_dirty','Innodb_data_reads','Innodb_data_writes','Innodb_data_fsyncs','Innodb_row_lock_current_waits','Innodb_row_lock_time','Binlog_cache_use','Binlog_cache_disk_use')`},
		{"table_space_estimates", `SELECT TABLE_SCHEMA,TABLE_NAME,TABLE_ROWS,DATA_LENGTH,INDEX_LENGTH,DATA_FREE
FROM information_schema.TABLES WHERE TABLE_SCHEMA IN ('cmi_storage','crypto_market_info','crypto_market_info_across','crypto_market_info_reserve','crypto_market_info_lst','crypto_market_info_justlend_keeper') ORDER BY TABLE_SCHEMA,TABLE_NAME`},
	}
	for _, query := range queries {
		started := time.Now()
		queryCtx, cancelQuery := context.WithTimeout(ctx, 5*time.Second)
		rows, err := auditRows(queryCtx, db, query.sql)
		cancelQuery()
		if err != nil {
			out[query.name] = map[string]any{"error": err.Error()}
		} else {
			out[query.name] = rows
		}
		out[query.name+"_ms"] = time.Since(started).Milliseconds()
	}
	return save(base+"/runtime-audit-latest.json", out)
}

func auditRows(ctx context.Context, db *sql.DB, query string) ([]map[string]string, error) {
	return auditBoundRows(ctx, db, query)
}

func auditBoundRows(ctx context.Context, db *sql.DB, query string, args ...any) ([]map[string]string, error) {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	out := []map[string]string{}
	for rows.Next() {
		if len(out) >= 512 {
			return nil, fmt.Errorf("audit row bound exceeded")
		}
		values, dest := make([]sql.NullString, len(columns)), make([]any, len(columns))
		for n := range dest {
			dest[n] = &values[n]
		}
		if err = rows.Scan(dest...); err != nil {
			return nil, err
		}
		row := map[string]string{}
		for n, name := range columns {
			if values[n].Valid {
				row[name] = values[n].String
			}
		}
		out = append(out, row)
	}
	return out, rows.Err()
}
