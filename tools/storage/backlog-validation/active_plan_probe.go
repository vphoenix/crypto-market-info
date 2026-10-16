package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	driver "github.com/go-sql-driver/mysql"
	"github.com/vphoenix/crypto-market-info/internal/storage/mysql"
)

// MySQL cannot EXPLAIN FOR CONNECTION a prepared statement. Read only our
// current SELECTs into process memory and EXPLAIN their expanded form instead.
// Original SQL/parameters are never serialized, printed or passed to a shell.
func probeActivePlans(ctx context.Context) error {
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
	type target struct {
		id                  uint64
		database, statement string
		seconds             int
	}
	rows, err := db.QueryContext(ctx, "SELECT ID,COALESCE(DB,''),TIME,INFO FROM information_schema.PROCESSLIST WHERE USER='cmi_live' AND INFO LIKE 'SELECT%' AND COMMAND!='Sleep' ORDER BY TIME DESC LIMIT 8")
	if err != nil {
		return err
	}
	targets := []target{}
	for rows.Next() {
		var t target
		var statement sql.NullString
		if err = rows.Scan(&t.id, &t.database, &t.seconds, &statement); err != nil {
			rows.Close()
			return err
		}
		t.statement = statement.String
		if (t.database == "crypto_market_info" || strings.HasPrefix(t.database, "crypto_market_info_")) && len(t.statement) <= 16000 && strings.HasPrefix(strings.ToUpper(strings.TrimSpace(t.statement)), "SELECT ") {
			targets = append(targets, t)
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	out := []map[string]any{}
	defer func() {
		_ = save(workspaceEvidence+"/active-query-plans.json", map[string]any{"read_only": true, "utc": time.Now().UTC(), "plans": out})
	}()
	for _, t := range targets {
		started := time.Now()
		plan, e := auditRows(ctx, db, "EXPLAIN "+t.statement)
		r := map[string]any{"connection_id": t.id, "database": t.database, "observed_seconds": t.seconds, "explain_ms": time.Since(started).Milliseconds(), "plan": plan}
		if e != nil {
			var server *driver.MySQLError
			if errors.As(e, &server) {
				r["error_code"] = server.Number
			} else {
				r["error_type"] = fmt.Sprintf("%T", e)
			}
		}
		out = append(out, r)
	}
	return save(workspaceEvidence+"/active-query-plans.json", map[string]any{"read_only": true, "utc": time.Now().UTC(), "plans": out})
}
