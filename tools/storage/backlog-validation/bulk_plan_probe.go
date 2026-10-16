package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	driver "github.com/go-sql-driver/mysql"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/storage/mysql"
)

// Compare only execution plans for 500 absent minute-delta identities. No
// fact query is executed and no market data is returned by either statement.
func probeBulkPlans(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()
	db, err := mysql.Connect(ctx, mysql.Config{Socket: "/home/ubuntu/crypto-market-info/var/storage/mysql/run/mysql.sock", Username: "root", PasswordFile: secretBase + "mysql-root-password", MaxConnections: 1, ReadOnly: true, SelectTimeout: 3 * time.Second})
	if err != nil {
		return err
	}
	defer db.Close()
	var uuid string
	if err = db.QueryRowContext(ctx, "SELECT @@server_uuid").Scan(&uuid); err != nil {
		return err
	}
	if uuid != "b820454a-c654-11f1-80fa-a04da03d4fd6" {
		return fmt.Errorf("diagnostic server UUID changed")
	}
	table, err := mysql.Mapping("crypto_market_info", "derivative_book_second_delta")
	if err != nil {
		return err
	}
	columns := make([]string, len(table.Columns))
	for n, c := range table.Columns {
		columns[n] = "`" + c.Name + "`"
	}
	ors, tuples := []string{}, []string{}
	args := []any{}
	at := time.Now().UTC().Truncate(time.Minute).Add(-5 * time.Minute)
	for n := range 500 {
		id, e := model.MinuteID(uint32(n/59+1), at)
		if e != nil {
			return e
		}
		ors = append(ors, "(`minute_id`=? AND `second_offset`=? AND `batch_id`=?)")
		tuples = append(tuples, "(?,?,?)")
		args = append(args, id, uint8(n%59+1), []byte(strings.Repeat("a", 64)))
	}
	prefix := "EXPLAIN SELECT " + strings.Join(columns, ",") + " FROM `crypto_market_info`.`derivative_book_second_delta` WHERE "
	queries := map[string]string{"or": prefix + strings.Join(ors, " OR ") + " FOR UPDATE", "tuple_in": prefix + "(`minute_id`,`second_offset`,`batch_id`) IN (" + strings.Join(tuples, ",") + ") FOR UPDATE"}
	out := []map[string]any{}
	for _, kind := range []string{"or", "tuple_in", "tuple_in", "or"} {
		started := time.Now()
		rows, e := auditBoundRows(ctx, db, queries[kind], args...)
		r := map[string]any{"shape": kind, "keys": 500, "query_bytes": len(queries[kind]), "explain_ms": time.Since(started).Milliseconds(), "plan": rows}
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
	return save(workspaceEvidence+"/bulk-predicate-plan-probe.json", map[string]any{"utc": time.Now().UTC(), "read_only": true, "plans": out})
}
