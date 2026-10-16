package mysql

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"github.com/google/uuid"
)

type MigrationTableReport struct {
	Source, Database, Table string
	State                   uint8
	Empty                   bool
	Upper, Last             []byte
	Error                   sql.NullString
	Chunks, Rows            uint64
}
type MigrationStatus struct {
	Run                string
	State              uint8
	Conflicts          uint64
	AcceptedSourceGaps uint64
	Tables             []MigrationTableReport
}

func MigrationReport(ctx context.Context, cfg Config, run uuid.UUID) (MigrationStatus, error) {
	out := MigrationStatus{Run: run.String()}
	cfg.ReadOnly = true
	db, e := Connect(ctx, cfg)
	if e != nil {
		return out, e
	}
	defer db.Close()
	if e = db.QueryRowContext(ctx, "SELECT state FROM cmi_storage.migration_run WHERE run_id=?", run[:]).Scan(&out.State); e != nil {
		return out, e
	}
	if e = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM cmi_storage.migration_conflict WHERE run_id=? AND resolved=0", run[:]).Scan(&out.Conflicts); e != nil {
		return out, e
	}
	if e = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM cmi_storage.bootstrap_source_gap WHERE run_id=?", run[:]).Scan(&out.AcceptedSourceGaps); e != nil {
		return out, e
	}
	rows, e := db.QueryContext(ctx, `SELECT m.source_database,m.database_name,m.table_name,m.state,m.source_empty,m.upper_key,m.last_key,m.error,COALESCE(c.chunks,0),COALESCE(c.rows_count,0) FROM cmi_storage.migration_table m LEFT JOIN (SELECT database_name,table_name,COUNT(*) chunks,SUM(source_count) rows_count FROM cmi_storage.migration_chunk WHERE run_id=? GROUP BY database_name,table_name) c ON c.database_name=m.database_name AND c.table_name=m.table_name WHERE m.run_id=? ORDER BY m.database_name,m.table_name LIMIT 126`, run[:], run[:])
	if e != nil {
		return out, e
	}
	defer rows.Close()
	for rows.Next() {
		var t MigrationTableReport
		if e = rows.Scan(&t.Source, &t.Database, &t.Table, &t.State, &t.Empty, &t.Upper, &t.Last, &t.Error, &t.Chunks, &t.Rows); e != nil {
			return out, e
		}
		out.Tables = append(out.Tables, t)
	}
	if len(out.Tables) > 125 {
		return out, fmt.Errorf("run contains unreviewed extra table plans")
	}
	return out, rows.Err()
}

// FinalizeMigration is explicit and requires every reviewed production table.
// A bootstrap or a partially readable source can never open full history.
func FinalizeMigration(ctx context.Context, db *sql.DB, run uuid.UUID) error {
	catalog, e := Mappings()
	if e != nil {
		return e
	}
	return finalizeMigrationTables(ctx, db, run, catalog.Tables)
}

// The public entry point always supplies the entire reviewed catalog.
func finalizeMigrationTables(ctx context.Context, db *sql.DB, run uuid.UUID, tables []Table) error {
	tx, e := db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	var gaps uint64
	if e = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM cmi_storage.bootstrap_source_gap g WHERE EXISTS (SELECT 1 FROM cmi_storage.migration_table m WHERE m.run_id=? AND m.database_name=g.database_name AND m.table_name=g.table_name)", run[:]).Scan(&gaps); e != nil {
		return e
	}
	if gaps != 0 {
		return fmt.Errorf("full history has explicitly accepted source gaps: %d", gaps)
	}
	var state uint8
	if e = tx.QueryRowContext(ctx, "SELECT state FROM cmi_storage.migration_run WHERE run_id=? FOR UPDATE", run[:]).Scan(&state); e != nil {
		return e
	}
	var conflicts int
	// Conflicts may precede a committed checkpoint, so include the identities of
	// all attempted chunks recorded with their table plan in the conflict audit.
	if e = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM cmi_storage.migration_conflict WHERE resolved=0 AND (run_id=? OR run_id IS NULL)", run[:]).Scan(&conflicts); e != nil {
		return e
	}
	if conflicts != 0 {
		return fmt.Errorf("unresolved migration conflicts: %d", conflicts)
	}
	for _, t := range tables {
		var upper, last, hash []byte
		var verified uint8
		var empty bool
		e = tx.QueryRowContext(ctx, "SELECT upper_key,last_key,mapping_hash,state,source_empty FROM cmi_storage.migration_table WHERE run_id=? AND source_database=? AND database_name=? AND table_name=? FOR UPDATE", run[:], t.Database, t.Database, t.Name).Scan(&upper, &last, &hash, &verified, &empty)
		want := MappingDigest(t)
		if e != nil || verified != 2 || string(hash) != string(want[:]) || (!empty && string(upper) != string(last)) {
			return fmt.Errorf("table not fully verified: %s.%s", t.Database, t.Name)
		}
		// Also protects runs written by older code that prematurely set state 2.
		if t.Name == "order_book_minute" {
			if e = verifyBookPublicationCoverage(ctx, tx, t.Database); e != nil {
				return e
			}
		}
		rangeID := sha256.Sum256(append(append([]byte(t.Database+"/"+t.Name), want[:]...), upper...))
		if _, e = tx.ExecContext(ctx, "INSERT INTO cmi_storage.history_coverage(database_name,table_name,range_hash,key_start,key_end,state,verified_at) VALUES(?,?,?,?,?,2,UTC_TIMESTAMP(6)) ON DUPLICATE KEY UPDATE state=2,verified_at=VALUES(verified_at)", t.Database, t.Name, rangeID[:], []byte{}, append([]byte{}, upper...)); e != nil {
			return e
		}
	}
	_, e = tx.ExecContext(ctx, "UPDATE cmi_storage.migration_run SET state=3 WHERE run_id=?", run[:])
	if e != nil {
		return e
	}
	return tx.Commit()
}
