package mysql

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

func TestMigrationBoundedBatchReadbackFailureRollbackAndVersionFallback(t *testing.T) {
	c := integrationClient(t)
	ctx := context.Background()
	tab, e := c.mapping("funding_rate_hourly")
	if e != nil {
		t.Fatal(e)
	}
	rows := []MigrationRow{}
	at := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	const rowCount = 129 // Cross both bounded key-read boundaries.
	for i := uint32(1); i <= rowCount; i++ {
		rows = append(rows, MigrationRow{"instrument_id": i, "hour_time": at, "funding_time": at, "rate": decimal.RequireFromString("-0.000000000000000001"), "is_actual": true})
	}
	run := uuid.New()
	upper, e := MigrationKey(tab, rows[len(rows)-1])
	if e != nil {
		t.Fatal(e)
	}
	if e = EnsureMigrationRun(ctx, c.db, run, sha256.Sum256([]byte("fixture"))); e != nil {
		t.Fatal(e)
	}
	if e = PlanMigrationTable(ctx, c.db, tab, c.database, run, upper); e != nil {
		t.Fatal(e)
	}
	h := sha256.New()
	for _, r := range rows {
		raw, e := PackMigrationRow(tab, r)
		if e != nil {
			t.Fatal(e)
		}
		var frame [4]byte
		binary.BigEndian.PutUint32(frame[:], uint32(len(raw)))
		h.Write(frame[:])
		h.Write(raw)
	}
	var digest [32]byte
	copy(digest[:], h.Sum(nil))
	chunk := sha256.Sum256(append(run[:], upper...))
	bad := digest
	bad[0] ^= 1
	if _, e = ImportMigrationChunk(ctx, c.db, c.database, tab, run, chunk, nil, upper, rowCount, bad, batchReader(rows)); e == nil {
		t.Fatal("changed input digest accepted")
	}
	var n int
	if e = c.db.QueryRowContext(ctx, "SELECT count(*) FROM "+c.table(tab.Name)).Scan(&n); e != nil || n != 0 {
		t.Fatal("failed batch partly committed", n, e)
	}
	r, e := ImportMigrationChunk(ctx, c.db, c.database, tab, run, chunk, nil, upper, rowCount, digest, batchReader(rows))
	if e != nil || r.Inserted != rowCount {
		t.Fatal(r, e)
	}
	if e = VerifyMigrationChunk(ctx, c.db, c.database, tab, run, chunk, rowCount, digest, batchReader(rows)); e != nil {
		t.Fatal("independent batch readback", e)
	}
	if _, e = c.db.ExecContext(ctx, "UPDATE "+c.table(tab.Name)+" SET rate=0.1 WHERE instrument_id=1"); e != nil {
		t.Fatal(e)
	}
	if e = VerifyMigrationChunk(ctx, c.db, c.database, tab, run, chunk, rowCount, digest, batchReader(rows)); e == nil {
		t.Fatal("changed target values accepted")
	}
	if _, e = c.db.ExecContext(ctx, "UPDATE "+c.table(tab.Name)+" SET rate=-0.000000000000000001 WHERE instrument_id=1"); e != nil {
		t.Fatal(e)
	}
	older := MigrationRow{}
	for k, v := range rows[0] {
		older[k] = v
	}
	older["is_actual"] = false
	older["rate"] = decimal.RequireFromString("0.9")
	result, e := migrateRows(t, c, tab.Name, []MigrationRow{older}, uuid.New())
	if e != nil || result.Superseded != 1 {
		t.Fatal("overlap lost original version rule", result, e)
	}
}
