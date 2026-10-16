package mysql

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
	"io"
	"strings"
	"testing"
	"time"
)

func migrateRows(t *testing.T, c *Client, name string, rows []MigrationRow, run uuid.UUID) (MigrationResult, error) {
	t.Helper()
	ctx := context.Background()
	mapping, e := c.mapping(name)
	if e != nil {
		t.Fatal(e)
	}
	upper, e := MigrationKey(mapping, rows[len(rows)-1])
	if e != nil {
		t.Fatal(e)
	}
	hash := sha256.Sum256([]byte("test inventory"))
	if e = EnsureMigrationRun(ctx, c.db, run, hash); e != nil {
		t.Fatal(e)
	}
	if _, _, _, e = LoadMigrationTablePlan(ctx, c.db, mapping, c.database, run); e != nil {
		if e = PlanMigrationTable(ctx, c.db, mapping, c.database, run, upper); e != nil {
			t.Fatal(e)
		}
	}
	digest := sha256.New()
	for _, row := range rows {
		raw, e := PackMigrationRow(mapping, row)
		if e != nil {
			t.Fatal(e)
		}
		var n [4]byte
		binary.BigEndian.PutUint32(n[:], uint32(len(raw)))
		digest.Write(n[:])
		digest.Write(raw)
	}
	var h [32]byte
	copy(h[:], digest.Sum(nil))
	id := sha256.Sum256(append(run[:], upper...))
	i := 0
	next := func() (MigrationRow, error) {
		if i == len(rows) {
			return nil, io.EOF
		}
		v := rows[i]
		i++
		return v, nil
	}
	return ImportMigrationChunk(ctx, c.db, c.database, mapping, run, id, nil, upper, uint64(len(rows)), h, next)
}
func TestMigrationChunkRetryRevisionPriorityAndConflict(t *testing.T) {
	c := integrationClient(t)
	ctx := context.Background()
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	row := MigrationRow{"instrument_id": uint32(1), "hour_time": at, "funding_time": at, "rate": decimal.RequireFromString("0.000000000000000001"), "is_actual": true}
	run := uuid.New()
	first, e := migrateRows(t, c, "funding_rate_hourly", []MigrationRow{row}, run)
	if e != nil || first.Inserted != 1 || !first.Verified {
		t.Fatal(first, e)
	}
	mapping, _ := c.mapping("funding_rate_hourly")
	upper, _ := MigrationKey(mapping, row)
	id := sha256.Sum256(append(run[:], upper...))
	rawHash, _ := hex.DecodeString(first.SourceHash)
	var hash [32]byte
	copy(hash[:], rawHash)
	used := false
	next := func() (MigrationRow, error) {
		if used {
			return nil, io.EOF
		}
		used = true
		return row, nil
	}
	if e = VerifyMigrationChunk(ctx, c.db, c.database, mapping, run, id, 1, hash, next); e != nil {
		t.Fatal("independent typed readback", e)
	}
	if e = FinalizeMigration(ctx, c.db, run); e == nil {
		t.Fatal("partial run opens full history")
	}
	retry, e := migrateRows(t, c, "funding_rate_hourly", []MigrationRow{row}, run)
	if e != nil || retry.Rows != 1 {
		t.Fatal("migration retry", retry, e)
	}
	older := MigrationRow{}
	for k, v := range row {
		older[k] = v
	}
	older["is_actual"] = false
	older["rate"] = decimal.RequireFromString("0.7")
	r, e := migrateRows(t, c, "funding_rate_hourly", []MigrationRow{older}, uuid.New())
	if e != nil || r.Superseded != 1 {
		t.Fatal("actual priority", r, e)
	}
	bad := MigrationRow{}
	for k, v := range row {
		bad[k] = v
	}
	bad["rate"] = decimal.RequireFromString("0.8")
	if _, e = migrateRows(t, c, "funding_rate_hourly", []MigrationRow{bad}, uuid.New()); e == nil {
		t.Fatal("same priority conflicting history accepted")
	}
	var count int
	if e = c.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+c.table("funding_rate_hourly")).Scan(&count); e != nil || count != 1 {
		t.Fatal("partial conflict transaction", count, e)
	}
}
func TestMigrationPreservesOrdinaryMergeTreeMultiplicity(t *testing.T) {
	c := integrationClient(t)
	ctx := context.Background()
	mapping, e := c.mapping("source_publication_intents")
	if e != nil {
		t.Fatal(e)
	}
	if !mapping.Multiplicity {
		t.Fatal("fixture needs ordinary MergeTree")
	}
	row := MigrationRow{"source_kind": "fixture", "source_id": "id", "content_hash": strings.Repeat("a", 64), "codec": "fixture-v1", "source_ms": int64(1000), "observed_ms": int64(2000), "known_from_ms": int64(3000), "instrument_id": uint32(1), "effective_ms": int64(1000), "source_count": uint16(0), "provenance_hash": strings.Repeat("b", 64)}
	result, e := migrateRows(t, c, mapping.Name, []MigrationRow{row, row}, uuid.New())
	if e != nil || result.Inserted != 2 {
		t.Fatal("duplicate source rows collapsed", result, e)
	}
	again, e := migrateRows(t, c, mapping.Name, []MigrationRow{row, row}, uuid.New())
	if e != nil || again.Unchanged != 2 {
		t.Fatal("bootstrap repetition changes multiplicity", again, e)
	}
	records, e := readRecords(ctx, c.db, c.table(mapping.Name), mapping, "WHERE source_kind=? AND source_id=?", []byte("fixture"), []byte("id"))
	if e != nil || len(records) != 2 {
		t.Fatal("source multiplicity", e)
	}
	a, _ := PackMigrationRow(mapping, MigrationRow(records[0]))
	b, _ := PackMigrationRow(mapping, MigrationRow(records[1]))
	logical, e := c.records(ctx, c.db, mapping.Name, "WHERE source_kind=? AND source_id=?", []byte("fixture"), []byte("id"))
	if e != nil || len(logical) != 2 {
		t.Fatal("runtime reader collapsed duplicate publication", e)
	}
	_, e = c.beginDiscoveryIntent(ctx, publicationRecord{Kind: "fixture", ID: "id", Hash: strings.Repeat("a", 64), Codec: "fixture-v1", SourceMS: 1000, ObservedMS: 2000, KnownMS: 3000, InstrumentID: 1, EffectiveMS: 1000, SourceCount: 0, ProvenanceHash: strings.Repeat("b", 64)})
	if !errors.Is(e, contracts.ErrConflict) {
		t.Fatal("duplicate intents must remain ambiguous", e)
	}
	changed := MigrationRow{}
	for key, value := range row {
		changed[key] = value
	}
	changed["content_hash"] = strings.Repeat("c", 64)
	_, e = migrateRows(t, c, mapping.Name, []MigrationRow{row, changed}, uuid.New())
	var conflict *MigrationConflict
	if !errors.As(e, &conflict) || conflict.Source == conflict.Target {
		t.Fatal("source conflict audit lacks both distinct hashes", e)
	}
	if !bytes.Equal(a, b) {
		t.Fatal("identical occurrences changed")
	}
}
