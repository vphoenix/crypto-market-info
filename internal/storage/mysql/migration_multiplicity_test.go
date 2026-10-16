package mysql

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/model"
)

func TestMigrationHeterogeneousPublicationsRetainAmbiguityAndAudit(t *testing.T) {
	c := integrationClient(t)
	ctx := context.Background()
	tab, _ := c.mapping("source_batch_status")
	base := MigrationRow{"source_kind": "fixture", "source_id": "mixed", "content_hash": strings.Repeat("a", 64), "codec": model.DiscoveryCodec, "source_ms": int64(1000), "observed_ms": int64(2000), "known_from_ms": int64(3000), "published_ms": int64(4000), "complete": uint8(1), "instrument_id": uint32(1), "effective_ms": int64(1000), "source_count": uint16(0), "provenance_hash": strings.Repeat("b", 64)}
	for _, count := range []int{3, 501} { // Exercise both bounded and streaming imports.
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			rows := make([]MigrationRow, count)
			for i := range rows {
				row := MigrationRow{}
				for k, v := range base {
					row[k] = v
				}
				row["source_id"] = uuid.NewSHA1(uuid.Nil, []byte{byte(count), byte(count >> 8)}).String()
				row["published_ms"] = int64(4000 + i%2)
				rows[i] = row
			}
			if count > 500 {
				prefix := MigrationRow{}
				for k, v := range base {
					prefix[k] = v
				}
				prefix["source_id"] = "!prefix"
				rows = append(rows, prefix)
			}
			sort.Slice(rows, func(i, j int) bool {
				a, _ := PackMigrationRow(tab, rows[i])
				b, _ := PackMigrationRow(tab, rows[j])
				return bytes.Compare(a, b) < 0
			})
			run := uuid.New()
			upper, _ := MigrationKey(tab, rows[len(rows)-1])
			if e := EnsureMigrationRun(ctx, c.db, run, sha256.Sum256([]byte("mixed fixture"))); e != nil {
				t.Fatal(e)
			}
			if e := PlanMigrationTable(ctx, c.db, tab, c.database, run, upper); e != nil {
				t.Fatal(e)
			}
			chunk := sha256.Sum256(append(run[:], upper...))
			firstDuplicate := 0
			if count > 500 {
				firstDuplicate = 1
			}
			a, _ := rowHash(tab, record(rows[firstDuplicate]))
			b, _ := rowHash(tab, record(rows[len(rows)-1]))
			key, _ := keyHash(tab, record(rows[firstDuplicate]))
			old := &MigrationConflict{Run: run, Chunk: chunk, Key: key, Source: a, Target: b, Reason: "source_immutable_key_has_different_contents"}
			if e := SaveMigrationConflict(ctx, c.db, old); e != nil {
				t.Fatal(e)
			}
			h := sha256.New()
			for _, row := range rows {
				raw, _ := PackMigrationRow(tab, row)
				var frame [4]byte
				binary.BigEndian.PutUint32(frame[:], uint32(len(raw)))
				h.Write(frame[:])
				h.Write(raw)
			}
			var digest [32]byte
			copy(digest[:], h.Sum(nil))
			result, e := ImportMigrationChunk(ctx, c.db, c.database, tab, run, chunk, nil, upper, uint64(len(rows)), digest, batchReader(rows))
			if e != nil || result.Inserted != uint64(len(rows)) {
				t.Fatal("heterogeneous source occurrences lost", result, e)
			}
			if e = VerifyMigrationChunk(ctx, c.db, c.database, tab, run, chunk, uint64(len(rows)), digest, batchReader(rows)); e != nil {
				t.Fatal("independent heterogeneous multiset verification", e)
			}
			if _, e = c.latestDiscoveryProof(ctx, "fixture", 1, time.UnixMilli(5000)); e == nil {
				t.Fatal("heterogeneous duplicate publication became valid")
			}
			var pending, resolutions int
			if e = c.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM cmi_storage.migration_conflict WHERE chunk_id=? AND resolved=0", chunk[:]).Scan(&pending); e != nil || pending != 0 {
				t.Fatal("old diagnostic not explained by exact multiset preservation", pending, e)
			}
			if e = c.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM cmi_storage.migration_resolution WHERE chunk_id=? AND source_hash=target_hash AND reason='preserved_complete_source_multiset_ambiguity'", chunk[:]).Scan(&resolutions); e != nil || resolutions != 1 {
				t.Fatal("missing retained audit resolution", resolutions, e)
			}
		})
	}
	// A current single publication must never be expanded into an ambiguous
	// source group just because its first occurrence happens to match.
	first := MigrationRow{}
	for k, v := range base {
		first[k] = v
	}
	first["source_id"] = "existing"
	if _, e := migrateRows(t, c, tab.Name, []MigrationRow{first}, uuid.New()); e != nil {
		t.Fatal(e)
	}
	second := MigrationRow{}
	for k, v := range first {
		second[k] = v
	}
	second["published_ms"] = int64(4001)
	_, e := migrateRows(t, c, tab.Name, []MigrationRow{first, second}, uuid.New())
	var conflict *MigrationConflict
	if !errors.As(e, &conflict) || conflict.Reason != "ambiguous_source_overlaps_existing_target" {
		t.Fatal("source ambiguity silently appended to existing publication", e)
	}
	got, e := c.records(ctx, c.db, tab.Name, "WHERE source_kind=? AND source_id=?", []byte("fixture"), []byte("existing"))
	if e != nil || len(got) != 1 {
		t.Fatal("failed overlap changed target multiplicity", len(got), e)
	}
}
