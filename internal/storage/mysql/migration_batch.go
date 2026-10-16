package mysql

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/storage/codec"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
)

// Small ordinary chunks can prove empty target key groups with a single locking
// read, then insert/read back one bounded batch. Overlaps and version resolution
// retain the original row-wise path; no overwrite or conflict rule is relaxed.
func collectMigrationBatch(expected uint64, next func() (MigrationRow, error)) ([]MigrationRow, error) {
	rows := make([]MigrationRow, 0, expected)
	for {
		r, e := next()
		if e == io.EOF {
			break
		}
		if e != nil {
			return nil, e
		}
		rows = append(rows, r)
		if len(rows) > 500 {
			return nil, fmt.Errorf("ordinary migration batch exceeded row bound")
		}
	}
	if uint64(len(rows)) != expected {
		return nil, fmt.Errorf("frozen migration batch row count changed")
	}
	return rows, nil
}
func batchReader(rows []MigrationRow) func() (MigrationRow, error) {
	i := 0
	return func() (MigrationRow, error) {
		if i == len(rows) {
			return nil, io.EOF
		}
		r := rows[i]
		i++
		return r, nil
	}
}
func canonicalBatch(t Table, rows []MigrationRow) ([][]byte, []uint64, [32]byte, error) {
	raw := make([][]byte, len(rows))
	rank := make([]uint64, len(rows))
	h := sha256.New()
	var previous []byte
	start := 0
	total := 0
	flush := func(end int) error {
		sort.SliceStable(raw[start:end], func(i, j int) bool { return bytes.Compare(raw[start+i], raw[start+j]) < 0 })
		if end-start > 1 && !t.Multiplicity {
			return fmt.Errorf("non-multiplicity source contains a repeated key")
		}
		for i := start; i < end; i++ {
			rank[i] = uint64(i - start)
			writeFramed(h, raw[i])
			r, e := UnpackMigrationRow(t, raw[i])
			if e != nil {
				return e
			}
			rows[i] = r
		}
		return nil
	}
	for i, r := range rows {
		key, e := MigrationKey(t, r)
		if e != nil {
			return nil, nil, [32]byte{}, e
		}
		if previous != nil && !bytes.Equal(previous, key) {
			cmp, e := CompareMigrationKeys(t, previous, key)
			if e != nil || cmp >= 0 {
				return nil, nil, [32]byte{}, fmt.Errorf("source ordering changed")
			}
			if e = flush(i); e != nil {
				return nil, nil, [32]byte{}, e
			}
			start = i
		}
		previous = key
		raw[i], e = PackMigrationRow(t, r)
		if e != nil {
			return nil, nil, [32]byte{}, e
		}
		total += len(raw[i])
		if total > 64<<20 {
			return nil, nil, [32]byte{}, contracts.ErrCapacity
		}
	}
	if e := flush(len(rows)); e != nil {
		return nil, nil, [32]byte{}, e
	}
	var digest [32]byte
	copy(digest[:], h.Sum(nil))
	return raw, rank, digest, nil
}
func batchPredicate(t Table, rows []MigrationRow) (string, []any, error) {
	clauses := []string{}
	args := []any{}
	seen := map[string]bool{}
	for _, r := range rows {
		key, e := MigrationKey(t, r)
		if e != nil {
			return "", nil, e
		}
		if seen[string(key)] {
			continue
		}
		seen[string(key)] = true
		p, a, e := rowPredicate(t, record(r))
		if e != nil {
			return "", nil, e
		}
		if t.Multiplicity {
			p = strings.Replace(p, " AND `_occurrence`=0", "", 1)
		}
		clauses = append(clauses, "("+p+")")
		args = append(args, a...)
	}
	return strings.Join(clauses, " OR "), args, nil
}
func sameBatchFields(t Table, want [][]byte, got []record) error {
	if len(want) != len(got) {
		return fmt.Errorf("batch target/source multiplicity differs")
	}
	a := append([][]byte(nil), want...)
	b := make([][]byte, len(got))
	for i, r := range got {
		var e error
		b[i], e = PackMigrationRow(t, MigrationRow(r))
		if e != nil {
			return e
		}
	}
	sort.Slice(a, func(i, j int) bool { return bytes.Compare(a[i], a[j]) < 0 })
	sort.Slice(b, func(i, j int) bool { return bytes.Compare(b[i], b[j]) < 0 })
	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			return contracts.ErrConflict
		}
	}
	return nil
}

// Bound each scattered primary-key read separately from the transaction size.
// Cold historical pages can exceed the socket timeout in one 500-key query.
// Complete business-key groups are read exactly once, including occurrences.
func readMigrationBatch(ctx context.Context, q executor, qualified string, t Table, rows []MigrationRow, lock bool) ([]record, error) {
	unique := []MigrationRow{}
	seen := map[string]bool{}
	for _, r := range rows {
		key, e := MigrationKey(t, r)
		if e != nil {
			return nil, e
		}
		if !seen[string(key)] {
			seen[string(key)] = true
			unique = append(unique, r)
		}
	}
	out := []record{}
	for start := 0; start < len(unique); start += 64 {
		predicate, args, e := batchPredicate(t, unique[start:min(start+64, len(unique))])
		if e != nil {
			return nil, e
		}
		where := "FORCE INDEX (PRIMARY) WHERE " + predicate
		if lock {
			where += " FOR UPDATE"
		}
		page, e := readRecords(ctx, q, qualified, t, where, args...)
		if e != nil {
			return nil, e
		}
		out = append(out, page...)
	}
	return out, nil
}
func insertMigrationBatch(ctx context.Context, tx *sql.Tx, qualified string, t Table, rows []MigrationRow, ranks []uint64) error {
	cols := []string{}
	for _, c := range t.Columns {
		cols = append(cols, "`"+c.Name+"`")
	}
	if t.HashedKey {
		cols = append(cols, "`_key_hash`")
	}
	if t.Multiplicity {
		cols = append(cols, "`_occurrence`")
	}
	if t.Name == "instrument" {
		cols = append(cols, "`_identity_hash`")
	}
	if len(cols)*len(rows) > 60000 {
		return contracts.ErrCapacity
	}
	groups := []string{}
	args := []any{}
	for i, r := range rows {
		for _, c := range t.Columns {
			v, e := codec.SQLValue(c.Type, r[c.Name])
			if e != nil {
				return e
			}
			args = append(args, v)
		}
		if t.HashedKey {
			key, e := keyHash(t, record(r))
			if e != nil {
				return e
			}
			args = append(args, key[:])
		}
		if t.Multiplicity {
			args = append(args, ranks[i])
		}
		if t.Name == "instrument" {
			hash, e := instrumentIdentityHash(record(r))
			if e != nil {
				return e
			}
			args = append(args, hash[:])
		}
		groups = append(groups, "("+placeholders(len(cols))+")")
	}
	_, e := tx.ExecContext(ctx, "INSERT INTO "+qualified+" ("+strings.Join(cols, ",")+") VALUES "+strings.Join(groups, ","), args...)
	return e
}
func tryImportMigrationBatch(ctx context.Context, db *sql.DB, target string, t Table, run uuid.UUID, chunk [32]byte, start, end []byte, expected uint64, hash [32]byte, rows []MigrationRow) (MigrationResult, bool, error) {
	out := MigrationResult{ChunkID: hex.EncodeToString(chunk[:]), SourceHash: hex.EncodeToString(hash[:])}
	rows = append([]MigrationRow(nil), rows...)
	raw, ranks, digest, e := canonicalBatch(t, rows)
	if e != nil {
		if errors.Is(e, contracts.ErrConflict) {
			// The original path records both conflicting physical row hashes.
			return out, false, nil
		}
		return out, true, e
	}
	if digest != hash {
		return out, true, fmt.Errorf("frozen source batch hash changed")
	}
	size := 0
	for _, v := range raw {
		size += len(v)
	}
	if size > 8<<20 {
		return out, false, nil
	}
	tx, e := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if e != nil {
		return out, true, e
	}
	defer tx.Rollback()
	var upper []byte
	var tableState uint8
	if e = tx.QueryRowContext(ctx, "SELECT upper_key,state FROM cmi_storage.migration_table WHERE run_id=? AND database_name=? AND table_name=? FOR UPDATE", run[:], target, t.Name).Scan(&upper, &tableState); e != nil {
		return out, true, e
	}
	cmp, e := CompareMigrationKeys(t, end, upper)
	if e != nil || cmp > 0 {
		return out, true, fmt.Errorf("batch exceeds frozen table range")
	}
	var stored []byte
	var count uint64
	var state uint8
	e = tx.QueryRowContext(ctx, "SELECT source_hash,source_count,state FROM cmi_storage.migration_chunk WHERE chunk_id=? FOR UPDATE", chunk[:]).Scan(&stored, &count, &state)
	if e == nil {
		if count != expected || !bytes.Equal(stored, hash[:]) {
			return out, true, contracts.ErrConflict
		}
		if state == 2 {
			out.Rows = count
			out.Verified = true
			return out, true, tx.Commit()
		}
	} else if e != sql.ErrNoRows {
		return out, true, e
	}
	qualified := "`" + target + "`.`" + t.Name + "`"
	old, e := readMigrationBatch(ctx, tx, qualified, t, rows, true)
	if e != nil {
		return out, true, e
	}
	if len(old) > 0 {
		return out, false, nil
	}
	if e = insertMigrationBatch(ctx, tx, qualified, t, rows, ranks); e != nil {
		return out, true, e
	}
	actual, e := readMigrationBatch(ctx, tx, qualified, t, rows, false)
	if e != nil {
		return out, true, e
	}
	if e = sameBatchFields(t, raw, actual); e != nil {
		return out, true, e
	}
	if e = resolvePreservedSourceMultiplicity(ctx, tx, t, run, chunk, raw); e != nil {
		return out, true, e
	}
	mapHash := MappingDigest(t)
	_, e = tx.ExecContext(ctx, "INSERT INTO cmi_storage.migration_chunk(chunk_id,run_id,database_name,table_name,key_start,key_end,mapping_hash,source_count,source_hash,target_hash,state,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,2,UTC_TIMESTAMP(6)) ON DUPLICATE KEY UPDATE target_hash=VALUES(target_hash),state=2,last_error=NULL,updated_at=UTC_TIMESTAMP(6)", chunk[:], run[:], target, t.Name, append([]byte{}, start...), append([]byte{}, end...), mapHash[:], expected, hash[:], hash[:])
	if e != nil {
		return out, true, e
	}
	if _, e = tx.ExecContext(ctx, "UPDATE cmi_storage.migration_table SET last_key=?,updated_at=UTC_TIMESTAMP(6) WHERE run_id=? AND database_name=? AND table_name=?", end, run[:], target, t.Name); e != nil {
		return out, true, e
	}
	out.Rows = expected
	out.Inserted = expected
	out.Verified = true
	out.TargetHash = hex.EncodeToString(hash[:])
	return out, true, tx.Commit()
}
func tryVerifyMigrationBatch(ctx context.Context, db *sql.DB, target string, t Table, run uuid.UUID, chunk [32]byte, expected uint64, hash [32]byte, rows []MigrationRow) (bool, error) {
	rows = append([]MigrationRow(nil), rows...)
	raw, _, digest, e := canonicalBatch(t, rows)
	if e != nil {
		if errors.Is(e, contracts.ErrConflict) {
			return false, nil
		}
		return true, e
	}
	if digest != hash {
		return true, fmt.Errorf("source batch changed during independent verify")
	}
	size := 0
	for _, v := range raw {
		size += len(v)
	}
	if size > 8<<20 {
		return false, nil
	}
	tx, e := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if e != nil {
		return true, e
	}
	defer tx.Rollback()
	var recorded []byte
	var n uint64
	if e = tx.QueryRowContext(ctx, "SELECT source_hash,source_count FROM cmi_storage.migration_chunk WHERE chunk_id=? AND run_id=?", chunk[:], run[:]).Scan(&recorded, &n); e != nil {
		return true, e
	}
	if n != expected || !bytes.Equal(recorded, hash[:]) {
		return true, contracts.ErrConflict
	}
	actual, e := readMigrationBatch(ctx, tx, "`"+target+"`.`"+t.Name+"`", t, rows, false)
	if e != nil {
		return true, e
	}
	if e = sameBatchFields(t, raw, actual); e != nil {
		return false, nil
	}
	if _, e = tx.ExecContext(ctx, "UPDATE cmi_storage.migration_chunk SET state=2,target_hash=?,updated_at=UTC_TIMESTAMP(6) WHERE chunk_id=?", hash[:], chunk[:]); e != nil {
		return true, e
	}
	return true, tx.Commit()
}
