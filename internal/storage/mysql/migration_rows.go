package mysql

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/storage/codec"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
	"io"
	"math/big"
	"reflect"
	"sort"
	"strings"
	"time"
)

// MigrationRow is restricted by the reviewed physical table descriptor. It is
// never a generic JSON fact and never accepts unmapped columns or float values.
type MigrationRow map[string]any

func PackMigrationRow(t Table, row MigrationRow) ([]byte, error) {
	var b bytes.Buffer
	for _, col := range t.Columns {
		v, ok := row[col.Name]
		if !ok {
			return nil, fmt.Errorf("missing physical source column")
		}
		raw, e := codec.Encode(col.Type, v)
		if e != nil {
			return nil, fmt.Errorf("%s: %w", col.Name, e)
		}
		if b.Len()+len(raw)+4 > 32<<20 {
			return nil, fmt.Errorf("migration row exceeds 32 MiB")
		}
		binary.Write(&b, binary.BigEndian, uint32(len(raw)))
		b.Write(raw)
	}
	return b.Bytes(), nil
}
func UnpackMigrationRow(t Table, raw []byte) (MigrationRow, error) {
	r := bytes.NewReader(raw)
	out := MigrationRow{}
	for _, col := range t.Columns {
		var n uint32
		if e := binary.Read(r, binary.BigEndian, &n); e != nil {
			return nil, e
		}
		if n > codec.MaxBytes || uint64(n) > uint64(r.Len()) {
			return nil, fmt.Errorf("migration field length")
		}
		v := make([]byte, n)
		if _, e := io.ReadFull(r, v); e != nil {
			return nil, e
		}
		value, e := codec.Decode(col.Type, v)
		if e != nil {
			return nil, e
		}
		out[col.Name] = value
	}
	if r.Len() != 0 {
		return nil, fmt.Errorf("trailing migration row")
	}
	return out, nil
}
func MigrationKey(t Table, row MigrationRow) ([]byte, error) {
	var b bytes.Buffer
	for _, name := range t.Key {
		found := false
		for _, col := range t.Columns {
			if col.Name == name {
				raw, e := codec.Encode(col.Type, row[name])
				if e != nil {
					return nil, e
				}
				binary.Write(&b, binary.BigEndian, uint32(len(raw)))
				b.Write(raw)
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("key outside mapping")
		}
	}
	return b.Bytes(), nil
}
func MigrationKeyValues(t Table, raw []byte) ([]any, error) {
	r := bytes.NewReader(raw)
	out := []any{}
	for _, name := range t.Key {
		for _, col := range t.Columns {
			if col.Name == name {
				var n uint32
				if e := binary.Read(r, binary.BigEndian, &n); e != nil {
					return nil, e
				}
				if n > codec.MaxBytes || int64(n) > int64(r.Len()) {
					return nil, fmt.Errorf("invalid migration key")
				}
				raw := make([]byte, n)
				io.ReadFull(r, raw)
				v, e := codec.Decode(col.Type, raw)
				if e != nil {
					return nil, e
				}
				out = append(out, v)
				break
			}
		}
	}
	if r.Len() != 0 {
		return nil, fmt.Errorf("trailing migration key")
	}
	return out, nil
}
func CompareMigrationKeys(t Table, a, b []byte) (int, error) {
	x, e := MigrationKeyValues(t, a)
	if e != nil {
		return 0, e
	}
	y, e := MigrationKeyValues(t, b)
	if e != nil {
		return 0, e
	}
	for i := range x {
		n, e := compareMigrationValue(x[i], y[i])
		if e != nil {
			return 0, e
		}
		if n != 0 {
			return n, nil
		}
	}
	return 0, nil
}
func compareMigrationValue(a, b any) (int, error) {
	if reflect.TypeOf(a) != reflect.TypeOf(b) {
		return 0, fmt.Errorf("migration version/key type mismatch")
	}
	switch x := a.(type) {
	case time.Time:
		y := b.(time.Time)
		if x.Before(y) {
			return -1, nil
		}
		if x.After(y) {
			return 1, nil
		}
		return 0, nil
	case bool:
		y := b.(bool)
		if x == y {
			return 0, nil
		}
		if !x {
			return -1, nil
		}
		return 1, nil
	case string:
		return strings.Compare(x, b.(string)), nil
	case uuid.UUID:
		y := b.(uuid.UUID)
		// ClickHouse compares UUIDs as UInt128: the trailing 64 bits first.
		if n := bytes.Compare(x[8:], y[8:]); n != 0 {
			return n, nil
		}
		return bytes.Compare(x[:8], y[:8]), nil
	case big.Int:
		y := b.(big.Int)
		return x.Cmp(&y), nil
	case *big.Int:
		return x.Cmp(b.(*big.Int)), nil
	}
	x, y := reflect.ValueOf(a), reflect.ValueOf(b)
	switch x.Kind() {
	case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if x.Uint() < y.Uint() {
			return -1, nil
		}
		if x.Uint() > y.Uint() {
			return 1, nil
		}
		return 0, nil
	case reflect.Int32, reflect.Int64:
		if x.Int() < y.Int() {
			return -1, nil
		}
		if x.Int() > y.Int() {
			return 1, nil
		}
		return 0, nil
	}
	return 0, fmt.Errorf("unsupported migration comparison")
}
func MappingDigest(t Table) [32]byte { return sha256.Sum256([]byte(t.DDL)) }

type MigrationResult struct {
	ChunkID                               string `json:"chunk_id"`
	Rows, Inserted, Unchanged, Superseded uint64
	SourceHash, TargetHash                string
	Verified                              bool
}

// ImportMigrationChunk commits rows and its durable checkpoint together. The
// source group may spill to disk; next is invoked one typed row at a time.
func ImportMigrationChunk(ctx context.Context, db *sql.DB, target string, t Table, run uuid.UUID, chunk [32]byte, start, end []byte, expectedRows uint64, expectedHash [32]byte, next func() (MigrationRow, error)) (MigrationResult, error) {
	out := MigrationResult{ChunkID: hex.EncodeToString(chunk[:]), SourceHash: hex.EncodeToString(expectedHash[:])}
	mapped, validationError := Mapping(t.Database, t.Name)
	if validationError != nil || MappingDigest(mapped) != MappingDigest(t) {
		return out, fmt.Errorf("migration table outside reviewed mapping")
	}
	if !identifier.MatchString(target) || run == uuid.Nil || next == nil {
		return out, fmt.Errorf("invalid migration chunk")
	}
	if expectedRows > 0 && expectedRows <= 500 {
		rows, e := collectMigrationBatch(expectedRows, next)
		if e != nil {
			return out, e
		}
		result, handled, e := tryImportMigrationBatch(ctx, db, target, t, run, chunk, start, end, expectedRows, expectedHash, rows)
		if handled {
			return result, e
		}
		next = batchReader(rows)
	}
	tx, e := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if e != nil {
		return out, e
	}
	defer tx.Rollback()
	qualified := "`" + target + "`.`" + t.Name + "`"
	var frozenUpper []byte
	var tableState uint8
	if e = tx.QueryRowContext(ctx, "SELECT upper_key,state FROM cmi_storage.migration_table WHERE run_id=? AND database_name=? AND table_name=? FOR UPDATE", run[:], target, t.Name).Scan(&frozenUpper, &tableState); e != nil {
		return out, fmt.Errorf("explicit frozen table plan required: %w", e)
	}
	if len(end) > 0 {
		cmp, e := CompareMigrationKeys(t, end, frozenUpper)
		if e != nil || cmp > 0 {
			return out, fmt.Errorf("chunk exceeds frozen upper key")
		}
	}
	mappingHash := MappingDigest(t)
	var storedSource []byte
	var storedCount uint64
	var state uint8
	e = tx.QueryRowContext(ctx, "SELECT source_hash,source_count,state FROM cmi_storage.migration_chunk WHERE chunk_id=? FOR UPDATE", chunk[:]).Scan(&storedSource, &storedCount, &state)
	if e == nil {
		if storedCount != expectedRows || !bytes.Equal(storedSource, expectedHash[:]) {
			return out, contracts.ErrConflict
		}
		if state == 2 {
			out.Rows = storedCount
			out.Verified = true
			return out, tx.Commit()
		}
	} else if e != sql.ErrNoRows {
		return out, e
	}
	sourceDigest, targetDigest := sha256.New(), sha256.New()
	var previousKey []byte
	var groupBytes uint64
	group := []MigrationRow{}
	groupHashes := [][]byte{}
	flush := func() error {
		if len(group) == 0 {
			return nil
		}
		sort.SliceStable(groupHashes, func(i, j int) bool { return bytes.Compare(groupHashes[i], groupHashes[j]) < 0 }) // Canonical bytes, including identical rows, define stable occurrence ordering.

		if t.Multiplicity {
			where, args, e := rowPredicate(t, record(group[0]))
			if e != nil {
				return e
			}
			where = strings.Replace(where, " AND `_occurrence`=0", "", 1)
			var count uint64
			if e = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+qualified+" WHERE "+where, args...).Scan(&count); e != nil {
				return e
			}
			if count > uint64(len(group)) {
				return fmt.Errorf("target multiplicity exceeds frozen source: %w", contracts.ErrConflict)
			}
			if len(group) > 1 && count > 0 && count != uint64(len(group)) {
				old, e := readRecords(ctx, tx, qualified, t, "WHERE "+where+" LIMIT 1", args...)
				if e != nil {
					return e
				}
				if len(old) != 1 {
					return contracts.ErrConflict
				}
				return recordMigrationConflict(ctx, tx, chunk, t, group[0], old[0], "ambiguous_source_overlaps_existing_target")
			}
		}
		ordered := make([]MigrationRow, len(groupHashes))
		for n, raw := range groupHashes {
			r, e := UnpackMigrationRow(t, raw)
			if e != nil {
				return e
			}
			ordered[n] = r
		}
		group = ordered
		for n, row := range group {
			raw, e := PackMigrationRow(t, row)
			if e != nil {
				return e
			}
			writeFramed(sourceDigest, raw)
			out.Rows++
			where, args, e := rowPredicate(t, record(row))
			if e != nil {
				return e
			}
			if t.Multiplicity {
				where = strings.Replace(where, "`_occurrence`=0", "`_occurrence`=?", 1)
				args = append([]any{args[0], uint64(n)}, args[1:]...)
			}
			old, e := readRecords(ctx, tx, qualified, t, "WHERE "+where+" FOR UPDATE", args...)
			if e != nil {
				return e
			}
			if len(old) > 1 {
				return contracts.ErrConflict
			}
			if len(old) == 1 {
				want, e := rowHash(t, record(row))
				if e != nil {
					return e
				}
				got, e := rowHash(t, old[0])
				if e != nil {
					return e
				}
				if want == got {
					out.Unchanged++
					writeFramed(targetDigest, raw)
					continue
				}
				if t.VersionColumn == "" {
					return recordMigrationConflict(ctx, tx, chunk, t, row, old[0], "immutable_same_key")
				}
				comparison, e := compareMigrationValue(row[t.VersionColumn], old[0][t.VersionColumn])
				if e != nil {
					return e
				}
				if comparison == 0 {
					return recordMigrationConflict(ctx, tx, chunk, t, row, old[0], "same_version_changed_content")
				}
				if comparison < 0 {
					if e := saveMigrationResolution(ctx, tx, chunk, t, row, old[0], "newer_target_version"); e != nil {
						return e
					}
					out.Superseded++
					targetRaw, e := PackMigrationRow(t, MigrationRow(old[0]))
					if e != nil {
						return e
					}
					writeFramed(targetDigest, targetRaw)
					continue
				}
				if old[0]["finality"] == "finalized" && (row["finality"] != "finalized" || row["canonical"] != old[0]["canonical"]) {
					return recordMigrationConflict(ctx, tx, chunk, t, row, old[0], "finalized_revision_downgrade")
				}
				if e := migrationRevisionCompatible(t, record(row), old[0]); e != nil {
					return recordMigrationConflict(ctx, tx, chunk, t, row, old[0], "revision_changed_frozen_identity")
				}
				if e := saveMigrationResolution(ctx, tx, chunk, t, row, old[0], "newer_source_version"); e != nil {
					return e
				}
				if _, e = tx.ExecContext(ctx, "DELETE FROM "+qualified+" WHERE "+where, args...); e != nil {
					return e
				}
			}
			if e = insertMigrationRecord(ctx, tx, qualified, t, row, uint64(n)); e != nil {
				return e
			}
			out.Inserted++
			target, e := readRecords(ctx, tx, qualified, t, "WHERE "+where, args...)
			if e != nil {
				return e
			}
			if len(target) != 1 {
				return fmt.Errorf("migration readback member count")
			}
			targetRaw, e := PackMigrationRow(t, MigrationRow(target[0]))
			if e != nil {
				return e
			}
			if !bytes.Equal(targetRaw, raw) {
				return fmt.Errorf("migration readback fields differ")
			}
			writeFramed(targetDigest, targetRaw)
		}
		if e := resolvePreservedSourceMultiplicity(ctx, tx, t, run, chunk, groupHashes); e != nil {
			return e
		}
		group = nil
		groupHashes = nil
		groupBytes = 0
		return nil
	}
	for {
		row, e := next()
		if e == io.EOF {
			break
		}
		if e != nil {
			return out, e
		}
		key, e := MigrationKey(t, row)
		if e != nil {
			return out, e
		}
		if previousKey != nil && !bytes.Equal(previousKey, key) {
			if e = flush(); e != nil {
				return out, e
			}
			cmp, e := CompareMigrationKeys(t, previousKey, key)
			if e != nil || cmp >= 0 {
				return out, fmt.Errorf("source order/key changed")
			}
		}
		previousKey = key
		raw, e := PackMigrationRow(t, row)
		if e != nil {
			return out, e
		}
		group = append(group, row)
		groupHashes = append(groupHashes, raw)
		groupBytes += uint64(len(raw))
		if groupBytes > 64<<20 || len(group) > 100000 {
			return out, contracts.ErrCapacity
		}
	}
	if e = flush(); e != nil {
		return out, e
	}
	if out.Rows != expectedRows || !bytes.Equal(sourceDigest.Sum(nil), expectedHash[:]) {
		return out, fmt.Errorf("frozen source chunk changed")
	}
	out.TargetHash = hex.EncodeToString(targetDigest.Sum(nil))
	out.Verified = true
	_, e = tx.ExecContext(ctx, "INSERT INTO cmi_storage.migration_chunk(chunk_id,run_id,database_name,table_name,key_start,key_end,mapping_hash,source_count,source_hash,target_hash,state,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,2,UTC_TIMESTAMP(6)) ON DUPLICATE KEY UPDATE target_hash=VALUES(target_hash),state=2,last_error=NULL,updated_at=UTC_TIMESTAMP(6)", chunk[:], run[:], target, t.Name, append([]byte{}, start...), append([]byte{}, end...), mappingHash[:], out.Rows, expectedHash[:], targetDigest.Sum(nil))
	if e != nil {
		return out, e
	}
	if _, e = tx.ExecContext(ctx, "UPDATE cmi_storage.migration_table SET last_key=?,updated_at=UTC_TIMESTAMP(6) WHERE run_id=? AND database_name=? AND table_name=?", end, run[:], target, t.Name); e != nil {
		return out, e
	}
	return out, tx.Commit()
}
func insertMigrationRecord(ctx context.Context, tx *sql.Tx, qualified string, t Table, row MigrationRow, occurrence uint64) error {
	if !t.Multiplicity {
		return insertRecord(ctx, tx, qualified, t, record(row))
	}
	cols, args := []string{}, []any{}
	for _, col := range t.Columns {
		v, e := codec.SQLValue(col.Type, row[col.Name])
		if e != nil {
			return e
		}
		cols = append(cols, "`"+col.Name+"`")
		args = append(args, v)
	}
	key, e := keyHash(t, record(row))
	if e != nil {
		return e
	}
	cols = append(cols, "`_key_hash`", "`_occurrence`")
	args = append(args, key[:], occurrence)
	_, e = tx.ExecContext(ctx, "INSERT INTO "+qualified+" ("+strings.Join(cols, ",")+") VALUES("+placeholders(len(args))+")", args...)
	return e
}

// The caller persists conflicts after rollback so a failed fact transaction
// cannot erase the diagnostic record. Error contains hashes, never row values.
type MigrationConflict struct {
	Run                        uuid.UUID
	Chunk, Key, Source, Target [32]byte
	Reason                     string
}

func (e *MigrationConflict) Error() string { return "migration conflict: " + e.Reason }
func recordMigrationConflict(ctx context.Context, tx *sql.Tx, chunk [32]byte, t Table, source MigrationRow, target record, reason string) error {
	key, e := keyHash(t, record(source))
	if e != nil {
		return e
	}
	s, e := rowHash(t, record(source))
	if e != nil {
		return e
	}
	d, e := rowHash(t, target)
	if e != nil {
		return e
	}
	return &MigrationConflict{Chunk: chunk, Key: key, Source: s, Target: d, Reason: reason}
}
func SaveMigrationConflict(ctx context.Context, db *sql.DB, c *MigrationConflict) error {
	id := sha256.Sum256(append(append(append([]byte{}, c.Chunk[:]...), c.Key[:]...), []byte(c.Reason)...))
	_, e := db.ExecContext(ctx, "INSERT INTO cmi_storage.migration_conflict(conflict_id,run_id,chunk_id,key_hash,source_hash,target_hash,reason) VALUES(?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE reason=VALUES(reason)", id[:], c.Run[:], c.Chunk[:], c.Key[:], c.Source[:], c.Target[:], c.Reason)
	return e
}
func BeginMigration(ctx context.Context, db *sql.DB, run uuid.UUID, inventoryHash [32]byte) error {
	if run == uuid.Nil {
		return fmt.Errorf("explicit run UUID required")
	}
	_, e := db.ExecContext(ctx, "INSERT INTO cmi_storage.migration_run(run_id,inventory_hash,mapping_version,started_at,frozen_at,state) VALUES(?,?,1,UTC_TIMESTAMP(6),UTC_TIMESTAMP(6),1)", run[:], inventoryHash[:])
	return e
}
func MigrationCheckpoint(ctx context.Context, db *sql.DB, target string, t Table, run uuid.UUID) ([]byte, error) {
	var key []byte
	e := db.QueryRowContext(ctx, "SELECT key_end FROM cmi_storage.migration_chunk WHERE run_id=? AND database_name=? AND table_name=? AND state=2 ORDER BY updated_at DESC LIMIT 1", run[:], target, t.Name).Scan(&key)
	if e == sql.ErrNoRows {
		return nil, nil
	}
	return key, e
}

func saveMigrationResolution(ctx context.Context, tx *sql.Tx, chunk [32]byte, t Table, source MigrationRow, target record, reason string) error {
	key, e := keyHash(t, record(source))
	if e != nil {
		return e
	}
	s, e := rowHash(t, record(source))
	if e != nil {
		return e
	}
	d, e := rowHash(t, target)
	if e != nil {
		return e
	}
	var typ *codec.Type
	for _, c := range t.Columns {
		if c.Name == t.VersionColumn {
			typ = c.Type
		}
	}
	if typ == nil {
		return fmt.Errorf("version type missing")
	}
	sv, e := codec.Encode(typ, source[t.VersionColumn])
	if e != nil {
		return e
	}
	tv, e := codec.Encode(typ, target[t.VersionColumn])
	if e != nil {
		return e
	}
	_, e = tx.ExecContext(ctx, "INSERT INTO cmi_storage.migration_resolution(chunk_id,key_hash,source_hash,target_hash,source_version,target_version,reason) VALUES(?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE reason=VALUES(reason)", chunk[:], key[:], s[:], d[:], sv, tv, reason)
	return e
}
func migrationRevisionCompatible(t Table, source, target record) error {
	switch t.Name {
	case "across_capture", "reserve_capture", "lst_capture":
		ignored := map[string]bool{"revision": true, "canonical": true, "finality": true, "reason": true}
		if t.Name == "reserve_capture" {
			ignored["payload_hash"] = true // refreshed finality evidence, as in Reconcile
		}
		for _, col := range t.Columns {
			if ignored[col.Name] {
				continue
			}
			a, e := codec.Encode(col.Type, source[col.Name])
			if e != nil {
				return e
			}
			b, e := codec.Encode(col.Type, target[col.Name])
			if e != nil {
				return e
			}
			if !bytes.Equal(a, b) {
				return contracts.ErrConflict
			}
		}
	case "dex_block":
		for _, name := range []string{"chain_id", "manifest_hash", "block_number", "block_hash", "block_time", "parent_hash", "base_fee_wei", "fee_recipient"} {
			if !reflect.DeepEqual(source[name], target[name]) {
				return contracts.ErrConflict
			}
		}
	case "dex_tx_receipt":
		for _, name := range []string{"receipt_payload_hash", "calldata_payload_hash"} {
			if source[name] != target[name] {
				return contracts.ErrConflict
			}
		}
	case "funding_rate_hourly": // actual supersedes an estimate under the existing priority.
	default:
		return fmt.Errorf("versioned table lacks explicit revision policy")
	}
	return nil
}
func PlanMigrationTable(ctx context.Context, db *sql.DB, t Table, target string, run uuid.UUID, upper []byte) error {
	if !identifier.MatchString(target) || run == uuid.Nil {
		return fmt.Errorf("invalid migration table plan")
	}
	h := MappingDigest(t)
	tx, e := db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	var savedHash, savedUpper []byte
	e = tx.QueryRowContext(ctx, "SELECT mapping_hash,upper_key FROM cmi_storage.migration_table WHERE run_id=? AND database_name=? AND table_name=? FOR UPDATE", run[:], target, t.Name).Scan(&savedHash, &savedUpper)
	if e == nil {
		if !bytes.Equal(savedHash, h[:]) || !bytes.Equal(savedUpper, upper) {
			return contracts.ErrConflict
		}
		return tx.Commit()
	}
	if e != sql.ErrNoRows {
		return e
	}
	_, e = tx.ExecContext(ctx, "INSERT INTO cmi_storage.migration_table(run_id,source_database,database_name,table_name,mapping_hash,upper_key,source_empty,state,updated_at) VALUES(?,?,?,?,?,?,?,0,UTC_TIMESTAMP(6))", run[:], t.Database, target, t.Name, h[:], upper, len(upper) == 0)
	if e != nil {
		return e
	}
	return tx.Commit()
}
func LoadMigrationTablePlan(ctx context.Context, db *sql.DB, t Table, target string, run uuid.UUID) ([]byte, []byte, uint8, error) {
	var upper, last, hash []byte
	var state uint8
	e := db.QueryRowContext(ctx, "SELECT upper_key,last_key,state,mapping_hash FROM cmi_storage.migration_table WHERE run_id=? AND database_name=? AND table_name=?", run[:], target, t.Name).Scan(&upper, &last, &state, &hash)
	h := MappingDigest(t)
	if e == nil && !bytes.Equal(h[:], hash) {
		return nil, nil, 0, contracts.ErrConflict
	}
	return upper, last, state, e
}
func RecordMigrationError(ctx context.Context, db *sql.DB, target string, t Table, run uuid.UUID, message string) error {
	if len(message) > 2048 {
		message = message[:2048]
	}
	_, e := db.ExecContext(ctx, "UPDATE cmi_storage.migration_table SET error=?,state=4,updated_at=UTC_TIMESTAMP(6) WHERE run_id=? AND database_name=? AND table_name=?", message, run[:], target, t.Name)
	return e
}
func EnsureMigrationRun(ctx context.Context, db *sql.DB, run uuid.UUID, inventory [32]byte) error {
	var hash []byte
	e := db.QueryRowContext(ctx, "SELECT inventory_hash FROM cmi_storage.migration_run WHERE run_id=?", run[:]).Scan(&hash)
	if e == sql.ErrNoRows {
		return BeginMigration(ctx, db, run, inventory)
	}
	if e != nil {
		return e
	}
	if !bytes.Equal(hash, inventory[:]) {
		return contracts.ErrConflict
	}
	return nil
}
func MarkMigrationTableCopied(ctx context.Context, db *sql.DB, t Table, target string, run uuid.UUID) error {
	_, e := db.ExecContext(ctx, "UPDATE cmi_storage.migration_table SET state=1,error=NULL,updated_at=UTC_TIMESTAMP(6) WHERE run_id=? AND database_name=? AND table_name=?", run[:], target, t.Name)
	return e
}

// VerifyMigrationChunk re-reads the frozen source membership and target fields.
// It permits only an explicitly newer target version, recording that decision.
func VerifyMigrationChunk(ctx context.Context, db *sql.DB, target string, t Table, run uuid.UUID, chunk [32]byte, expected uint64, sourceHash [32]byte, next func() (MigrationRow, error)) error {
	if expected > 0 && expected <= 500 {
		rows, e := collectMigrationBatch(expected, next)
		if e != nil {
			return e
		}
		handled, e := tryVerifyMigrationBatch(ctx, db, target, t, run, chunk, expected, sourceHash, rows)
		if handled {
			return e
		}
		next = batchReader(rows)
	}
	tx, e := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if e != nil {
		return e
	}
	defer tx.Rollback()
	var recorded []byte
	var count uint64
	if e = tx.QueryRowContext(ctx, "SELECT source_hash,source_count FROM cmi_storage.migration_chunk WHERE chunk_id=? AND run_id=? FOR UPDATE", chunk[:], run[:]).Scan(&recorded, &count); e != nil {
		return e
	}
	if count != expected || !bytes.Equal(recorded, sourceHash[:]) {
		return fmt.Errorf("frozen source verification differs")
	}
	digest := sha256.New()
	targetDigest := sha256.New()
	rows := uint64(0)
	var previousKey []byte
	occurrence := uint64(0)
	var groupPredicate string
	var groupArgs []any
	verifyCount := func() error {
		if !t.Multiplicity || groupPredicate == "" {
			return nil
		}
		var count uint64
		e := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM `"+target+"`.`"+t.Name+"` WHERE "+groupPredicate, groupArgs...).Scan(&count)
		if e != nil {
			return e
		}
		if count != occurrence+1 {
			return fmt.Errorf("verified target/source multiplicity differs")
		}
		return nil
	}
	for {
		r, e := next()
		if e == io.EOF {
			break
		}
		if e != nil {
			return e
		}
		key, e := MigrationKey(t, r)
		if e != nil {
			return e
		}
		if bytes.Equal(key, previousKey) {
			occurrence++
		} else {
			if e = verifyCount(); e != nil {
				return e
			}
			occurrence = 0
			previousKey = key
		}
		raw, e := PackMigrationRow(t, r)
		if e != nil {
			return e
		}
		writeFramed(digest, raw)
		rows++
		where, args, e := rowPredicate(t, record(r))
		if e != nil {
			return e
		}
		if t.Multiplicity {
			if occurrence == 0 {
				groupPredicate = strings.Replace(where, " AND `_occurrence`=0", "", 1)
				groupArgs = append([]any{}, args...)
			}
			where = strings.Replace(where, "`_occurrence`=0", "`_occurrence`=?", 1)
			args = append([]any{args[0], occurrence}, args[1:]...)
		}
		targetRows, e := readRecords(ctx, tx, "`"+target+"`.`"+t.Name+"`", t, "WHERE "+where, args...)
		if e != nil {
			return e
		}
		if len(targetRows) != 1 {
			return fmt.Errorf("migration target member missing/ambiguous")
		}
		targetRaw, e := PackMigrationRow(t, MigrationRow(targetRows[0]))
		if e != nil {
			return e
		}
		if !bytes.Equal(raw, targetRaw) {
			if t.VersionColumn == "" {
				return recordMigrationConflict(ctx, tx, chunk, t, r, targetRows[0], "verify_immutable_content_changed")
			}
			comparison, e := compareMigrationValue(targetRows[0][t.VersionColumn], r[t.VersionColumn])
			if e != nil || comparison <= 0 {
				return recordMigrationConflict(ctx, tx, chunk, t, r, targetRows[0], "verify_target_version_not_newer")
			}
			if e = saveMigrationResolution(ctx, tx, chunk, t, r, targetRows[0], "verified_newer_target_version"); e != nil {
				return e
			}
		}
		writeFramed(targetDigest, targetRaw)
	}
	if e = verifyCount(); e != nil {
		return e
	}
	if rows != expected || !bytes.Equal(digest.Sum(nil), sourceHash[:]) {
		return fmt.Errorf("source row count/hash changed during verify")
	}
	_, e = tx.ExecContext(ctx, "UPDATE cmi_storage.migration_chunk SET state=2,target_hash=?,updated_at=UTC_TIMESTAMP(6) WHERE chunk_id=?", targetDigest.Sum(nil), chunk[:])
	if e != nil {
		return e
	}
	return tx.Commit()
}

// MigrationRowsVerified means the raw CEX table has passed field verification,
// but historical publication must still finish before the table is complete.
const MigrationRowsVerified uint8 = 5

// Startup subsets remain at rows-verified state, which finalize cannot accept.
func MarkMigrationBootstrapRowsVerified(ctx context.Context, db *sql.DB, t Table, target string, run uuid.UUID) error {
	result, e := db.ExecContext(ctx, "UPDATE cmi_storage.migration_table SET state=5,error=NULL,updated_at=UTC_TIMESTAMP(6) WHERE run_id=? AND database_name=? AND table_name=? AND state IN (1,5)", run[:], target, t.Name)
	if e != nil {
		return e
	}
	n, e := result.RowsAffected()
	if e == nil && n != 1 {
		return fmt.Errorf("startup rows not ready for verification")
	}
	return e
}

func MarkMigrationTableVerified(ctx context.Context, db *sql.DB, t Table, target string, run uuid.UUID) error {
	state := uint8(2)
	if t.Name == "order_book_minute" || t.Name == "order_book_second_delta" {
		state = MigrationRowsVerified
	}
	result, e := db.ExecContext(ctx, "UPDATE cmi_storage.migration_table SET state=?,error=NULL,updated_at=UTC_TIMESTAMP(6) WHERE run_id=? AND database_name=? AND table_name=? AND state IN (1,2,5)", state, run[:], target, t.Name)
	if e != nil {
		return e
	}
	n, e := result.RowsAffected()
	if e == nil && n != 1 {
		return fmt.Errorf("migration table not ready for verification")
	}
	return e
}

// MigrationPublisher has no producer authority and is restricted to verified
// historical publication methods; ordinary transaction writers reject it.
func MigrationPublisher(db *sql.DB, database, family string) *Client {
	return &Client{db: db, database: database, family: family}
}
