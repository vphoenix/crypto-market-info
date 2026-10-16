// Package migrate reads only explicitly mapped source tables and freezes bounded
// typed chunks. No production migration is started by a collector constructor.
package migrate

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"

	"fmt"
	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/vphoenix/crypto-market-info/internal/storage/codec"
	"github.com/vphoenix/crypto-market-info/internal/storage/mysql"
	"io"
	"math/big"
	"os"
	"reflect"
	"sort"
	"strings"
	"time"
)

type SourceConfig struct{ Address, User, PasswordFile, SnapshotFile string }
type Source struct {
	conn     driver.Conn
	snapshot map[string]SnapshotTable
}

func OpenSource(ctx context.Context, c SourceConfig) (*Source, error) {
	var snapshot map[string]SnapshotTable
	if c.SnapshotFile != "" {
		var e error
		snapshot, _, e = loadSnapshot(c.SnapshotFile)
		if e != nil {
			return nil, e
		}
	}
	if c.Address == "" {
		return nil, fmt.Errorf("explicit source address required")
	}
	password := ""
	if c.PasswordFile != "" {
		s, e := os.Stat(c.PasswordFile)
		if e != nil {
			return nil, e
		}
		if s.Mode().Perm()&0077 != 0 {
			return nil, fmt.Errorf("source password file must be private")
		}
		raw, e := os.ReadFile(c.PasswordFile)
		if e != nil {
			return nil, e
		}
		password = string(raw)
	}
	if c.User == "" {
		c.User = "default"
	}
	conn, e := ch.Open(&ch.Options{Addr: []string{c.Address}, Auth: ch.Auth{Database: "default", Username: c.User, Password: password}, DialTimeout: 5 * time.Second, ReadTimeout: 40 * time.Second, MaxOpenConns: 1, MaxIdleConns: 1, Settings: ch.Settings{"readonly": 2, "max_threads": 1, "max_execution_time": 30, "max_memory_usage": 128 << 20, "max_read_buffer_size": 256 << 10, "max_block_size": 500, "max_result_bytes": 64 << 20, "result_overflow_mode": "throw"}})
	if e != nil {
		return nil, e
	}
	if e = conn.Ping(ctx); e != nil {
		conn.Close()
		return nil, e
	}
	return &Source{conn: conn, snapshot: snapshot}, nil
}
func (s *Source) Close() error { return s.conn.Close() }
func (s *Source) CheckMapping(ctx context.Context, t mysql.Table) error {
	var ddl string
	if e := s.conn.QueryRow(ctx, "SELECT create_table_query FROM system.tables WHERE database=? AND name=?", t.Database, t.Name).Scan(&ddl); e != nil {
		return e
	}
	h := sha256.Sum256([]byte(ddl))
	if hex.EncodeToString(h[:]) != t.SourceDDLHash {
		return fmt.Errorf("source DDL changed: %s.%s", t.Database, t.Name)
	}
	return s.checkSnapshot(ctx, t)
}
func qualified(t mysql.Table) string { return "`" + t.Database + "`.`" + t.Name + "`" }
func sourceColumns(t mysql.Table) string {
	cols := []string{}
	for _, col := range t.Columns {
		cols = append(cols, "`"+col.Name+"`")
	}
	return strings.Join(cols, ",")
}
func keySQL(t mysql.Table) string {
	cols := []string{}
	for _, k := range t.Key {
		cols = append(cols, "`"+k+"`")
	}
	return "tuple(" + strings.Join(cols, ",") + ")"
}

// Explicit key columns match MergeTree's sorting prefix and let LIMIT stream
// rows in order. ORDER BY tuple(...) has the same ordering but can force a
// full sort of large array columns before the bounded limit is applied.
func keyOrderSQL(t mysql.Table, descending bool) string {
	cols := make([]string, len(t.Key))
	for i, k := range t.Key {
		cols[i] = "`" + k + "`"
		if descending {
			cols[i] += " DESC"
		}
	}
	return strings.Join(cols, ",")
}

func keyProjection(t mysql.Table) mysql.Table {
	keys := t
	keys.Columns = nil
	for _, key := range t.Key {
		for _, col := range t.Columns {
			if col.Name == key {
				keys.Columns = append(keys.Columns, col)
				break
			}
		}
	}
	return keys
}

// sourceKeyTuple binds a decoded checkpoint using its original column types.
// The native driver's default positional binding truncates time.Time to seconds
// and does not format big.Int values as SQL integers. Keep checkpoint decoding
// independent of SQL and convert only at this source-query boundary.
func sourceKeyTuple(t mysql.Table, key []byte) (string, []any, error) {
	columns := make(map[string]*codec.Type, len(t.Columns))
	for _, col := range t.Columns {
		columns[col.Name] = col.Type
	}
	if len(t.Key) == 0 {
		return "", nil, fmt.Errorf("source cursor requires mapped key columns")
	}
	for _, name := range t.Key {
		if columns[name] == nil {
			return "", nil, fmt.Errorf("source key column %s lacks a type", name)
		}
	}
	values, err := mysql.MigrationKeyValues(t, key)
	if err != nil {
		return "", nil, err
	}
	if len(values) != len(t.Key) {
		return "", nil, fmt.Errorf("source cursor width mismatch")
	}
	expressions := make([]string, len(values))
	for i, name := range t.Key {
		typ := columns[name]
		expressions[i] = "?"
		switch typ.Name {
		case "UInt256":
			n, ok := values[i].(big.Int)
			if !ok {
				return "", nil, fmt.Errorf("source UInt256 cursor type mismatch")
			}
			expressions[i], values[i] = "toUInt256(?)", n.String()
		case "DateTime", "DateTime64":
			at, ok := values[i].(time.Time)
			if !ok {
				return "", nil, fmt.Errorf("source time cursor type mismatch")
			}
			layout := "2006-01-02 15:04:05"
			if typ.Name == "DateTime" {
				expressions[i] = "toDateTime(?, 'UTC')"
			} else {
				if typ.Scale != 0 && typ.Scale != 3 && typ.Scale != 6 {
					return "", nil, fmt.Errorf("unsupported source cursor precision")
				}
				if typ.Scale != 0 {
					layout += "." + strings.Repeat("0", typ.Scale)
				}
				expressions[i] = fmt.Sprintf("toDateTime64(?, %d, 'UTC')", typ.Scale)
			}
			values[i] = at.UTC().Format(layout)
		}
	}
	return "tuple(" + strings.Join(expressions, ",") + ")", values, nil
}

func sourceBounds(t mysql.Table, start, upper []byte) (string, []any, error) {
	if len(upper) == 0 {
		return "", nil, fmt.Errorf("explicit source upper key required")
	}
	where := []string{}
	args := []any{}
	if len(start) > 0 {
		tuple, values, err := sourceKeyTuple(t, start)
		if err != nil {
			return "", nil, err
		}
		where = append(where, keySQL(t)+">"+tuple)
		args = append(args, values...)
	}
	tuple, values, err := sourceKeyTuple(t, upper)
	if err != nil {
		return "", nil, err
	}
	where = append(where, keySQL(t)+"<="+tuple)
	args = append(args, values...)
	return strings.Join(where, " AND "), args, nil
}

func final(t mysql.Table) string {
	if t.Engine == "ReplacingMergeTree" {
		return " FINAL"
	}
	return ""
}
func readSourceRow(rows driver.Rows, t mysql.Table) (mysql.MigrationRow, error) {
	types := rows.ColumnTypes()
	if len(types) != len(t.Columns) {
		return nil, fmt.Errorf("source physical width mismatch")
	}
	dest := make([]any, len(types))
	for n, typ := range types {
		dest[n] = reflect.New(typ.ScanType()).Interface()
	}
	if e := rows.Scan(dest...); e != nil {
		return nil, e
	}
	out := mysql.MigrationRow{}
	for n, col := range t.Columns {
		v := reflect.ValueOf(dest[n]).Elem().Interface()
		raw, e := codec.Encode(col.Type, v)
		if e != nil {
			return nil, fmt.Errorf("source %s: %w", col.Name, e)
		}
		value, e := codec.Decode(col.Type, raw)
		if e != nil {
			return nil, e
		}
		out[col.Name] = value
	}
	return out, nil
}
func (s *Source) UpperKey(ctx context.Context, t mysql.Table) ([]byte, error) {
	if e := s.CheckMapping(ctx, t); e != nil {
		return nil, e
	}
	name, e := s.tableSQL(t)
	if e != nil {
		return nil, e
	}
	keys := keyProjection(t)
	// As in chunkUpper, replacement changes values but never removes a key in
	// the reviewed mappings. The physical maximum is therefore the logical
	// maximum. FINAL would disable reverse index reading and sort the full
	// history merely to select this single boundary; full rows still use FINAL.
	rows, e := s.conn.Query(ctx, "SELECT "+sourceColumns(keys)+" FROM "+name+" ORDER BY "+keyOrderSQL(t, true)+" LIMIT 1 SETTINGS max_block_size=500,max_bytes_before_external_sort=33554432")
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, rows.Err()
	}
	row, e := readSourceRow(rows, keys)
	if e != nil {
		return nil, e
	}
	return mysql.MigrationKey(t, row)
}

type FrozenChunk struct {
	File        string
	Table       mysql.Table
	Start, End  []byte
	Rows, Bytes uint64
	Hash        [32]byte
}

func (c *FrozenChunk) Close() error { return os.Remove(c.File) }

// Choose the complete key range using only physical key columns. The reviewed
// ReplacingMergeTree mappings deduplicate versions without deleting keys, so
// FINAL is unnecessary here and would prevent the small LIMIT from streaming.
// The full-field query still applies FINAL to every version of the selected keys.
func (s *Source) chunkUpper(ctx context.Context, t mysql.Table, name, where string, args []any, limit int) ([]byte, error) {
	keys := keyProjection(t)
	rows, e := s.conn.Query(ctx, "SELECT "+sourceColumns(keys)+" FROM "+name+" WHERE "+where+" ORDER BY "+keyOrderSQL(t, false)+" LIMIT "+fmt.Sprint(limit)+" WITH TIES SETTINGS max_block_size="+fmt.Sprint(limit), args...)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	var end []byte
	var count uint64
	for rows.Next() {
		row, e := readSourceRow(rows, keys)
		if e != nil {
			return nil, e
		}
		end, e = mysql.MigrationKey(t, row)
		if e != nil {
			return nil, e
		}
		count++
		if count > 100000 {
			return nil, fmt.Errorf("source key group exceeds bounded row capacity")
		}
	}
	return end, rows.Err()
}

// Freeze bounds disk and memory, keeps complete business-key groups together,
// and uses a private typed file. A source failure never advances the cursor.
func (s *Source) Freeze(ctx context.Context, t mysql.Table, start, upper []byte, rowsLimit int, directory string) (*FrozenChunk, error) {
	return s.freeze(ctx, t, start, upper, rowsLimit, directory, true)
}

// FreezeRange independently rereads an already recorded complete key range.
// Its block size bounds memory, not the number of keys: physical replacement
// versions must not redefine an older checkpoint's logical row boundary.
func (s *Source) FreezeRange(ctx context.Context, t mysql.Table, start, end []byte, blockSize int, directory string) (*FrozenChunk, error) {
	return s.freeze(ctx, t, start, end, blockSize, directory, false)
}

func (s *Source) freeze(ctx context.Context, t mysql.Table, start, upper []byte, rowsLimit int, directory string, selectEnd bool) (*FrozenChunk, error) {
	if rowsLimit < 1 || rowsLimit > 500 || len(upper) == 0 {
		return nil, fmt.Errorf("bounded rows and explicit upper key required")
	}
	if e := s.CheckMapping(ctx, t); e != nil {
		return nil, e
	}
	where, args, e := sourceBounds(t, start, upper)
	if e != nil {
		return nil, e
	}
	name, e := s.tableSQL(t)
	if e != nil {
		return nil, e
	}
	if selectEnd {
		selectedEnd, e := s.chunkUpper(ctx, t, name, where, args, rowsLimit)
		if e != nil {
			return nil, e
		}
		if len(selectedEnd) > 0 {
			where, args, e = sourceBounds(t, start, selectedEnd)
			if e != nil {
				return nil, e
			}
		} else {
			where = "0"
			args = nil
		}
	}
	query := "SELECT " + sourceColumns(t) + " FROM " + name + final(t) + " PREWHERE " + where + " ORDER BY " + keyOrderSQL(t, false) + " SETTINGS max_block_size=" + fmt.Sprint(rowsLimit)
	rows, e := s.conn.Query(ctx, query, args...)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	file, e := os.CreateTemp(directory, "cmi-migration-*.typed")
	if e != nil {
		return nil, e
	}
	chunk := &FrozenChunk{File: file.Name(), Table: t, Start: bytes.Clone(start)}
	success := false
	defer func() {
		file.Close()
		if !success {
			os.Remove(file.Name())
		}
	}()
	// Ordinary MergeTree rows are sorted canonically within a key group before
	// hashing, preserving identical repetitions rather than applying DISTINCT.
	group := [][]byte{}
	groupBytes := uint64(0)
	var groupKey []byte
	digest := sha256.New()
	flush := func() error {
		sort.SliceStable(group, func(i, j int) bool { return bytes.Compare(group[i], group[j]) < 0 })
		for _, raw := range group {
			var n [4]byte
			binary.BigEndian.PutUint32(n[:], uint32(len(raw)))
			if _, e := file.Write(n[:]); e != nil {
				return e
			}
			if _, e := file.Write(raw); e != nil {
				return e
			}
			digest.Write(n[:])
			digest.Write(raw)
		}
		group = nil
		groupBytes = 0
		return nil
	}
	for rows.Next() {
		if e = ctx.Err(); e != nil {
			return nil, e
		}
		row, e := readSourceRow(rows, t)
		if e != nil {
			return nil, e
		}
		key, e := mysql.MigrationKey(t, row)
		if e != nil {
			return nil, e
		}
		if groupKey != nil && !bytes.Equal(key, groupKey) {
			if e = flush(); e != nil {
				return nil, e
			}
			cmp, e := mysql.CompareMigrationKeys(t, groupKey, key)
			if e != nil || cmp >= 0 {
				return nil, fmt.Errorf("unstable source ordering")
			}
		}
		raw, e := mysql.PackMigrationRow(t, row)
		if e != nil {
			return nil, e
		}
		chunk.Rows++
		chunk.Bytes += uint64(len(raw) + 4)
		groupBytes += uint64(len(raw) + 4)
		if chunk.Bytes > 64<<20 || groupBytes > 64<<20 || chunk.Rows > 100000 {
			return nil, fmt.Errorf("source key group exceeds bounded 64 MiB staging capacity")
		}
		group = append(group, raw)
		groupKey = key
		chunk.End = bytes.Clone(key)
	}
	if e = rows.Err(); e != nil {
		return nil, e
	}
	if e = flush(); e != nil {
		return nil, e
	}
	if e = file.Sync(); e != nil {
		return nil, e
	}
	copy(chunk.Hash[:], digest.Sum(nil))
	success = true
	return chunk, nil
}
func (c *FrozenChunk) Reader() (func() (mysql.MigrationRow, error), func() error, error) {
	f, e := os.Open(c.File)
	if e != nil {
		return nil, nil, e
	}
	next := func() (mysql.MigrationRow, error) {
		var n uint32
		e := binary.Read(f, binary.BigEndian, &n)
		if e != nil {
			return nil, e
		}
		if n > 32<<20 {
			return nil, fmt.Errorf("invalid staged row length")
		}
		raw := make([]byte, n)
		if _, e = io.ReadFull(f, raw); e != nil {
			return nil, e
		}
		return mysql.UnpackMigrationRow(c.Table, raw)
	}
	return next, f.Close, nil
}
