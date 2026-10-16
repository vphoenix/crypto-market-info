package mysql

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"fmt"
	"hash"
	"reflect"
	"strings"

	"github.com/vphoenix/crypto-market-info/internal/storage/codec"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
)

type executor interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}
type record map[string]any

func writeFramed(h hash.Hash, a []byte) {
	var n [4]byte
	binary.BigEndian.PutUint32(n[:], uint32(len(a)))
	h.Write(n[:])
	h.Write(a)
}
func keyHash(t Table, r record) ([32]byte, error) {
	h := sha256.New()
	for _, name := range t.Key {
		for _, c := range t.Columns {
			if c.Name == name {
				v, e := codec.Encode(c.Type, r[name])
				if e != nil {
					return [32]byte{}, e
				}
				writeFramed(h, v)
				break
			}
		}
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out, nil
}
func rowHash(t Table, r record) ([32]byte, error) {
	h := sha256.New()
	for _, c := range t.Columns {
		v, ok := r[c.Name]
		if !ok {
			return [32]byte{}, fmt.Errorf("missing column %s", c.Name)
		}
		a, e := codec.Encode(c.Type, v)
		if e != nil {
			return [32]byte{}, fmt.Errorf("%s: %w", c.Name, e)
		}
		writeFramed(h, a)
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out, nil
}

func (c *Client) mapping(name string) (Table, error) {
	family := c.family
	if family == "" {
		family = c.database
	}
	return Mapping(family, name)
}
func columns(t Table) string {
	out := make([]string, len(t.Columns))
	for i, col := range t.Columns {
		out[i] = "`" + col.Name + "`"
	}
	return strings.Join(out, ",")
}
func readRecords(ctx context.Context, q executor, qualified string, t Table, where string, args ...any) ([]record, error) {
	rows, e := q.QueryContext(ctx, "SELECT "+columns(t)+" FROM "+qualified+" "+where, args...)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []record{}
	for rows.Next() {
		v := make([]any, len(t.Columns))
		dest := make([]any, len(v))
		for i := range v {
			dest[i] = &v[i]
		}
		if e = rows.Scan(dest...); e != nil {
			return nil, e
		}
		r := record{}
		for i, col := range t.Columns {
			x, e := codec.FromSQL(col.Type, v[i])
			if e != nil {
				return nil, fmt.Errorf("%s: %w", col.Name, e)
			}
			r[col.Name] = x
		}
		if len(t.Aliases) > 0 {
			if e := expandAliases(t, r); e != nil {
				return nil, e
			}
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
func modelRecord(row any) (record, error) {
	v := reflect.ValueOf(row)
	if v.Kind() != reflect.Struct {
		return nil, fmt.Errorf("typed row must be struct")
	}
	r := record{}
	for i := 0; i < v.NumField(); i++ {
		name := v.Type().Field(i).Tag.Get("ch")
		if name == "" {
			return nil, fmt.Errorf("typed row missing column tag")
		}
		r[name] = v.Field(i).Interface()
	}
	return r, nil
}
func readModels[T any](ctx context.Context, c *Client, q executor, name, where string, args ...any) ([]T, error) {
	t, e := c.mapping(name)
	if e != nil {
		return nil, e
	}
	rows, e := readRecords(ctx, q, c.table(name), t, where, args...)
	if e != nil {
		return nil, e
	}
	out := make([]T, len(rows))
	for i, r := range rows {
		v := reflect.ValueOf(&out[i]).Elem()
		for j := 0; j < v.NumField(); j++ {
			field := v.Type().Field(j)
			name := field.Tag.Get("ch")
			x, ok := r[name]
			if !ok {
				return nil, fmt.Errorf("missing mapped field %s", name)
			}
			if e = codec.Assign(v.Field(j), x); e != nil {
				return nil, fmt.Errorf("%s: %w", name, e)
			}
		}
	}
	return out, nil
}
func insertRecord(ctx context.Context, q executor, qualified string, t Table, r record) error {
	cols := []string{}
	args := []any{}
	for _, col := range t.Columns {
		v, ok := r[col.Name]
		if !ok {
			return fmt.Errorf("missing column %s", col.Name)
		}
		x, e := codec.SQLValue(col.Type, v)
		if e != nil {
			return fmt.Errorf("%s: %w", col.Name, e)
		}
		cols = append(cols, "`"+col.Name+"`")
		args = append(args, x)
	}
	if t.HashedKey {
		h, e := keyHash(t, r)
		if e != nil {
			return e
		}
		cols = append(cols, "`_key_hash`")
		args = append(args, h[:])
	}
	if t.Multiplicity {
		cols = append(cols, "`_occurrence`")
		args = append(args, uint64(0))
	}
	if t.Name == "instrument" {
		h, e := instrumentIdentityHash(r)
		if e != nil {
			return e
		}
		cols = append(cols, "`_identity_hash`")
		args = append(args, h[:])
	}
	_, e := q.ExecContext(ctx, "INSERT INTO "+qualified+" ("+strings.Join(cols, ",")+") VALUES("+strings.TrimSuffix(strings.Repeat("?,", len(cols)), ",")+")", args...)
	return e
}
func rowPredicate(t Table, r record) (string, []any, error) {
	where := []string{}
	args := []any{}
	if t.HashedKey {
		h, e := keyHash(t, r)
		if e != nil {
			return "", nil, e
		}
		where = append(where, "`_key_hash`=?")
		args = append(args, h[:])
		if t.Multiplicity {
			where = append(where, "`_occurrence`=0")
		}
	}
	for _, name := range t.Key {
		if t.HashedKey && name != "minute_time" && name != "minute_id" {
			continue
		}
		for _, col := range t.Columns {
			if col.Name == name {
				x, e := codec.SQLValue(col.Type, r[name])
				if e != nil {
					return "", nil, e
				}
				where = append(where, "`"+name+"`=?")
				args = append(args, x)
				break
			}
		}
	}
	return strings.Join(where, " AND "), args, nil
}

// putImmutable verifies existing full typed content including the full business
// key. Hash collisions and same-key mutations are errors, never upserts.
func putImmutable(ctx context.Context, q executor, qualified string, t Table, r record) error {
	want, e := rowHash(t, r)
	if e != nil {
		return e
	}
	where, args, e := rowPredicate(t, r)
	if e != nil {
		return e
	}
	old, e := readRecords(ctx, q, qualified, t, "WHERE "+where+" FOR UPDATE", args...)
	if e != nil {
		return e
	}
	if len(old) > 1 {
		return contracts.ErrConflict
	}
	if len(old) == 1 {
		got, e := rowHash(t, old[0])
		if e != nil {
			return e
		}
		if got != want {
			return contracts.ErrConflict
		}
		return nil
	}
	return insertRecord(ctx, q, qualified, t, r)
}
