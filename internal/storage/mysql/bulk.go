package mysql

import (
	"context"
	"errors"
	"fmt"
	driver "github.com/go-sql-driver/mysql"
	"github.com/vphoenix/crypto-market-info/internal/storage/codec"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
	"strings"
	"time"
)

// putImmutableRows batches both retry checks and inserts. Quotas bound packet
// bytes, parameter count, and rows; it never makes a websocket message a write.
func putImmutableRows(ctx context.Context, q executor, qualified string, t Table, input []record) error {
	return writeImmutableRows(ctx, q, qualified, t, input, true)
}

// insertAbsentImmutableRows requires either an absence check in this transaction
// or a caller that fully rolls back and rechecks immutable identities on 1062.
// A concurrent insert or hash collision remains a normal INSERT error here.
func insertAbsentImmutableRows(ctx context.Context, q executor, qualified string, t Table, input []record) error {
	return writeImmutableRows(ctx, q, qualified, t, input, false)
}

func writeImmutableRows(ctx context.Context, q executor, qualified string, t Table, input []record, checkExisting bool) error {
	if len(input) == 0 {
		return nil
	}
	insertFirst := checkExisting && insertNewMinuteFacts(ctx, t)
	size := 500
	if n := 60000 / (len(t.Columns) + 3); n < size {
		size = n
	}
	for offset := 0; offset < len(input); offset += size {
		started := time.Now()
		trace := bulkTrace(ctx, t.Name)
		end := min(len(input), offset+size)
		rows := input[offset:end]
		unique := map[[32]byte]record{}
		hashes := map[[32]byte][32]byte{}
		ordered := [][32]byte{}
		bytes := 0
		for _, r := range rows {
			key, e := keyHash(t, r)
			if e != nil {
				return e
			}
			hash, e := rowHash(t, r)
			if e != nil {
				return e
			}
			if old, ok := hashes[key]; ok {
				if old != hash {
					return contracts.ErrConflict
				}
				continue
			}
			unique[key] = r
			hashes[key] = hash
			ordered = append(ordered, key)
			raw, e := PackMigrationRow(t, MigrationRow(r))
			if e != nil {
				return e
			}
			bytes += len(raw)
		}
		if bytes > 8<<20 {
			if len(rows) == 1 {
				return fmt.Errorf("immutable row exceeds packet budget")
			}
			middle := len(rows) / 2
			if e := writeImmutableRows(ctx, q, qualified, t, rows[:middle], checkExisting); e != nil {
				return e
			}
			if e := writeImmutableRows(ctx, q, qualified, t, rows[middle:], checkExisting); e != nil {
				return e
			}
			continue
		}
		where := []string{}
		args := []any{}
		for _, key := range ordered {
			r := unique[key]
			predicate, values, e := rowPredicate(t, r)
			if e != nil {
				return e
			}
			if t.Multiplicity {
				predicate = strings.Replace(predicate, " AND `_occurrence`=0", "", 1)
			}
			where = append(where, "("+predicate+")")
			args = append(args, values...)
		}
		if trace != nil {
			trace.PrepareMS += float64(time.Since(started)) / float64(time.Millisecond)
			trace.Rows += len(rows)
		}
		readStarted := time.Now()
		var stored []record
		var e error
		if checkExisting && !insertFirst {
			stored, e = readRecords(ctx, q, qualified, t, "WHERE "+strings.Join(where, " OR ")+" FOR UPDATE", args...)
			if trace != nil {
				trace.ReadMS += float64(time.Since(readStarted)) / float64(time.Millisecond)
			}
		}
		if e != nil {
			return e
		}
		started = time.Now()
		seen := map[[32]byte]bool{}
		for _, r := range stored {
			key, e := keyHash(t, r)
			if e != nil {
				return e
			}
			want, ok := hashes[key]
			if !ok || seen[key] {
				return contracts.ErrConflict
			}
			seen[key] = true
			got, e := rowHash(t, r)
			if e != nil {
				return e
			}
			if got != want {
				return contracts.ErrConflict
			}
		}
		insertCols := []string{}
		for _, c := range t.Columns {
			insertCols = append(insertCols, "`"+c.Name+"`")
		}
		if t.HashedKey {
			insertCols = append(insertCols, "`_key_hash`")
		}
		if t.Multiplicity {
			insertCols = append(insertCols, "`_occurrence`")
		}
		if t.Name == "instrument" {
			insertCols = append(insertCols, "`_identity_hash`")
		}
		values := []string{}
		parameters := []any{}
		for _, key := range ordered {
			if seen[key] {
				continue
			}
			r := unique[key]
			for _, col := range t.Columns {
				x, e := codec.SQLValue(col.Type, r[col.Name])
				if e != nil {
					return e
				}
				parameters = append(parameters, x)
			}
			if t.HashedKey {
				parameters = append(parameters, key[:])
			}
			if t.Multiplicity {
				parameters = append(parameters, uint64(0))
			}
			if t.Name == "instrument" {
				hash, e := instrumentIdentityHash(r)
				if e != nil {
					return e
				}
				parameters = append(parameters, hash[:])
			}
			values = append(values, "("+placeholders(len(insertCols))+")")
		}
		if len(values) > 0 {
			if trace != nil {
				trace.PrepareMS += float64(time.Since(started)) / float64(time.Millisecond)
			}
			insertStarted := time.Now()
			if _, e = q.ExecContext(ctx, "INSERT INTO "+qualified+" ("+strings.Join(insertCols, ",")+") VALUES "+strings.Join(values, ","), parameters...); e != nil {
				var duplicate *driver.MySQLError
				if insertFirst && errors.As(e, &duplicate) && duplicate.Number == 1062 {
					return fmt.Errorf("%w: %w", errMinuteInsertCollision, e)
				}
				return e
			}
			if trace != nil {
				trace.InsertMS += float64(time.Since(insertStarted)) / float64(time.Millisecond)
			}
		}
	}
	return nil
}
