package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

func putModels[T any](ctx context.Context, c *Client, q executor, name string, rows []T) error {
	t, e := c.mapping(name)
	if e != nil {
		return e
	}
	for _, row := range rows {
		r, e := modelRecord(row)
		if e != nil {
			return e
		}
		if e = compactRecord(name, r); e != nil {
			return e
		}
		if e = putImmutable(ctx, q, c.table(name), t, r); e != nil {
			return fmt.Errorf("%s: %w", name, e)
		}
	}
	return nil
}

func putModelsBulk[T any](ctx context.Context, c *Client, q executor, name string, rows []T) error {
	t, e := c.mapping(name)
	if e != nil {
		return e
	}
	// Convert at most one SQL block at a time rather than duplicating all wide
	// evidence objects in a second unbounded record slice.
	for start := 0; start < len(rows); start += 500 {
		records := make([]record, 0, min(500, len(rows)-start))
		for _, row := range rows[start:min(start+500, len(rows))] {
			r, e := modelRecord(row)
			if e != nil {
				return e
			}
			if e = compactRecord(name, r); e != nil {
				return e
			}
			records = append(records, r)
		}
		if e = putImmutableRows(ctx, q, c.table(name), t, records); e != nil {
			return fmt.Errorf("%s: %w", name, e)
		}
	}
	return nil
}

// replaceModel is used only after a domain writer proves a valid revision. It
// stays inside that writer's transaction and cannot change its logical key.
func replaceModel(ctx context.Context, c *Client, tx *sql.Tx, name string, value any) error {
	t, e := c.mapping(name)
	if e != nil {
		return e
	}
	r, e := modelRecord(value)
	if e != nil {
		return e
	}
	if _, e = rowHash(t, r); e != nil {
		return e
	}
	where, args, e := rowPredicate(t, r)
	if e != nil {
		return e
	}
	if _, e = tx.ExecContext(ctx, "DELETE FROM "+c.table(name)+" WHERE "+where, args...); e != nil {
		return e
	}
	return insertRecord(ctx, tx, c.table(name), t, r)
}

func compactRecord(name string, r record) error {
	var names []string
	var values []any
	for n, v := range r {
		names = append(names, n)
		values = append(values, v)
	}
	cols, rows, e := compactInsertValues(name, strings.Join(names, ","), [][]any{values})
	if e != nil {
		return e
	}
	for k := range r {
		delete(r, k)
	}
	for i, n := range strings.Split(cols, ",") {
		r[n] = rows[0][i]
	}
	return nil
}
