package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/vphoenix/crypto-market-info/internal/storage/codec"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
)

// Read both representations in one statement snapshot. Push the original
// predicate/order/limit into both branches, then apply it to their union. This
// preserves global LIMIT semantics and physical multiplicity without scanning
// the whole historical table for an identity lookup.
func (c *Client) sourceRecords(ctx context.Context, q executor, name, where string, args ...any) ([]record, error) {
	t, err := c.mapping(name)
	if err != nil {
		return nil, err
	}
	if !c.sourceStorageAvailable {
		return readRecords(ctx, q, c.table(name), t, where, args...)
	}
	if strings.Contains(strings.ToUpper(where), "FOR UPDATE") {
		return nil, fmt.Errorf("source union is read-only")
	}
	branch := func(table string) string {
		return "(SELECT " + columns(t) + " FROM " + c.table(table) + " " + where + ")"
	}
	qualified := "(" + branch(name) + " UNION ALL " + branch(name+"_v2") + ") AS source_union"
	parameters := make([]any, 0, len(args)*3)
	for range 3 {
		parameters = append(parameters, args...)
	}
	return readRecords(ctx, q, qualified, t, where, parameters...)
}

// Both tables are locked in a fixed order. A rollback/configuration change may
// select either write target, but must still inspect every historical occurrence.
func (c *Client) sourceLockedRows(ctx context.Context, tx *sql.Tx, name, where string, args ...any) ([]record, error) {
	t, err := c.mapping(name)
	if err != nil {
		return nil, err
	}
	qualified := c.table(name)
	if c.sourceStorageAvailable {
		qualified += " FORCE INDEX (business_lookup)"
	}
	rows, err := readRecords(ctx, tx, qualified, t, where, args...)
	if err != nil || !c.sourceStorageAvailable {
		return rows, err
	}
	more, err := readRecords(ctx, tx, c.table(name+"_v2")+" FORCE INDEX (business_lookup)", t, where, args...)
	return append(rows, more...), err
}

func (c *Client) insertSourceRows(ctx context.Context, tx *sql.Tx, name string, rows []record) error {
	target := name
	if c.sourceStorageV2 {
		target += "_v2"
	}
	t, err := c.mapping(target)
	if err != nil {
		return err
	}
	return insertAbsentImmutableRows(ctx, tx, c.table(target), t, rows)
}

// Full identity equality is checked through the existing business index. The
// old hash-clustered pages are fetched only for actual existing identities;
// new full records go to append pages, with a narrow unique hash index.
func sourceIdentityPredicate(t Table, rows []record) (string, []any, error) {
	if len(rows) == 0 || len(rows) > 500 {
		return "", nil, fmt.Errorf("source identity query quota")
	}
	parts := make([]string, 0, len(rows))
	args := []any{}
	for _, r := range rows {
		keys := []string{}
		for _, key := range t.Key {
			for _, col := range t.Columns {
				if col.Name != key {
					continue
				}
				value, err := codec.SQLValue(col.Type, r[key])
				if err != nil {
					return "", nil, err
				}
				keys = append(keys, "`"+key+"`=?")
				args = append(args, value)
			}
		}
		parts = append(parts, "("+strings.Join(keys, " AND ")+")")
	}
	return "WHERE " + strings.Join(parts, " OR "), args, nil
}

func (c *Client) putProvenanceRows(ctx context.Context, tx *sql.Tx, input []record) error {
	const name = "public_observation_provenance"
	t, err := c.mapping(name)
	if err != nil {
		return err
	}
	if !c.sourceStorageAvailable {
		return putImmutableRows(ctx, tx, c.table(name), t, input)
	}
	for offset := 0; offset < len(input); offset += 500 {
		rows := input[offset:min(offset+500, len(input))]
		want := map[[32]byte][32]byte{}
		ordered := make([]record, 0, len(rows))
		for _, r := range rows {
			key, err := keyHash(t, r)
			if err != nil {
				return err
			}
			hash, err := rowHash(t, r)
			if err != nil {
				return err
			}
			if old, ok := want[key]; ok {
				if old != hash {
					return contracts.ErrConflict
				}
				continue
			}
			want[key] = hash
			ordered = append(ordered, r)
		}
		where, args, err := sourceIdentityPredicate(t, ordered)
		if err != nil {
			return err
		}
		stored, err := c.sourceLockedRows(ctx, tx, name, where+" FOR UPDATE", args...)
		if err != nil {
			return err
		}
		seen := map[[32]byte]bool{}
		for _, r := range stored {
			key, err := keyHash(t, r)
			if err != nil {
				return err
			}
			hash, err := rowHash(t, r)
			if err != nil {
				return err
			}
			expected, ok := want[key]
			if !ok || seen[key] || hash != expected {
				return contracts.ErrConflict
			}
			seen[key] = true
		}
		missing := []record{}
		for _, r := range ordered {
			key, _ := keyHash(t, r)
			if !seen[key] {
				missing = append(missing, r)
			}
		}
		if err = c.insertSourceRows(ctx, tx, name, missing); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) prefetchSourceIdentities(ctx context.Context, t Table, input []record) error {
	for offset := 0; offset < len(input); offset += 500 {
		where, args, err := sourceIdentityPredicate(t, input[offset:min(offset+500, len(input))])
		if err != nil {
			return err
		}
		for _, name := range []string{t.Name, t.Name + "_v2"} {
			rows, err := c.db.QueryContext(ctx, "SELECT `_key_hash` FROM "+c.table(name)+" FORCE INDEX (business_lookup) "+where+" LIMIT 500", args...)
			if err != nil {
				return err
			}
			for rows.Next() {
				var key []byte
				if err = rows.Scan(&key); err != nil {
					rows.Close()
					return err
				}
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return err
			}
		}
	}
	return nil
}
