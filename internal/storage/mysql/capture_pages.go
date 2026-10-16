package mysql

import (
	"context"
	"fmt"
	"strings"

	"github.com/vphoenix/crypto-market-info/internal/storage/codec"
)

// Startup needs all capture declarations, but sorting all their BLOB fields in
// MySQL can spill to disk before returning even the first row. Read bounded
// primary-key pages; callers restore the documented order on typed models.
func readCaptureModels[T any](ctx context.Context, c *Client, name, filter string, args ...any) ([]T, error) {
	t, e := c.mapping(name)
	if e != nil {
		return nil, e
	}
	if t.Multiplicity || len(t.Key) == 0 {
		return nil, fmt.Errorf("capture paging requires a unique primary key")
	}
	keys := append([]string(nil), t.Key...)
	if t.HashedKey {
		keys = []string{"_key_hash"}
	}
	quoted := make([]string, len(keys))
	for i, key := range keys {
		quoted[i] = "`" + key + "`"
	}
	order := strings.Join(quoted, ",")
	var cursor []any
	out := []T{}
	for {
		conditions := []string{filter}
		values := append([]any(nil), args...)
		if cursor != nil {
			predicate, parameters := captureAfter(quoted, cursor)
			conditions = append(conditions, predicate)
			values = append(values, parameters...)
		}
		page, e := readModels[T](ctx, c, c.db, name, "FORCE INDEX (PRIMARY) WHERE "+strings.Join(conditions, " AND ")+" ORDER BY "+order+" LIMIT 500", values...)
		if e != nil {
			return nil, e
		}
		out = append(out, page...)
		if len(page) < 500 {
			return out, nil
		}
		last, e := modelRecord(page[len(page)-1])
		if e != nil {
			return nil, e
		}
		if t.HashedKey {
			hash, e := keyHash(t, last)
			if e != nil {
				return nil, e
			}
			cursor = []any{hash[:]}
		} else {
			cursor = make([]any, len(keys))
			for i, key := range keys {
				for _, col := range t.Columns {
					if col.Name == key {
						cursor[i], e = codec.SQLValue(col.Type, last[key])
						if e != nil {
							return nil, e
						}
						break
					}
				}
			}
		}
	}
}

// A tuple inequality mixed with equality filters can make MySQL use only the
// fixed key prefix and rescan earlier pages. Expand the same lexicographic
// comparison so every primary-key component can bound the range scan.
func captureAfter(keys []string, cursor []any) (string, []any) {
	branches := make([]string, len(keys))
	var parameters []any
	for n := range keys {
		terms := make([]string, n+1)
		for prior := 0; prior < n; prior++ {
			terms[prior] = keys[prior] + "=?"
			parameters = append(parameters, cursor[prior])
		}
		terms[n] = keys[n] + ">?"
		parameters = append(parameters, cursor[n])
		branches[n] = "(" + strings.Join(terms, " AND ") + ")"
	}
	return "(" + strings.Join(branches, " OR ") + ")", parameters
}
