package mysql

import (
	"bytes"
	"context"
	"fmt"
	"slices"
)

// This is only a cache hint, never a publication/absence proof. The final
// transaction still reads every occurrence and checks all content and first
// knowledge. Reading just primary-index keys also bounds wide-row memory.
func (c *Client) prefetchCEXPublications(ctx context.Context, publications []publicationRecord) error {
	if !c.prefetchMinuteIndexes || len(publications) == 0 {
		return nil
	}
	if len(publications) > 100 {
		return fmt.Errorf("publication prefetch quota exceeded")
	}
	for _, name := range []string{"cex_book_minute_commit", "source_publication_intents", "source_batch_status"} {
		table, err := c.mapping(name)
		if err != nil {
			return err
		}
		if !table.HashedKey {
			return fmt.Errorf("publication prefetch requires the pinned primary key")
		}
		input := make([]record, 0, len(publications))
		for _, p := range publications {
			input = append(input, record{"source_kind": p.Kind, "source_id": p.ID})
		}
		if err := c.prefetchImmutableKeys(ctx, table, input); err != nil {
			return err
		}
	}
	return nil
}

// Prefetch only bounded covering keys. It never returns absence/content proofs;
// the caller's final transaction must still inspect every physical occurrence.
func (c *Client) prefetchImmutableKeys(ctx context.Context, table Table, input []record) error {
	if c.sourceStorageAvailable && sourceStorageTable(table.Name) {
		return c.prefetchSourceIdentities(ctx, table, input)
	}
	if !table.HashedKey || len(input) == 0 {
		return nil
	}
	for offset := 0; offset < len(input); offset += 500 {
		chunk := input[offset:min(offset+500, len(input))]
		keys := make([][32]byte, 0, len(chunk))
		for _, r := range chunk {
			key, err := keyHash(table, r)
			if err != nil {
				return err
			}
			keys = append(keys, key)
		}
		slices.SortFunc(keys, func(a, b [32]byte) int { return bytes.Compare(a[:], b[:]) })
		keys = slices.Compact(keys)
		parameters := make([]any, len(keys))
		for n := range keys {
			parameters[n] = keys[n][:]
		}
		rows, err := c.db.QueryContext(ctx, "SELECT `_key_hash` FROM "+c.table(table.Name)+" FORCE INDEX (PRIMARY) WHERE `_key_hash` IN ("+placeholders(len(keys))+") ORDER BY `_key_hash` LIMIT 500", parameters...)
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
	return nil
}
