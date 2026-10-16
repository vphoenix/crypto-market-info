package mysql

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"

	"github.com/google/uuid"
)

// Earlier code mistook heterogeneous ordinary MergeTree occurrences for an
// immutable-key overwrite. Resolve only that diagnostic, after every original
// occurrence has been read back exactly in the current atomic import. Business
// publication ambiguity remains intact and is rejected by production readers.
func resolvePreservedSourceMultiplicity(ctx context.Context, tx *sql.Tx, t Table, run uuid.UUID, chunk [32]byte, raw [][]byte) error {
	if !t.Multiplicity {
		return nil
	}
	rows, e := tx.QueryContext(ctx, "SELECT conflict_id,key_hash,source_hash,target_hash FROM cmi_storage.migration_conflict WHERE run_id=? AND chunk_id=? AND reason='source_immutable_key_has_different_contents' AND resolved=0 FOR UPDATE", run[:], chunk[:])
	if e != nil {
		return e
	}
	type conflict struct{ id, key, source, target []byte }
	conflicts := []conflict{}
	for rows.Next() {
		var c conflict
		if e = rows.Scan(&c.id, &c.key, &c.source, &c.target); e != nil {
			rows.Close()
			return e
		}
		conflicts = append(conflicts, c)
	}
	e = rows.Err()
	rows.Close()
	if e != nil || len(conflicts) == 0 {
		return e
	}
	for _, c := range conflicts {
		h := sha256.New()
		foundSource, foundTarget, count := false, false, 0
		for _, b := range raw {
			r, e := UnpackMigrationRow(t, b)
			if e != nil {
				return e
			}
			key, e := keyHash(t, record(r))
			if e != nil {
				return e
			}
			if !bytes.Equal(key[:], c.key) {
				continue
			}
			digest, e := rowHash(t, record(r))
			if e != nil {
				return e
			}
			foundSource = foundSource || bytes.Equal(digest[:], c.source)
			foundTarget = foundTarget || bytes.Equal(digest[:], c.target)
			writeFramed(h, b)
			count++
		}
		// The streaming path calls this after each complete key group. A
		// diagnostic for a later group must wait for that group's readback.
		if count == 0 {
			continue
		}
		if count < 2 || !foundSource || !foundTarget {
			return fmt.Errorf("previous source multiplicity conflict is absent from verified frozen group")
		}
		digest := h.Sum(nil)
		if _, e = tx.ExecContext(ctx, "INSERT INTO cmi_storage.migration_resolution(chunk_id,key_hash,source_hash,target_hash,source_version,target_version,reason) VALUES(?,?,?,?,?,?,?) ON DUPLICATE KEY UPDATE source_hash=VALUES(source_hash),target_hash=VALUES(target_hash),reason=VALUES(reason)", chunk[:], c.key, digest, digest, []byte{}, []byte{}, "preserved_complete_source_multiset_ambiguity"); e != nil {
			return e
		}
		if _, e = tx.ExecContext(ctx, "UPDATE cmi_storage.migration_conflict SET resolved=1 WHERE conflict_id=? AND resolved=0", c.id); e != nil {
			return e
		}
	}
	return nil
}
