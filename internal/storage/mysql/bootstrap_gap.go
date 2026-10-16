package mysql

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"

	"github.com/google/uuid"
)

// AcceptedBootstrapGap retains an explicitly abandoned historical declaration.
// Its facts and original committed/revision fields are never rewritten. Only
// bootstrap domain validation may acknowledge this exact original row as lost.
type AcceptedBootstrapGap struct {
	Run                                       uuid.UUID
	Database, Family, Table, Reason, Evidence string
	Row                                       MigrationRow
}

func RegisterBootstrapGap(ctx context.Context, db *sql.DB, g AcceptedBootstrapGap) error {
	if g.Run == uuid.Nil || !identifier.MatchString(g.Database) || g.Reason == "" || len(g.Reason) > 512 || g.Evidence == "" || len(g.Evidence) > 1024 {
		return fmt.Errorf("explicit bounded bootstrap gap declaration required")
	}
	if !(g.Family == "crypto_market_info" && g.Table == "dex_block" || g.Family == "crypto_market_info_justlend_keeper" && g.Table == "jl_keeper_capture") {
		return fmt.Errorf("bootstrap gap is outside supported historical marker tables")
	}
	t, e := Mapping(g.Family, g.Table)
	if e != nil {
		return e
	}
	if g.Row["committed"] != true {
		return fmt.Errorf("gap must retain an original committed declaration")
	}
	key, e := MigrationKey(t, g.Row)
	if e != nil {
		return e
	}
	kh := sha256.Sum256(key)
	raw, e := PackMigrationRow(t, g.Row)
	if e != nil {
		return e
	}
	rh := sha256.Sum256(raw)
	tx, e := db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	var oldKey, oldRow, oldHash []byte
	var reason, evidence string
	e = tx.QueryRowContext(ctx, "SELECT source_key,source_row,source_row_hash,reason,evidence FROM cmi_storage.bootstrap_source_gap WHERE database_name=? AND table_name=? AND key_hash=? FOR UPDATE", g.Database, g.Table, kh[:]).Scan(&oldKey, &oldRow, &oldHash, &reason, &evidence)
	if e == nil {
		if !bytes.Equal(key, oldKey) || !bytes.Equal(raw, oldRow) || !bytes.Equal(rh[:], oldHash) || reason != g.Reason || evidence != g.Evidence {
			return fmt.Errorf("accepted bootstrap gap changed")
		}
		return tx.Commit()
	}
	if e != sql.ErrNoRows {
		return e
	}
	_, e = tx.ExecContext(ctx, "INSERT INTO cmi_storage.bootstrap_source_gap(run_id,source_database,database_name,table_name,key_hash,source_key,source_row,source_row_hash,reason,evidence,accepted_at) VALUES(?,?,?,?,?,?,?,?,?,?,UTC_TIMESTAMP(6))", g.Run[:], g.Family, g.Database, g.Table, kh[:], key, raw, rh[:], g.Reason, g.Evidence)
	if e != nil {
		return e
	}
	return tx.Commit()
}

func (c *Client) bootstrapGaps(ctx context.Context, name string) (map[[32]byte][32]byte, error) {
	out := map[[32]byte][32]byte{}
	if name != "dex_block" && name != "jl_keeper_capture" {
		return out, nil
	}
	rs, e := c.db.QueryContext(ctx, "SELECT key_hash,source_row_hash FROM cmi_storage.bootstrap_source_gap WHERE database_name=? AND table_name=?", c.database, name)
	if e != nil {
		return nil, e
	}
	defer rs.Close()
	for rs.Next() {
		var key, row []byte
		if e = rs.Scan(&key, &row); e != nil {
			return nil, e
		}
		if len(key) != 32 || len(row) != 32 {
			return nil, fmt.Errorf("invalid accepted gap digest")
		}
		var k, h [32]byte
		copy(k[:], key)
		copy(h[:], row)
		out[k] = h
	}
	return out, rs.Err()
}

func bootstrapGapMatches(t Table, row MigrationRow, gaps map[[32]byte][32]byte) (bool, error) {
	if len(gaps) == 0 {
		return false, nil
	}
	key, e := MigrationKey(t, row)
	if e != nil {
		return false, e
	}
	kh := sha256.Sum256(key)
	want, ok := gaps[kh]
	if !ok {
		return false, nil
	}
	raw, e := PackMigrationRow(t, row)
	if e != nil {
		return false, e
	}
	got := sha256.Sum256(raw)
	if got != want {
		return false, fmt.Errorf("accepted historical marker contents changed")
	}
	return true, nil
}
