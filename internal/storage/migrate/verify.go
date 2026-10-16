package migrate

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"

	"fmt"
	"github.com/vphoenix/crypto-market-info/internal/storage/mysql"
)

func Verify(ctx context.Context, c Config) error {
	return verify(ctx, c, false)
}

// VerifyBootstrapRows verifies an explicitly pinned startup subset without
// treating unimported history as required or opening history completion.
func VerifyBootstrapRows(ctx context.Context, c Config) error {
	if c.Source.SnapshotFile == "" {
		return fmt.Errorf("bootstrap row verification requires a pinned subset snapshot")
	}
	return verify(ctx, c, true)
}

func verify(ctx context.Context, c Config, bootstrap bool) error {
	t, e := mapping(c)
	if e != nil {
		return e
	}
	source, e := OpenSource(ctx, c.Source)
	if e != nil {
		return e
	}
	defer source.Close()
	db, e := mysql.Connect(ctx, c.Target)
	if e != nil {
		return e
	}
	defer db.Close()
	inventory, e := configInventoryHash(c)
	if e != nil {
		return e
	}
	if e = mysql.EnsureMigrationRun(ctx, db, c.Run, inventory); e != nil {
		return e
	}
	upper, _, state, e := mysql.LoadMigrationTablePlan(ctx, db, t, c.Database, c.Run)
	if e != nil {
		return e
	}
	if state != 1 && state != 2 && state != mysql.MigrationRowsVerified {
		return fmt.Errorf("table copy must finish before verify")
	}
	var last []byte
	for len(upper) > 0 {

		var end, storedHash []byte
		var storedRows uint64
		e := db.QueryRowContext(ctx, "SELECT key_end,source_hash,source_count FROM cmi_storage.migration_chunk WHERE run_id=? AND database_name=? AND table_name=? AND key_start=?", c.Run[:], c.Database, t.Name, append([]byte{}, last...)).Scan(&end, &storedHash, &storedRows)
		if e == sql.ErrNoRows {
			if bytes.Equal(last, upper) {
				break
			}
			return fmt.Errorf("missing checkpoint in frozen range")
		}
		if e != nil {
			return e
		}
		rowsLimit := 500
		if storedRows > 0 && storedRows < 500 {
			rowsLimit = int(storedRows)
		}
		chunk, e := source.FreezeRange(ctx, t, last, end, rowsLimit, c.StagingDir)
		if e != nil {
			return e
		}
		if chunk.Rows != storedRows || !bytes.Equal(chunk.Hash[:], storedHash) || !bytes.Equal(chunk.End, end) {
			chunk.Close()
			return fmt.Errorf("frozen chunk changed before independent verification")
		}
		hash := sha256.New()
		hash.Write(c.Run[:])
		hash.Write([]byte(c.Database))
		hash.Write([]byte(t.Name))
		hash.Write(chunk.Start)
		hash.Write(chunk.End)
		mapping := mysql.MappingDigest(t)
		hash.Write(mapping[:])
		var id [32]byte
		copy(id[:], hash.Sum(nil))
		next, close, e := chunk.Reader()
		if e != nil {
			return e
		}
		e = mysql.VerifyMigrationChunk(ctx, db, c.Database, t, c.Run, id, chunk.Rows, chunk.Hash, next)
		close()
		chunk.Close()
		if e != nil {
			return e
		}
		last = chunk.End
	}
	if bootstrap {
		return mysql.MarkMigrationBootstrapRowsVerified(ctx, db, t, c.Database, c.Run)
	}
	cfg := c.Target
	cfg.Database = c.Database
	cfg.Family = c.Family
	cfg.ReadOnly = true
	cfg.MaxConnections = 1
	db.Close()
	client, e := mysql.Open(ctx, cfg)
	if e != nil {
		return e
	}
	e = client.VerifyMigratedDomain(ctx, t.Name)
	client.Close()
	if e != nil {
		return e
	}
	db, e = mysql.Connect(ctx, c.Target)
	if e != nil {
		return e
	}
	defer db.Close()
	if e = mysql.MarkMigrationTableVerified(ctx, db, t, c.Database, c.Run); e != nil {
		return e
	}
	if t.Name == "order_book_minute" || t.Name == "order_book_second_delta" {
		return mysql.MigrationPublisher(db, c.Database, c.Family).CompleteMigratedBooks(ctx, c.Run)
	}
	return nil
}
