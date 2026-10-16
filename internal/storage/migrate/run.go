package migrate

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/storage/mysql"
	"github.com/vphoenix/crypto-market-info/internal/storage/redisbuffer"
	"golang.org/x/sys/unix"
	"os"
	"path/filepath"
	"time"
)

type Config struct {
	Source                              SourceConfig
	Target                              mysql.Config
	Run                                 uuid.UUID
	Family, Database, Table, StagingDir string
	Rows                                int
	MinFreeBytes                        uint64
	RedisSocket                         string
	Delay                               time.Duration
}

func mapping(c Config) (mysql.Table, error) { return mysql.Mapping(c.Family, c.Table) }
func catalogHash() ([32]byte, error) {
	catalog, e := mysql.Mappings()
	if e != nil {
		return [32]byte{}, e
	}
	h := sha256.New()
	for _, t := range catalog.Tables {
		fmt.Fprintf(h, "%s/%s/%s\n", t.Database, t.Name, t.SourceDDLHash)
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out, nil
}
func Plan(ctx context.Context, c Config) error {
	t, e := mapping(c)
	if e != nil {
		return e
	}
	if c.Run == uuid.Nil || c.Database == "" {
		return fmt.Errorf("explicit run and target required")
	}
	source, e := OpenSource(ctx, c.Source)
	if e != nil {
		return e
	}
	defer source.Close()
	if e = source.CheckMapping(ctx, t); e != nil {
		return e
	}
	db, e := mysql.Connect(ctx, c.Target)
	if e != nil {
		return e
	}
	defer db.Close()
	hash, e := configInventoryHash(c)
	if e != nil {
		return e
	}
	if e = mysql.EnsureMigrationRun(ctx, db, c.Run, hash); e != nil {
		return e
	}
	if _, _, _, e = mysql.LoadMigrationTablePlan(ctx, db, t, c.Database, c.Run); e == nil {
		return nil
	} else if e != sql.ErrNoRows {
		return e
	}
	upper, e := source.UpperKey(ctx, t)
	if e != nil {
		return e
	}
	return mysql.PlanMigrationTable(ctx, db, t, c.Database, c.Run, upper)
}
func checkCapacity(ctx context.Context, c Config, db *sql.DB) error {

	entries, e := os.ReadDir(c.StagingDir)
	if e != nil {
		return e
	}
	var used uint64
	for _, entry := range entries {
		if entry.IsDir() || entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("unexpected migration staging entry")
		}
		info, e := entry.Info()
		if e != nil {
			return e
		}
		used += uint64(info.Size())
	}
	if len(entries) > 100000 || used+(64<<20) > 20<<30 {
		return fmt.Errorf("migration staging quota exhausted")
	}
	var fs unix.Statfs_t
	if e := unix.Statfs(c.StagingDir, &fs); e != nil {
		return e
	}
	floor := c.MinFreeBytes
	if floor < 50<<30 {
		floor = 50 << 30
	}
	if fs.Bavail*uint64(fs.Bsize) < floor {
		return fmt.Errorf("migration paused: free disk below configured reserve")
	}
	if c.RedisSocket != "" {
		buffer, e := redisbuffer.Open(ctx, redisbuffer.Config{Socket: c.RedisSocket, WaitTimeout: 2 * time.Second})
		if e != nil {
			return e
		}
		defer buffer.Close()
		catalog, e := mysql.Mappings()
		if e != nil {
			return e
		}
		names := map[string]bool{}
		for _, t := range catalog.Tables {
			names[t.Database] = true
		}
		for name := range names {
			lag, e := buffer.OldestReadyLag(ctx, name, time.Now().UTC())
			if e != nil {
				return e
			}
			if lag >= 30*time.Second {
				return fmt.Errorf("migration paused: sealed live backlog %s", lag)
			}
		}
	}
	return nil
}
func Copy(ctx context.Context, c Config, progress func(mysql.MigrationResult)) error {
	t, e := mapping(c)
	if e != nil {
		return e
	}
	if c.Run == uuid.Nil || c.Rows < 1 || c.Rows > 500 || !filepath.IsAbs(c.StagingDir) {
		return fmt.Errorf("explicit run, bounded rows and absolute staging directory required")
	}
	if e = os.MkdirAll(c.StagingDir, 0700); e != nil {
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
	upper, last, state, e := mysql.LoadMigrationTablePlan(ctx, db, t, c.Database, c.Run)
	if e != nil {
		return e
	}
	if state == 2 || state == mysql.MigrationRowsVerified {
		return nil
	}
	if len(upper) == 0 {
		return mysql.MarkMigrationTableCopied(ctx, db, t, c.Database, c.Run)
	}
	for {
		if e = ctx.Err(); e != nil {
			return e
		}
		if e = checkCapacity(ctx, c, db); e != nil {
			return e
		}
		chunk, e := source.Freeze(ctx, t, last, upper, c.Rows, c.StagingDir)
		if e != nil {
			mysql.RecordMigrationError(context.WithoutCancel(ctx), db, c.Database, t, c.Run, e.Error())
			return e
		}
		if chunk.Rows == 0 {
			chunk.Close()
			if len(last) == 0 {
				return fmt.Errorf("nonempty source plan produced no records")
			}
			comparison, e := mysql.CompareMigrationKeys(t, last, upper)
			if e != nil || comparison != 0 {
				return fmt.Errorf("source range ended before frozen upper key")
			}
			return mysql.MarkMigrationTableCopied(ctx, db, t, c.Database, c.Run)
		}
		// Checkpoint identity covers the frozen range and reviewed mapping. The source
		// digest is checked separately, so a changed retry cannot become a new chunk.
		hash := sha256.New()
		hash.Write(c.Run[:])
		hash.Write([]byte(c.Database))
		hash.Write([]byte(t.Name))
		hash.Write(chunk.Start)
		hash.Write(chunk.End)
		mapHash := mysql.MappingDigest(t)
		hash.Write(mapHash[:])
		var id [32]byte
		copy(id[:], hash.Sum(nil))
		meta := struct {
			Run                             string
			Database, SourceDatabase, Table string
			Chunk, Mapping, SourceHash      string
			Start, End                      []byte
			Rows, Bytes                     uint64
			File                            string
		}{c.Run.String(), c.Database, t.Database, t.Name, hex.EncodeToString(id[:]), hex.EncodeToString(mapHash[:]), hex.EncodeToString(chunk.Hash[:]), chunk.Start, chunk.End, chunk.Rows, chunk.Bytes, chunk.File}
		encoded, e := json.MarshalIndent(meta, "", "  ")
		if e != nil {
			return e
		}
		if e = os.WriteFile(chunk.File+".manifest.json", encoded, 0600); e != nil {
			return e
		}
		next, close, e := chunk.Reader()
		if e != nil {
			return e
		}
		result, e := mysql.ImportMigrationChunk(ctx, db, c.Database, t, c.Run, id, chunk.Start, chunk.End, chunk.Rows, chunk.Hash, next)
		close()
		if e != nil {
			var conflict *mysql.MigrationConflict
			if errors.As(e, &conflict) {
				conflict.Run = c.Run
				mysql.SaveMigrationConflict(context.WithoutCancel(ctx), db, conflict)
			}
			mysql.RecordMigrationError(context.WithoutCancel(ctx), db, c.Database, t, c.Run, e.Error())
			return fmt.Errorf("chunk retained at %s: %w", chunk.File, e)
		}
		if progress != nil {
			progress(result)
		}
		last = chunk.End
		chunk.Close()
		os.Remove(chunk.File + ".manifest.json")
		if c.Delay > 0 {
			timer := time.NewTimer(c.Delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
	}
}
