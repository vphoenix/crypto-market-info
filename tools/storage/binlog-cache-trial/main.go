// binlog-cache-trial changes only the transaction cache on two pinned local
// MySQL instances. It never changes logging, durability, facts or generations.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/storage/mysql"
)

const base = "/home/ubuntu/crypto-market-info/var/storage/write-backlog-improvement/binlog-cache-4m"
const cacheSize = 4 << 20

type server struct{ Name, Socket, UUID string }

var servers = []server{
	{"production", "/home/ubuntu/crypto-market-info/var/storage/mysql/run/mysql.sock", "b820454a-c654-11f1-80fa-a04da03d4fd6"},
	{"isolated", "/home/ubuntu/crypto-market-info/var/storage/write-backlog-improvement/bench-mysql/run/mysql.sock", "a93e84b2-c7c3-11f1-bf43-7ab521822a4d"},
}

type snapshot struct {
	UUID                                    string
	CacheBytes, BufferBytes, MaxCacheBytes  uint64
	FlushAtCommit, SyncBinlog               int
	Doublewrite                             string
	RedoCapacity, IOCapacity, IOCapacityMax uint64
	FlushNeighbors, LRUScanDepth            uint64
	Counters                                map[string]uint64
}
type report struct {
	UTC                time.Time
	Action             string
	Servers            map[string]snapshot
	FreshSessionsAfter time.Time
}

func save(name string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	path := filepath.Join(base, name)
	f, err := os.OpenFile(path+".tmp", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	if _, err = f.Write(append(b, '\n')); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(path+".tmp", path); err != nil {
		return err
	}
	dir, err := os.Open(base)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
func inspect(ctx context.Context, db *sql.DB, s server) (snapshot, error) {
	r := snapshot{Counters: map[string]uint64{}}
	err := db.QueryRowContext(ctx, "SELECT @@server_uuid,@@binlog_cache_size,@@innodb_buffer_pool_size,@@max_binlog_cache_size,@@innodb_flush_log_at_trx_commit,@@sync_binlog,@@innodb_doublewrite").Scan(&r.UUID, &r.CacheBytes, &r.BufferBytes, &r.MaxCacheBytes, &r.FlushAtCommit, &r.SyncBinlog, &r.Doublewrite)
	if err != nil {
		return r, err
	}
	if r.UUID != s.UUID || r.FlushAtCommit != 1 || r.SyncBinlog != 1 || r.Doublewrite != "ON" || r.MaxCacheBytes < cacheSize {
		return r, fmt.Errorf("%s identity or durability guard failed", s.Name)
	}
	if err = db.QueryRowContext(ctx, "SELECT @@innodb_redo_log_capacity,@@innodb_io_capacity,@@innodb_io_capacity_max,@@innodb_flush_neighbors,@@innodb_lru_scan_depth").Scan(&r.RedoCapacity, &r.IOCapacity, &r.IOCapacityMax, &r.FlushNeighbors, &r.LRUScanDepth); err != nil {
		return r, err
	}
	rows, err := db.QueryContext(ctx, "SHOW GLOBAL STATUS WHERE Variable_name IN ('Binlog_cache_use','Binlog_cache_disk_use','Binlog_stmt_cache_use','Binlog_stmt_cache_disk_use','Innodb_os_log_written','Innodb_log_waits','Innodb_redo_log_current_lsn','Innodb_redo_log_checkpoint_lsn','Innodb_redo_log_capacity_resized','Innodb_buffer_pool_pages_dirty','Innodb_buffer_pool_pages_flushed','Innodb_data_written')")
	if err != nil {
		return r, err
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		var n uint64
		if err = rows.Scan(&name, &n); err != nil {
			return r, err
		}
		r.Counters[name] = n
	}
	return r, rows.Err()
}
func run() error {
	action := flag.String("action", "inspect", "inspect, apply, rollback")
	flag.Parse()
	if *action != "inspect" && *action != "apply" && *action != "rollback" {
		return fmt.Errorf("invalid action")
	}
	if err := os.MkdirAll(base, 0700); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	r := report{UTC: time.Now().UTC(), Action: *action, Servers: map[string]snapshot{}}
	dbs := map[string]*sql.DB{}
	for _, s := range servers {
		db, err := mysql.Connect(ctx, mysql.Config{Socket: s.Socket, Username: "root", PasswordFile: "/home/ubuntu/.config/crypto-market-info-storage/mysql-root-password", MaxConnections: 1, ReadOnly: *action == "inspect", SelectTimeout: 3 * time.Second, ReadTimeout: 10 * time.Second})
		if err != nil {
			return err
		}
		defer db.Close()
		dbs[s.Name] = db
		value, err := inspect(ctx, db, s)
		if err != nil {
			return err
		}
		r.Servers[s.Name] = value
	}
	if *action == "inspect" {
		name := "inspect-" + r.UTC.Format("20060102T150405.000000000Z") + ".json"
		if err := save(name, r); err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(r)
	}
	var before report
	if *action == "apply" {
		if _, err := os.Stat(base + "/before.json"); !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("trial already recorded; inspect or rollback")
		}
		for _, v := range r.Servers {
			if v.CacheBytes != 32768 {
				return fmt.Errorf("expected original 32 KiB cache")
			}
		}
		before = r
		if err := save("before.json", before); err != nil {
			return err
		}
	} else {
		data, err := os.ReadFile(base + "/before.json")
		if err != nil {
			return err
		}
		if err = json.Unmarshal(data, &before); err != nil {
			return err
		}
	}
	for _, s := range servers {
		target := uint64(cacheSize)
		if *action == "rollback" {
			old, ok := before.Servers[s.Name]
			if !ok || old.UUID != s.UUID || old.CacheBytes != 32768 {
				return fmt.Errorf("invalid rollback record")
			}
			target = old.CacheBytes
		}
		current := r.Servers[s.Name]
		if current.CacheBytes != 32768 && current.CacheBytes != cacheSize {
			return fmt.Errorf("cache setting changed outside this trial")
		}
		if _, err := dbs[s.Name].ExecContext(ctx, "SET GLOBAL binlog_cache_size=?", target); err != nil {
			return err
		}
		value, err := inspect(ctx, dbs[s.Name], s)
		if err != nil {
			return err
		}
		if value.CacheBytes != target {
			return fmt.Errorf("cache setting was not applied")
		}
		r.Servers[s.Name] = value
		if err = save(*action+"-progress.json", r); err != nil {
			return err
		}
	}
	// Existing pools naturally replace physical connections after ten minutes;
	// allow another minute for a bounded transaction. No collector restart.
	r.FreshSessionsAfter = time.Now().UTC().Add(11 * time.Minute)
	if err := save(*action+".json", r); err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(r)
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
