// mysql-io-trial bounds background flushing on the two pinned project instances.
// Commit fsync, binlog sync, doublewrite and synchronous checkpoint flushing are
// checked on every action and never changed.
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

const base = "/home/ubuntu/crypto-market-info/var/storage/write-backlog-improvement/mysql-io-capacity-200"

type server struct{ Name, Socket, UUID string }

var servers = []server{
	{"production", "/home/ubuntu/crypto-market-info/var/storage/mysql/run/mysql.sock", "b820454a-c654-11f1-80fa-a04da03d4fd6"},
	{"isolated", "/home/ubuntu/crypto-market-info/var/storage/write-backlog-improvement/bench-mysql/run/mysql.sock", "a93e84b2-c7c3-11f1-bf43-7ab521822a4d"},
}

type snapshot struct {
	UUID                                         string
	Capacity, Maximum, RedoCapacity, BufferBytes uint64
	FlushAtCommit, SyncBinlog, FlushSync         int
	Doublewrite                                  string
	Counters                                     map[string]uint64
}
type report struct {
	UTC     time.Time
	Action  string
	Servers map[string]snapshot
}

func save(name string, v any) error {
	raw, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	path := filepath.Join(base, name)
	f, err := os.OpenFile(path+".tmp", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	if _, err = f.Write(append(raw, '\n')); err != nil {
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
	err := db.QueryRowContext(ctx, "SELECT @@server_uuid,@@innodb_io_capacity,@@innodb_io_capacity_max,@@innodb_redo_log_capacity,@@innodb_buffer_pool_size,@@innodb_flush_log_at_trx_commit,@@sync_binlog,@@innodb_doublewrite,@@innodb_flush_sync").Scan(&r.UUID, &r.Capacity, &r.Maximum, &r.RedoCapacity, &r.BufferBytes, &r.FlushAtCommit, &r.SyncBinlog, &r.Doublewrite, &r.FlushSync)
	if err != nil {
		return r, err
	}
	if r.UUID != s.UUID || r.FlushAtCommit != 1 || r.SyncBinlog != 1 || r.Doublewrite != "ON" || r.FlushSync != 1 {
		return r, fmt.Errorf("%s identity or durability guard failed", s.Name)
	}
	rows, err := db.QueryContext(ctx, "SHOW GLOBAL STATUS WHERE Variable_name IN ('Innodb_os_log_written','Innodb_log_waits','Innodb_redo_log_current_lsn','Innodb_redo_log_checkpoint_lsn','Innodb_buffer_pool_pages_dirty','Innodb_buffer_pool_pages_flushed','Innodb_buffer_pool_reads','Innodb_data_written','Innodb_data_fsyncs')")
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
		if err := save("inspect-"+r.UTC.Format("20060102T150405.000000000Z")+".json", r); err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(r)
	}
	var before report
	if *action == "apply" {
		if _, err := os.Stat(base + "/before.json"); !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("trial already recorded")
		}
		for _, v := range r.Servers {
			if v.Capacity != 10000 || v.Maximum != 20000 {
				return fmt.Errorf("expected original I/O capacity")
			}
		}
		before = r
		if err := save("before.json", before); err != nil {
			return err
		}
	} else {
		raw, err := os.ReadFile(base + "/before.json")
		if err != nil {
			return err
		}
		if err = json.Unmarshal(raw, &before); err != nil {
			return err
		}
	}
	for _, s := range servers {
		old, ok := before.Servers[s.Name]
		if !ok || old.UUID != s.UUID || old.Capacity != 10000 || old.Maximum != 20000 {
			return fmt.Errorf("invalid rollback snapshot")
		}
		current := r.Servers[s.Name]
		if current.Capacity != 10000 && current.Capacity != 200 {
			return fmt.Errorf("capacity changed outside trial")
		}
		if current.Maximum != 20000 && current.Maximum != 400 {
			return fmt.Errorf("maximum changed outside trial")
		}
		if *action == "apply" {
			if _, err := dbs[s.Name].ExecContext(ctx, "SET GLOBAL innodb_io_capacity=200"); err != nil {
				return err
			}
			if _, err := dbs[s.Name].ExecContext(ctx, "SET GLOBAL innodb_io_capacity_max=400"); err != nil {
				return err
			}
		} else {
			if _, err := dbs[s.Name].ExecContext(ctx, "SET GLOBAL innodb_io_capacity_max=20000"); err != nil {
				return err
			}
			if _, err := dbs[s.Name].ExecContext(ctx, "SET GLOBAL innodb_io_capacity=10000"); err != nil {
				return err
			}
		}
		value, err := inspect(ctx, dbs[s.Name], s)
		if err != nil {
			return err
		}
		r.Servers[s.Name] = value
		if *action == "apply" && (value.Capacity != 200 || value.Maximum != 400) {
			return fmt.Errorf("capacity not applied")
		}
		if *action == "rollback" && (value.Capacity != old.Capacity || value.Maximum != old.Maximum) {
			return fmt.Errorf("capacity not restored")
		}
		if err = save(*action+"-progress.json", r); err != nil {
			return err
		}
	}
	r.UTC = time.Now().UTC()
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
