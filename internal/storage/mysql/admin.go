package mysql

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	driver "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"os"
	"path/filepath"
	"regexp"
	"sort"
)

var accountName = regexp.MustCompile(`^cmi_[a-z_]{1,40}$`)

func CurrentProducer(ctx context.Context, db *sql.DB, service string) (Producer, error) {
	var p Producer
	p.Service = service
	var holder []byte
	e := db.QueryRowContext(ctx, "SELECT generation,holder FROM cmi_storage.producer_generation WHERE service=?", service).Scan(&p.Generation, &holder)
	if e != nil {
		return p, e
	}
	id, e := uuid.FromBytes(holder)
	p.Holder = id
	return p, e
}
func InitAllSchemas(ctx context.Context, db *sql.DB) error {
	catalog, e := Mappings()
	if e != nil {
		return e
	}
	names := map[string]bool{}
	for _, t := range catalog.Tables {
		names[t.Database] = true
	}
	sorted := []string{}
	for n := range names {
		sorted = append(sorted, n)
	}
	sort.Strings(sorted)
	for _, name := range sorted {
		if e = InitSchema(ctx, db, name, name); e != nil {
			return e
		}
	}
	return nil
}

// ProvisionAccount installs a bounded local account against the explicit project
// schemas. It never grants producer activation to the live or migration roles.
func ProvisionAccount(ctx context.Context, db *sql.DB, name, role, passwordFile string) error {
	if !accountName.MatchString(name) || !filepath.IsAbs(passwordFile) {
		return fmt.Errorf("explicit local account and absolute private password path required")
	}
	quota := 0
	switch role {
	case "query":
		quota = 3
	case "live":
		quota = 16
	case "migration":
		quota = 1
	case "ddl":
		quota = 2
	default:
		return fmt.Errorf("unknown account role")
	}
	var password string
	raw, e := os.ReadFile(passwordFile)
	if os.IsNotExist(e) {
		bytes := make([]byte, 32)
		if _, e = rand.Read(bytes); e != nil {
			return e
		}
		password = hex.EncodeToString(bytes)
		if e = os.MkdirAll(filepath.Dir(passwordFile), 0700); e != nil {
			return e
		}
		file, e := os.OpenFile(passwordFile, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
		if e != nil {
			return e
		}
		_, e = file.WriteString(password)
		if e == nil {
			e = file.Sync()
		}
		closeErr := file.Close()
		if e != nil {
			return e
		}
		if closeErr != nil {
			return closeErr
		}
	} else if e != nil {
		return e
	} else {
		stat, e := os.Stat(passwordFile)
		if e != nil {
			return e
		}
		if stat.Mode().Perm()&0077 != 0 {
			return fmt.Errorf("password file is not private")
		}
		password = string(raw)
	}
	if len(password) != 64 {
		return fmt.Errorf("account password must be a generated 32-byte hex secret")
	}
	if _, e = hex.DecodeString(password); e != nil {
		return fmt.Errorf("account password file format mismatch")
	}
	account := "'" + name + "'@'localhost'"
	exec := func(query string) error {
		_, e := db.ExecContext(ctx, query)
		if e == nil {
			return nil
		}
		var mysqlErr *driver.MySQLError
		if errors.As(e, &mysqlErr) {
			return fmt.Errorf("account administration failed (MySQL %d)", mysqlErr.Number)
		}
		return fmt.Errorf("account administration connection failed")
	}
	if e = exec("CREATE USER IF NOT EXISTS " + account + " IDENTIFIED BY '" + password + "' WITH MAX_USER_CONNECTIONS " + fmt.Sprint(quota)); e != nil {
		return e
	}
	// A rerun proves the credential instead of silently replacing an existing one.
	var n int
	if e = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM mysql.user WHERE User=? AND Host='localhost'", name).Scan(&n); e != nil || n != 1 {
		return fmt.Errorf("account verification failed")
	}
	catalog, e := Mappings()
	if e != nil {
		return e
	}
	databases := map[string]bool{}
	for _, t := range catalog.Tables {
		databases[t.Database] = true
	}
	priv := "SELECT"
	if role == "live" || role == "migration" {
		priv = "SELECT,INSERT,UPDATE,DELETE"
	}
	if role == "ddl" {
		priv = "ALL PRIVILEGES"
	}
	for database := range databases {
		if e = exec("GRANT " + priv + " ON `" + database + "`.* TO " + account); e != nil {
			return e
		}
	}
	if role == "ddl" {
		return exec("GRANT ALL PRIVILEGES ON cmi_storage.* TO " + account)
	}
	if e = exec("GRANT SELECT ON cmi_storage.* TO " + account); e != nil {
		return e
	}
	if role == "query" {
		return nil
	}
	if role == "live" {
		// MySQL 8.4 locking reads require an additional locking/DML privilege.
		// Keep generation rows SELECT-only; grant locking on the private control DB.
		if e = exec("GRANT LOCK TABLES ON cmi_storage.* TO " + account); e != nil {
			return e
		}
	}
	names := []string{"identity_allocator", "ingest_batch", "ingest_member"}
	if role == "migration" {
		names = append(names, "migration_run", "migration_table", "migration_resolution", "migration_chunk", "migration_conflict", "history_coverage", "bootstrap_source_gap")
	}
	for _, name := range names {
		if e = exec("GRANT SELECT,INSERT,UPDATE,DELETE ON cmi_storage.`" + name + "` TO " + account); e != nil {
			return e
		}
	}
	return nil
}

func (c *Client) CurrentProducer(ctx context.Context, service string) (Producer, error) {
	return CurrentProducer(ctx, c.db, service)
}
func (c *Client) SetProducer(p Producer) { c.producer = p }
