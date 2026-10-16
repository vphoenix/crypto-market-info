// Package backend selects an explicit implementation at the application edge.
// A MySQL error never falls back to a ClickHouse writer.
package backend

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/storage/clickhouse"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
	"github.com/vphoenix/crypto-market-info/internal/storage/mysql"
	"github.com/vphoenix/crypto-market-info/internal/storage/redisbuffer"
	"github.com/vphoenix/crypto-market-info/internal/storage/spool"
	"os"
	"strconv"
	"time"
)

type Config struct {
	Name         string
	ClickHouse   clickhouse.Config
	MySQL        mysql.Config
	Redis        redisbuffer.Config
	SpoolDir     string
	SpoolBytes   int64
	QueueMinutes bool
	Purpose      string
}

func Environment(prefix string, ch clickhouse.Config, readOnly bool) (Config, error) {
	value := func(k, fallback string) string {
		if s := os.Getenv(prefix + k); s != "" {
			return s
		}
		if prefix != "" {
			if s := os.Getenv(k); s != "" {
				return s
			}
		}
		return fallback
	}
	name := value("STORAGE_BACKEND", os.Getenv("STORAGE_BACKEND"))
	if name == "" {
		name = "clickhouse"
	}
	cfg := Config{Name: name, ClickHouse: ch, Purpose: prefix}
	if readOnly {
		cfg.Purpose = "reader"
	}
	if name == "clickhouse" {
		return cfg, nil
	}
	if name != "mysql" {
		return cfg, fmt.Errorf("unsupported storage backend %q", name)
	}
	connections, e := strconv.Atoi(value("MYSQL_MAX_CONNECTIONS", "2"))
	if e != nil || connections < 1 || connections > 16 {
		return cfg, fmt.Errorf("invalid MySQL connection quota")
	}
	cfg.MySQL = mysql.Config{Socket: value("MYSQL_SOCKET", os.Getenv("MYSQL_SOCKET")), Username: value("MYSQL_USER", os.Getenv("MYSQL_USER")), PasswordFile: value("MYSQL_PASSWORD_FILE", os.Getenv("MYSQL_PASSWORD_FILE")), Database: value("MYSQL_DATABASE", ch.Database), Family: value("MYSQL_SCHEMA_FAMILY", ""), MaxConnections: connections, ReadOnly: readOnly}
	cfg.MySQL.CapitalRuleVersions, e = strconv.ParseBool(value("MYSQL_CAPITAL_RULE_VERSIONS", "false"))
	if e != nil {
		return cfg, fmt.Errorf("invalid MYSQL_CAPITAL_RULE_VERSIONS")
	}
	if cfg.MySQL.Socket == "" || cfg.MySQL.Username == "" || cfg.MySQL.PasswordFile == "" {
		return cfg, fmt.Errorf("MySQL requires explicit socket, user, password-file")
	}
	if !readOnly {
		var holder uuid.UUID
		var gen uint64
		if raw := value("STORAGE_HOLDER", ""); raw != "" {
			var e error
			holder, e = uuid.Parse(raw)
			if e != nil {
				return cfg, fmt.Errorf("invalid STORAGE_HOLDER")
			}
		}
		if raw := value("STORAGE_GENERATION", ""); raw != "" {
			var e error
			gen, e = strconv.ParseUint(raw, 10, 64)
			if e != nil || gen == 0 {
				return cfg, fmt.Errorf("invalid STORAGE_GENERATION")
			}
		}
		cfg.MySQL.Producer = mysql.Producer{Service: value("STORAGE_SERVICE", ""), Generation: gen, Holder: holder, Scope: value("STORAGE_SCOPE", prefix)}
	}

	cfg.Redis = redisbuffer.Config{Socket: value("REDIS_SOCKET", os.Getenv("REDIS_SOCKET")), Prefix: value("REDIS_PREFIX", "cmi:v1"), WaitTimeout: 2 * time.Second, SoftBytes: 320 << 20}
	cfg.SpoolDir = value("STORAGE_SPOOL_DIR", os.Getenv("STORAGE_SPOOL_DIR"))
	cfg.SpoolBytes = 20 << 30
	return cfg, nil
}
func open[T contracts.Lifecycle](ctx context.Context, cfg Config, chOpen func(context.Context, clickhouse.Config) (*clickhouse.Client, error)) (T, error) {
	var zero T
	var candidate contracts.Lifecycle
	var e error
	switch cfg.Name {
	case "", "clickhouse":
		candidate, e = chOpen(ctx, cfg.ClickHouse)
	case "mysql":
		if !cfg.MySQL.ReadOnly && (cfg.MySQL.Producer.Service == "" || cfg.MySQL.Producer.Generation == 0 || cfg.MySQL.Producer.Holder == uuid.Nil) {
			return zero, fmt.Errorf("MySQL writer requires an explicitly activated STORAGE_SERVICE, STORAGE_GENERATION and STORAGE_HOLDER")
		}
		var c *mysql.Client
		c, e = mysql.Open(ctx, cfg.MySQL)
		if e == nil && cfg.QueueMinutes && !cfg.MySQL.ReadOnly {
			if cfg.SpoolDir == "" {
				c.Close()
				return zero, fmt.Errorf("minute writer requires bounded disk spool")
			}
			disk, e2 := spool.Open(cfg.SpoolDir, cfg.SpoolBytes)
			if e2 != nil {
				c.Close()
				return zero, e2
			}
			var buffer *redisbuffer.Buffer
			if cfg.Redis.Socket != "" {
				buffer, e2 = redisbuffer.Open(ctx, cfg.Redis)
				if e2 != nil {
					c.Close()
					return zero, e2
				}
			}
			q := redisbuffer.Queue{Redis: buffer, Spool: disk}
			var close func() error
			if buffer != nil {
				pump := buffer.StartPreview(context.WithoutCancel(ctx), cfg.MySQL.Database, cfg.MySQL.Producer.Service, cfg.MySQL.Producer.Generation, c.PreviewCommitted)
				c.SetPreview(pump.OfferPreview)
				close = func() error { pump.Close(); return buffer.Close() }
			}
			c.SetDeliveryQueue(q, close)
		}
		candidate = c
	default:
		return zero, fmt.Errorf("unsupported backend %q", cfg.Name)
	}
	if e != nil {
		return zero, e
	}
	store, ok := candidate.(T)
	if !ok {
		candidate.Close()
		return zero, fmt.Errorf("backend %s lacks requested domain interface", cfg.Name)
	}
	return store, nil
}
func chWriter(cfg Config, domain string) func(context.Context, clickhouse.Config) (*clickhouse.Client, error) {
	if cfg.MySQL.ReadOnly || cfg.Purpose == "reader" {
		return clickhouse.OpenReadOnly
	}
	switch domain {
	case "reserve":
		return clickhouse.Open
	case "lst":
		return clickhouse.OpenLSTWriter
	case "keeper":
		return clickhouse.OpenKeeperWriter
	default:
		return clickhouse.Open
	}
}
func OpenMain(ctx context.Context, c Config) (contracts.MainStore, error) {
	c.QueueMinutes = c.Name == "mysql" && !c.MySQL.ReadOnly
	return open[contracts.MainStore](ctx, c, chWriter(c, "main"))
}
func OpenOptions(ctx context.Context, c Config) (contracts.OptionsStore, error) {
	c.QueueMinutes = c.Name == "mysql" && !c.MySQL.ReadOnly
	return open[contracts.OptionsStore](ctx, c, chWriter(c, "options"))
}
func OpenDEX(ctx context.Context, c Config) (contracts.DEXStore, error) {
	return open[contracts.DEXStore](ctx, c, chWriter(c, "dex"))
}
func OpenAcross(ctx context.Context, c Config) (contracts.AcrossStore, error) {
	if c.Name == "mysql" {
		c.MySQL.MaxConnections = 1
	}
	return open[contracts.AcrossStore](ctx, c, chWriter(c, "across"))
}
func OpenReserve(ctx context.Context, c Config) (contracts.ReserveStore, error) {
	if c.Name == "mysql" {
		c.MySQL.MaxConnections = 1
	}
	return open[contracts.ReserveStore](ctx, c, chWriter(c, "reserve"))
}
func OpenLST(ctx context.Context, c Config) (contracts.LSTStore, error) {
	if c.Name == "mysql" {
		c.MySQL.MaxConnections = 1
	}
	return open[contracts.LSTStore](ctx, c, chWriter(c, "lst"))
}
func OpenKeeper(ctx context.Context, c Config) (contracts.KeeperStore, error) {
	if c.Name == "mysql" {
		c.MySQL.MaxConnections = 1
	}
	return open[contracts.KeeperStore](ctx, c, chWriter(c, "keeper"))
}
func OpenPerpetualReader(ctx context.Context, c Config) (contracts.PerpetualReader, error) {
	c.MySQL.ReadOnly = true
	c.Purpose = "reader"
	return open[contracts.PerpetualReader](ctx, c, clickhouse.OpenReadOnly)
}
func Reader(c Config) Config {
	c.MySQL.ReadOnly = true
	c.Purpose = "reader"
	c.QueueMinutes = false
	return c
}
func WithDatabase(c Config, database string) Config {
	c.ClickHouse.Database = database
	c.MySQL.Database = database
	return c
}

// The legacy options writer shares its existing identity allocator lock. MySQL
// allocates under a database transaction and has a separate bounded pool.
func OptionsWriter(ctx context.Context, c Config, parent contracts.MainStore) (contracts.OptionsStore, error) {
	if c.Name == "" || c.Name == "clickhouse" {
		p, ok := parent.(*clickhouse.Client)
		if !ok {
			return nil, fmt.Errorf("legacy options writer requires ClickHouse parent")
		}
		return p.OptionsWriter(ctx, c.ClickHouse)
	}
	return OpenOptions(ctx, c)
}

// User-facing historical reports stay closed during bootstrap and copy. Internal
// replay, health, and recovery readers remain usable for cutover verification.
func RequireCompleteHistory(ctx context.Context, store any) error {
	if c, ok := store.(interface {
		HistoryReady(context.Context) (bool, error)
	}); ok {
		ready, e := c.HistoryReady(ctx)
		if e != nil {
			return e
		}
		if !ready {
			return contracts.ErrHistoryNotReady
		}
	}
	return nil
}
