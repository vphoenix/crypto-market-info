package mysql

import (
	"context"
	"database/sql"
	sqlDriver "database/sql/driver"
	"errors"
	"fmt"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	driver "github.com/go-sql-driver/mysql"
)

type Config struct {
	Socket         string
	Database       string
	Username       string
	Password       string
	PasswordFile   string
	MaxConnections int
	ReadOnly       bool
	// ReadTimeout is an explicit operator override for bounded migration reads.
	// Zero retains the 30-second collector default; never exceed two minutes.
	ReadTimeout time.Duration
	// Server-side limits are set by the connector on every physical connection.
	SelectTimeout     time.Duration
	LockWaitTimeout   time.Duration
	SchemaTables      []string
	SchemaInvalidated func()
	// The minute writer prepares cold publication/fact index pages before fencing.
	PrefetchMinuteIndexes bool
	CapitalRuleVersions   bool
	// QualityStorageV2 writes new minutes to the optional lossless quality tables.
	// Installed extensions remain readable when this write switch is disabled.
	QualityStorageV2 bool
	SourceStorageV2  bool
	// Family selects an explicit reviewed mapping when Database is an isolated
	// validation database. Production normally leaves this empty.
	Family   string
	Producer Producer
}
type Client struct {
	keeperDynamicSchemaMu        sync.Mutex
	keeperDynamicSchemaChecked   bool
	keeperDynamicSchemaAvailable bool
	keeperRentalSchemaMu         sync.Mutex
	keeperRentalSchemaChecked    bool
	keeperRentalSchemaAvailable  bool
	db                           *sql.DB
	database                     string
	readOnly                     bool
	family                       string
	producer                     Producer
	writeOnce                    sync.Once
	writeSlot                    chan struct{}
	discoveryEnabled             atomic.Bool
	queue                        contracts.DurableQueue
	queueClose                   func() error
	preview                      func(contracts.PreviewFrame)
	metadata                     *MetadataCache
	prefetchMinuteIndexes        bool
	capitalRuleVersions          bool
	qualityStorageV2             bool
	qualityStorageAvailable      bool
	sourceStorageV2              bool
	sourceStorageAvailable       bool
}

func Connect(ctx context.Context, cfg Config) (*sql.DB, error) {
	if cfg.Socket == "" || !strings.HasPrefix(cfg.Socket, "/") || cfg.Username == "" || cfg.MaxConnections < 1 || cfg.MaxConnections > 16 {
		return nil, fmt.Errorf("MySQL requires explicit Unix socket, user and bounded connection quota")
	}
	if cfg.Database != "" && !identifier.MatchString(cfg.Database) {
		return nil, fmt.Errorf("invalid MySQL database")
	}
	if cfg.ReadTimeout < 0 || cfg.ReadTimeout > 2*time.Minute {
		return nil, fmt.Errorf("MySQL read timeout must be between zero and two minutes")
	}
	if cfg.PasswordFile != "" {
		if cfg.Password != "" {
			return nil, fmt.Errorf("password has two sources")
		}
		s, e := os.Stat(cfg.PasswordFile)
		if e != nil {
			return nil, e
		}
		if s.Mode().Perm()&0077 != 0 {
			return nil, fmt.Errorf("password file must be private")
		}
		p, e := os.ReadFile(cfg.PasswordFile)
		if e != nil {
			return nil, e
		}
		cfg.Password = string(p)
	}
	c := driver.NewConfig()
	c.Net = "unix"
	c.Addr = cfg.Socket
	c.DBName = cfg.Database
	c.User = cfg.Username
	c.Passwd = cfg.Password
	c.ParseTime = true
	c.Loc = time.UTC
	c.Timeout = 5 * time.Second
	c.ReadTimeout = 30 * time.Second
	if cfg.ReadTimeout > 0 {
		c.ReadTimeout = cfg.ReadTimeout
	}
	c.WriteTimeout = 30 * time.Second
	c.Params = map[string]string{"time_zone": "'+00:00'", "sql_mode": "'STRICT_ALL_TABLES,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION,NO_ZERO_DATE,NO_ZERO_IN_DATE'"}
	if cfg.SelectTimeout < 0 || cfg.SelectTimeout > 2*time.Minute || cfg.LockWaitTimeout < 0 || cfg.LockWaitTimeout > 40*time.Second {
		return nil, fmt.Errorf("invalid server execution or lock wait timeout")
	}
	if cfg.ReadOnly && cfg.SelectTimeout == 0 {
		cfg.SelectTimeout = 15 * time.Second
		if cfg.ReadTimeout > 0 {
			cfg.SelectTimeout = cfg.ReadTimeout
		}
	}
	if cfg.SelectTimeout > 0 {
		c.Params["max_execution_time"] = fmt.Sprint(max(1, cfg.SelectTimeout.Milliseconds()))
	}
	if cfg.LockWaitTimeout > 0 {
		c.Params["innodb_lock_wait_timeout"] = fmt.Sprint(max(1, int64(cfg.LockWaitTimeout/time.Second)))
	}
	if cfg.ReadOnly {
		c.Params["transaction_read_only"] = "1"
	}
	connector, e := driver.NewConnector(c)
	if e != nil {
		return nil, e
	}
	var physical sqlDriver.Connector = connector
	if len(cfg.SchemaTables) > 0 {
		checked, e := schemaCheckedConnector(connector, cfg)
		if e != nil {
			return nil, e
		}
		physical = checked
	}
	db := sql.OpenDB(physical)
	db.SetMaxOpenConns(cfg.MaxConnections)
	db.SetMaxIdleConns(cfg.MaxConnections)
	// Keep healthy pooled connections for the client's lifetime. Forced churn
	// can reopen a slot before MySQL finishes closing the previous connection,
	// exhausting the shared live-account quota during recovery. Reconnection
	// still uses the checked connector; the writer also checks schema periodically.
	db.SetConnMaxLifetime(0)
	// With a one-connection migration account, the previous pool's COM_QUIT
	// can still be pending when the next table opens. Wait briefly for that
	// same quota to become available; never increase it or evict another user.
	deadline := time.Now().Add(2 * time.Second)
	for {
		e = db.PingContext(ctx)
		var quota *driver.MySQLError
		if e == nil || !errors.As(e, &quota) || quota.Number != 1226 || !time.Now().Before(deadline) {
			break
		}
		timer := time.NewTimer(50 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			e = ctx.Err()
		case <-timer.C:
		}
		if ctx.Err() != nil {
			break
		}
	}
	if e != nil {
		db.Close()
		return nil, fmt.Errorf("connect MySQL: %w", e)
	}
	return db, nil
}
func Open(ctx context.Context, cfg Config) (*Client, error) {
	if cfg.Database == "" {
		return nil, fmt.Errorf("MySQL database must be explicit")
	}
	family := cfg.Family
	if family == "" {
		family = cfg.Database
	}
	if cfg.ReadTimeout == 0 {
		if family == "crypto_market_info_justlend_keeper" && cfg.SelectTimeout == 0 {
			cfg.SelectTimeout = 15 * time.Second
		}
		if !cfg.ReadOnly && cfg.LockWaitTimeout == 0 {
			cfg.LockWaitTimeout = 5 * time.Second
		}
	}
	singleConnectionDomain := false
	for _, marker := range []string{"across_capture", "reserve_capture", "lst_capture", "jl_keeper_capture"} {
		if _, e := Mapping(family, marker); e == nil {
			singleConnectionDomain = true
		}
	}
	if !cfg.ReadOnly && cfg.MaxConnections < 2 && !singleConnectionDomain {
		return nil, fmt.Errorf("domain writer requires two connections for reference checks")
	}
	if cfg.CapitalRuleVersions {
		if family != "crypto_market_info" {
			return nil, fmt.Errorf("capital rule versions require main schema family")
		}
		cfg.SchemaTables = append(append([]string(nil), cfg.SchemaTables...), CapitalVersionTableNames()...)
	}
	if cfg.QualityStorageV2 {
		if family != "crypto_market_info" {
			return nil, fmt.Errorf("quality storage requires main schema family")
		}
		cfg.SchemaTables = append(append([]string(nil), cfg.SchemaTables...), QualityStorageTableNames()...)
	}
	if cfg.SourceStorageV2 {
		if family != "crypto_market_info" {
			return nil, fmt.Errorf("source storage requires main schema family")
		}
		cfg.SchemaTables = append(append([]string(nil), cfg.SchemaTables...), SourceStorageTableNames()...)
	}
	db, e := Connect(ctx, cfg)
	if e != nil {
		return nil, e
	}
	c := &Client{db: db, database: cfg.Database, family: cfg.Family, readOnly: cfg.ReadOnly, producer: cfg.Producer, prefetchMinuteIndexes: cfg.PrefetchMinuteIndexes, capitalRuleVersions: cfg.CapitalRuleVersions, qualityStorageV2: cfg.QualityStorageV2}
	c.sourceStorageV2 = cfg.SourceStorageV2
	if family == "crypto_market_info" {
		if e = c.discoverQualityStorage(ctx); e != nil {
			db.Close()
			return nil, e
		}
		if e = c.discoverSourceStorage(ctx); e != nil {
			db.Close()
			return nil, e
		}
	}
	return c, nil
}
func (c *Client) Close() error {
	if c == nil || c.db == nil {
		return nil
	}
	var e error
	if c.queueClose != nil {
		e = c.queueClose()
	}
	return errors.Join(e, c.db.Close())
}
func (c *Client) table(name string) string { return "`" + c.database + "`.`" + name + "`" }
func splitDDL(s string) []string {
	out := []string{}
	for _, v := range strings.Split(s, ";") {
		if v = strings.TrimSpace(v); v != "" {
			out = append(out, v)
		}
	}
	return out
}

// SetDeliveryQueue makes minute writes acknowledge durable queue ownership.
// Low-frequency and metadata calls continue to acknowledge a MySQL commit.
func (c *Client) SetDeliveryQueue(q contracts.DurableQueue, close func() error) {
	c.queue = q
	c.queueClose = close
}

func (c *Client) SetPreview(offer func(contracts.PreviewFrame)) { c.preview = offer }
func (c *Client) OfferPreview(f contracts.PreviewFrame) {
	if c.preview != nil {
		c.preview(f)
	}
}
