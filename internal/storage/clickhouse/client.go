package clickhouse

import (
	"context"
	"fmt"
	"sync"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type Config struct {
	Addresses    []string
	Database     string
	Username     string
	Password     string
	DialTimeout  time.Duration
	WriteTimeout time.Duration
	MaxAttempts  int
	RetryDelay   time.Duration
}

type Client struct {
	conn         driver.Conn
	database     string
	writeTimeout time.Duration
	maxAttempts  int
	retryDelay   time.Duration
	yieldMu      sync.Mutex
	yieldLoaded  bool
	yieldByKey   map[string]yieldRouteEntry
	yieldMaxID   uint32
}

func Open(ctx context.Context, cfg Config) (*Client, error) {
	if len(cfg.Addresses) == 0 {
		cfg.Addresses = []string{"127.0.0.1:9000"}
	}
	if cfg.Database == "" {
		cfg.Database = "crypto_market_info"
	}
	if !identifierPattern.MatchString(cfg.Database) {
		return nil, fmt.Errorf("invalid ClickHouse database identifier %q", cfg.Database)
	}
	if cfg.Username == "" {
		cfg.Username = "default"
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = 5 * time.Second
	}
	if cfg.WriteTimeout <= 0 {
		cfg.WriteTimeout = 10 * time.Second
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 3
	}
	if cfg.RetryDelay <= 0 {
		cfg.RetryDelay = 250 * time.Millisecond
	}
	options := func(database string) *ch.Options {
		return &ch.Options{Addr: cfg.Addresses, Auth: ch.Auth{Database: database, Username: cfg.Username, Password: cfg.Password}, DialTimeout: cfg.DialTimeout, Settings: ch.Settings{"date_time_input_format": "best_effort"}}
	}
	bootstrap, err := ch.Open(options("default"))
	if err != nil {
		return nil, err
	}
	statements, err := SchemaStatements(cfg.Database)
	if err != nil {
		bootstrap.Close()
		return nil, err
	}
	if err = bootstrap.Exec(ctx, statements[0]); err != nil {
		bootstrap.Close()
		return nil, fmt.Errorf("create ClickHouse database: %w", err)
	}
	if err = bootstrap.Close(); err != nil {
		return nil, err
	}
	conn, err := ch.Open(options(cfg.Database))
	if err != nil {
		return nil, err
	}
	client := &Client{conn: conn, database: cfg.Database, writeTimeout: cfg.WriteTimeout, maxAttempts: cfg.MaxAttempts, retryDelay: cfg.RetryDelay}
	if err = client.conn.Ping(ctx); err != nil {
		client.conn.Close()
		return nil, fmt.Errorf("ping ClickHouse: %w", err)
	}
	return client, nil
}

func (c *Client) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

func (c *Client) InitSchema(ctx context.Context) error {
	if c == nil || c.conn == nil {
		return fmt.Errorf("ClickHouse client is nil")
	}
	statements, err := SchemaStatements(c.database)
	if err != nil {
		return err
	}
	for _, statement := range statements[1:] {
		if err = c.conn.Exec(ctx, statement); err != nil {
			return fmt.Errorf("initialize ClickHouse schema: %w", err)
		}
	}
	return nil
}

func (c *Client) table(name string) string { return "`" + c.database + "`.`" + name + "`" }

func (c *Client) retryWrite(ctx context.Context, operation func(context.Context) error) error {
	var last error
	for attempt := 1; attempt <= c.maxAttempts; attempt++ {
		writeCtx, cancel := context.WithTimeout(ctx, c.writeTimeout)
		last = operation(writeCtx)
		cancel()
		if last == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if attempt < c.maxAttempts {
			timer := time.NewTimer(c.retryDelay * time.Duration(1<<(attempt-1)))
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
	}
	return last
}
