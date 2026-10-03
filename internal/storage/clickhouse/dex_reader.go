package clickhouse

import (
	"context"
	"fmt"
	ch "github.com/ClickHouse/clickhouse-go/v2"
	"time"
)

func openDEXReader(ctx context.Context, cfg Config) (*Client, error) {
	if cfg.Database == "" {
		cfg.Database = "crypto_market_info"
	}
	if !identifierPattern.MatchString(cfg.Database) {
		return nil, fmt.Errorf("invalid database")
	}
	if len(cfg.Addresses) == 0 {
		cfg.Addresses = []string{"127.0.0.1:9000"}
	}
	if cfg.Username == "" {
		cfg.Username = "default"
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = 5 * time.Second
	}
	conn, e := ch.Open(&ch.Options{Addr: cfg.Addresses, Auth: ch.Auth{Database: cfg.Database, Username: cfg.Username, Password: cfg.Password}, DialTimeout: cfg.DialTimeout, Settings: ch.Settings{"readonly": 1}})
	if e != nil {
		return nil, e
	}
	if e = conn.Ping(ctx); e != nil {
		conn.Close()
		return nil, e
	}
	return &Client{conn: conn, database: cfg.Database, readOnly: true}, nil
}
