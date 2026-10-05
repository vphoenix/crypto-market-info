package clickhouse

import (
	"context"
	ch "github.com/ClickHouse/clickhouse-go/v2"
	"sync"
	"time"
)

// A dedicated pool keeps catalog fanout from occupying CEX writer connections.
// Three minute slots leave five connections for discovery and evidence writes.
func (c *Client) OptionsWriter(ctx context.Context, cfg Config) (*Client, error) {
	c.instrumentOnce.Do(func() {
		if c.instrumentMu == nil {
			c.instrumentMu = &sync.Mutex{}
		}
	})
	addr := cfg.Addresses
	if len(addr) == 0 {
		addr = []string{"127.0.0.1:9000"}
	}
	user := cfg.Username
	if user == "" {
		user = "default"
	}
	timeout := cfg.DialTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	conn, err := ch.Open(&ch.Options{Addr: addr, Auth: ch.Auth{Database: c.database, Username: user, Password: cfg.Password}, DialTimeout: timeout, MaxOpenConns: 8, MaxIdleConns: 8, Settings: ch.Settings{"date_time_input_format": "best_effort"}})
	if err != nil {
		return nil, err
	}
	out := &Client{conn: conn, database: c.database, storeIdentity: c.storeIdentity, writeTimeout: c.writeTimeout, maxAttempts: c.maxAttempts, retryDelay: c.retryDelay, instrumentMu: c.instrumentMu}
	if err = conn.Ping(ctx); err != nil {
		conn.Close()
		return nil, err
	}
	return out, nil
}
