package mysql

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/lendingmarket"
	"github.com/vphoenix/crypto-market-info/internal/storage/codec"
)

// The JustLend market table is an optional runtime table outside the frozen
// 125-table migration catalog, like the capital-version tables. Collectors
// only check it; an administrator installs it explicitly.
var justLendMarketSchemaOnce sync.Once
var justLendMarketTables []Table
var justLendMarketSchemaErr error

const justLendMarketMaxRange = 31 * 24 * time.Hour

func JustLendMarketTableNames() []string { return []string{lendingmarket.TableName} }

func justLendMarketMappings() ([]Table, error) {
	justLendMarketSchemaOnce.Do(func() {
		text := func(name string) Column { return Column{Name: name, SourceType: "String", MySQLType: "VARBINARY(64)"} }
		amount := func(name string) Column {
			return Column{Name: name, SourceType: "Decimal(38,18)", MySQLType: "DECIMAL(38,18)"}
		}
		// Time leads the key: rows append in key order and a batch is one
		// contiguous range; one market's history is a bounded range scan.
		t := Table{Name: lendingmarket.TableName, Key: []string{"observation_time", "market_address"}, Columns: []Column{
			text("market_address"),
			{Name: "observation_time", SourceType: "DateTime64(3, 'UTC')", MySQLType: "DATETIME(3)"},
			{Name: "batch_market_count", SourceType: "UInt16", MySQLType: "SMALLINT UNSIGNED"},
			text("market_symbol"), text("underlying_address"), text("underlying_symbol"),
			{Name: "underlying_decimals", SourceType: "UInt8", MySQLType: "TINYINT UNSIGNED"},
			amount("supply_rate_apr"), amount("borrow_rate_apr"), amount("cash"), amount("total_borrows"),
			amount("reserves"), amount("total_supply"), amount("exchange_rate"), amount("borrow_index"),
			amount("collateral_factor"), amount("reserve_factor"), amount("underlying_price_trx"),
			{Name: "payload_hash", SourceType: "FixedString(32)", MySQLType: "BINARY(32)"},
		}}
		t.Database = "crypto_market_info"
		t.Engine = "RuntimeImmutable"
		t.TargetKey = append([]string(nil), t.Key...)
		lines := []string{}
		for n := range t.Columns {
			col := &t.Columns[n]
			col.Type, justLendMarketSchemaErr = codec.Parse(col.SourceType)
			if justLendMarketSchemaErr != nil {
				return
			}
			lines = append(lines, "  `"+col.Name+"` "+col.MySQLType+" NOT NULL")
		}
		lines = append(lines, "  PRIMARY KEY (`"+strings.Join(t.Key, "`,`")+"`)")
		t.DDL = "CREATE TABLE IF NOT EXISTS `crypto_market_info`.`" + t.Name + "` (\n" + strings.Join(lines, ",\n") + "\n) ENGINE=InnoDB ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8"
		justLendMarketTables = []Table{t}
	})
	return justLendMarketTables, justLendMarketSchemaErr
}

// InstallJustLendMarketSchema is explicit administration, never collector DDL.
// An unregistered pre-existing table is refused rather than silently blessed.
func InstallJustLendMarketSchema(ctx context.Context, db *sql.DB, target string) error {
	if !identifier.MatchString(target) {
		return fmt.Errorf("invalid JustLend market schema target")
	}
	tables, err := justLendMarketMappings()
	if err != nil {
		return err
	}
	for _, t := range tables {
		hash := sha256.Sum256([]byte(t.DDL))
		var version uint16
		var old []byte
		err = db.QueryRowContext(ctx, "SELECT version,ddl_hash FROM cmi_storage.schema_version WHERE database_name=? AND table_name=?", target, t.Name).Scan(&version, &old)
		if err == nil {
			if version != 1 || string(old) != string(hash[:]) {
				return fmt.Errorf("JustLend market schema version conflict for %s", t.Name)
			}
			continue
		}
		if err != sql.ErrNoRows {
			return err
		}
		var exists int
		if err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_NAME=?", target, t.Name).Scan(&exists); err != nil {
			return err
		}
		if exists != 0 {
			return fmt.Errorf("unregistered JustLend market table %s: verify physical definition before registering", t.Name)
		}
		ddl := strings.Replace(t.DDL, "`crypto_market_info`.", "`"+target+"`.", 1)
		if _, err = db.ExecContext(ctx, ddl); err != nil {
			return err
		}
		if _, err = db.ExecContext(ctx, "INSERT INTO cmi_storage.schema_version(database_name,table_name,version,ddl_hash,installed_at) VALUES(?,?,?,?,UTC_TIMESTAMP(6))", target, t.Name, 1, hash[:]); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) CheckJustLendMarketSchema(ctx context.Context) error {
	if err := c.checkSchema(ctx, JustLendMarketTableNames()); err != nil {
		return err
	}
	_, err := readModels[lendingmarket.Observation](ctx, c, c.db, lendingmarket.TableName, "LIMIT 0")
	return err
}

// WriteJustLendMarketBatch stores one complete batch in one transaction. A
// retry of the identical batch is a no-op; any different row under the same
// (observation_time, market_address) is a conflict, never an overwrite.
func (c *Client) WriteJustLendMarketBatch(ctx context.Context, b lendingmarket.Batch) error {
	if err := b.Validate(); err != nil {
		return err
	}
	return c.transaction(ctx, func(tx *sql.Tx) error {
		return putModelsBulk(ctx, c, tx, lendingmarket.TableName, b.Markets)
	})
}

// ReadJustLendMarketBatches returns the complete batches with
// from <= observation_time < to, oldest first. A stored batch whose rows do
// not match its own batch_market_count or payload hash is reported as an
// error, not returned as a smaller market list.
func (c *Client) ReadJustLendMarketBatches(ctx context.Context, from, to time.Time) ([]lendingmarket.Batch, error) {
	if !from.Before(to) || to.Sub(from) > justLendMarketMaxRange {
		return nil, fmt.Errorf("JustLend market read requires 0 < range <= %s", justLendMarketMaxRange)
	}
	rows, err := readModels[lendingmarket.Observation](ctx, c, c.db, lendingmarket.TableName,
		"WHERE observation_time>=? AND observation_time<? ORDER BY observation_time,market_address", from.UTC(), to.UTC())
	if err != nil {
		return nil, err
	}
	grouped := map[int64][]lendingmarket.Observation{}
	for _, row := range rows {
		row.ObservationTime = row.ObservationTime.UTC()
		grouped[row.ObservationTime.UnixMilli()] = append(grouped[row.ObservationTime.UnixMilli()], row)
	}
	keys := make([]int64, 0, len(grouped))
	for k := range grouped {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	out := make([]lendingmarket.Batch, 0, len(keys))
	for _, k := range keys {
		batch := lendingmarket.Batch{CollectedAt: time.UnixMilli(k).UTC(), Markets: grouped[k]}
		if err = batch.Validate(); err != nil {
			return nil, fmt.Errorf("stored JustLend market batch %s: %w", batch.CollectedAt.Format(time.RFC3339Nano), err)
		}
		out = append(out, batch)
	}
	return out, nil
}
