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

	"github.com/vphoenix/crypto-market-info/internal/perpcontext"
	"github.com/vphoenix/crypto-market-info/internal/storage/codec"
)

// The Hyperliquid asset-context table is an optional runtime table outside
// the frozen 125-table migration catalog, like the JustLend market table.
// Collectors only check it; an administrator installs it explicitly.
var hyperliquidContextSchemaOnce sync.Once
var hyperliquidContextTables []Table
var hyperliquidContextSchemaErr error

const hyperliquidContextMaxRange = 7 * 24 * time.Hour

func HyperliquidContextTableNames() []string { return []string{perpcontext.TableName} }

func hyperliquidContextMappings() ([]Table, error) {
	hyperliquidContextSchemaOnce.Do(func() {
		text := func(name string, size int) Column {
			return Column{Name: name, SourceType: "String", MySQLType: fmt.Sprintf("VARBINARY(%d)", size)}
		}
		amount := func(name string) Column {
			return Column{Name: name, SourceType: "Decimal(38,18)", MySQLType: "DECIMAL(38,18)"}
		}
		small := func(name string) Column {
			return Column{Name: name, SourceType: "UInt16", MySQLType: "SMALLINT UNSIGNED"}
		}
		tiny := func(name string) Column {
			return Column{Name: name, SourceType: "UInt8", MySQLType: "TINYINT UNSIGNED"}
		}
		// Time leads the key: rows append in key order and a batch is one
		// contiguous range; one coin's history is a bounded range scan.
		t := Table{Name: perpcontext.TableName, Key: []string{"observation_time", "dex", "coin"}, Columns: []Column{
			text("dex", 16), text("coin", 64),
			{Name: "observation_time", SourceType: "DateTime64(3, 'UTC')", MySQLType: "DATETIME(3)"},
			small("universe_count"), small("batch_asset_count"), small("max_leverage"),
			tiny("sz_decimals"), tiny("only_isolated"),
			amount("mark_px"), amount("oracle_px"), amount("prev_day_px"), amount("funding_rate_hourly"),
			amount("open_interest"), amount("day_ntl_vlm"), amount("day_base_vlm"),
			tiny("book_valid"), amount("mid_px"), amount("premium"), amount("impact_bid_px"), amount("impact_ask_px"),
			{Name: "payload_hash", SourceType: "FixedString(32)", MySQLType: "BINARY(32)"},
		}}
		t.Database = "crypto_market_info"
		t.Engine = "RuntimeImmutable"
		t.TargetKey = append([]string(nil), t.Key...)
		lines := []string{}
		for n := range t.Columns {
			col := &t.Columns[n]
			col.Type, hyperliquidContextSchemaErr = codec.Parse(col.SourceType)
			if hyperliquidContextSchemaErr != nil {
				return
			}
			lines = append(lines, "  `"+col.Name+"` "+col.MySQLType+" NOT NULL")
		}
		lines = append(lines, "  PRIMARY KEY (`"+strings.Join(t.Key, "`,`")+"`)")
		t.DDL = "CREATE TABLE IF NOT EXISTS `crypto_market_info`.`" + t.Name + "` (\n" + strings.Join(lines, ",\n") + "\n) ENGINE=InnoDB ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8"
		hyperliquidContextTables = []Table{t}
	})
	return hyperliquidContextTables, hyperliquidContextSchemaErr
}

// InstallHyperliquidContextSchema is explicit administration, never collector
// DDL. An unregistered pre-existing table is refused rather than silently
// blessed.
func InstallHyperliquidContextSchema(ctx context.Context, db *sql.DB, target string) error {
	if !identifier.MatchString(target) {
		return fmt.Errorf("invalid Hyperliquid context schema target")
	}
	tables, err := hyperliquidContextMappings()
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
				return fmt.Errorf("Hyperliquid context schema version conflict for %s", t.Name)
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
			return fmt.Errorf("unregistered Hyperliquid context table %s: verify physical definition before registering", t.Name)
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

func (c *Client) CheckHyperliquidContextSchema(ctx context.Context) error {
	if err := c.checkSchema(ctx, HyperliquidContextTableNames()); err != nil {
		return err
	}
	_, err := readModels[perpcontext.Observation](ctx, c, c.db, perpcontext.TableName, "LIMIT 0")
	return err
}

// WriteHyperliquidContextBatch stores one complete batch in one transaction.
// A retry of the identical batch is a no-op; any different row under the same
// (observation_time, dex, coin) is a conflict, never an overwrite.
func (c *Client) WriteHyperliquidContextBatch(ctx context.Context, b perpcontext.Batch) error {
	if err := b.Validate(); err != nil {
		return err
	}
	return c.transaction(ctx, func(tx *sql.Tx) error {
		return putModelsBulk(ctx, c, tx, perpcontext.TableName, b.Assets)
	})
}

// ReadHyperliquidContextBatches returns one dex's complete batches with
// from <= observation_time < to, oldest first. A stored batch whose rows do
// not match its own batch_asset_count or payload hash is reported as an
// error, not returned as a smaller asset list.
func (c *Client) ReadHyperliquidContextBatches(ctx context.Context, dex string, from, to time.Time) ([]perpcontext.Batch, error) {
	if !perpcontext.ValidDex(dex) {
		return nil, fmt.Errorf("invalid dex %q", dex)
	}
	if !from.Before(to) || to.Sub(from) > hyperliquidContextMaxRange {
		return nil, fmt.Errorf("Hyperliquid context read requires 0 < range <= %s", hyperliquidContextMaxRange)
	}
	rows, err := readModels[perpcontext.Observation](ctx, c, c.db, perpcontext.TableName,
		"WHERE observation_time>=? AND observation_time<? AND dex=? ORDER BY observation_time,coin", from.UTC(), to.UTC(), []byte(dex))
	if err != nil {
		return nil, err
	}
	grouped := map[int64][]perpcontext.Observation{}
	for _, row := range rows {
		row.ObservationTime = row.ObservationTime.UTC()
		grouped[row.ObservationTime.UnixMilli()] = append(grouped[row.ObservationTime.UnixMilli()], row)
	}
	keys := make([]int64, 0, len(grouped))
	for k := range grouped {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	out := make([]perpcontext.Batch, 0, len(keys))
	for _, k := range keys {
		assets := grouped[k]
		batch := perpcontext.Batch{Dex: dex, CollectedAt: time.UnixMilli(k).UTC(), UniverseCount: assets[0].UniverseCount, Assets: assets}
		if err = batch.Validate(); err != nil {
			return nil, fmt.Errorf("stored Hyperliquid context batch %s: %w", batch.CollectedAt.Format(time.RFC3339Nano), err)
		}
		out = append(out, batch)
	}
	return out, nil
}
