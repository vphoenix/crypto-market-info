package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
	"time"
)

type TableHealth struct {
	Database      string `json:"database"`
	Table         string `json:"table"`
	SchemaVersion uint16 `json:"schema_version"`
	Rows          uint64 `json:"rows"`
	RowsExact     bool   `json:"rows_exact"`
}
type StreamHealth struct {
	Exchange   string           `json:"exchange"`
	MarketType model.MarketType `json:"market_type"`
	Symbol     string           `json:"exchange_symbol"`
	Latest     *time.Time       `json:"latest_minute"`
	Age        int64            `json:"age_seconds"`
	Valid      uint64           `json:"valid_seconds"`
}
type Health struct {
	Backend              string `json:"backend"`
	Version              string `json:"version"`
	UTC                  bool   `json:"utc"`
	RedoSync, BinlogSync uint8
	Doublewrite          string
	Tables               []TableHealth  `json:"tables"`
	RecentBatches        uint64         `json:"recent_committed_batches"`
	LatestCommit         *time.Time     `json:"latest_commit"`
	HistoryComplete      bool           `json:"history_complete"`
	Streams              []StreamHealth `json:"streams,omitempty"`
	YieldAge             *int64         `json:"yield_age_seconds"`
}

func (c *Client) Inspect(ctx context.Context, countFacts bool) (Health, error) {
	out := Health{Backend: "mysql"}
	var zone string
	if e := c.db.QueryRowContext(ctx, "SELECT VERSION(),@@session.time_zone,@@global.innodb_flush_log_at_trx_commit,@@global.sync_binlog,@@global.innodb_doublewrite").Scan(&out.Version, &zone, &out.RedoSync, &out.BinlogSync, &out.Doublewrite); e != nil {
		return out, e
	}
	out.UTC = zone == "+00:00"
	if !out.UTC || out.RedoSync != 1 || out.BinlogSync != 1 || out.Doublewrite != "ON" {
		return out, fmt.Errorf("MySQL durability/UTC configuration mismatch")
	}
	catalog, e := Mappings()
	if e != nil {
		return out, e
	}
	for _, t := range catalog.Tables {
		if t.Database != c.database {
			continue
		}
		var h TableHealth
		h.Database = c.database
		h.Table = t.Name
		if e = c.checkSchema(ctx, []string{t.Name}); e != nil {
			return out, e
		}
		h.SchemaVersion = 1
		if countFacts {
			if e = c.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+c.table(t.Name)).Scan(&h.Rows); e != nil {
				return out, e
			}
			h.RowsExact = true
		}
		out.Tables = append(out.Tables, h)
	}
	var latest sql.NullTime
	if e = c.db.QueryRowContext(ctx, "SELECT COUNT(*),MAX(committed_at) FROM cmi_storage.ingest_batch WHERE database_name=? AND committed_at>=UTC_TIMESTAMP()-INTERVAL 10 MINUTE", c.database).Scan(&out.RecentBatches, &latest); e != nil {
		return out, e
	}
	if latest.Valid {
		out.LatestCommit = &latest.Time
	}
	var complete int
	if e = c.db.QueryRowContext(ctx, "SELECT COUNT(DISTINCT m.table_name) FROM cmi_storage.migration_table m JOIN cmi_storage.migration_run r ON r.run_id=m.run_id WHERE r.state=3 AND m.state=2 AND m.database_name=?", c.database).Scan(&complete); e != nil {
		return out, e
	}
	out.HistoryComplete = complete == len(out.Tables) && len(out.Tables) > 0
	if c.database != "crypto_market_info" {
		return out, nil
	}
	rows, e := c.db.QueryContext(ctx, `SELECT i.exchange,i.market_type,i.exchange_symbol,b.minute_time,COALESCE(TIMESTAMPDIFF(SECOND,b.minute_time,UTC_TIMESTAMP()),999999),COALESCE(BIT_COUNT(b.valid_bitmap),0) FROM (SELECT exchange,market_type,exchange_symbol,MAX(instrument_id) AS instrument_id FROM `+c.table("instrument")+` WHERE (exchange='Binance' AND exchange_symbol='BTCUSDT') OR (exchange='OKX' AND exchange_symbol IN ('BTC-USDT','BTC-USDT-SWAP')) OR (exchange='Bybit' AND exchange_symbol='BTCUSDT') GROUP BY exchange,market_type,exchange_symbol) i LEFT JOIN (SELECT b1.instrument_id,b1.minute_time,b1.valid_bitmap FROM `+c.table("order_book_minute")+` b1 JOIN (SELECT instrument_id,MAX(minute_time) AS minute_time FROM `+c.table("order_book_minute")+` WHERE minute_time>=UTC_TIMESTAMP()-INTERVAL 10 MINUTE GROUP BY instrument_id) b2 ON b1.instrument_id=b2.instrument_id AND b1.minute_time=b2.minute_time JOIN cmi_storage.ingest_member m ON m.database_name=? AND m.kind='cex' AND m.member_id=b1.id JOIN cmi_storage.ingest_batch c ON c.database_name=m.database_name AND c.kind=m.kind AND c.batch_id=m.batch_id) b ON b.instrument_id=i.instrument_id ORDER BY i.exchange,i.market_type`, c.database)
	if e != nil {
		return out, e
	}
	for rows.Next() {
		var s StreamHealth
		var latest sql.NullTime
		var market string
		if e = rows.Scan(&s.Exchange, &market, &s.Symbol, &latest, &s.Age, &s.Valid); e != nil {
			rows.Close()
			return out, e
		}
		s.MarketType = model.MarketType(market)
		if s.MarketType != model.MarketSpot && s.MarketType != model.MarketPerpetual {
			rows.Close()
			return out, fmt.Errorf("unsupported desktop market")
		}

		if latest.Valid {
			s.Latest = &latest.Time
		}
		out.Streams = append(out.Streams, s)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return out, e
	}
	var age sql.NullInt64
	if e = c.db.QueryRowContext(ctx, "SELECT TIMESTAMPDIFF(SECOND,MAX(collected_at),UTC_TIMESTAMP()) FROM "+c.table("yield_observation")+" WHERE collected_at>=UTC_TIMESTAMP()-INTERVAL 8 HOUR").Scan(&age); e != nil {
		return out, e
	}
	if age.Valid {
		out.YieldAge = &age.Int64
	}
	return out, nil
}

func (c *Client) PreviewCommitted(ctx context.Context, frame contracts.PreviewFrame) (bool, error) {
	minute := frame.At.UTC().Truncate(time.Minute)
	if frame.Kind == "options_live" || frame.Kind == "options_catalog" {
		id, e := uuid.Parse(frame.Scope)
		if e != nil {
			return false, e
		}
		if frame.Kind == "options_catalog" {
			_, e = c.LoadOptionsCatalogMinute(ctx, id, minute)
		} else {
			_, e = c.LoadOptionsMinute(ctx, id, minute)
		}
		if errors.Is(e, contracts.ErrNotFound) {
			return false, nil
		}
		return e == nil, e
	}
	ids := []any{c.database}
	for _, book := range frame.Books {
		if !book.Anchored {
			continue
		}
		id, e := model.MinuteID(book.InstrumentID, minute)
		if e != nil {
			return false, e
		}
		ids = append(ids, id)
	}
	if len(ids) == 1 {
		return true, nil
	}
	var count int
	e := c.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM cmi_storage.ingest_member m JOIN cmi_storage.ingest_batch b ON b.database_name=m.database_name AND b.kind=m.kind AND b.batch_id=m.batch_id WHERE m.database_name=? AND m.kind='cex' AND m.member_id IN ("+placeholders(len(ids)-1)+")", ids...).Scan(&count)
	return count == len(ids)-1, e
}

func (c *Client) HistoryReady(ctx context.Context) (bool, error) {
	catalog, e := Mappings()
	if e != nil {
		return false, e
	}
	family := c.family
	if family == "" {
		family = c.database
	}
	expected := 0
	for _, t := range catalog.Tables {
		if t.Database == family {
			expected++
		}
	}
	var count int
	e = c.db.QueryRowContext(ctx, "SELECT COUNT(DISTINCT m.table_name) FROM cmi_storage.migration_table m JOIN cmi_storage.migration_run r ON r.run_id=m.run_id WHERE r.state=3 AND m.state=2 AND m.database_name=?", c.database).Scan(&count)
	return expected > 0 && count == expected, e
}
