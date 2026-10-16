package mysql

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/across"
	keeper "github.com/vphoenix/crypto-market-info/internal/justlendkeeper"
	"github.com/vphoenix/crypto-market-info/internal/lst"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/replay"
	"github.com/vphoenix/crypto-market-info/internal/reserve"
	"github.com/vphoenix/crypto-market-info/internal/storage/codec"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
	"reflect"
	"strings"
	"time"
)

// VerifyMigratedDomain validates completed markers through the production
// readers. Uncommitted source rows retain their status and are not published.
func (c *Client) VerifyMigratedDomain(ctx context.Context, name string) error {

	supported := map[string]bool{"across_capture": true, "reserve_capture": true, "lst_capture": true, "jl_keeper_capture": true, "dex_block": true, "options_live_run": true, "options_collection_plan": true, "options_live_minute_commit": true, "options_catalog_live_minute_commit": true, "derivative_book_foundation_commit": true, "perpetual_universe_run": true}
	if !supported[name] {
		return nil
	}
	gaps, e := c.bootstrapGaps(ctx, name)
	if e != nil {
		return e
	}
	t, e := c.mapping(name)
	if e != nil {
		return e
	}
	filter := ""
	if name == "across_capture" || name == "reserve_capture" || name == "lst_capture" || name == "jl_keeper_capture" || name == "dex_block" {
		filter = "committed=1"
	}
	return c.walkMigrationRecordPages(ctx, name, filter, func(rows []record) error {
		if name == "across_capture" {
			caps := make([]across.Capture, 0, len(rows))
			for _, v := range rows {
				accepted, e := bootstrapGapMatches(t, MigrationRow(v), gaps)
				if e != nil {
					return e
				}
				if accepted {
					continue
				}
				cap, e := migrationModel[across.Capture](v)
				if e != nil {
					return e
				}
				caps = append(caps, cap)
			}
			load, e := c.AcrossBatches(ctx, caps)
			if e != nil {
				return e
			}
			for _, cap := range caps {
				if e := load.Errors[cap.CaptureId]; e != nil {
					return e
				}
				if _, ok := load.Batches[cap.CaptureId]; !ok {
					return fmt.Errorf("Across capture missing from bulk verification")
				}
			}
			return nil
		}
		for _, v := range rows {
			if e := c.verifyMigratedRecord(ctx, name, t, v, gaps); e != nil {
				return e
			}
		}
		return nil
	})
}

func (c *Client) verifyMigratedRecord(ctx context.Context, name string, t Table, v record, gaps map[[32]byte][32]byte) error {
	accepted, e := bootstrapGapMatches(t, MigrationRow(v), gaps)
	if e != nil {
		return e
	}
	if accepted {
		return nil
	}
	switch name {
	case "across_capture":
		cap, e := migrationModel[across.Capture](v)
		if e != nil {
			return e
		}
		_, e = c.AcrossBatch(ctx, cap)
		return e
	case "reserve_capture":
		cap, e := migrationModel[reserve.Capture](v)
		if e != nil {
			return e
		}
		_, e = c.ReserveBatch(ctx, cap)
		return e
	case "lst_capture":
		cap, e := migrationModel[lst.Capture](v)
		if e != nil {
			return e
		}
		_, e = c.LSTBatch(ctx, cap)
		return e
	case "jl_keeper_capture":
		cap, e := migrationModel[keeper.Capture](v)
		if e != nil {
			return e
		}
		_, e = c.KeeperBatch(ctx, cap)
		return e
	case "dex_block":
		return c.verifyDEXMembers(ctx, c.db, blockFrom(v))
	case "options_live_run":
		_, e := c.LoadOptionsRun(ctx, v["run_id"].(uuid.UUID))
		return e
	case "options_collection_plan":
		_, e := c.OptionsPlanAt(ctx, v["effective_minute"].(time.Time))
		return e
	case "options_live_minute_commit":
		_, e := c.LoadOptionsMinute(ctx, v["run_id"].(uuid.UUID), v["minute_time"].(time.Time))
		return e
	case "options_catalog_live_minute_commit":
		_, e := c.LoadOptionsCatalogMinute(ctx, v["run_id"].(uuid.UUID), v["minute_time"].(time.Time))
		return e
	case "derivative_book_foundation_commit":
		_, e := c.LoadDerivativeBookEnvelope(ctx, v["run_id"].(uuid.UUID), v["minute_time"].(time.Time))
		return e
	case "perpetual_universe_run":
		_, _, e := c.PerpetualUniverse(ctx, v["run_id"].(uuid.UUID))
		return e
	}
	return nil
}
func migrationModel[T any](r record) (T, error) {
	var out T
	v := reflect.ValueOf(&out).Elem()
	for i := 0; i < v.NumField(); i++ {
		name := v.Type().Field(i).Tag.Get("ch")
		x, ok := r[name]
		if !ok {
			return out, fmt.Errorf("missing migration model field %s", name)
		}
		if e := codec.Assign(v.Field(i), x); e != nil {
			return out, e
		}
	}
	return out, nil
}

// Each page closes its SQL rows before dependent recovery readers use the same
// one-connection migration pool. Key seeking bounds both memory and query work.
func (c *Client) walkMigrationRecords(ctx context.Context, name, filter string, visit func(record) error) error {
	return c.walkMigrationRecordPages(ctx, name, filter, func(rows []record) error {
		for _, r := range rows {
			if e := visit(r); e != nil {
				return e
			}
		}
		return nil
	})
}

func (c *Client) walkMigrationRecordPages(ctx context.Context, name, filter string, visit func([]record) error) error {
	t, e := c.mapping(name)
	if e != nil {
		return e
	}
	keys := []string{}
	types := map[string]*codec.Type{}
	for _, col := range t.Columns {
		types[col.Name] = col.Type
	}
	for _, key := range t.Key {
		keys = append(keys, "`"+key+"`")
	}
	if t.HashedKey {
		keys = []string{"`_key_hash`"}
	}
	tuple := "(" + strings.Join(keys, ",") + ")"
	var last record
	for {
		conditions := []string{}
		args := []any{}
		if filter != "" {
			conditions = append(conditions, filter)
		}
		if last != nil {
			conditions = append(conditions, tuple+">("+placeholders(len(keys))+")")
			if t.HashedKey {
				hash, e := keyHash(t, last)
				if e != nil {
					return e
				}
				args = append(args, hash[:])
			} else {
				for _, key := range t.Key {
					value, e := codec.SQLValue(types[key], last[key])
					if e != nil {
						return e
					}
					args = append(args, value)
				}
			}
		}
		where := ""
		if len(conditions) > 0 {
			where = "WHERE " + strings.Join(conditions, " AND ") + " "
		}
		rows, e := readRecords(ctx, c.db, c.table(name), t, "FORCE INDEX (PRIMARY) "+where+"ORDER BY "+strings.Join(keys, ",")+" LIMIT 100", args...)
		if e != nil {
			return e
		}
		if len(rows) == 0 {
			return nil
		}
		if e = visit(rows); e != nil {
			return e
		}
		last = rows[len(rows)-1]
	}
}

// PublishMigratedBook proves a complete historical minute after BOTH table
// copies were verified. It does not allocate identities or activate producers.
func (c *Client) PublishMigratedBook(ctx context.Context, run uuid.UUID, id uint32, minute time.Time) error {
	for _, name := range []string{"order_book_minute", "order_book_second_delta"} {
		var state uint8
		if e := c.db.QueryRowContext(ctx, "SELECT state FROM cmi_storage.migration_table WHERE run_id=? AND database_name=? AND table_name=?", run[:], c.database, name).Scan(&state); e != nil {
			return e
		}
		if state != 2 && state != MigrationRowsVerified {
			return contracts.ErrHistoryNotReady
		}
	}
	m, e := c.loadMinute(ctx, id, minute)
	if e != nil {
		return e
	}
	ds, e := c.loadDeltas(ctx, m.ID, 59)
	if e != nil {
		return e
	}
	batch := model.MinuteBatch{Minute: m, Deltas: ds}
	if e = contracts.ValidateMinuteBatch(batch); e != nil {
		return e
	}
	for second := uint8(0); second < 60; second++ {
		if _, _, e = replay.AtSecond(m, ds, second); e != nil {
			return e
		}
	}
	hash := digestBytes(model.DiscoveryHash(model.TradingMinuteBody(batch)))
	identifier := fmt.Sprintf("history:%s:%d", run.String(), m.ID)
	payloadHash := sha256.Sum256(hash)
	tx, e := c.db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	var old []byte
	e = tx.QueryRowContext(ctx, "SELECT member_hash FROM cmi_storage.ingest_member WHERE database_name=? AND kind='cex' AND member_id=? FOR UPDATE", c.database, m.ID).Scan(&old)
	if e == nil {
		if string(old) != string(hash) {
			return contracts.ErrConflict
		}
		return tx.Commit()
	}
	if e != sql.ErrNoRows {
		return e
	}
	_, e = tx.ExecContext(ctx, "INSERT INTO cmi_storage.ingest_batch(database_name,kind,batch_id,service,generation,logical_time,codec,content_hash,member_count,committed_at) VALUES(?,'cex',?,'history-migration',0,?,1,?,1,UTC_TIMESTAMP(6))", c.database, identifier, minute, payloadHash[:])
	if e != nil {
		return e
	}
	_, e = tx.ExecContext(ctx, "INSERT INTO cmi_storage.ingest_member(database_name,kind,member_id,batch_id,member_hash) VALUES(?,'cex',?,?,?)", c.database, m.ID, identifier, hash)
	if e != nil {
		return e
	}
	return tx.Commit()
}

// CompleteMigratedBooks is resumable: proofs already committed are checked on
// retry, and neither CEX table reaches state 2 until every minute is visible.
func (c *Client) CompleteMigratedBooks(ctx context.Context, run uuid.UUID) error {
	var count int
	if e := c.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM cmi_storage.migration_table WHERE run_id=? AND database_name=? AND table_name IN ('order_book_minute','order_book_second_delta') AND state IN (2,5)", run[:], c.database).Scan(&count); e != nil {
		return e
	}
	if count != 2 {
		return nil
	} // The companion table must be verified first.
	var lastID uint32
	lastTime := time.Date(1000, 1, 1, 0, 0, 0, 0, time.UTC)
	for {
		rows, e := c.db.QueryContext(ctx, "SELECT instrument_id,minute_time FROM "+c.table("order_book_minute")+" WHERE instrument_id>? OR (instrument_id=? AND minute_time>?) ORDER BY instrument_id,minute_time LIMIT 500", lastID, lastID, lastTime)
		if e != nil {
			return e
		}
		type key struct {
			id uint32
			at time.Time
		}
		keys := []key{}
		for rows.Next() {
			var k key
			if e = rows.Scan(&k.id, &k.at); e != nil {
				rows.Close()
				return e
			}
			keys = append(keys, k)
		}
		e = rows.Err()
		rows.Close()
		if e != nil {
			return e
		}
		if len(keys) == 0 {
			break
		}
		for _, k := range keys {
			if e = c.PublishMigratedBook(ctx, run, k.id, k.at); e != nil {
				return e
			}
			lastID, lastTime = k.id, k.at
		}
	}
	tx, e := c.db.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	// Lock both state rows before the last completeness check and transition.
	rows, e := tx.QueryContext(ctx, "SELECT state FROM cmi_storage.migration_table WHERE run_id=? AND database_name=? AND table_name IN ('order_book_minute','order_book_second_delta') FOR UPDATE", run[:], c.database)
	if e != nil {
		return e
	}
	count = 0
	for rows.Next() {
		var state uint8
		if e = rows.Scan(&state); e != nil {
			rows.Close()
			return e
		}
		if state != 2 && state != MigrationRowsVerified {
			rows.Close()
			return contracts.ErrHistoryNotReady
		}
		count++
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return e
	}
	if count != 2 {
		return contracts.ErrHistoryNotReady
	}
	if e = verifyBookPublicationCoverage(ctx, tx, c.database); e != nil {
		return e
	}
	if _, e = tx.ExecContext(ctx, "UPDATE cmi_storage.migration_table SET state=2,error=NULL,updated_at=UTC_TIMESTAMP(6) WHERE run_id=? AND database_name=? AND table_name IN ('order_book_minute','order_book_second_delta')", run[:], c.database); e != nil {
		return e
	}
	return tx.Commit()
}

func verifyBookPublicationCoverage(ctx context.Context, q executor, database string) error {
	if !identifier.MatchString(database) {
		return fmt.Errorf("invalid publication database")
	}
	var missing int
	e := q.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM `"+database+"`.`order_book_minute` f WHERE NOT EXISTS (SELECT 1 FROM cmi_storage.ingest_member m JOIN cmi_storage.ingest_batch b ON b.database_name=m.database_name AND b.kind=m.kind AND b.batch_id=m.batch_id WHERE m.database_name=? AND m.kind='cex' AND m.member_id=f.id))", database).Scan(&missing)
	if e != nil {
		return e
	}
	if missing != 0 {
		return fmt.Errorf("historical book publication incomplete: %w", contracts.ErrHistoryNotReady)
	}
	return nil
}
