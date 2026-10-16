package mysql

import (
	"context"
	"database/sql"
	"fmt"
)

// Query indexes are additive administrative schema changes. They do not change
// the pinned physical-column mapping or a migration's source/target row proof.
const keeperCaptureTimeIndex = "cmi_keeper_capture_time"

func checkKeeperCaptureTimeIndex(ctx context.Context, db *sql.DB, target string) (bool, error) {
	rows, e := db.QueryContext(ctx, "SELECT COLUMN_NAME,SUB_PART,NON_UNIQUE,IS_VISIBLE,COLLATION FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=? AND TABLE_NAME='jl_keeper_capture' AND INDEX_NAME=? ORDER BY SEQ_IN_INDEX", target, keeperCaptureTimeIndex)
	if e != nil {
		return false, e
	}
	defer rows.Close()
	want := []string{"capture_started_at", "capture_id"}
	i := 0
	for rows.Next() {
		var column, visible, order string
		var prefix sql.NullInt64
		var nonUnique int
		if e = rows.Scan(&column, &prefix, &nonUnique, &visible, &order); e != nil {
			return false, e
		}
		if i >= len(want) || column != want[i] || prefix.Valid || nonUnique != 1 || visible != "YES" || order != "A" {
			return false, fmt.Errorf("Keeper capture time index shape conflict")
		}
		i++
	}
	if e = rows.Err(); e != nil {
		return false, e
	}
	if i == 0 {
		return false, nil
	}
	if i != len(want) {
		return false, fmt.Errorf("Keeper capture time index shape conflict")
	}
	return true, nil
}

func initQueryIndexes(ctx context.Context, db *sql.DB, family, target string) error {
	if family == "crypto_market_info" {
		if err := initOptionsPlanQueryIndexes(ctx, db, target); err != nil {
			return err
		}
		return InstallDEXQueryIndexes(ctx, db, target)
	}
	if family == "crypto_market_info_reserve" {
		return InstallReceiptHashIndex(ctx, db, target)
	}
	if family != "crypto_market_info_justlend_keeper" {
		return nil
	}
	installed, e := checkKeeperCaptureTimeIndex(ctx, db, target)
	if e != nil {
		return e
	}
	if !identifier.MatchString(target) {
		return fmt.Errorf("invalid target database")
	}
	if !installed {
		if _, e = db.ExecContext(ctx, "ALTER TABLE `"+target+"`.`jl_keeper_capture` ADD INDEX `"+keeperCaptureTimeIndex+"` (`capture_started_at`,`capture_id`), ALGORITHM=INPLACE, LOCK=NONE"); e != nil {
			return fmt.Errorf("Keeper capture time index: %w", e)
		}
	}
	installed, e = checkKeeperCaptureTimeIndex(ctx, db, target)
	if e == nil && !installed {
		return fmt.Errorf("Keeper capture time index missing after installation")
	}
	if e != nil {
		return e
	}
	return initKeeperEnrichmentIndexes(ctx, db, target)
}

type keeperIndexColumn struct {
	name   string
	prefix int64
}

func initKeeperEnrichmentIndexes(ctx context.Context, db *sql.DB, target string) error {
	added := false
	indexes := []struct {
		name    string
		columns []keeperIndexColumn
	}{
		{"cmi_keeper_evidence_frontier", []keeperIndexColumn{{"config_hash", 0}, {"capture_kind", 32}, {"committed", 0}, {"status", 16}, {"event_kind", 32}, {"requested_to", 0}, {"requested_from", 0}, {"capture_started_at", 0}, {"capture_id", 0}}},
		{"cmi_keeper_enrichment_parent", []keeperIndexColumn{{"config_hash", 0}, {"capture_kind", 32}, {"committed", 0}, {"status", 16}, {"requested_from", 0}, {"capture_started_at", 0}, {"capture_id", 0}}},
		{"cmi_keeper_enrichment_child", []keeperIndexColumn{{"config_hash", 0}, {"capture_kind", 32}, {"committed", 0}, {"parent_capture_id", 0}, {"status", 16}, {"available_at", 0}}},
	}
	for _, index := range indexes {
		rows, err := db.QueryContext(ctx, "SELECT COLUMN_NAME,SUB_PART,NON_UNIQUE,IS_VISIBLE,COLLATION FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=? AND TABLE_NAME='jl_keeper_capture' AND INDEX_NAME=? ORDER BY SEQ_IN_INDEX", target, index.name)
		if err != nil {
			return err
		}
		n := 0
		for rows.Next() {
			var column, visible, order string
			var prefix sql.NullInt64
			var nonUnique int
			if err = rows.Scan(&column, &prefix, &nonUnique, &visible, &order); err != nil {
				rows.Close()
				return err
			}
			if n >= len(index.columns) || column != index.columns[n].name || prefix.Valid != (index.columns[n].prefix != 0) || (prefix.Valid && prefix.Int64 != index.columns[n].prefix) || nonUnique != 1 || visible != "YES" || order != "A" {
				rows.Close()
				return fmt.Errorf("Keeper enrichment index shape conflict: %s", index.name)
			}
			n++
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		if n == len(index.columns) {
			continue
		}
		if n != 0 {
			return fmt.Errorf("Keeper enrichment index incomplete: %s", index.name)
		}
		columns := ""
		for i, col := range index.columns {
			if i != 0 {
				columns += ","
			}
			columns += "`" + col.name + "`"
			if col.prefix != 0 {
				columns += fmt.Sprintf("(%d)", col.prefix)
			}
		}
		if _, err = db.ExecContext(ctx, "ALTER TABLE `"+target+"`.`jl_keeper_capture` ADD INDEX `"+index.name+"` ("+columns+"), ALGORITHM=INPLACE, LOCK=NONE"); err != nil {
			return fmt.Errorf("Keeper enrichment index %s: %w", index.name, err)
		}
		added = true
	}
	if added {
		return initKeeperEnrichmentIndexes(ctx, db, target)
	}
	return nil
}
