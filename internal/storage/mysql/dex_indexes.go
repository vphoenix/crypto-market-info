package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

type dexQueryIndex struct {
	name    string
	columns []keeperIndexColumn
}

var dexBlockIndexes = []dexQueryIndex{
	{"cmi_dex_committed", []keeperIndexColumn{{"chain_id", 0}, {"manifest_hash", 0}, {"canonical", 0}, {"committed", 0}, {"block_number", 0}}},
	{"cmi_dex_pending", []keeperIndexColumn{{"chain_id", 0}, {"manifest_hash", 0}, {"canonical", 0}, {"committed", 0}, {"log_coverage", 16}, {"receipt_coverage", 16}, {"block_number", 0}}},
	{"cmi_dex_finality", []keeperIndexColumn{{"chain_id", 0}, {"manifest_hash", 0}, {"finality", 16}, {"block_number", 0}}},
}

func checkDEXBlockIndex(ctx context.Context, db *sql.DB, target string, index dexQueryIndex) (bool, error) {
	rows, err := db.QueryContext(ctx, "SELECT COLUMN_NAME,SUB_PART,NON_UNIQUE,IS_VISIBLE,COLLATION FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=? AND TABLE_NAME='dex_block' AND INDEX_NAME=? ORDER BY SEQ_IN_INDEX", target, index.name)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		var column, visible, order string
		var prefix sql.NullInt64
		var nonUnique int
		if err = rows.Scan(&column, &prefix, &nonUnique, &visible, &order); err != nil {
			return false, err
		}
		if n >= len(index.columns) || column != index.columns[n].name || prefix.Valid != (index.columns[n].prefix != 0) || (prefix.Valid && prefix.Int64 != index.columns[n].prefix) || nonUnique != 1 || visible != "YES" || order != "A" {
			return false, fmt.Errorf("DEX index shape conflict: %s", index.name)
		}
		n++
	}
	if err = rows.Err(); err != nil {
		return false, err
	}
	if n != 0 && n != len(index.columns) {
		return false, fmt.Errorf("DEX index incomplete: %s", index.name)
	}
	return n == len(index.columns), nil
}

// InstallDEXQueryIndexes preserves every query predicate and row. It replaces
// repeated scans of wide historical block rows with narrow, nonunique indexes.
func InstallDEXQueryIndexes(ctx context.Context, db *sql.DB, target string) error {
	if !identifier.MatchString(target) {
		return fmt.Errorf("invalid target database")
	}
	if err := InstallReceiptHashIndex(ctx, db, target); err != nil {
		return err
	}
	missing := []string{}
	for _, index := range dexBlockIndexes {
		installed, err := checkDEXBlockIndex(ctx, db, target, index)
		if err != nil {
			return err
		}
		if installed {
			continue
		}
		columns := []string{}
		for _, col := range index.columns {
			s := "`" + col.name + "`"
			if col.prefix != 0 {
				s += fmt.Sprintf("(%d)", col.prefix)
			}
			columns = append(columns, s)
		}
		missing = append(missing, "ADD INDEX `"+index.name+"` ("+strings.Join(columns, ",")+")")
	}
	if len(missing) == 0 {
		return nil
	}
	if _, err := db.ExecContext(ctx, "ALTER TABLE `"+target+"`.`dex_block` "+strings.Join(missing, ",")+", ALGORITHM=INPLACE, LOCK=NONE"); err != nil {
		return fmt.Errorf("DEX query indexes: %w", err)
	}
	for _, index := range dexBlockIndexes {
		installed, err := checkDEXBlockIndex(ctx, db, target, index)
		if err != nil {
			return err
		}
		if !installed {
			return fmt.Errorf("DEX query index missing after installation: %s", index.name)
		}
	}
	return nil
}
