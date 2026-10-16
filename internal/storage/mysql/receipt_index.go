package mysql

import (
	"context"
	"database/sql"
	"fmt"
)

const receiptHashIndex = "cmi_receipt_hash"

func checkReceiptHashIndex(ctx context.Context, db *sql.DB, target string) (bool, error) {
	rows, err := db.QueryContext(ctx, "SELECT COLUMN_NAME,SUB_PART,NON_UNIQUE,IS_VISIBLE,COLLATION FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=? AND TABLE_NAME='dex_tx_receipt' AND INDEX_NAME=? ORDER BY SEQ_IN_INDEX", target, receiptHashIndex)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	want := []string{"chain_id", "block_hash", "tx_hash"}
	n := 0
	for rows.Next() {
		var column, visible, order string
		var prefix sql.NullInt64
		var nonUnique int
		if err = rows.Scan(&column, &prefix, &nonUnique, &visible, &order); err != nil {
			return false, err
		}
		if n >= len(want) || column != want[n] || prefix.Valid || nonUnique != 1 || visible != "YES" || order != "A" {
			return false, fmt.Errorf("receipt hash index shape conflict")
		}
		n++
	}
	if err = rows.Err(); err != nil {
		return false, err
	}
	if n != 0 && n != len(want) {
		return false, fmt.Errorf("receipt hash index incomplete")
	}
	return n == len(want), nil
}

// InstallReceiptHashIndex is an additive administrative operation. The
// nonunique index preserves all physical matches, including conflicting
// receipts at different block heights with the same supplied hashes.
func InstallReceiptHashIndex(ctx context.Context, db *sql.DB, target string) error {
	if !identifier.MatchString(target) {
		return fmt.Errorf("invalid target database")
	}
	installed, err := checkReceiptHashIndex(ctx, db, target)
	if err != nil || installed {
		return err
	}
	if _, err = db.ExecContext(ctx, "ALTER TABLE `"+target+"`.`dex_tx_receipt` ADD INDEX `"+receiptHashIndex+"` (`chain_id`,`block_hash`,`tx_hash`), ALGORITHM=INPLACE, LOCK=NONE"); err != nil {
		return fmt.Errorf("receipt hash index: %w", err)
	}
	installed, err = checkReceiptHashIndex(ctx, db, target)
	if err == nil && !installed {
		return fmt.Errorf("receipt hash index missing after installation")
	}
	return err
}
