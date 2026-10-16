package mysql

import (
	"context"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/dex"
)

func TestDEXIndexesPreserveMaintenanceScopeAndGaps(t *testing.T) {
	if os.Getenv("MYSQL_INTEGRATION") != "1" {
		t.Skip("isolated MySQL required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	db, err := Connect(ctx, Config{Socket: os.Getenv("CMI_MYSQL_TEST_SOCKET"), Username: os.Getenv("CMI_MYSQL_TEST_USER"), PasswordFile: os.Getenv("CMI_MYSQL_TEST_PASSWORD_FILE"), MaxConnections: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	target := "cmi_test_dexidx_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err = db.ExecContext(ctx, "CREATE DATABASE `"+target+"`"); err != nil {
		t.Fatal(err)
	}
	defer db.ExecContext(context.Background(), "DROP DATABASE `"+target+"`")
	for _, name := range []string{"dex_block", "dex_tx_receipt"} {
		mapping, e := Mapping("crypto_market_info", name)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = db.ExecContext(ctx, strings.Replace(mapping.DDL, "`crypto_market_info`", "`"+target+"`", 1)); e != nil {
			t.Fatal(e)
		}
	}
	c := &Client{db: db, database: target, family: "crypto_market_info"}
	b := dexTestBatch(50)
	b.Quotes = nil
	b.Block.ExpectedQuotes = 0
	b.Seal()
	mapping, _ := c.mapping("dex_block")
	for n := range 10 {
		block := b.Block
		block.Number += uint64(n)
		block.Hash = dex.ObjectHash(block.Number)
		switch n {
		case 0:
			block.ReceiptCoverage = "partial"
		case 1:
			block.LogCoverage = "partial"
		case 2:
			block.Finality = "finalized"
		case 3:
			block.Canonical = false
		case 4:
			block.Committed = false
		case 5:
			block.Finality = "orphaned"
		case 6:
			block.Manifest = dex.ObjectHash(999)
		}
		r, e := namedRecord(dexBlockColumns, blockValues(block))
		if e != nil {
			t.Fatal(e)
		}
		if e = insertRecord(ctx, db, c.table(mapping.Name), mapping, r); e != nil {
			t.Fatal(e)
		}
	}
	read := func(indexed bool) []any {
		t.Helper()
		receipts, e := c.dexBlocks(ctx, db, "WHERE chain_id=1 AND manifest_hash=? AND canonical=1 AND committed=1 AND log_coverage='complete' AND receipt_coverage!='complete' ORDER BY block_number DESC LIMIT 100", b.Block.Manifest[:])
		if indexed {
			receipts, e = c.DEXPendingReceiptBlocks(ctx, b.Block.Manifest)
		}
		if e != nil || len(receipts) != 1 || receipts[0].Number != 50 {
			t.Fatal("pending receipt scope", receipts, e)
		}
		logs, e := c.dexBlocks(ctx, db, "WHERE chain_id=1 AND manifest_hash=? AND canonical=1 AND committed=1 AND log_coverage!='complete' ORDER BY block_number LIMIT 100", b.Block.Manifest[:])
		if indexed {
			logs, e = c.DEXPendingLogBlocks(ctx, b.Block.Manifest)
		}
		if e != nil || len(logs) != 1 || logs[0].Number != 51 {
			t.Fatal("pending log scope", logs, e)
		}
		unfinalized, e := c.dexBlocks(ctx, db, "WHERE chain_id=1 AND manifest_hash=? AND block_number<? AND committed=1 AND finality NOT IN ('finalized','orphaned') ORDER BY block_number LIMIT 100", b.Block.Manifest[:], 60)
		if indexed {
			unfinalized, e = c.DEXUnfinalizedBlocks(ctx, b.Block.Manifest, 60)
		}
		if e != nil || len(unfinalized) != 6 {
			t.Fatal("unfinalized scope", len(unfinalized), e)
		}
		var gap uint64
		e = db.QueryRowContext(ctx, "SELECT COALESCE(MIN(block_number+1),0) FROM (SELECT block_number,LEAD(block_number) OVER(ORDER BY block_number) AS next_block FROM "+c.table("dex_block")+" WHERE chain_id=1 AND manifest_hash=? AND canonical=1 AND committed=1) AS coverage WHERE next_block>block_number+1", b.Block.Manifest[:]).Scan(&gap)
		if indexed {
			gap, e = c.DEXGapStart(ctx, b.Block.Manifest)
		}
		if e != nil || gap != 53 {
			t.Fatal("gap advancement", gap, e)
		}
		return []any{receipts, logs, unfinalized, gap}
	}
	before := read(false)
	for range 2 {
		if err = InstallDEXQueryIndexes(ctx, db, target); err != nil {
			t.Fatal(err)
		}
	}
	if !reflect.DeepEqual(before, read(true)) {
		t.Fatal("query indexes changed maintenance results")
	}
	if _, err = db.ExecContext(ctx, "ALTER TABLE "+c.table("dex_block")+" ALTER INDEX cmi_dex_pending INVISIBLE"); err != nil {
		t.Fatal(err)
	}
	if err = InstallDEXQueryIndexes(ctx, db, target); err == nil {
		t.Fatal("invisible index accepted")
	}
}
