package mysql

import (
	"context"
	"errors"
	"math/big"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/dex"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
)

func TestReceiptHashIndexPreservesCrossHeightAmbiguity(t *testing.T) {
	if os.Getenv("MYSQL_INTEGRATION") != "1" {
		t.Skip("isolated MySQL required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	db, err := Connect(ctx, Config{Socket: os.Getenv("CMI_MYSQL_TEST_SOCKET"), Username: os.Getenv("CMI_MYSQL_TEST_USER"), PasswordFile: os.Getenv("CMI_MYSQL_TEST_PASSWORD_FILE"), MaxConnections: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	target := "cmi_test_receipt_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err = db.ExecContext(ctx, "CREATE DATABASE `"+target+"`"); err != nil {
		t.Fatal(err)
	}
	defer db.ExecContext(context.Background(), "DROP DATABASE `"+target+"`")
	mapping, err := Mapping("crypto_market_info", "dex_tx_receipt")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.ExecContext(ctx, strings.Replace(mapping.DDL, "`crypto_market_info`", "`"+target+"`", 1)); err != nil {
		t.Fatal(err)
	}
	c := &Client{db: db, database: target, family: "crypto_market_info"}
	anchor := dexTestBatch(7).Block.Anchor
	r := dex.Receipt{Anchor: anchor, TxHash: dex.ObjectHash(101), Value: new(big.Int).Lsh(big.NewInt(1), 200), GasPrice: big.NewInt(99), ReceiptHash: dex.ObjectHash(102), AvailableAt: dex.Now()}
	for n := range 3 {
		row := r
		row.Number += uint64(n)
		if n == 2 {
			row.Hash = dex.ObjectHash(999)
		}
		values, e := namedRecord(dexReceiptColumns, receiptValues(row))
		if e != nil {
			t.Fatal(e)
		}
		if e = insertRecord(ctx, db, c.table(mapping.Name), mapping, values); e != nil {
			t.Fatal(e)
		}
	}
	read := func() []dex.Receipt {
		t.Helper()
		rows, e := c.DEXReceipts(ctx, dex.Block{Anchor: anchor})
		if e != nil || len(rows) != 2 {
			t.Fatal("all physical hash matches must remain visible", len(rows), e)
		}
		sort.Slice(rows, func(i, j int) bool { return rows[i].Number < rows[j].Number })
		if _, _, e = c.ReserveReceipt(ctx, anchor, r.TxHash); !errors.Is(e, contracts.ErrConflict) {
			t.Fatal("cross-height ambiguity was hidden", e)
		}
		return rows
	}
	before := read()
	for range 2 {
		if err = InstallReceiptHashIndex(ctx, db, target); err != nil {
			t.Fatal(err)
		}
	}
	if after := read(); !reflect.DeepEqual(before, after) {
		t.Fatal("index installation changed typed facts")
	}
	if _, err = db.ExecContext(ctx, "ALTER TABLE "+c.table(mapping.Name)+" DROP INDEX "+receiptHashIndex+", ADD INDEX "+receiptHashIndex+" (block_hash,chain_id,tx_hash)"); err != nil {
		t.Fatal(err)
	}
	if err = InstallReceiptHashIndex(ctx, db, target); err == nil {
		t.Fatal("conflicting index shape accepted")
	}
}
