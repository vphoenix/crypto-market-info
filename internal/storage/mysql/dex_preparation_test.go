package mysql

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/dex"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
)

func TestDEXBulkFactsRollbackRetryAndReorg(t *testing.T) {
	if os.Getenv("MYSQL_INTEGRATION") != "1" {
		t.Skip("isolated MySQL required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	db, err := Connect(ctx, Config{Socket: os.Getenv("CMI_MYSQL_TEST_SOCKET"), Username: os.Getenv("CMI_MYSQL_TEST_USER"), PasswordFile: os.Getenv("CMI_MYSQL_TEST_PASSWORD_FILE"), MaxConnections: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	target := "cmi_test_dexbulk_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err = db.ExecContext(ctx, "CREATE DATABASE `"+target+"`"); err != nil {
		t.Fatal(err)
	}
	defer db.ExecContext(context.Background(), "DROP DATABASE `"+target+"`")
	for _, statement := range strings.Split(controlDDL, ";") {
		if strings.Contains(statement, "CREATE DATABASE") || strings.Contains(statement, "CREATE TABLE IF NOT EXISTS cmi_storage.producer_generation (") {
			if _, err = db.ExecContext(ctx, statement); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, name := range []string{"dex_block", "dex_route_quote", "dex_sky_state", "dex_log", "dex_tx_receipt"} {
		mapping, e := Mapping("crypto_market_info", name)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = db.ExecContext(ctx, strings.Replace(mapping.DDL, "`crypto_market_info`", "`"+target+"`", 1)); e != nil {
			t.Fatal(e)
		}
	}
	if err = InstallDEXQueryIndexes(ctx, db, target); err != nil {
		t.Fatal(err)
	}
	holder := uuid.New()
	generation, err := ActivateProducer(ctx, db, target, 0, holder)
	if err != nil {
		t.Fatal(err)
	}
	defer db.ExecContext(context.Background(), "DELETE FROM cmi_storage.producer_generation WHERE service=?", target)
	c := &Client{db: db, database: target, family: "crypto_market_info", producer: Producer{Service: target, Generation: generation, Holder: holder}}
	b := dexTestBatch(100)
	quote := b.Quotes[0]
	b.Quotes = nil
	for n := range 60 {
		q := quote
		q.Route = fmt.Sprintf("route-%02d", n)
		q.SetID()
		b.Quotes = append(b.Quotes, q)
	}
	b.Block.ExpectedQuotes = 60
	log := dex.Log{Anchor: b.Block.Anchor, TxHash: dex.ObjectHash(200), Data: []byte("public-log"), Event: "swap"}
	b.Logs = []dex.Log{log}
	b.Receipts = []dex.Receipt{{Anchor: b.Block.Anchor, TxHash: log.TxHash, Value: new(big.Int), GasPrice: big.NewInt(1), LogCount: 1, ReceiptHash: dex.ObjectHash(201), CalldataHash: dex.ObjectHash(202), AvailableAt: dex.Now()}}
	b.Seal()
	conflict := log
	conflict.Data = []byte("different-public-log")
	if err = c.putValues(ctx, db, "dex_log", dexLogColumns, logValues(conflict)); err != nil {
		t.Fatal(err)
	}
	if err = c.WriteDEXBatch(ctx, b); !errors.Is(err, contracts.ErrConflict) {
		t.Fatal("fact conflict was not rejected", err)
	}
	for _, name := range []string{"dex_block", "dex_route_quote", "dex_sky_state", "dex_tx_receipt"} {
		var count int
		if err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+c.table(name)).Scan(&count); err != nil || count != 0 {
			t.Fatal("partial facts escaped transaction", name, count, err)
		}
	}
	if _, err = db.ExecContext(ctx, "DELETE FROM "+c.table("dex_log")); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err = c.WriteDEXBatch(ctx, b); err != nil {
			t.Fatal(err)
		}
	}
	quotes, err := c.DEXQuotes(ctx, []dex.Block{b.Block})
	if err != nil || len(quotes[b.Block.Batch]) != 60 || dex.QuoteDigest(quotes[b.Block.Batch]) != b.Block.QuoteMembers {
		t.Fatal("complete typed quote proof changed", err)
	}
	last, err := c.DEXLastBlock(ctx, b.Block.Manifest, false)
	if err != nil || last == nil || last.QuoteMembers != b.Block.QuoteMembers {
		t.Fatal("committed block or shared receipt proof missing", err)
	}
	orphan := b.Block
	orphan.Canonical, orphan.Finality = false, "orphaned"
	orphan.Revision++
	if err = c.WriteDEXBlock(ctx, orphan); err != nil {
		t.Fatal(err)
	}
	last, err = c.DEXLastBlock(ctx, b.Block.Manifest, false)
	if err != nil || last != nil {
		t.Fatal("orphan confused with canonical data", err)
	}
}
