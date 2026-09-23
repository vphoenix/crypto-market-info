package clickhouse

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/vphoenix/crypto-market-info/internal/dex"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func dexIntegrationClient(t *testing.T) *Client {
	t.Helper()
	if os.Getenv("CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set CLICKHOUSE_INTEGRATION=1 for isolated DEX integration")
	}
	db := fmt.Sprintf("crypto_dex_it_%d", time.Now().UnixNano())
	c, e := Open(context.Background(), Config{Database: db, MaxAttempts: 1})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { c.conn.Exec(context.Background(), "DROP DATABASE `"+db+"` SYNC"); c.Close() })
	if e = c.InitDEXSchema(context.Background()); e != nil {
		t.Fatal(e)
	}
	if e = c.InitDEXSchema(context.Background()); e != nil {
		t.Fatal(e)
	}
	return c
}
func dexTestBatch(n uint64) dex.Batch {
	now := dex.Now()
	a := dex.Anchor{ChainID: 1, Number: n, Hash: dex.ObjectHash(n), Manifest: dex.Digest([]byte("manifest")), Batch: dex.ObjectHash(n + 1000), Payload: dex.Digest([]byte("payload")), Time: now.Add(-time.Hour)}
	amount := new(big.Int).Lsh(big.NewInt(1), 200)
	q := dex.Quote{Anchor: a, Role: "strategy", Route: "r", Mode: "exact_in", Requested: amount, AmountIn: dex.Copy(amount), AmountOut: new(big.Int).Add(amount, big.NewInt(123)), DustDAI: new(big.Int), DustUSDS: new(big.Int), V3In: dex.Copy(amount), V3Out: dex.Copy(amount), SqrtAfter: big.NewInt(2), GasEstimate: big.NewInt(99), Status: "ok", AvailableAt: now}
	q.SetID()
	b := dex.Batch{Block: dex.Block{Anchor: a, Parent: dex.ObjectHash(n - 1), BaseFee: big.NewInt(123), ReceivedAt: now, Canonical: true, Finality: "head", Capture: "live", ExpectedQuotes: 1, QuoteCoverage: "complete", LogCoverage: "complete", ReceiptCoverage: "complete"}, Sky: &dex.Sky{Anchor: a, Module: "sky", Reason: "intentional_missing_state"}, Quotes: []dex.Quote{q}}
	b.Seal()
	return b
}
func TestDEXIntegrationAtomicVisibilityRetryUInt256AndLatestRevision(t *testing.T) {
	c := dexIntegrationClient(t)
	ctx := context.Background()
	b := dexTestBatch(7)
	// Simulate interrupted writer: facts exist but no committed marker.
	if e := c.dexInsert(ctx, "dex_route_quote", dexQuoteColumns, [][]any{quoteValues(b.Quotes[0])}); e != nil {
		t.Fatal(e)
	}
	bs, e := c.DEXBlocks(ctx, b.Block.Time.Add(-time.Minute), dex.Now())
	if e != nil || len(bs) != 0 {
		t.Fatal("partial batch visible", len(bs), e)
	}
	for i := 0; i < 2; i++ {
		if e = c.WriteDEXBatch(ctx, b); e != nil {
			t.Fatal(e)
		}
	}
	bs, e = c.DEXBlocks(ctx, b.Block.Time.Add(-time.Minute), dex.Now())
	if e != nil || len(bs) != 1 {
		t.Fatal(len(bs), e)
	}
	qs, e := c.DEXQuotes(ctx, bs)
	if e != nil {
		t.Fatal(e)
	}
	got := qs[b.Block.Batch]
	if len(got) != 1 || got[0].AmountIn.Cmp(b.Quotes[0].AmountIn) != 0 || dex.QuoteDigest(got) != b.Block.QuoteMembers {
		t.Fatal("UInt256 or retry/hash roundtrip", got)
	}
	var tin *big.Int
	if e = c.conn.QueryRow(ctx, "SELECT tin FROM "+c.table("dex_sky_state")+" FINAL LIMIT 1").Scan(&tin); e != nil || tin != nil {
		t.Fatal("unknown state became zero", tin, e)
	}
	orphan := b.Block
	orphan.Canonical = false
	orphan.Finality = "orphaned"
	orphan.Revision++
	if e = c.WriteDEXBlock(ctx, orphan); e != nil {
		t.Fatal(e)
	}
	if e = c.WriteDEXBlock(ctx, b.Block); e != nil {
		t.Fatal(e)
	} // ambiguous older insert arriving late
	latest, e := c.DEXLastBlock(ctx, b.Block.Manifest, false)
	if e != nil || latest != nil {
		t.Fatal("old canonical row resurrected", latest, e)
	}
	bs, e = c.DEXBlockRange(ctx, b.Block.Manifest, 7, 7)
	if e != nil || len(bs) != 1 || bs[0].Canonical || bs[0].Batch != b.Block.Batch || bs[0].QuoteMembers != b.Block.QuoteMembers {
		t.Fatal("orphan revision lost identity", bs, e)
	}
	// The report connection enforces readonly at the server, not just convention.
	reader, e := OpenDEXReader(ctx, Config{Database: c.database})
	if e != nil {
		t.Fatal(e)
	}
	defer reader.Close()
	if e = reader.InitDEXSchema(ctx); e == nil {
		t.Fatal("report connection permits DDL")
	}
}
func TestDEXIntegrationGapAndFinalizedSelection(t *testing.T) {
	c := dexIntegrationClient(t)
	ctx := context.Background()
	a := dexTestBatch(1)
	b := dexTestBatch(3)
	b.Block.Manifest = a.Block.Manifest
	for _, x := range []dex.Batch{a, b} {
		if e := c.WriteDEXBatch(ctx, x); e != nil {
			t.Fatal(e)
		}
	}
	gap, e := c.DEXGapStart(ctx, a.Block.Manifest)
	if e != nil || gap != 2 {
		t.Fatal(gap, e)
	}
	a.Block.Finality = "finalized"
	a.Block.Revision++
	if e = c.WriteDEXBlock(ctx, a.Block); e != nil {
		t.Fatal(e)
	}
	last, e := c.DEXLastBlock(ctx, a.Block.Manifest, true)
	if e != nil || last == nil || last.Number != 1 {
		t.Fatal(last, e)
	}
}
func TestDEXIntegrationLogAndReceiptRecoveryPreservesQuotes(t *testing.T) {
	c := dexIntegrationClient(t)
	ctx := context.Background()
	b := dexTestBatch(22)
	b.Block.LogCoverage = "missing"
	b.Block.ReceiptCoverage = "missing"
	if e := c.WriteDEXBatch(ctx, b); e != nil {
		t.Fatal(e)
	}
	pending, e := c.DEXPendingLogBlocks(ctx, b.Block.Manifest)
	if e != nil || len(pending) != 1 {
		t.Fatal(pending, e)
	}
	log := dex.Log{Anchor: b.Block.Anchor, TxHash: dex.Digest([]byte("tx")), Index: 2, Topics: []dex.Hash{dex.Digest([]byte("topic"))}, Data: []byte{0, 1}, Event: "Swap"}
	logs := []dex.Log{log}
	complete := b.Block
	complete.LogCoverage = "complete"
	complete.LogCount = 1
	complete.LogMembers = dex.LogDigest(logs)
	complete.Revision++
	if e = c.CompleteDEXLogs(ctx, complete, logs); e != nil {
		t.Fatal(e)
	}
	pending, e = c.DEXPendingLogBlocks(ctx, b.Block.Manifest)
	if e != nil || len(pending) != 0 {
		t.Fatal("recovered block still pending", pending, e)
	}
	got, e := c.DEXLogs(ctx, complete)
	if e != nil || dex.LogDigest(got) != complete.LogMembers {
		t.Fatal("logs roundtrip", got, e)
	}
	receipt := dex.Receipt{Anchor: b.Block.Anchor, TxHash: log.TxHash, Value: new(big.Int).Lsh(big.NewInt(1), 200), GasPrice: big.NewInt(2), GasUsed: 99, Status: 1, CalldataHash: dex.Digest(nil), ReceiptHash: dex.Digest([]byte("receipt")), AvailableAt: dex.Now()}
	receipt.Batch = dex.Digest([]byte("receipt batch"))
	if e = c.WriteDEXReceipts(ctx, []dex.Receipt{receipt}); e != nil {
		t.Fatal(e)
	}
	receipts, e := c.DEXReceipts(ctx, complete)
	if e != nil || len(receipts) != 1 || dex.ReceiptDigest(receipts) != dex.ReceiptDigest([]dex.Receipt{receipt}) {
		t.Fatal("receipt roundtrip", receipts, e)
	}
	complete.ReceiptCoverage = "complete"
	complete.ReceiptCount = 1
	complete.ReceiptMembers = dex.ReceiptDigest(receipts)
	complete.Revision++
	if e = c.WriteDEXBlock(ctx, complete); e != nil {
		t.Fatal(e)
	}
	pending, e = c.DEXPendingReceiptBlocks(ctx, b.Block.Manifest)
	if e != nil || len(pending) != 0 {
		t.Fatal("completed receipts occupy pending window", pending, e)
	}
	bs, e := c.DEXBlockRange(ctx, b.Block.Manifest, 22, 22)
	if e != nil || len(bs) != 1 {
		t.Fatal(bs, e)
	}
	q, e := c.DEXQuotes(ctx, bs)
	if e != nil || dex.QuoteDigest(q[b.Block.Batch]) != b.Block.QuoteMembers || !bs[0].AvailableAt.Equal(b.Block.AvailableAt) {
		t.Fatal("recovery altered original quote visibility", e)
	}
}
func TestDEXIntegrationArchivedPublicSample(t *testing.T) {
	dir := os.Getenv("DEX_SAMPLE_DIR")
	if dir == "" {
		t.Skip("set DEX_SAMPLE_DIR for real public sample storage validation")
	}
	raw, e := os.ReadFile(filepath.Join(dir, "sample.json"))
	if e != nil {
		t.Fatal(e)
	}
	var batch dex.Batch
	if e = json.Unmarshal(raw, &batch); e != nil {
		t.Fatal(e)
	}
	c := dexIntegrationClient(t)
	ctx := context.Background()
	if e = c.WriteDEXBatch(ctx, batch); e != nil {
		t.Fatal(e)
	}
	bs, e := c.DEXBlockRange(ctx, batch.Block.Manifest, batch.Block.Number, batch.Block.Number)
	if e != nil || len(bs) != 1 {
		t.Fatal(bs, e)
	}
	quotes, e := c.DEXQuotes(ctx, bs)
	if e != nil || dex.QuoteDigest(quotes[batch.Block.Batch]) != batch.Block.QuoteMembers {
		t.Fatal("real quote roundtrip", e)
	}
	logs, e := c.DEXLogs(ctx, batch.Block)
	if e != nil || dex.LogDigest(logs) != batch.Block.LogMembers {
		t.Fatal("real logs roundtrip", e)
	}
	receipts, e := c.DEXReceipts(ctx, batch.Block)
	if e != nil || dex.ReceiptDigest(receipts) != batch.Block.ReceiptMembers {
		t.Fatal("real receipts roundtrip", e)
	}
	var bytes uint64
	if e = c.conn.QueryRow(ctx, "SELECT sum(data_compressed_bytes) FROM system.parts WHERE database = ? AND active", c.database).Scan(&bytes); e != nil {
		t.Fatal(e)
	}
	t.Logf("one research block: quotes=%d logs=%d receipts=%d compressed_table_bytes=%d", len(batch.Quotes), len(batch.Logs), len(batch.Receipts), bytes)
}
