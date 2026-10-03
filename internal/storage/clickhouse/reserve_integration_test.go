package clickhouse

import (
	"context"
	"fmt"
	"github.com/vphoenix/crypto-market-info/internal/dex"
	"github.com/vphoenix/crypto-market-info/internal/reserve"
	"math/big"
	"os"
	"reflect"
	"testing"
	"time"
)

func reserveIntegration(t *testing.T) *Client {
	t.Helper()
	if os.Getenv("CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set CLICKHOUSE_INTEGRATION=1")
	}
	db := fmt.Sprintf("crypto_reserve_it_%d", time.Now().UnixNano())
	c, e := Open(context.Background(), Config{Database: db, MaxAttempts: 1})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = c.conn.Exec(context.Background(), "DROP DATABASE `"+db+"` SYNC"); _ = c.Close() })
	if e = c.InitReserveSchema(context.Background()); e != nil {
		t.Fatal(e)
	}
	if e = c.InitReserveSchema(context.Background()); e != nil {
		t.Fatal(e)
	}
	return c
}
func reserveFixture(n uint64) reserve.Batch {
	now := dex.Now()
	h := dex.ObjectHash(n)
	m := dex.Digest([]byte("m"))
	batch := dex.ObjectHash(n + 100)
	cap := reserve.Capture{ChainId: 1, ManifestHash: reserve.Hash(m), CaptureId: reserve.Hash(h), BatchId: reserve.Hash(batch), CaptureKind: "snapshot", CaptureMode: "live", FromBlock: n, ToBlock: n, FromHash: reserve.Hash(h), ToHash: reserve.Hash(h), FromTime: now, ToTime: now, ReceivedAt: now, Canonical: true, Finality: "head", LogCoverage: "not_requested", StateCoverage: "partial", QuoteCoverage: "partial", ReceiptCoverage: "not_requested", PayloadHash: reserve.Hash(m), PlanHash: reserve.Hash(h)}
	num := new(big.Int).Lsh(big.NewInt(1), 200)
	token := reserve.Address(reserve.USDC)
	s := reserve.State{ChainId: 1, ManifestHash: cap.ManifestHash, CaptureId: cap.CaptureId, BatchId: cap.BatchId, BlockNumber: n, BlockHash: cap.ToHash, BlockTime: now, AvailableAt: now, PayloadHash: cap.PayloadHash, Folio: token, TotalSupplyRaw: new(big.Int).Set(num), Reason: "intentional_partial", Basket: []reserve.Asset{{Token: token, AmountRaw: new(big.Int).Set(num)}, {Token: token, AmountRaw: nil}}}
	q := reserve.Quote{ChainId: 1, ManifestHash: cap.ManifestHash, CaptureId: cap.CaptureId, BatchId: cap.BatchId, BlockNumber: n, BlockHash: cap.ToHash, BlockTime: now, AvailableAt: now, PayloadHash: cap.PayloadHash, Folio: token, RouteId: reserve.ID("r"), QuoteId: reserve.ID("q"), RouteKind: "mint", BudgetToken: token, RequestedBudgetRaw: num, TokenIn: token, TokenOut: token, Quality: "incomplete", Status: "failed", BasketAmounts: []reserve.Amount{{Token: token, AmountRaw: new(big.Int).Set(num)}}, DexLegs: []reserve.Leg{{LegIndex: 0, Side: "entry", QuoteMode: "exact_output", TokenIn: token, TokenOut: token, RequestedRaw: new(big.Int).Set(num), AmountInRaw: nil, AmountOutRaw: new(big.Int).Set(num), PathTokens: []string{}, PathPools: []string{}, PoolFees: []uint32{}, AvailableAt: now, PayloadHash: cap.PayloadHash, Status: "failed"}}, SharedPools: []string{}}
	cap.ExpectedStates = 1
	cap.ExpectedQuotes = 1
	b := reserve.Batch{Capture: cap, States: []reserve.State{s}, Quotes: []reserve.Quote{q}}
	b.Seal()
	return b
}
func TestReserveIntegrationTypedTupleUInt256NullRetryAndCommit(t *testing.T) {
	c := reserveIntegration(t)
	ctx := context.Background()
	b := reserveFixture(7)
	if e := c.dexInsert(ctx, "reserve_route_quote", reserveColumns(reflect.TypeOf(reserve.Quote{})), reserveRows(b.Quotes)); e != nil {
		t.Fatal(e)
	}
	caps, e := c.ReserveCaptures(ctx, b.Capture.ManifestHash)
	if e != nil || len(caps) != 0 {
		t.Fatal("uncommitted visible", caps, e)
	}
	for i := 0; i < 2; i++ {
		if e = c.WriteReserveBatch(ctx, b); e != nil {
			t.Fatal(e)
		}
	}
	caps, e = c.ReserveCaptures(ctx, b.Capture.ManifestHash)
	if e != nil || len(caps) != 1 {
		t.Fatal(caps, e)
	}
	got, e := c.ReserveBatch(ctx, caps[0])
	if e != nil {
		t.Fatal(e)
	}
	if reserve.StateMembers(got.States) != b.Capture.StateMembers || reserve.QuoteMembers(got.Quotes) != b.Capture.QuoteMembers {
		t.Fatal("inexact typed tuple roundtrip")
	}
	if got.States[0].Basket[1].AmountRaw != nil || got.Quotes[0].DexLegs[0].AmountInRaw != nil {
		t.Fatal("unknown became zero")
	}
	orphan := b.Capture
	orphan.Canonical = false
	orphan.Revision++
	if e = c.WriteReserveRevision(ctx, orphan); e != nil {
		t.Fatal(e)
	}
	if e = c.WriteReserveRevision(ctx, b.Capture); e != nil {
		t.Fatal(e)
	}
	caps, e = c.ReserveCaptures(ctx, b.Capture.ManifestHash)
	if e != nil || len(caps) != 1 || caps[0].Canonical {
		t.Fatal("older revision revived", caps, e)
	}
}
func TestReserveIntegrationReceiptReferencesStayHistorical(t *testing.T) {
	c := reserveIntegration(t)
	ctx := context.Background()
	b := reserveFixture(11)
	b.States = nil
	b.Quotes = nil
	b.Capture.CaptureKind = "logs"
	b.Capture.LogCoverage = "complete"
	b.Capture.ReceiptCoverage = "partial"
	var h, m, bid dex.Hash
	copy(h[:], b.Capture.ToHash)
	copy(m[:], b.Capture.ManifestHash)
	copy(bid[:], b.Capture.BatchId)
	a := dex.Anchor{ChainID: 1, Number: 11, Hash: h, Manifest: m, Batch: bid, Payload: m, Time: b.Capture.ToTime}
	tx := dex.Digest([]byte("tx"))
	l := dex.Log{Anchor: a, TxHash: tx, Emitter: reserve.USDC, Event: "unknown"}
	b.Logs = []dex.Log{l}
	b.Capture.ExpectedReceipts = 1
	b.Seal()
	if e := c.WriteReserveBatch(ctx, b); e != nil {
		t.Fatal(e)
	}
	firstTime := dex.Now()
	r := dex.Receipt{Anchor: a, TxHash: tx, From: reserve.USDC, To: reserve.USDT, HasTo: true, Value: big.NewInt(0), GasPrice: big.NewInt(2), CalldataHash: dex.Digest([]byte("calldata")), ReceiptHash: dex.Digest([]byte("receipt")), AvailableAt: firstTime}
	newBatch := b
	newBatch.Capture.BatchId = reserve.ID("newbatch")
	copy(l.Batch[:], newBatch.Capture.BatchId)
	newBatch.Logs = []dex.Log{l}
	newBatch.Receipts = []dex.Receipt{r}
	newBatch.Capture.ReceiptCoverage = "complete"
	newBatch.Seal()
	if e := c.WriteReserveBatch(ctx, newBatch); e != nil {
		t.Fatal(e)
	}
	old, e := c.ReserveBatch(ctx, b.Capture)
	if e != nil || len(old.Receipts) != 0 {
		t.Fatal("old capture changed after later receipt", e, len(old.Receipts))
	}
	fresh, e := c.ReserveBatch(ctx, newBatch.Capture)
	if e != nil || len(fresh.Receipts) != 1 {
		t.Fatal("shared receipt read failed", e)
	}
	r.AvailableAt = firstTime.Add(time.Hour)
	r.Batch = dex.Digest([]byte("different observation"))
	newBatch.Receipts = []dex.Receipt{r}
	newBatch.Seal()
	if e = c.WriteReserveBatch(ctx, newBatch); e != nil {
		t.Fatal(e)
	}
	stored, ok, e := c.ReserveReceipt(ctx, a, tx)
	if e != nil || !ok || !stored.AvailableAt.Equal(firstTime) {
		t.Fatal("first receipt observation overwritten", stored.AvailableAt, e)
	}
	conflict := newBatch
	conflict.Capture.BatchId = reserve.ID("thirdbatch")
	conflict.Logs = append([]dex.Log{}, newBatch.Logs...)
	copy(conflict.Logs[0].Batch[:], conflict.Capture.BatchId)
	conflict.Logs[0].Data = []byte{1}
	conflict.Seal()
	if e = c.WriteReserveBatch(ctx, conflict); e == nil {
		t.Fatal("conflicting chain log accepted")
	}
}
