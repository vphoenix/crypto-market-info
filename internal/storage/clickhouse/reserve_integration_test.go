package clickhouse

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/vphoenix/crypto-market-info/internal/dex"
	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
	"github.com/vphoenix/crypto-market-info/internal/reserve"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
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
	bulk, e := c.ReserveBatches(ctx, caps)
	if e != nil || reserve.StateMembers(bulk[caps[0].BatchId].States) != b.Capture.StateMembers || reserve.QuoteMembers(bulk[caps[0].BatchId].Quotes) != b.Capture.QuoteMembers {
		t.Fatal("bulk exact members", e)
	}
	bad := caps[0]
	bad.StateMembers = reserve.ID("corrupt-members")
	if _, e = c.ReserveBatches(ctx, []reserve.Capture{bad}); e == nil {
		t.Fatal("bulk skipped digest validation")
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
	r := dex.Receipt{Anchor: a, TxHash: tx, From: reserve.USDC, To: reserve.USDT, HasTo: true, Value: big.NewInt(0), GasPrice: big.NewInt(2), Selector: []byte("call"), CalldataHash: dex.Digest([]byte("calldata")), ReceiptHash: dex.Digest([]byte("receipt")), LogCount: 1, AvailableAt: firstTime}
	d := reserve.ReceiptData{ChainId: 1, BlockNumber: r.Number, BlockHash: reserve.Hash(r.Hash), BlockTime: r.Time, TxHash: reserve.Hash(tx), TxIndex: r.TxIndex, ReceiptHash: reserve.Hash(r.ReceiptHash), CalldataHash: reserve.Hash(r.CalldataHash), Calldata: "calldata", LogCount: 1, Logs: []reserve.ReceiptLog{{LogIndex: 0, Emitter: reserve.Address(l.Emitter), Topics: []string{}, Data: ""}}, MaterializedAt: firstTime}
	d.DataHash = reserve.ReceiptDataHash(d)
	newBatch := b
	newBatch.Capture.BatchId = reserve.ID("newbatch")
	copy(l.Batch[:], newBatch.Capture.BatchId)
	newBatch.Logs = []dex.Log{l}
	newBatch.Receipts = []dex.Receipt{r}
	newBatch.ReceiptData = []reserve.ReceiptData{d}
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
	bulk, e := c.ReserveBatches(ctx, []reserve.Capture{b.Capture, newBatch.Capture})
	if e != nil || len(bulk[b.Capture.BatchId].Receipts) != 0 || len(bulk[newBatch.Capture.BatchId].Receipts) != 1 {
		t.Fatal("bulk changed historical receipt references", e)
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

func TestReserveIntegrationReceiptMigrationAndLiveReuseWithoutFiles(t *testing.T) {
	c := reserveIntegration(t)
	ctx := context.Background()
	b := reserveFixture(19)
	b.States, b.Quotes = nil, nil
	b.Capture.FromTime = b.Capture.FromTime.Truncate(time.Second)
	b.Capture.ToTime = b.Capture.FromTime
	b.Capture.CaptureKind, b.Capture.LogCoverage, b.Capture.ReceiptCoverage = "logs", "complete", "complete"
	var block, manifest, batch dex.Hash
	copy(block[:], b.Capture.ToHash)
	copy(manifest[:], b.Capture.ManifestHash)
	copy(batch[:], b.Capture.BatchId)
	a := dex.Anchor{ChainID: 1, Number: 19, Hash: block, Manifest: manifest, Batch: batch, Payload: manifest, Time: b.Capture.ToTime}
	tx, topic := dex.Digest([]byte("retained-tx")), dex.Digest([]byte("retained-topic"))
	input := []byte{0x12, 0x34, 0x56, 0x78, 0xff, 0, 0xfe}
	l := dex.Log{Anchor: a, TxHash: tx, TxIndex: 3, Index: 12, Emitter: reserve.USDC, Topics: []dex.Hash{topic}, Data: []byte{0xff, 0, 0xfe}, Event: "unknown"}
	sourceLog := func(index uint32, emitter dex.Address, data []byte) map[string]any {
		return map[string]any{"blockNumber": "0x13", "blockHash": block.String(), "transactionHash": tx.String(), "transactionIndex": "0x3", "logIndex": fmt.Sprintf("0x%x", index), "address": emitter.String(), "topics": []string{topic.String()}, "data": "0x" + hex.EncodeToString(data), "removed": false}
	}
	selectedRaw := sourceLog(l.Index, l.Emitter, l.Data)
	price := new(big.Int).Lsh(big.NewInt(1), 200)
	r := dex.Receipt{Anchor: a, TxHash: tx, TxIndex: 3, From: reserve.USDC, To: reserve.USDT, HasTo: true, Type: 2, Value: new(big.Int).Lsh(big.NewInt(1), 220), Selector: input[:4], CalldataHash: dex.Digest(input), Status: 1, GasUsed: 21000, GasPrice: price, LogCount: 2, AvailableAt: a.Time.Add(time.Second)}
	raw, e := json.Marshal(map[string]any{"transactionHash": tx.String(), "blockHash": block.String(), "blockNumber": "0x13", "transactionIndex": "0x3", "from": r.From.String(), "to": r.To.String(), "type": "0x2", "status": "0x1", "gasUsed": "0x5208", "effectiveGasPrice": "0x" + price.Text(16), "logs": []any{selectedRaw, sourceLog(13, reserve.USDT, []byte{0x80, 0, 0x81})}})
	if e != nil {
		t.Fatal(e)
	}
	r.ReceiptHash = dex.Digest(raw)
	b.Logs, b.Receipts = []dex.Log{l}, []dex.Receipt{r}
	b.Capture.ExpectedReceipts = 1
	b.Seal()
	// Simulate pre-migration persisted facts. No new writer can commit a receipt
	// without its validated typed detail, and migration must not rewrite these rows.
	for _, row := range []struct {
		table, columns string
		values         [][]any
	}{{"dex_log", dexLogColumns, [][]any{logValues(l)}}, {"dex_tx_receipt", dexReceiptColumns, [][]any{receiptValues(r)}}, {"reserve_capture", reserveColumns(reflect.TypeOf(b.Capture)), reserveRows([]reserve.Capture{b.Capture})}} {
		if e = c.dexInsert(ctx, row.table, row.columns, row.values); e != nil {
			t.Fatal(e)
		}
	}
	if _, e = c.ReserveBatch(ctx, b.Capture); e == nil {
		t.Fatal("unmaterialized legacy receipt treated as complete")
	}
	archive := ethereum.Archive{Dir: t.TempDir()}
	for _, source := range [][]byte{raw, input} {
		if _, e = archive.Put(source); e != nil {
			t.Fatal(e)
		}
	}
	if n, e := reserve.MigrateReceiptData(ctx, c, archive); e != nil || n != 1 {
		t.Fatal("legacy migration failed", n, e)
	}
	if e = os.RemoveAll(archive.Dir); e != nil {
		t.Fatal(e)
	}
	archive.HashOnly = true
	if n, e := reserve.MigrateReceiptData(ctx, c, archive); e != nil || n != 1 {
		t.Fatal("idempotent migration required deleted files", n, e)
	}
	got, e := c.ReserveBatch(ctx, b.Capture)
	if e != nil || len(got.ReceiptData) != 1 || len(got.ReceiptData[0].Logs) != 2 || got.ReceiptData[0].Calldata != string(input) || got.ReceiptData[0].Logs[1].Data != string([]byte{0x80, 0, 0x81}) || got.Receipts[0].GasPrice.Cmp(price) != 0 || !got.Receipts[0].AvailableAt.Equal(r.AvailableAt) {
		t.Fatal("typed persistence lost binary data, exact integer, or first availability", e)
	}
	if bulk, e := c.ReserveBatches(ctx, []reserve.Capture{b.Capture}); e != nil || len(bulk[b.Capture.BatchId].ReceiptData) != 1 {
		t.Fatal("bulk read depended on archive", e)
	}
	var forbidden int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var calls []struct {
			ID     uint64
			Method string
		}
		if json.NewDecoder(req.Body).Decode(&calls) != nil {
			t.Error("bad local test RPC request")
		}
		out := []map[string]any{}
		for _, call := range calls {
			var result any
			switch call.Method {
			case "eth_getLogs":
				result = []any{selectedRaw}
			case "eth_getBlockByNumber":
				result = map[string]string{"number": "0x13", "hash": block.String(), "parentHash": dex.Digest([]byte("parent")).String(), "timestamp": fmt.Sprintf("0x%x", a.Time.Unix()), "baseFeePerGas": "0x1", "miner": reserve.USDC.String()}
			case "eth_getStorageAt":
				result = "0x" + strings.Repeat("0", 24) + reserve.USDT.String()[2:]
			case "eth_call":
				encoded, _ := reserve.ABI.Methods["version"].Outputs.Pack("5.0.0")
				result = "0x" + hex.EncodeToString(encoded)
			default:
				atomic.AddInt64(&forbidden, 1)
				out = append(out, map[string]any{"jsonrpc": "2.0", "id": call.ID, "error": map[string]any{"code": -32601, "message": "receipt refetch forbidden"}})
				continue
			}
			out = append(out, map[string]any{"jsonrpc": "2.0", "id": call.ID, "result": result})
		}
		json.NewEncoder(w).Encode(out)
	}))
	defer server.Close()
	rpc, e := ethereum.NewClient(server.URL, t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	rpc.Archive.HashOnly = true
	collector := reserve.Collector{RPC: rpc, Store: c, Manifest: reserve.Manifest{Hash: manifest, Implementation: reserve.USDT, Folios: []reserve.Folio{{Address: reserve.USDC}}}}
	head := dex.Block{Anchor: a}
	live, e := collector.Logs(ctx, head, head, "live", "head")
	if e != nil || len(live.Receipts) != 1 || len(live.ReceiptData) != 1 || live.Capture.ReceiptCoverage != "complete" || atomic.LoadInt64(&forbidden) != 0 {
		t.Fatal("live reuse read a file or refetched an existing receipt", e, forbidden)
	}
	if e = c.WriteReserveBatch(ctx, live); e != nil {
		t.Fatal("shared receipt retry failed", e)
	}
	old, e := c.ReserveBatch(ctx, b.Capture)
	if e != nil || reserve.ReceiptMembers(old.Receipts) != b.Capture.ReceiptMembers || !old.Receipts[0].AvailableAt.Equal(r.AvailableAt) {
		t.Fatal("migration/reuse changed old membership or availability", e)
	}
	bad := got.ReceiptData[0]
	bad.Logs = append([]reserve.ReceiptLog{}, bad.Logs...)
	bad.Logs[1].Data = "different external log"
	bad.DataHash = reserve.ReceiptDataHash(bad)
	if e = c.WriteReserveReceiptData(ctx, bad, r); e == nil {
		t.Fatal("conflicting complete receipt detail overwritten")
	}
	bad = got.ReceiptData[0]
	bad.Logs = bad.Logs[:1]
	bad.DataHash = reserve.ReceiptDataHash(bad)
	if e = c.dexInsert(ctx, "reserve_receipt_data", reserveColumns(reflect.TypeOf(bad)), reserveRows([]reserve.ReceiptData{bad})); e != nil {
		t.Fatal(e)
	}
	if _, _, e = c.ReserveReceiptData(ctx, a, tx); e == nil {
		t.Fatal("persisted incomplete receipt detail returned as complete")
	}
}
