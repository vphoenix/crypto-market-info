package across

import (
	"context"
	"encoding/json"
	"math/big"
	"strings"
	"testing"

	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
)

func TestTransferMigrationOnlyRefundsAndIdempotent(t *testing.T) {
	m := reportTestManifest()
	a := ethereum.Archive{Dir: t.TempDir()}
	receipt := historyReceiptFixture(t, m, "refund receipt", "receipts", ID("refund tx"), true)
	r := &receipt.Receipts[0]
	r.Success = true
	raw, _ := json.Marshal([]any{map[string]any{"result": map[string]any{"transactionHash": Hex(r.TxHash), "blockHash": Hex(r.BlockHash), "status": "0x1", "logs": []any{}}}})
	h, err := a.PutObject(struct{ Response []byte }{raw})
	if err != nil {
		t.Fatal(err)
	}
	r.ReceiptPayloadHash = string(h[:])
	if err := ArchiveBatch(a, &receipt, nil, nil); err != nil {
		t.Fatal(err)
	}
	logs := historyReceiptFixture(t, m, "refund event", "logs", r.TxHash, true)
	logs.Fills = nil
	logs.Refunds = []Refund{{CaptureId: logs.Capture.CaptureId, ChainId: r.ChainId, SpokePool: m.Chains[0].SpokePool, BlockNumber: r.BlockNumber, BlockHash: r.BlockHash, BlockTime: r.BlockTime, TxHash: r.TxHash, EventKind: "leaf_execution", Token: m.Chains[0].USDC, RefundAddresses: []string{strings.Repeat("a", 20)}, RefundAmountsRaw: []*big.Int{big.NewInt(7)}, Caller: strings.Repeat("a", 20), AvailableAt: r.AvailableAt, PayloadHash: ID("event")}}
	if err := ArchiveBatch(a, &logs, nil, nil); err != nil {
		t.Fatal(err)
	}
	ordinary := historyReceiptFixture(t, m, "ordinary", "receipts", ID("ordinary tx"), true)
	store := &coreStore{caps: []Capture{logs.Capture, receipt.Capture, ordinary.Capture}, batches: map[string]Batch{logs.Capture.CaptureId: logs, receipt.Capture.CaptureId: receipt, ordinary.Capture.CaptureId: ordinary}}
	original := ID(receipt)
	stats, err := MigrateReceiptTransfers(context.Background(), store, m, a)
	if err != nil || stats.RowsWritten != 1 || stats.ReceiptsChecked != 1 || stats.BodiesAlreadyAbsent != 0 {
		t.Fatal(stats, err)
	}
	if ID(store.batches[receipt.Capture.CaptureId]) != original {
		t.Fatal("old receipt changed")
	}
	stats, err = MigrateReceiptTransfers(context.Background(), store, m, a)
	if err != nil || stats.RowsWritten != 0 || stats.ReceiptsChecked != 0 {
		t.Fatal("migration repeated", stats, err)
	}
}

func TestRefundReceiptWithKnownFeeStillRepairsMissingTransfers(t *testing.T) {
	m := reportTestManifest()
	a := ethereum.Archive{Dir: t.TempDir()}
	receipt := historyReceiptFixture(t, m, "known fee missing transfers", "receipts", ID("refund tx"), true)
	r := receipt.Receipts[0]
	logs := historyReceiptFixture(t, m, "refund missing transfers", "logs", r.TxHash, true)
	logs.Fills = nil
	logs.Refunds = []Refund{{CaptureId: logs.Capture.CaptureId, ChainId: r.ChainId, SpokePool: m.Chains[0].SpokePool, BlockNumber: r.BlockNumber, BlockHash: r.BlockHash, BlockTime: r.BlockTime, TxHash: r.TxHash, EventKind: "leaf_execution", Token: m.Chains[0].USDC, RefundAddresses: []string{strings.Repeat("a", 20)}, RefundAmountsRaw: []*big.Int{big.NewInt(7)}, Caller: strings.Repeat("a", 20), AvailableAt: r.AvailableAt, PayloadHash: ID("event")}}
	if err := ArchiveBatch(a, &logs, nil, nil); err != nil {
		t.Fatal(err)
	}
	store := &coreStore{caps: []Capture{logs.Capture, receipt.Capture}, batches: map[string]Batch{logs.Capture.CaptureId: logs, receipt.Capture.CaptureId: receipt}}
	c := Collector{Manifest: m, Store: store, receiptQueue: map[string]receiptTask{}}
	if err := c.RefreshRepairReceipts(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(c.receiptQueue) != 1 {
		t.Fatal("complete fee hid missing transfers", c.receiptQueue)
	}
	v := receiptTransferRow(r, m.Chains[0].USDC, nil)
	v.CaptureId = ID("migrated")
	b := Batch{Capture: receipt.Capture, Transfers: []ReceiptTransfers{v}}
	b.Capture.CaptureId = v.CaptureId
	b.Capture.CaptureKind = "receipt_transfers"
	if err := ArchiveBatch(a, &b, nil, nil); err != nil {
		t.Fatal(err)
	}
	store.caps = append(store.caps, b.Capture)
	store.batches[b.Capture.CaptureId] = b
	if err := c.RefreshRepairReceipts(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(c.receiptQueue) != 0 {
		t.Fatal("stored transfer set still retries", c.receiptQueue)
	}
}
