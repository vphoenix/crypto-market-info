package across

import (
	"context"
	"encoding/json"
	"math/big"
	"os"
	"strings"
	"testing"
)

func TestReceiptTransferStrictParsingAndFrozenMembership(t *testing.T) {
	b, a := coreBatch(t)
	d := b.Deposits[0]
	r := TxReceipt{CaptureId: b.Capture.CaptureId, ChainId: d.ChainId, BlockNumber: d.BlockNumber, BlockHash: d.BlockHash, BlockTime: d.BlockTime, TxHash: d.TxHash, Success: true, ReceiptPayloadHash: ID("receipt")}
	token := strings.Repeat("u", 20)
	log := map[string]any{"address": Hex(token), "topics": []string{transferTopic, Hex(WordAddress(strings.Repeat("s", 20))), Hex(WordAddress(strings.Repeat("r", 20)))}, "data": "0x" + strings.Repeat("f", 64), "logIndex": "0x2", "transactionHash": Hex(r.TxHash), "blockHash": Hex(r.BlockHash), "blockNumber": "0xa", "transactionIndex": "0x0", "removed": false}
	env := map[string]any{"transactionHash": Hex(r.TxHash), "blockHash": Hex(r.BlockHash), "status": "0x1", "logs": []any{log}}
	encode := func() json.RawMessage {
		raw, err := json.Marshal(env)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	ts, err := parseReceiptTransferResult(encode(), r, token)
	if err != nil || len(ts) != 1 || ts[0].amount.BitLen() != 256 {
		t.Fatal(ts, err)
	}
	v := receiptTransferRow(r, token, ts)
	b.Transfers = []ReceiptTransfers{v}
	if err := ArchiveBatch(a, &b, nil, []string{r.ReceiptPayloadHash}); err != nil {
		t.Fatal(err)
	}
	if len(b.Capture.TableIds) != 7 {
		t.Fatal("transfer set not frozen")
	}
	if _, err := ReadCaptureEvidence(a, b.Capture); err != nil {
		t.Fatal(err)
	}
	b.Transfers[0].AmountsRaw[0] = big.NewInt(1)
	if Validate(b) == nil {
		t.Fatal("transfer mutation hidden")
	}
	env["logs"] = []any{}
	ts, err = parseReceiptTransferResult(encode(), r, token)
	if err != nil || len(ts) != 0 {
		t.Fatal("empty receipt is unknown", err)
	}
	env["logs"] = []any{log, log}
	if _, err := parseReceiptTransferResult(encode(), r, token); err == nil {
		t.Fatal("duplicate accepted")
	}
	env["logs"] = []any{log}
	log["blockNumber"] = "0xb"
	if _, err := parseReceiptTransferResult(encode(), r, token); err == nil {
		t.Fatal("wrong anchor accepted")
	}
	log["blockNumber"] = "0xa"
	log["topics"] = []string{transferTopic, Hex(strings.Repeat("a", 32)), Hex(WordAddress(strings.Repeat("r", 20)))}
	if _, err := parseReceiptTransferResult(encode(), r, token); err == nil {
		t.Fatal("non-EVM address accepted")
	}
}

func TestSourceArchiveHashOnlyLeavesNoResponseFile(t *testing.T) {
	r := mockReader(t, 42161, func(string, json.RawMessage) any { return []any{} })
	r.RPC.Archive.HashOnly = true
	if _, err := r.Logs(context.Background(), 1, 2); err != nil {
		t.Fatal(err)
	}
	if len(r.Members) == 0 || len(r.Members[0]) != 32 {
		t.Fatal("source hash missing")
	}
	entries, err := os.ReadDir(r.RPC.Archive.Dir)
	if err != nil || len(entries) != 0 {
		t.Fatal("response persisted", entries, err)
	}
}

func TestStoredRefundTransfersWorkWithoutRawBody(t *testing.T) {
	at := Now()
	r := TxReceipt{ChainId: 8453, BlockNumber: 10, BlockHash: ID("block"), BlockTime: at, TxHash: ID("tx"), Success: true, ReceiptPayloadHash: ID("raw")}
	v := receiptTransferRow(r, strings.Repeat("u", 20), []receiptTransfer{{strings.Repeat("s", 20), strings.Repeat("r", 20), big.NewInt(7), 2}})
	v.CaptureId = ID("capture")
	out, err := storedReceiptTransfers(v, r, v.Token)
	if err != nil || len(out) != 1 || out[0].amount.Cmp(big.NewInt(7)) != 0 {
		t.Fatal(out, err)
	}
	r.BlockHash = ID("orphan")
	if _, err := storedReceiptTransfers(v, r, v.Token); err == nil {
		t.Fatal("wrong branch accepted")
	}
	if err := ValidateReceiptTransfers(v); err != nil {
		t.Fatal(err)
	}
}
