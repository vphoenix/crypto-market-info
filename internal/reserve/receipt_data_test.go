package reserve

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/dex"
	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
)

func receiptDataFixture(t *testing.T, tag string) (dex.Receipt, map[string]any, []byte, []dex.Log) {
	t.Helper()
	anchor := dex.Anchor{ChainID: 1, Number: 42, Hash: dex.Digest([]byte("block:" + tag)), Time: time.Unix(1728000000, 0).UTC(), Manifest: dex.Digest([]byte("manifest:" + tag))}
	tx := dex.Digest([]byte("tx:" + tag))
	calldata := []byte{0x12, 0x34, 0x56, 0x78, 0xff, 0, 0xfe}
	topic := dex.Digest([]byte("transfer"))
	wanted := []dex.Log{{Anchor: anchor, TxHash: tx, TxIndex: 3, Index: 8, Emitter: testShare, Topics: []dex.Hash{topic}, Data: []byte{0xff, 0, 0xfe}}}
	logs := []any{}
	for _, l := range append(append([]dex.Log{}, wanted...), dex.Log{Anchor: anchor, TxHash: tx, TxIndex: 3, Index: 9, Emitter: USDT, Data: []byte{0x80, 0, 0x81}}) {
		topics := []string{}
		for _, v := range l.Topics {
			topics = append(topics, v.String())
		}
		logs = append(logs, map[string]any{"blockNumber": "0x2a", "blockHash": anchor.Hash.String(), "transactionHash": tx.String(), "transactionIndex": "0x3", "logIndex": "0x" + new(big.Int).SetUint64(uint64(l.Index)).Text(16), "address": l.Emitter.String(), "topics": topics, "data": "0x" + hex.EncodeToString(l.Data), "removed": false})
	}
	r := dex.Receipt{Anchor: anchor, TxHash: tx, TxIndex: 3, From: USDC, To: testShare, HasTo: true, Type: 2, Value: Uint(0), Selector: calldata[:4], CalldataHash: dex.Digest(calldata), Status: 1, GasUsed: 21000, GasPrice: Uint(2), LogCount: 2, AvailableAt: anchor.Time.Add(time.Second)}
	v := map[string]any{"transactionHash": tx.String(), "blockHash": anchor.Hash.String(), "blockNumber": "0x2a", "transactionIndex": "0x3", "from": r.From.String(), "to": r.To.String(), "type": "0x2", "status": "0x1", "gasUsed": "0x5208", "effectiveGasPrice": "0x2", "logs": logs}
	r.ReceiptHash = dex.Digest(receiptDataJSON(t, v))
	return r, v, calldata, wanted
}

func receiptDataJSON(t *testing.T, v any) []byte {
	t.Helper()
	raw, e := json.Marshal(v)
	if e != nil {
		t.Fatal(e)
	}
	return raw
}

func TestReceiptDataRetainsExternalBinaryLogsAndRejectsCorruption(t *testing.T) {
	r, v, input, wanted := receiptDataFixture(t, "complete")
	d, e := DecodeReceiptData(r, receiptDataJSON(t, v), input)
	if e != nil || len(d.Logs) != 2 || d.Logs[1].Emitter != Address(USDT) || d.Logs[1].Data != string([]byte{0x80, 0, 0x81}) || d.Calldata != string(input) {
		t.Fatal("full typed receipt lost external log or binary data", e)
	}
	if e = ReceiptDataContains(d, r, wanted); e != nil {
		t.Fatal("selected protocol log was not found", e)
	}
	for _, tc := range []struct {
		name string
		edit func(*ReceiptData)
	}{
		{"missing_external_log", func(d *ReceiptData) { d.Logs = d.Logs[:1]; d.DataHash = ReceiptDataHash(*d) }},
		{"duplicate_index", func(d *ReceiptData) { d.Logs[1].LogIndex = d.Logs[0].LogIndex; d.DataHash = ReceiptDataHash(*d) }},
		{"wrong_transaction", func(d *ReceiptData) { d.TxHash = Hash(dex.Digest([]byte("other"))); d.DataHash = ReceiptDataHash(*d) }},
		{"changed_input", func(d *ReceiptData) { d.Calldata += "x"; d.DataHash = ReceiptDataHash(*d) }},
		{"bad_topic", func(d *ReceiptData) { d.Logs[0].Topics = []string{"short"}; d.DataHash = ReceiptDataHash(*d) }},
		{"stale_digest", func(d *ReceiptData) { d.Logs[1].Data = string([]byte{0x82, 0, 0x81}) }},
		{"missing_materialization", func(d *ReceiptData) { d.MaterializedAt = time.Time{} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := d
			bad.Logs = append([]ReceiptLog{}, d.Logs...)
			tc.edit(&bad)
			if ValidateReceiptData(bad, r) == nil {
				t.Fatal("corrupt typed receipt accepted")
			}
		})
	}
	other := append([]dex.Log{}, wanted...)
	other[0].Data = []byte{0xfe, 0, 0xfe}
	if ReceiptDataContains(d, r, other) == nil {
		t.Fatal("conflicting selected log accepted")
	}
	later := d
	later.MaterializedAt = d.MaterializedAt.Add(time.Hour)
	if ReceiptDataHash(later) != d.DataHash {
		t.Fatal("retry timestamp changed immutable receipt identity")
	}
	binaryChange := d
	binaryChange.Logs = append([]ReceiptLog{}, d.Logs...)
	binaryChange.Logs[1].Data = string([]byte{0x82, 0, 0x81})
	if ReceiptDataHash(binaryChange) == d.DataHash {
		t.Fatal("non-UTF8 log bytes collided")
	}
}

func TestDecodeReceiptDataRejectsMissingAndConflictingSource(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(map[string]any)
	}{
		{"null_logs", func(v map[string]any) { v["logs"] = nil }},
		{"missing_logs", func(v map[string]any) { delete(v, "logs") }},
		{"wrong_gas", func(v map[string]any) { v["gasUsed"] = "0x1" }},
		{"wrong_sender", func(v map[string]any) { v["from"] = USDT.String() }},
		{"wrong_tx_index", func(v map[string]any) { v["transactionIndex"] = "0x4" }},
		{"duplicate_log", func(v map[string]any) { logs := v["logs"].([]any); v["logs"] = []any{logs[0], logs[0]} }},
		{"removed_log", func(v map[string]any) { v["logs"].([]any)[1].(map[string]any)["removed"] = true }},
		{"wrong_log_transaction", func(v map[string]any) {
			v["logs"].([]any)[1].(map[string]any)["transactionHash"] = dex.Digest([]byte("other")).String()
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, v, input, _ := receiptDataFixture(t, tc.name)
			tc.edit(v)
			raw := receiptDataJSON(t, v)
			r.ReceiptHash = dex.Digest(raw)
			if _, e := DecodeReceiptData(r, raw, input); e == nil {
				t.Fatal("conflicting source accepted")
			}
		})
	}
	r, v, input, _ := receiptDataFixture(t, "hash")
	if _, e := DecodeReceiptData(r, append(receiptDataJSON(t, v), ' '), input); e == nil {
		t.Fatal("different source bytes accepted under original receipt hash")
	}
}

func TestReceiptDataExplicitZeroLogs(t *testing.T) {
	r, v, input, _ := receiptDataFixture(t, "empty")
	r.LogCount = 0
	v["logs"] = []any{}
	raw := receiptDataJSON(t, v)
	r.ReceiptHash = dex.Digest(raw)
	d, e := DecodeReceiptData(r, raw, input)
	if e != nil || d.Logs == nil || d.LogCount != 0 {
		t.Fatal("explicit empty receipt not distinguished from missing", e)
	}
	if e = ReceiptDataContains(d, r, nil); e != nil {
		t.Fatal(e)
	}
}

type receiptMigrationMemory struct {
	receipts []dex.Receipt
	data     map[string]ReceiptData
	writes   int
}

func receiptMigrationKey(a dex.Anchor, tx dex.Hash) string { return Hash(a.Hash) + Hash(tx) }
func (s *receiptMigrationMemory) ReserveReceipts(context.Context) ([]dex.Receipt, error) {
	return append([]dex.Receipt{}, s.receipts...), nil
}
func (s *receiptMigrationMemory) ReserveReceiptData(_ context.Context, a dex.Anchor, tx dex.Hash) (ReceiptData, bool, error) {
	d, ok := s.data[receiptMigrationKey(a, tx)]
	return d, ok, nil
}
func (s *receiptMigrationMemory) WriteReserveReceiptData(_ context.Context, d ReceiptData, r dex.Receipt) error {
	if e := ValidateReceiptData(d, r); e != nil {
		return e
	}
	s.data[receiptMigrationKey(r.Anchor, r.TxHash)] = d
	s.writes++
	return nil
}

func TestMigrateReceiptDataAllManifestsIdempotentWithoutArchive(t *testing.T) {
	archive := ethereum.Archive{Dir: t.TempDir()}
	store := &receiptMigrationMemory{data: map[string]ReceiptData{}}
	for _, tag := range []string{"old_manifest", "orphan_branch"} {
		r, v, input, _ := receiptDataFixture(t, tag)
		if _, e := archive.Put(receiptDataJSON(t, v)); e != nil {
			t.Fatal(e)
		}
		if _, e := archive.Put(input); e != nil {
			t.Fatal(e)
		}
		store.receipts = append(store.receipts, r)
	}
	if n, e := MigrateReceiptData(context.Background(), store, archive); e != nil || n != 2 || store.writes != 2 {
		t.Fatal("all stored identities not migrated", n, store.writes, e)
	}
	if e := os.RemoveAll(archive.Dir); e != nil {
		t.Fatal(e)
	}
	archive.HashOnly = true
	if n, e := MigrateReceiptData(context.Background(), store, archive); e != nil || n != 2 || store.writes != 2 {
		t.Fatal("migration retry depended on deleted files or rewrote facts", n, store.writes, e)
	}
	r := store.receipts[0]
	d := store.data[receiptMigrationKey(r.Anchor, r.TxHash)]
	d.LogCount++
	store.data[receiptMigrationKey(r.Anchor, r.TxHash)] = d
	if _, e := MigrateReceiptData(context.Background(), store, archive); e == nil {
		t.Fatal("corrupt existing materialization passed deletion gate")
	}
}

func TestMigrateReceiptDataFailureAndCancellationAreNotComplete(t *testing.T) {
	r, _, _, _ := receiptDataFixture(t, "missing")
	store := &receiptMigrationMemory{receipts: []dex.Receipt{r}, data: map[string]ReceiptData{}}
	if n, e := MigrateReceiptData(context.Background(), store, ethereum.Archive{Dir: t.TempDir()}); e == nil || n != 0 || store.writes != 0 {
		t.Fatal("missing source marked migrated", n, store.writes, e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, e := MigrateReceiptData(ctx, store, ethereum.Archive{HashOnly: true}); !errors.Is(e, context.Canceled) {
		t.Fatal("cancellation lost", e)
	}
}

func TestHashOnlySourceClassificationUsesHTTPErrorInMemory(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		json.NewEncoder(w).Encode([]map[string]any{{"jsonrpc": "2.0", "id": 1, "error": map[string]any{"code": -32602, "message": "Archive requests require a personal token."}}})
	}))
	defer server.Close()
	rpc, e := ethereum.NewClient(server.URL, t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	rpc.Archive.HashOnly = true
	result := rpc.One(context.Background(), "eth_getBlockByNumber", []any{"0x1", false})
	if got := SourceError(rpc, result); got != "rpc_archive_auth_required" {
		t.Fatal("HTTP 403 classification depended on discarded response file", got)
	}
	if entries, e := os.ReadDir(rpc.Archive.Dir); e != nil || len(entries) != 0 {
		t.Fatal("error classification wrote response files", e)
	}
}
