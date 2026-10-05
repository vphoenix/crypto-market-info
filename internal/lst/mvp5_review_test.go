package lst

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"strings"
	"testing"
	"time"
)

// This exercises the production plan plus real fetch/validate/commit/cursor
// boundaries. HTTP is entirely mocked; the real clock preserves source and
// capture ordering while the original 500ms send gate remains enabled.
func TestOptionalReceiptPieceUsesRemainingBudgetWithoutFailedCoverage(t *testing.T) {
	for _, tc := range []struct {
		planned uint64
		slots   int
		want    uint64
	}{{8, 0, 0}, {8, 5, 0}, {8, 6, 1}, {8, 8, 1}, {8, 11, 2}, {8, 21, 6}, {8, 32, 8}, {2, 21, 2}, {0, 32, 0}} {
		if got := receiptPieceBlocks(tc.planned, tc.slots); got != tc.want {
			t.Fatalf("plan=%d slots=%d got=%d want=%d", tc.planned, tc.slots, got, tc.want)
		}
	}
	template, _, receipts := receiptFixture(t)
	m, err := LoadManifest("../../config/lst-lido-ethereum.json")
	if err != nil {
		t.Fatal(err)
	}
	blockData := func(n uint64) (map[string]any, []map[string]any) {
		hash, number := fmt.Sprintf("0x%064x", n), fmt.Sprintf("0x%x", n)
		txs := []string{fmt.Sprintf("0x%064x", n*2), fmt.Sprintf("0x%064x", n*2+1)}
		out := make([]map[string]any, len(receipts))
		bloom := make([]byte, 256)
		for i, original := range receipts {
			copy := map[string]any{}
			for k, v := range original {
				copy[k] = v
			}
			copy["blockHash"], copy["blockNumber"], copy["transactionHash"] = hash, number, txs[i]
			logs := append([]chainLog(nil), original["logs"].([]chainLog)...)
			for j := range logs {
				logs[j].BlockHash, logs[j].BlockNumber, logs[j].TransactionHash = hash, number, txs[i]
				logs[j].Topics = append([]string(nil), logs[j].Topics...)
				if logs[j].Topics[0] == requestedTopic {
					logs[j].Topics[1] = fmt.Sprintf("0x%064x", n-99)
				}
				address, e := ParseHex(logs[j].Address, 20)
				if e != nil {
					t.Fatal(e)
				}
				bloomAdd(bloom, address)
				for _, topic := range logs[j].Topics {
					word, e := ParseHex(topic, 32)
					if e != nil {
						t.Fatal(e)
					}
					bloomAdd(bloom, word)
				}
			}
			copy["logs"] = logs
			out[i] = copy
		}
		head := map[string]any{"number": number, "hash": hash, "parentHash": fmt.Sprintf("0x%064x", n-1), "timestamp": fmt.Sprintf("0x%x", template.Time.Add(-time.Hour).Add(time.Duration(n-100)*12*time.Second).Unix()), "logsBloom": Hex(string(bloom)), "transactions": txs}
		return head, out
	}
	calls, finalizedCalls, receiptCalls := 0, 0, 0
	tr, _ := transportFixture(t, func(req *http.Request) (*http.Response, error) {
		calls++
		var call struct {
			Id     uint64
			Method string
			Params []json.RawMessage
		}
		if e := json.NewDecoder(req.Body).Decode(&call); e != nil {
			t.Fatal(e)
		}
		var result any
		switch call.Method {
		case "eth_getBlockByNumber":
			var tag string
			if e := json.Unmarshal(call.Params[0], &tag); e != nil {
				t.Fatal(e)
			}
			n := uint64(115)
			if tag == "finalized" {
				finalizedCalls++
			} else {
				var e error
				n, e = q64(tag)
				if e != nil {
					t.Fatal(e)
				}
			}
			result, _ = blockData(n)
		case "eth_getBlockReceipts":
			var hash string
			if e := json.Unmarshal(call.Params[0], &hash); e != nil {
				t.Fatal(e)
			}
			encoded, e := ParseHex(hash, 32)
			if e != nil {
				t.Fatal(e)
			}
			n := new(big.Int).SetBytes([]byte(encoded)).Uint64()
			_, result = blockData(n)
			receiptCalls++
		case "eth_call":
			result = fmt.Sprintf("0x%024s%s", "", strings.TrimPrefix(m.Addresses["queue_impl"], "0x"))
		default:
			t.Fatal("unexpected mocked RPC", call.Method)
		}
		raw, e := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": call.Id, "result": result})
		if e != nil {
			t.Fatal(e)
		}
		return transportReply(200, string(raw)), nil
	})
	tr.cfg.Clock = realClock{}
	tr.started = Now().Add(-2 * time.Minute)
	tr.cfg.RPCRequestsPerMinute = 32
	tr.MarkInitialized()
	store := &runnerMemoryStore{}
	c := &Collector{Manifest: m, RPC: &RPC{Transport: tr, URL: "https://rpc.example"}, Store: store, StateDir: t.TempDir(), Archive: tr.archive, LogMode: "receipts"}
	p := logProgress{Manifest: m.Hash, Start: 100, Next: 100, RangeSize: 8}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	for pass, expected := range []struct {
		from, to, next uint64
		calls          int
	}{{100, 104, 105, 18}, {105, 107, 108, 30}} {
		planned := uint64(5)
		if pass > 0 {
			slots, e := tr.AvailableRPCSlots(c.RPC.URL)
			if e != nil || slots != 14 {
				t.Fatal("wrong remaining shared quota", slots, e)
			}
			planned = receiptPieceBlocks(planned, slots)
		}
		// Include the runner's outer head, in addition to Logs' own verification.
		end, e := c.RPC.Header(ctx, "finalized")
		if e != nil {
			t.Fatal(e)
		}
		to := min(p.Next+min(p.RangeSize, planned)-1, end.Number)
		b, e := c.Logs(ctx, p.Next, to, "live")
		if e != nil {
			t.Fatal("bounded piece failed", pass, e)
		}
		if e = Validate(b); e != nil {
			t.Fatal(e)
		}
		if e = c.Commit(context.Background(), b); e != nil {
			t.Fatal(e)
		}
		p.result(c, to, nil)
		if p.Next != expected.next || calls != expected.calls || b.Capture.Status != "complete" || !b.Capture.Canonical || b.Capture.Finality != "finalized" || *b.Capture.FromBlock != expected.from || *b.Capture.ToBlock != expected.to || len(b.Requests) != int(expected.to-expected.from+1) {
			t.Fatal("wrong exact committed coverage", pass, p.Next, calls, b.Capture)
		}
	}
	if len(store.batches) != 2 || finalizedCalls != 4 || receiptCalls != 8 {
		t.Fatal("missing independent pieces or wrong source costs", len(store.batches), finalizedCalls, receiptCalls)
	}
	requests := 0
	for _, b := range store.batches {
		if b.Capture.Status != "complete" {
			t.Fatal("failed coverage was committed")
		}
		requests += len(b.Requests)
	}
	if requests != 8 {
		t.Fatal("request facts lost", requests)
	}
	slots, err := tr.AvailableRPCSlots(c.RPC.URL)
	if err != nil || slots != 2 || receiptPieceBlocks(c.liveLogBlocks(), slots) != 0 || calls != 30 || p.Next != 108 {
		t.Fatal("deferred piece changed HTTP or coverage", slots, calls, p.Next, err)
	}
}
