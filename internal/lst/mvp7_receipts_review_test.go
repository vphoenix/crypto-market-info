package lst

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
)

// Build a genuine address bloom false positive from other-address topics.
// Adding bits to only the header would hide an incomplete-receipts bug.
func mvp7QueueFalsePositiveTopics(t *testing.T, queue string) []string {
	t.Helper()
	h := crypto.Keccak256([]byte(queue))
	var topics []string
	for i := 0; i < 6; i += 2 {
		want := (uint16(h[i])<<8 | uint16(h[i+1])) & 2047
		found := false
		for n := int64(1); n <= 50000 && !found; n++ {
			word := big.NewInt(n).FillBytes(make([]byte, 32))
			v := crypto.Keccak256(word)
			for j := 0; j < 6; j += 2 {
				if (uint16(v[j])<<8|uint16(v[j+1]))&2047 == want {
					topics = append(topics, Hex(string(word)))
					found = true
					break
				}
			}
		}
		if !found {
			t.Fatal("cannot construct a genuine bloom false positive")
		}
	}
	return topics
}

func TestMVP7ReceiptIdentityOnlyForRawQueueLogBlocks(t *testing.T) {
	tests := []struct {
		name         string
		live         bool
		to           uint64
		cap          int
		wantCalls    int
		wantIds      []uint64
		wantTo       uint64
		wantRequests int
		wantError    string
	}{
		{name: "negative", to: 102, cap: 20, wantCalls: 5, wantTo: 102},
		{name: "complete_receipts_no_queue_logs", to: 102, cap: 20, wantCalls: 8, wantTo: 102},
		{name: "queue_event_blocks_only", to: 102, cap: 20, wantCalls: 9, wantIds: []uint64{100, 102}, wantTo: 102, wantRequests: 2},
		{name: "unknown_impl", to: 102, cap: 20, wantCalls: 8, wantIds: []uint64{100, 102}, wantError: "historical_abi_unknown"},
		{name: "anonymous_queue_unknown_impl", to: 102, cap: 20, wantCalls: 8, wantIds: []uint64{100, 102}, wantError: "historical_abi_unknown"},
		{name: "unknown_topic_queue_unknown_impl", to: 102, cap: 20, wantCalls: 8, wantIds: []uint64{100, 102}, wantError: "historical_abi_unknown"},
		{name: "live_full_range", live: true, to: 102, cap: 20, wantCalls: 11, wantIds: []uint64{100, 101, 102}, wantTo: 102, wantRequests: 3},
		{name: "live_budget_prefix", live: true, to: 107, cap: 20, wantCalls: 18, wantIds: []uint64{100, 101, 102, 103, 104}, wantTo: 104, wantRequests: 5},
		{name: "live_budget_before_first", live: true, to: 107, cap: 4, wantCalls: 2, wantError: "live_receipt_piece_budget_exhausted_before_first_block"},
		{name: "live_real_source_failure", live: true, to: 107, cap: 20, wantCalls: 9, wantIds: []uint64{100, 101}, wantError: "source_http_500"},
		{name: "live_unknown_impl", live: true, to: 107, cap: 20, wantCalls: 10, wantIds: []uint64{100, 101, 102}, wantError: "historical_abi_unknown"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			template, queue, originals := receiptFixture(t)
			m, err := LoadManifest("../../config/lst-lido-ethereum.json")
			if err != nil {
				t.Fatal(err)
			}
			falseTopics := mvp7QueueFalsePositiveTopics(t, queue)
			blockData := func(n uint64) (map[string]any, []map[string]any) {
				hash, number := fmt.Sprintf("0x%064x", n), fmt.Sprintf("0x%x", n)
				txs := []string{fmt.Sprintf("0x%064x", n*2), fmt.Sprintf("0x%064x", n*2+1)}
				negative := tc.name == "negative" || (!tc.live && tc.name != "complete_receipts_no_queue_logs" && n == 101)
				bloom := make([]byte, 256)
				var receipts []map[string]any
				if negative {
					txs = []string{}
					receipts = []map[string]any{}
				} else {
					for i, original := range originals {
						copy := map[string]any{}
						for key, value := range original {
							copy[key] = value
						}
						copy["blockHash"], copy["blockNumber"], copy["transactionHash"] = hash, number, txs[i]
						logs := append([]chainLog(nil), original["logs"].([]chainLog)...)
						for j := range logs {
							logs[j].BlockHash, logs[j].BlockNumber, logs[j].TransactionHash = hash, number, txs[i]
							logs[j].Topics = append([]string(nil), logs[j].Topics...)
							if i == 1 {
								logs[j].Topics[1] = fmt.Sprintf("0x%064x", n-99)
							}
							if n == 102 && tc.name == "anonymous_queue_unknown_impl" {
								logs[j].Topics, logs[j].Data = []string{}, "0x"
							}
							if n == 102 && tc.name == "unknown_topic_queue_unknown_impl" {
								logs[j].Topics, logs[j].Data = []string{fmt.Sprintf("0x%064x", 444)}, "0x"
							}
						}
						if tc.name == "complete_receipts_no_queue_logs" {
							if i == 0 {
								logs[0].Topics = falseTopics
							} else {
								logs = []chainLog{}
							}
						}
						for _, log := range logs {
							address, e := ParseHex(log.Address, 20)
							if e != nil {
								t.Fatal(e)
							}
							bloomAdd(bloom, address)
							for _, topic := range log.Topics {
								word, e := ParseHex(topic, 32)
								if e != nil {
									t.Fatal(e)
								}
								bloomAdd(bloom, word)
							}
						}
						copy["logs"] = logs
						receipts = append(receipts, copy)
					}
				}
				if tc.name == "complete_receipts_no_queue_logs" {
					positive, e := bloomContains(string(bloom), queue)
					if e != nil || !positive {
						t.Fatal("fixture is not a real bloom false positive", e)
					}
				}
				return map[string]any{"number": number, "hash": hash, "parentHash": fmt.Sprintf("0x%064x", n-1), "timestamp": fmt.Sprintf("0x%x", template.Time.Add(-time.Hour).Add(time.Duration(n-100)*12*time.Second).Unix()), "logsBloom": Hex(string(bloom)), "transactions": txs}, receipts
			}
			calls, finalizedCalls, receiptCalls := 0, 0, 0
			var identities []uint64
			tr, _ := transportFixture(t, func(req *http.Request) (*http.Response, error) {
				calls++
				var call struct {
					ID     uint64            `json:"id"`
					Method string            `json:"method"`
					Params []json.RawMessage `json:"params"`
				}
				if e := json.NewDecoder(req.Body).Decode(&call); e != nil {
					t.Fatal(e)
				}
				var result any
				switch call.Method {
				case "eth_getBlockByNumber":
					var tag string
					if json.Unmarshal(call.Params[0], &tag) != nil {
						t.Fatal("header tag")
					}
					n := uint64(115)
					if tag == "finalized" {
						finalizedCalls++
					} else {
						n, err = q64(tag)
						if err != nil {
							t.Fatal(err)
						}
					}
					result, _ = blockData(n)
				case "eth_getBlockReceipts":
					receiptCalls++
					var hash string
					if json.Unmarshal(call.Params[0], &hash) != nil {
						t.Fatal("receipt hash")
					}
					word, e := ParseHex(hash, 32)
					if e != nil {
						t.Fatal(e)
					}
					n := new(big.Int).SetBytes([]byte(word)).Uint64()
					if tc.name == "live_real_source_failure" && n == 102 {
						return transportReply(500, `{"error":"upstream unavailable"}`), nil
					}
					_, result = blockData(n)
				case "eth_call":
					var target map[string]string
					var ref struct {
						BlockHash        string `json:"blockHash"`
						RequireCanonical bool   `json:"requireCanonical"`
					}
					if len(call.Params) != 2 || json.Unmarshal(call.Params[0], &target) != nil || json.Unmarshal(call.Params[1], &ref) != nil || !ref.RequireCanonical || target["to"] != Hex(queue) || target["data"] != Hex(string(crypto.Keccak256([]byte("proxy__getImplementation()"))[:4])) {
						t.Fatal("wrong queue identity call", target, ref)
					}
					word, e := ParseHex(ref.BlockHash, 32)
					if e != nil {
						t.Fatal(e)
					}
					n := new(big.Int).SetBytes([]byte(word)).Uint64()
					identities = append(identities, n)
					result = fmt.Sprintf("0x%024s%s", "", strings.TrimPrefix(m.Addresses["queue_impl"], "0x"))
					if n == 102 && strings.Contains(tc.name, "unknown_impl") {
						result = fmt.Sprintf("0x%064x", 0)
					}
				default:
					t.Fatal("unexpected method", call.Method)
				}
				raw, e := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": call.ID, "result": result})
				if e != nil {
					t.Fatal(e)
				}
				return transportReply(200, string(raw)), nil
			})
			tr.cfg.Clock, tr.started, tr.cfg.RPCRequestsPerMinute = realClock{}, Now().Add(-2*time.Minute), tc.cap
			tr.MarkInitialized()
			store := &runnerMemoryStore{}
			c := Collector{Manifest: m, RPC: &RPC{Transport: tr, URL: "https://rpc.example", ProtocolMulticall: true}, Store: store, StateDir: t.TempDir(), Archive: tr.archive, LogMode: "receipts"}
			ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
			defer cancel()
			end, e := c.RPC.Header(ctx, "finalized")
			if e != nil {
				t.Fatal(e)
			}
			batch, e := c.logsRange(ctx, 100, tc.to, "live", &end, tc.live)
			if calls != tc.wantCalls || finalizedCalls != 1 || !reflect.DeepEqual(identities, tc.wantIds) {
				t.Fatal("unexpected methods or block identities", calls, finalizedCalls, identities, e)
			}
			if tc.name == "negative" && receiptCalls != 0 {
				t.Fatal("negative bloom fetched receipts")
			}
			p := logProgress{Manifest: m.Hash, Start: 100, Next: 100, RangeSize: 8}
			if tc.wantError != "" {
				if e == nil || !strings.Contains(e.Error(), tc.wantError) || batch.Capture.Status != "failed" || batch.Capture.Canonical || len(batch.Requests)+len(batch.Claims)+len(batch.Finalizations) != 0 {
					t.Fatal("failure was converted to prefix coverage", batch.Capture, e)
				}
			} else if e != nil || batch.Capture.Status != "complete" || !batch.Capture.Canonical || batch.Capture.FromBlock == nil || *batch.Capture.FromBlock != 100 || batch.Capture.ToBlock == nil || *batch.Capture.ToBlock != tc.wantTo || batch.Capture.ToBlockHash == nil || *batch.Capture.ToBlockHash != mustMVP7Hash(t, tc.wantTo) || len(batch.Requests) != tc.wantRequests {
				t.Fatal("wrong exact verified coverage", batch.Capture, len(batch.Requests), e)
			}
			if tc.name == "live_budget_prefix" && !strings.Contains(batch.Capture.Reason, "planned_to=107 covered_to=104") {
				t.Fatal("bounded prefix not explicit", batch.Capture.Reason)
			}
			if ve := Validate(batch); ve != nil {
				t.Fatal(ve)
			}
			if ce := c.Commit(context.Background(), batch); ce != nil {
				t.Fatal(ce)
			}
			coveredTo := tc.to
			if e == nil {
				coveredTo = *batch.Capture.ToBlock
			}
			p.result(&c, coveredTo, e)
			if tc.wantError != "" && p.Next != 100 || tc.wantError == "" && p.Next != tc.wantTo+1 {
				t.Fatal("cursor did not follow committed coverage", p)
			}
		})
	}
}

func mustMVP7Hash(t *testing.T, n uint64) string {
	t.Helper()
	h, e := ParseHex(fmt.Sprintf("0x%064x", n), 32)
	if e != nil {
		t.Fatal(e)
	}
	return h
}
