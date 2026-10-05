package lst

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/dex"
)

// Independent fixture order is deliberately explicit: it does not derive the
// expected target/selectors or protocol fields from protocolViewSpecs().
func TestMVP7MulticallAllViewsExactAndFailureEvidence(t *testing.T) {
	m, err := LoadManifest("../../config/lst-lido-ethereum.json")
	if err != nil {
		t.Fatal(err)
	}
	b := Block{Number: 100, Hash: strings.Repeat("h", 32), Time: time.Unix(1790000000, 0).UTC(), BaseFee: big.NewInt(17)}
	b.Response = Response{PayloadHash: HashBytes([]byte("independent head evidence")), RequestedAt: time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC), ReceivedAt: time.Date(2026, 10, 1, 0, 0, 1, 0, time.UTC), AvailableAt: time.Date(2026, 10, 1, 0, 0, 1, 0, time.UTC)}
	specs := []struct{ role, signature, output string }{
		{"steth", "implementation()", "address"}, {"queue", "proxy__getImplementation()", "address"},
		{"steth", "getTotalPooledEther()", "uint256"}, {"steth", "getTotalShares()", "uint256"},
		{"queue", "MIN_STETH_WITHDRAWAL_AMOUNT()", "uint256"}, {"queue", "MAX_STETH_WITHDRAWAL_AMOUNT()", "uint256"},
		{"queue", "getLastRequestId()", "uint256"}, {"queue", "getLastFinalizedRequestId()", "uint256"},
		{"queue", "unfinalizedStETH()", "uint256"}, {"queue", "getLockedEtherAmount()", "uint256"},
		{"wsteth", "getStETHByWstETH(uint256)", "uint256"}, {"queue", "isPaused()", "bool"}, {"queue", "isBunkerModeActive()", "bool"},
		{"multicall", "getBlockNumber()", "uint256"}, {"multicall", "getCurrentBlockTimestamp()", "uint256"},
	}
	large := func(bits uint, delta int64) *big.Int {
		return new(big.Int).Add(new(big.Int).Lsh(big.NewInt(1), bits), big.NewInt(delta))
	}
	values := []any{common.HexToAddress(m.Addresses["steth_impl"]), common.HexToAddress(m.Addresses["queue_impl"]), large(100, 101), large(99, 303), large(60, 5), large(61, 7), large(70, 11), large(70, 9), large(100, 25), large(101, 33), large(54, 3), false, true, new(big.Int).SetUint64(b.Number), big.NewInt(b.Time.Unix())}
	contract, err := abi.JSON(strings.NewReader(multicallABIJSON))
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"valid", "implementation_mismatch", "protocol_invariant", "subcall_failure", "execution_time_mismatch", "source_429"} {
		t.Run(name, func(t *testing.T) {
			results := make([]multicallResult, len(specs))
			for i, s := range specs {
				args, e := arguments([]string{s.output})
				if e != nil {
					t.Fatal(e)
				}
				data, e := args.Pack(values[i])
				if e != nil {
					t.Fatal(e)
				}
				results[i] = multicallResult{Success: true, ReturnData: data}
			}
			switch name {
			case "implementation_mismatch":
				results[0].ReturnData = make([]byte, 32)
			case "protocol_invariant":
				results[3].ReturnData = make([]byte, 32)
			case "subcall_failure":
				results[8].Success = false
			case "execution_time_mismatch":
				results[14].ReturnData = make([]byte, 32)
			}
			payload, e := contract.Methods["aggregate3"].Outputs.Pack(results)
			if e != nil {
				t.Fatal(e)
			}
			calls := 0
			tr, _ := transportFixture(t, func(req *http.Request) (*http.Response, error) {
				calls++
				var call struct {
					ID     uint64            `json:"id"`
					Method string            `json:"method"`
					Params []json.RawMessage `json:"params"`
				}
				if e := json.NewDecoder(req.Body).Decode(&call); e != nil || call.Method != "eth_call" || len(call.Params) != 2 {
					t.Fatal("unexpected request", call, e)
				}
				var target map[string]string
				var ref struct {
					BlockHash        string `json:"blockHash"`
					RequireCanonical bool   `json:"requireCanonical"`
				}
				if json.Unmarshal(call.Params[0], &target) != nil || json.Unmarshal(call.Params[1], &ref) != nil || ref.BlockHash != Hex(b.Hash) || !ref.RequireCanonical || target["to"] != Hex(multicallAddress()) || target["value"] != "" {
					t.Fatal("aggregate lost its real block/zero-value identity", target, ref)
				}
				data, e := decodeBytes(target["data"])
				if e != nil || len(data) < 4 || !bytes.Equal(data[:4], contract.Methods["aggregate3"].ID) {
					t.Fatal("unexpected aggregate selector", e)
				}
				decoded, e := contract.Methods["aggregate3"].Inputs.Unpack(data[4:])
				if e != nil {
					t.Fatal(e)
				}
				inputs := *abi.ConvertType(decoded[0], new([]multicallInput)).(*[]multicallInput)
				if len(inputs) != len(specs) {
					t.Fatal("protocol member count", len(inputs))
				}
				for i, s := range specs {
					address := m.Address(s.role)
					if s.role == "multicall" {
						address = multicallAddress()
					}
					want := append([]byte(nil), crypto.Keccak256([]byte(s.signature))[:4]...)
					if i == 10 {
						word := big.NewInt(1_000_000_000_000_000_000).FillBytes(make([]byte, 32))
						want = append(want, word...)
					}
					if inputs[i].Target != common.BytesToAddress([]byte(address)) || !inputs[i].AllowFailure || !bytes.Equal(inputs[i].CallData, want) {
						t.Fatal("protocol view changed or omitted", i, s.signature)
					}
				}
				if name == "source_429" {
					return transportReply(429, fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"error":{"code":15,"message":"rate limit"}}`, call.ID)), nil
				}
				return transportReply(200, fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"result":"0x%s"}`, call.ID, hex.EncodeToString(payload))), nil
			})
			rpc := RPC{Transport: tr, URL: "https://rpc.example", ProtocolMulticall: true}
			p, proof, e := rpc.Protocol(context.Background(), m, b)
			if calls != 1 || len(proof) != 2 || len(p.PayloadHashes) != 2 || p.PayloadHashes[0] != b.Response.PayloadHash || p.PayloadHashes[1] != proof[1].PayloadHash || p.RequestedAt == nil || p.ReceivedAt == nil || p.SourceAvailableAt == nil {
				t.Fatal("aggregate fabricated subresponse proofs or dropped the real header/response", calls, p, len(proof))
			}
			if name == "valid" {
				if e != nil || p.StateStatus != "ok" || !p.IdentityOk || p.QueuePaused == nil || *p.QueuePaused || p.BunkerActive == nil || !*p.BunkerActive {
					t.Fatal("valid exact protocol rejected", p, e)
				}
				actual := []*big.Int{p.StethTotalPooledEthWei, p.StethTotalSharesRaw, p.MinRequestStethWei, p.MaxRequestStethWei, p.LastRequestId, p.LastFinalizedRequestId, p.UnfinalizedStethWei, p.LockedEthWei, p.OneWstethToStethWei}
				for i, v := range actual {
					if v == nil || v.Cmp(values[i+2].(*big.Int)) != 0 {
						t.Fatal("integer field/order changed", i, v, values[i+2])
					}
				}
				if p.BaseFeePerGasWei.Cmp(b.BaseFee) != 0 || !p.ReceivedAt.Equal(proof[1].ReceivedAt) || !p.SourceAvailableAt.Equal(proof[1].AvailableAt) {
					t.Fatal("real source time/base fee changed")
				}
				return
			}
			if e == nil || p.StateStatus != "unknown" || p.Reason == "" || !strings.Contains(p.Reason, "http_attempted=true") || !strings.Contains(p.Reason, "response="+Hex(proof[1].PayloadHash)) {
				t.Fatal("failed aggregate lost its actual HTTP evidence", name, p.Reason, e)
			}
			if name == "source_429" && (!strings.Contains(p.Reason, "http=429") || !strings.Contains(p.Reason, "rpc=15")) {
				t.Fatal("source cooldown/error semantics erased", p.Reason)
			}
			if name != "protocol_invariant" && (p.IdentityOk || p.StethTotalPooledEthWei != nil || p.QueuePaused != nil || p.BunkerActive != nil) {
				t.Fatal("failed aggregate invented successful protocol members", p)
			}
		})
	}
}

func TestMVP7FinalityNoPendingUsesZeroRPC(t *testing.T) {
	for _, name := range []string{"empty", "finalized", "orphaned", "logs", "uncommitted"} {
		t.Run(name, func(t *testing.T) {
			calls := 0
			tr, _ := transportFixture(t, func(*http.Request) (*http.Response, error) { calls++; return nil, fmt.Errorf("unexpected RPC") })
			store := &runnerMemoryStore{batches: map[uuid.UUID]Batch{}}
			if name != "empty" {
				id := uuid.New()
				cap := Capture{CaptureId: id, CaptureKind: "market", Committed: true, Finality: "head", ToBlock: Ptr(uint64(100))}
				switch name {
				case "logs":
					cap.CaptureKind = "logs"
				case "uncommitted":
					cap.Committed = false
				default:
					cap.Finality = name
				}
				store.batches[id] = Batch{Capture: cap}
			}
			c := Collector{RPC: &RPC{Transport: tr, URL: "https://rpc.example"}, Store: store}
			if e := c.FinalizePending(context.Background()); e != nil || calls != 0 {
				t.Fatal("no eligible pending market consumed RPC", name, calls, e)
			}
		})
	}
}

func TestMVP7FinalityReusesObservedHeaderAndStillChecksEachMarketHash(t *testing.T) {
	for _, reuse := range []bool{false, true} {
		t.Run(fmt.Sprintf("reuse_%t", reuse), func(t *testing.T) {
			store := &runnerMemoryStore{batches: map[uuid.UUID]Batch{}}
			ids := map[uint64]uuid.UUID{}
			for _, n := range []uint64{100, 101, 103} {
				id := uuid.New()
				ids[n] = id
				hash, e := ParseHex(fmt.Sprintf("0x%064x", n), 32)
				if e != nil {
					t.Fatal(e)
				}
				if n == 101 {
					hash = strings.Repeat("x", 32)
				}
				store.batches[id] = Batch{Capture: Capture{CaptureId: id, CaptureKind: "market", Committed: true, Finality: "head", Revision: 1, ToBlock: Ptr(n), ToBlockHash: &hash}}
			}
			var tags []string
			tr, _ := transportFixture(t, func(req *http.Request) (*http.Response, error) {
				var call struct {
					ID     uint64            `json:"id"`
					Method string            `json:"method"`
					Params []json.RawMessage `json:"params"`
				}
				if e := json.NewDecoder(req.Body).Decode(&call); e != nil || call.Method != "eth_getBlockByNumber" || len(call.Params) != 2 {
					t.Fatal("unexpected finality RPC", e, call)
				}
				var tag string
				if json.Unmarshal(call.Params[0], &tag) != nil {
					t.Fatal("invalid tag")
				}
				tags = append(tags, tag)
				n := uint64(102)
				if tag != "finalized" {
					var e error
					n, e = q64(tag)
					if e != nil || n == 103 {
						t.Fatal("premature or invalid market finality", tag)
					}
				}
				return transportReply(200, fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"result":{"number":"0x%x","hash":"0x%064x","parentHash":"0x%064x","timestamp":"0x6aaaaaaa","transactions":[]}}`, call.ID, n, n, n-1)), nil
			})
			c := Collector{RPC: &RPC{Transport: tr, URL: "https://rpc.example"}, Store: store}
			var observed *Block
			if reuse {
				hash, e := ParseHex(fmt.Sprintf("0x%064x", 102), 32)
				if e != nil {
					t.Fatal(e)
				}
				observed = &Block{Number: 102, Hash: hash, Time: time.Unix(1789569706, 0).UTC(), Response: Response{PayloadHash: HashBytes([]byte("prior actual finalized response")), ReceivedAt: Now()}}
			}
			if e := c.finalizePending(context.Background(), observed); e != nil {
				t.Fatal(e)
			}
			want := 3
			if reuse {
				want = 2
			}
			if len(tags) != want {
				t.Fatal("duplicate finalized read or missing canonical verification", tags)
			}
			for _, tag := range tags {
				if reuse && tag == "finalized" {
					t.Fatal("reuse issued an extra finalized request")
				}
			}
			good, bad, future := store.batches[ids[100]].Capture, store.batches[ids[101]].Capture, store.batches[ids[103]].Capture
			if good.Finality != "finalized" || !good.Canonical || good.Revision != 2 || bad.Finality != "orphaned" || bad.Canonical || bad.Revision != 2 || future.Finality != "head" || future.Revision != 1 {
				t.Fatal("finality reuse changed eligibility or hash checks", good, bad, future)
			}
		})
	}
}

func TestMVP7SequentialLocalGateFailureDoesNotBorrowEarlierResponse(t *testing.T) {
	m, e := LoadManifest("../../config/lst-lido-ethereum.json")
	if e != nil {
		t.Fatal(e)
	}
	calls := 0
	tr, _ := transportFixture(t, func(req *http.Request) (*http.Response, error) {
		calls++
		var call struct {
			ID uint64 `json:"id"`
		}
		if json.NewDecoder(req.Body).Decode(&call) != nil {
			t.Fatal("request decode")
		}
		return transportReply(200, fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"result":"0x%024s%s"}`, call.ID, "", strings.TrimPrefix(m.Addresses["steth_impl"], "0x"))), nil
	})
	tr.cfg.Clock = realClock{}
	tr.started = Now().Add(-2 * time.Minute)
	tr.cfg.RPCRequestsPerMinute = 1
	tr.MarkInitialized()
	r := RPC{Transport: tr, URL: "https://rpc.example"}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	p, proof, e := r.Protocol(ctx, m, Block{Number: 100, Hash: strings.Repeat("h", 32), Time: Now()})
	if e == nil || calls != 1 || len(proof) != 2 || !strings.Contains(p.Reason, "cause=local_gate_budget_exhausted") || !strings.Contains(p.Reason, "http_attempted=false http=0") || strings.Contains(p.Reason, "response=") {
		t.Fatal("unsent sequential failure reused the previous successful response", calls, p.Reason, e)
	}
}

func TestMVP7SharedFinalizedLogsPreserveCoverageWhenQuotaEnds(t *testing.T) {
	template, _, originalReceipts := receiptFixture(t)
	m, e := LoadManifest("../../config/lst-lido-ethereum.json")
	if e != nil {
		t.Fatal(e)
	}
	blockData := func(n uint64) (map[string]any, []map[string]any) {
		hash, number := fmt.Sprintf("0x%064x", n), fmt.Sprintf("0x%x", n)
		txs := []string{fmt.Sprintf("0x%064x", n*2), fmt.Sprintf("0x%064x", n*2+1)}
		receipts := make([]map[string]any, len(originalReceipts))
		bloom := make([]byte, 256)
		for i, original := range originalReceipts {
			copy := map[string]any{}
			for key, value := range original {
				copy[key] = value
			}
			copy["blockHash"], copy["blockNumber"], copy["transactionHash"] = hash, number, txs[i]
			logs := append([]chainLog(nil), original["logs"].([]chainLog)...)
			for j := range logs {
				logs[j].BlockHash, logs[j].BlockNumber, logs[j].TransactionHash = hash, number, txs[i]
				logs[j].Topics = append([]string(nil), logs[j].Topics...)
				if logs[j].Topics[0] == requestedTopic {
					logs[j].Topics[1] = fmt.Sprintf("0x%064x", n-99)
				}
				address, err := ParseHex(logs[j].Address, 20)
				if err != nil {
					t.Fatal(err)
				}
				bloomAdd(bloom, address)
				for _, topic := range logs[j].Topics {
					word, err := ParseHex(topic, 32)
					if err != nil {
						t.Fatal(err)
					}
					bloomAdd(bloom, word)
				}
			}
			copy["logs"] = logs
			receipts[i] = copy
		}
		return map[string]any{"number": number, "hash": hash, "parentHash": fmt.Sprintf("0x%064x", n-1), "timestamp": fmt.Sprintf("0x%x", template.Time.Add(-time.Hour).Add(time.Duration(n-100)*12*time.Second).Unix()), "logsBloom": Hex(string(bloom)), "transactions": txs}, receipts
	}
	calls, finalizedCalls := 0, 0
	tr, _ := transportFixture(t, func(req *http.Request) (*http.Response, error) {
		calls++
		var call struct {
			ID     uint64            `json:"id"`
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		if json.NewDecoder(req.Body).Decode(&call) != nil {
			t.Fatal("request decode")
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
				var err error
				n, err = q64(tag)
				if err != nil {
					t.Fatal(err)
				}
			}
			result, _ = blockData(n)
		case "eth_getBlockReceipts":
			var hash string
			if json.Unmarshal(call.Params[0], &hash) != nil {
				t.Fatal("receipt hash")
			}
			decoded, err := ParseHex(hash, 32)
			if err != nil {
				t.Fatal(err)
			}
			_, result = blockData(new(big.Int).SetBytes([]byte(decoded)).Uint64())
		case "eth_call":
			result = fmt.Sprintf("0x%024s%s", "", strings.TrimPrefix(m.Addresses["queue_impl"], "0x"))
		default:
			t.Fatal("unexpected RPC", call.Method)
		}
		raw, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": call.ID, "result": result})
		if err != nil {
			t.Fatal(err)
		}
		return transportReply(200, string(raw)), nil
	})
	tr.cfg.Clock = realClock{}
	tr.started = Now().Add(-2 * time.Minute)
	tr.cfg.RPCRequestsPerMinute = 20
	tr.MarkInitialized()
	store := &runnerMemoryStore{}
	c := Collector{Manifest: m, RPC: &RPC{Transport: tr, URL: "https://rpc.example", ProtocolMulticall: true}, Store: store, StateDir: t.TempDir(), Archive: tr.archive, LogMode: "receipts"}
	p := logProgress{Manifest: m.Hash, Start: 100, Next: 100, RangeSize: 8}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	end, err := c.RPC.Header(ctx, "finalized")
	if err != nil {
		t.Fatal(err)
	}
	first, err := c.logsWithFinalized(ctx, 100, 104, "live", &end)
	if err != nil || calls != 17 || finalizedCalls != 1 || len(first.Requests) != 5 || first.Capture.Status != "complete" || !first.Capture.Canonical {
		t.Fatal("head reuse lost coverage or added a finalized request", calls, finalizedCalls, first.Capture, err)
	}
	if err = Validate(first); err != nil {
		t.Fatal(err)
	}
	if err = c.Commit(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	p.result(&c, 104, nil)
	if p.Next != 105 {
		t.Fatal("committed range did not advance exactly", p)
	}
	slots, err := tr.AvailableRPCSlots(c.RPC.URL)
	if err != nil || slots != 3 || receiptPieceBlocksWithFinalized(8, max(slots-2, 0)) != 0 || calls != 17 {
		t.Fatal("optional planner attempted an unaffordable range", slots, calls, err)
	}
	// Even an accidental direct overrun must preserve the already committed
	// piece. This tests the original gate rather than resetting its recent list.
	failed, err := c.logsWithFinalized(ctx, 105, 112, "live", &end)
	if err == nil || !strings.Contains(err.Error(), "local_gate_budget_exhausted") || calls != 20 || finalizedCalls != 1 || failed.Capture.Status != "failed" || failed.Capture.Canonical || len(failed.Requests)+len(failed.Claims)+len(failed.Finalizations) != 0 {
		t.Fatal("budget failure produced coverage or escaped the original gate", calls, failed.Capture, err)
	}
	if e = Validate(failed); e != nil {
		t.Fatal(e)
	}
	if e = c.Commit(context.Background(), failed); e != nil {
		t.Fatal(e)
	}
	p.result(&c, 112, err)
	if p.Next != 105 || p.RangeSize != 4 || len(store.batches) != 2 || len(store.batches[first.Capture.CaptureId].Requests) != 5 {
		t.Fatal("failed range advanced or damaged committed coverage", p, store.batches)
	}
	var root struct{ Responses []string }
	hash := dex.Hash{}
	copy(hash[:], first.Capture.EvidenceRootHash)
	raw, e := c.Archive.Get(hash)
	if e != nil {
		t.Fatal(e)
	}
	if e = json.Unmarshal(raw, &root); e != nil {
		t.Fatal(e)
	}
	found := false
	for _, hash := range root.Responses {
		if hash == Hex(end.Response.PayloadHash) {
			found = true
		}
	}
	if !found {
		t.Fatal("shared finalized proof was silently replaced or omitted")
	}
}
