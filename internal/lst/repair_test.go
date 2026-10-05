package lst

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

func TestCooldownReturnsWithoutHTTPOrSleeping(t *testing.T) {
	calls := 0
	tr, clock := transportFixture(t, func(*http.Request) (*http.Response, error) { calls++; return transportReply(200, `{}`), nil })
	clock.at = Now()
	g, e := tr.gate("rpc", "https://rpc.example")
	if e != nil {
		t.Fatal(e)
	}
	g.state.CooldownUntil = clock.Now().Add(30 * time.Minute)
	if e = saveGate(g); e != nil {
		t.Fatal(e)
	}
	before := clock.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	res, e := tr.Do(ctx, "rpc", "POST", "https://rpc.example", transportRPCBody, "normal")
	var wait *gateWaitError
	if !errors.As(e, &wait) || wait.Cause != "source_cooldown" || calls != 0 || !clock.Now().Equal(before) || !res.RequestedAt.IsZero() || res.RequestHash == "" {
		t.Fatal("cooldown became a network timeout", res, e, calls)
	}
	if !strings.Contains(failureReason("head", e, res), "http_attempted=false") {
		t.Fatal("wrong attempt classification")
	}
}

func TestNetworkClassificationAndAttemptEvidence(t *testing.T) {
	for _, tc := range []struct {
		name  string
		err   error
		cause string
	}{
		{"dns", &net.DNSError{Err: "temporary credential-url", Name: "private-host"}, "transport_dns_error"},
		{"dial", &net.OpError{Op: "dial", Err: errors.New("credential")}, "transport_connect_error"},
		{"timeout", context.DeadlineExceeded, "transport_http_timeout"},
		{"cancel", context.Canceled, "transport_request_cancelled"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tr, _ := transportFixture(t, func(*http.Request) (*http.Response, error) { return nil, tc.err })
			res, e := tr.Do(context.Background(), "rpc", "POST", "https://rpc.example/credential", transportRPCBody, "normal")
			reason := failureReason("shares", e, res)
			if !strings.Contains(reason, tc.cause) || !strings.Contains(reason, "http_attempted=true") || len(res.PayloadHash) != 66 || strings.Contains(reason, "credential") || strings.Contains(reason, "private-host") {
				t.Fatal(reason)
			}
		})
	}
}

func TestCapabilityPauseSurvivesRestartWithoutRequests(t *testing.T) {
	calls := 0
	c := runnerReadyCollector(t, &runnerMemoryStore{}, &calls)
	p := logProgress{Manifest: c.Manifest.Hash, Start: 100, Next: 100, RangeSize: 512}
	p.result(c, 611, errors.Join(ErrLogSourceUnsupported, &RPCError{35, "ranges over 10000 blocks are not supported on free plan"}))
	if p.Next != 100 || p.RangeSize != 512 || p.sourceAllowed(c.logSourceIdentity()) {
		t.Fatal(p)
	}
	if e := writeGob(filepath.Join(c.StateDir, "live-logs.gob"), p); e != nil {
		t.Fatal(e)
	}
	recovered, e := c.logProgress(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	if recovered.sourceAllowed(c.logSourceIdentity()) || calls != 0 || recovered.Next != 100 {
		t.Fatal("pause did not persist", recovered, calls)
	}
	c.LogMode = "receipts"
	if !recovered.sourceAllowed(c.logSourceIdentity()) || recovered.Next != 100 {
		t.Fatal("mode change skipped the gap")
	}
	recovered.RangeSize = 8
	recovered.result(c, 107, errors.New("local_gate_budget_exhausted"))
	if recovered.Next != 100 || recovered.RangeSize != 4 {
		t.Fatal("failed scan advanced", recovered)
	}
	recovered.result(c, 103, nil)
	if recovered.Next != 104 || recovered.RangeSize != 5 {
		t.Fatal("success did not advance exact range", recovered)
	}
}

func receiptFixture(t *testing.T) (Block, string, []map[string]any) {
	t.Helper()
	m, e := LoadManifest("../../config/lst-lido-ethereum.json")
	if e != nil {
		t.Fatal(e)
	}
	h := Block{Number: 100, Hash: strings.Repeat("h", 32), Time: Now(), Transactions: []string{Hex(strings.Repeat("a", 32)), Hex(strings.Repeat("b", 32))}}
	logs := []chainLog{
		{Address: m.Addresses["steth"], Topics: []string{eventTopic("Transfer(address,address,uint256)")}, Data: "0x", BlockNumber: "0x64", BlockHash: Hex(h.Hash), TransactionHash: h.Transactions[0], TransactionIndex: "0x0", LogIndex: "0x0"},
		{Address: m.Addresses["queue"], Topics: []string{requestedTopic, fmt.Sprintf("0x%064x", 1), fmt.Sprintf("0x%064x", 2), fmt.Sprintf("0x%064x", 3)}, Data: fmt.Sprintf("0x%064x%064x", big.NewInt(1).Lsh(big.NewInt(1), 100), 100), BlockNumber: "0x64", BlockHash: Hex(h.Hash), TransactionHash: h.Transactions[1], TransactionIndex: "0x1", LogIndex: "0x1"},
	}
	bloom := make([]byte, 256)
	for _, l := range logs {
		bloomAdd(bloom, string(common.HexToAddress(l.Address).Bytes()))
		for _, topic := range l.Topics {
			v, _ := ParseHex(topic, 32)
			bloomAdd(bloom, v)
		}
	}
	h.LogsBloom = string(bloom)
	recs := []map[string]any{}
	for i, tx := range h.Transactions {
		recs = append(recs, map[string]any{"transactionHash": tx, "transactionIndex": fmt.Sprintf("0x%x", i), "blockHash": Hex(h.Hash), "blockNumber": "0x64", "status": "0x1", "logs": []chainLog{logs[i]}})
	}
	return h, m.Address("queue"), recs
}

func TestCompleteReceiptsValidationAndBloom(t *testing.T) {
	for _, name := range []string{"valid", "uppercase_address", "missing_address_prefix", "missing_receipt", "empty_array", "duplicate_tx", "wrong_block", "wrong_tx_index", "missing_logs", "removed", "noncontiguous_log", "bad_topic", "bloom_mismatch", "missing_header_transactions", "missing_bloom"} {
		t.Run(name, func(t *testing.T) {
			h, queue, recs := receiptFixture(t)
			switch name {
			case "uppercase_address":
				l := recs[1]["logs"].([]chainLog)
				l[0].Address = "0x" + strings.ToUpper(l[0].Address[2:])
			case "missing_address_prefix":
				l := recs[1]["logs"].([]chainLog)
				l[0].Address = l[0].Address[2:]
			case "missing_receipt":
				recs = recs[:1]
			case "empty_array":
				recs = []map[string]any{}
			case "duplicate_tx":
				recs[1]["transactionHash"] = recs[0]["transactionHash"]
			case "wrong_block":
				recs[1]["blockHash"] = Hex(strings.Repeat("z", 32))
			case "wrong_tx_index":
				recs[1]["transactionIndex"] = "0x0"
			case "missing_logs":
				delete(recs[0], "logs")
			case "removed":
				l := recs[1]["logs"].([]chainLog)
				l[0].Removed = true
			case "noncontiguous_log":
				l := recs[1]["logs"].([]chainLog)
				l[0].LogIndex = "0x2"
			case "bad_topic":
				l := recs[1]["logs"].([]chainLog)
				l[0].Topics[0] = "0x01"
			case "bloom_mismatch":
				h.LogsBloom = strings.Repeat("\x00", 256)
			case "missing_header_transactions":
				h.Transactions = nil
			case "missing_bloom":
				h.LogsBloom = ""
			}
			raw, _ := json.Marshal(recs)
			logs, e := queueLogsFromReceipts(raw, h, queue)
			if name == "valid" || name == "uppercase_address" {
				if e != nil || len(logs) != 1 {
					t.Fatal(logs, e)
				}
				if b, e := bloomContains(h.LogsBloom, queue); e != nil || !b {
					t.Fatal("false negative", e)
				}
			} else if e == nil {
				t.Fatal("incomplete receipts accepted", name)
			}
		})
	}
	if _, e := bloomContains("", strings.Repeat("q", 20)); e == nil {
		t.Fatal("missing bloom treated as absence")
	}
}

func TestSharesAndDepthKeepUnderlyingFailure(t *testing.T) {
	m, e := LoadManifest("../../config/lst-lido-ethereum.json")
	if e != nil {
		t.Fatal(e)
	}
	calls := 0
	tr, _ := transportFixture(t, func(req *http.Request) (*http.Response, error) {
		calls++
		var call struct{ Id uint64 }
		json.NewDecoder(req.Body).Decode(&call)
		if calls == 1 {
			return transportReply(200, fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"result":"0x%064x"}`, call.Id, 100)), nil
		}
		return transportReply(429, fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"error":{"code":15,"message":"rate limit exceeded"}}`, call.Id)), nil
	})
	c := Collector{RPC: &RPC{Transport: tr, URL: "https://rpc.example"}, Manifest: m}
	q := Quote{RouteId: "A"}
	p := ProtocolState{StateStatus: "ok", QueuePaused: Ptr(false), MinRequestStethWei: big.NewInt(1), MaxRequestStethWei: big.NewInt(1000)}
	c.chainEntry(context.Background(), Block{}, p, &q, big.NewInt(1), Response{}, 0)
	for _, part := range []string{"stage=steth_shares", "source_rate_limited", "http_attempted=true", "http=429", "rpc=15", "request=0x", "response=0x"} {
		if !strings.Contains(q.ConversionReason, part) {
			t.Fatal(q.ConversionReason)
		}
	}
	q = Quote{}
	ApplyHedge(&q, cexTestMetadata(), CEXMarket{DepthErr: errors.New("round_budget_exhausted")}, nil)
	if !strings.Contains(q.HedgeReason, "stage=cex_depth cause=round_budget_exhausted http_attempted=false") {
		t.Fatal(q.HedgeReason)
	}
}

// Selectors are used only to give the fixture independent ABI responses.
func selector(sig string) string { return fmt.Sprintf("%x", crypto.Keccak256([]byte(sig))[:4]) }

func TestMarketScheduledMembersCompleteAndUnplannedMembersRemainBlank(t *testing.T) {
	for _, scheduled := range []int{1, 2} {
		t.Run(fmt.Sprint(scheduled), func(t *testing.T) {
			t.Parallel()
			m, e := LoadManifest("../../config/lst-lido-ethereum.json")
			if e != nil {
				t.Fatal(e)
			}
			rpcCalls := 0
			tr, clock := transportFixture(t, func(req *http.Request) (*http.Response, error) {
				ms := Now().UnixMilli()
				if req.Method == "GET" {
					if req.URL.Path == "/fapi/v1/depth" {
						return transportReply(200, fmt.Sprintf(`{"lastUpdateId":1,"E":%d,"T":%d,"bids":[["2000.00","1000.000"]],"asks":[["2001.00","1000.000"]]}`, ms, ms)), nil
					}
					return transportReply(200, fmt.Sprintf(`{"symbol":"ETHUSDT","time":%d,"nextFundingTime":%d,"markPrice":"2000.00","indexPrice":"2000.00","lastFundingRate":"0.0001"}`, ms, ms+3600000)), nil
				}
				rpcCalls++
				var call struct {
					Id     uint64
					Method string
					Params []json.RawMessage
				}
				if e := json.NewDecoder(req.Body).Decode(&call); e != nil {
					t.Fatal(e)
				}
				var result any
				if call.Method == "eth_getBlockByNumber" {
					result = map[string]any{"number": "0x64", "hash": Hex(strings.Repeat("h", 32)), "parentHash": Hex(strings.Repeat("p", 32)), "timestamp": fmt.Sprintf("0x%x", Now().Add(-time.Second).Unix())}
				} else {
					var data struct{ Data string }
					json.Unmarshal(call.Params[0], &data)
					sig := data.Data[2:10]
					n := big.NewInt(1)
					switch sig {
					case selector("implementation()"):
						result = fmt.Sprintf("0x%024s%s", "", strings.TrimPrefix(m.Addresses["steth_impl"], "0x"))
					case selector("proxy__getImplementation()"):
						result = fmt.Sprintf("0x%024s%s", "", strings.TrimPrefix(m.Addresses["queue_impl"], "0x"))
					case selector("quoteExactInput(bytes,uint256)"):
						args, _ := arguments([]string{"uint256", "uint160[]", "uint32[]", "uint256"})
						out, err := args.Pack(new(big.Int).Mul(big.NewInt(10), big.NewInt(1e18)), []*big.Int{big.NewInt(1)}, []uint32{1}, big.NewInt(100))
						if err != nil {
							t.Fatal(err)
						}
						result = Hex(string(out))
					default:
						switch sig {
						case selector("isPaused()"), selector("isBunkerModeActive()"):
							n = big.NewInt(0)
						case selector("MAX_STETH_WITHDRAWAL_AMOUNT()"):
							n = new(big.Int).Exp(big.NewInt(10), big.NewInt(30), nil)
						case selector("get_dy(int128,int128,uint256)"), selector("getStETHByWstETH(uint256)"), selector("getSharesByPooledEth(uint256)"):
							n = new(big.Int).Mul(big.NewInt(10), big.NewInt(1e18))
						}
						result = fmt.Sprintf("0x%064x", n)
					}
				}
				raw, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": call.Id, "result": result})
				return transportReply(200, string(raw)), nil
			})
			clock.at = Now().Add(-2 * time.Minute)
			tr.MarkInitialized()
			cex, e := NewCEX(tr, "https://cex.example")
			if e != nil {
				t.Fatal(e)
			}
			c := Collector{EntryRoutesPerRound: scheduled, RPC: &RPC{Transport: tr, URL: "https://rpc.example"}, CEX: cex, Manifest: m, Metadata: cexTestMetadata(), Archive: tr.archive}
			b, e := c.Market(context.Background(), nil)
			if e != nil {
				t.Fatal(e)
			}
			if e = Validate(b); e != nil {
				t.Fatal(e)
			}
			expectedRPC := 23
			if scheduled == 1 {
				expectedRPC = 19
				if b.Quotes[0].RouteId == "B" {
					expectedRPC = 20
				}
			}
			if rpcCalls != expectedRPC || len(b.Quotes) != 8 || b.Protocols[0].StateStatus != "ok" {
				t.Fatal("unexpected scheduled work", rpcCalls, b.Protocols[0].Reason)
			}
			for i, q := range b.Quotes {
				if i < scheduled {
					if q.BuyStatus != "ok" || q.ConversionStatus != "ok" || q.ExitStatus != "ok" {
						t.Fatal("scheduled route failed", q)
					}
				} else if q.TimingStatus != "not_scheduled" || len(q.ChainPayloadHashes) != 0 || q.HedgeDepthPayloadHash != nil || q.MarkPayloadHash != nil || q.BuyLstOutRaw != nil {
					t.Fatal("unplanned member has source data", q)
				}
			}
			if b.Capture.Status != "partial" {
				t.Fatal("eight-member coverage claimed complete")
			}
			b.Quotes[scheduled].ChainRequestedAt = Ptr(Now())
			if e = b.Seal(); e == nil {
				t.Fatal("unplanned member accepted a source time")
			}
		})
	}
}

func TestLogsReceiptsCompleteAndReorgLeavesNoEvents(t *testing.T) {
	for _, reorg := range []bool{false, true} {
		t.Run(fmt.Sprint(reorg), func(t *testing.T) {
			h, queue, recs := receiptFixture(t)
			calls, headerCalls := 0, 0
			tr, _ := transportFixture(t, func(req *http.Request) (*http.Response, error) {
				calls++
				var call struct {
					Id     uint64
					Method string
				}
				json.NewDecoder(req.Body).Decode(&call)
				var result any
				switch call.Method {
				case "eth_getBlockByNumber":
					headerCalls++
					bh := h.Hash
					if reorg && headerCalls == 3 {
						bh = strings.Repeat("z", 32)
					}
					result = map[string]any{"number": "0x64", "hash": Hex(bh), "parentHash": Hex(strings.Repeat("p", 32)), "timestamp": fmt.Sprintf("0x%x", h.Time.Unix()), "logsBloom": Hex(h.LogsBloom), "transactions": h.Transactions}
				case "eth_call":
					m, _ := LoadManifest("../../config/lst-lido-ethereum.json")
					result = fmt.Sprintf("0x%024s%s", "", strings.TrimPrefix(m.Addresses["queue_impl"], "0x"))
				case "eth_getBlockReceipts":
					result = recs
				default:
					t.Fatal("unexpected log method", call.Method)
				}
				raw, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": call.Id, "result": result})
				return transportReply(200, string(raw)), nil
			})
			m, _ := LoadManifest("../../config/lst-lido-ethereum.json")
			if queue != m.Address("queue") {
				t.Fatal("fixture queue")
			}
			c := Collector{RPC: &RPC{Transport: tr, URL: "https://rpc.example"}, Manifest: m, LogMode: "receipts", Archive: tr.archive}
			b, e := c.Logs(context.Background(), 100, 100, "live")
			if e2 := Validate(b); e2 != nil {
				t.Fatal(e2)
			}
			if reorg {
				if e == nil || b.Capture.Status != "failed" || b.Capture.Canonical || len(b.Requests) != 0 {
					t.Fatal("reorg became coverage", b, e)
				}
			} else if e != nil || b.Capture.Status != "complete" || !b.Capture.Canonical || len(b.Requests) != 1 || b.Requests[0].AmountStethWei.BitLen() != 101 || calls != 5 {
				t.Fatal("complete receipt log failed", b, e, calls)
			}
		})
	}
}

func TestReceiptScanRejectsWrongMiddleHeightOrParent(t *testing.T) {
	for _, fault := range []string{"none", "height", "parent"} {
		t.Run(fault, func(t *testing.T) {
			m, _ := LoadManifest("../../config/lst-lido-ethereum.json")
			tr, _ := transportFixture(t, func(req *http.Request) (*http.Response, error) {
				var call struct {
					Id     uint64
					Method string
					Params []json.RawMessage
				}
				json.NewDecoder(req.Body).Decode(&call)
				var result any
				if call.Method == "eth_call" {
					result = fmt.Sprintf("0x%024s%s", "", strings.TrimPrefix(m.Addresses["queue_impl"], "0x"))
				} else if call.Method == "eth_getBlockByNumber" {
					var tag string
					json.Unmarshal(call.Params[0], &tag)
					n := uint64(102)
					if tag != "finalized" {
						n, _ = q64(tag)
					}
					parent := n - 1
					hash := n
					if n == 101 {
						if fault == "height" {
							n = 105
						}
						if fault == "parent" {
							parent = 10
						}
					}
					result = map[string]any{"number": fmt.Sprintf("0x%x", n), "hash": fmt.Sprintf("0x%064x", hash), "parentHash": fmt.Sprintf("0x%064x", parent), "timestamp": fmt.Sprintf("0x%x", Now().Unix()), "logsBloom": "0x" + strings.Repeat("00", 256), "transactions": []string{}}
				} else {
					t.Fatal("negative bloom issued receipts request", call.Method)
				}
				raw, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": call.Id, "result": result})
				return transportReply(200, string(raw)), nil
			})
			c := Collector{RPC: &RPC{Transport: tr, URL: "https://rpc.example"}, Manifest: m, LogMode: "receipts", Archive: tr.archive}
			b, e := c.Logs(context.Background(), 100, 102, "live")
			if fault == "none" {
				if e != nil || b.Capture.Status != "complete" {
					t.Fatal(b, e)
				}
			} else if e == nil || b.Capture.Status != "failed" || b.Capture.Canonical {
				t.Fatal("unanchored middle became complete", b, e)
			}
		})
	}
}

func TestRateAndBanRPCDiagnosticsNeverDriveCapabilityPause(t *testing.T) {
	for _, status := range []int{200, 400, 429, 403, 418, 500} {
		re := &RPCError{35, "ranges over 10000 blocks are not supported on free plan"}
		var e error = re
		if status != 200 {
			cause := fmt.Sprintf("source_http_%d", status)
			if status == 429 {
				cause = "source_rate_limited"
			}
			if status == 403 || status == 418 {
				cause = "source_disabled"
			}
			e = errors.Join(errors.New(cause), re)
		}
		got := classifyLogRPCError(e, Response{HTTPStatus: status})
		if (got != nil) != (status == 200 || status == 400) {
			t.Fatal("transport semantics reinterpreted", status, got)
		}
	}
	if classifyLogRPCError(errors.Join(errors.New("source_rate_limited"), &RPCError{-32601, "method not supported"}), Response{HTTPStatus: 200}) != nil {
		t.Fatal("body rate limit became capability rejection")
	}
}

func TestTwoMinuteSingleEntryRotationCoversEveryAmountAndRoute(t *testing.T) {
	c := Collector{MarketInterval: 2 * time.Minute, EntryRoutesPerRound: 1}
	at := time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
	seen := map[string]int{}
	for i := 0; i < 16; i++ {
		order, routes, scheduled := c.entryPlan(at.Add(time.Duration(i) * 2 * time.Minute))
		if scheduled != 1 || len(order) != 4 || len(routes) != 2 {
			t.Fatal("bad plan", order, routes, scheduled)
		}
		budgets := map[int]bool{}
		for _, n := range order {
			budgets[n] = true
		}
		if len(budgets) != 4 {
			t.Fatal("missing budget identity")
		}
		seen[fmt.Sprintf("%d:%s", order[0], routes[0])]++
	}
	if len(seen) != 8 {
		t.Fatal("two-minute rotation starved amount or route", seen)
	}
	for k, n := range seen {
		if n != 2 {
			t.Fatal(k, n)
		}
	}
	for i := 0; i < 3; i++ {
		order, _, _ := c.entryPlan(time.Date(2026, 10, 3, 12, 2*i, 0, 0, time.UTC))
		if order[0] != 2 {
			t.Fatal("daily seed amount not prioritized")
		}
	}
	c.EntryRoutesPerRound = 2
	seenBudget := map[int]bool{}
	for i := 0; i < 4; i++ {
		order, _, n := c.entryPlan(at.Add(time.Duration(i) * 2 * time.Minute))
		seenBudget[order[0]] = true
		if n != 2 {
			t.Fatal("pair mode")
		}
	}
	if len(seenBudget) != 4 {
		t.Fatal("pair mode amount parity alias")
	}
}

func TestRPCMinuteLimitPreservesRecentRequestsAcrossRestart(t *testing.T) {
	var sends []time.Time
	var clock *transportFakeClock
	tr, fakeClock := transportFixture(t, func(*http.Request) (*http.Response, error) {
		sends = append(sends, clock.Now())
		return transportReply(200, `{"jsonrpc":"2.0","id":1,"result":"0x1"}`), nil
	})
	clock = fakeClock
	tr.cfg.RPCRequestsPerMinute = 32
	clock.at = clock.Now().Add(2 * time.Minute)
	tr.MarkInitialized()
	for i := 0; i < 80; i++ {
		if i == 40 {
			var e error
			tr, e = NewTransport(tr.cfg)
			if e != nil {
				t.Fatal(e)
			}
			tr.MarkInitialized()
		}
		if _, e := tr.Do(context.Background(), "rpc", "POST", "https://rpc.example", transportRPCBody, "normal"); e != nil {
			t.Fatal(e)
		}
	}
	for _, at := range sends {
		count := 0
		for _, v := range sends {
			if !v.After(at) && v.After(at.Add(-time.Minute)) {
				count++
			}
		}
		if count > 32 {
			t.Fatal("durable minute limit exceeded", at, count)
		}
	}
	if len(sends) != 80 {
		t.Fatal("missing sends")
	}
}

func TestMarketRejectsInvalidSchedulingBeforeAnySource(t *testing.T) {
	for _, c := range []Collector{{MarketInterval: 500 * time.Millisecond}, {MarketInterval: -time.Minute}, {MarketInterval: time.Second}, {EntryRoutesPerRound: 3}, {EntryRoutesPerRound: -1}} {
		if _, e := c.Market(context.Background(), nil); e == nil {
			t.Fatal("invalid schedule accepted")
		}
	}
}

func TestMarketCapacityWaitHappensWithoutHTTPOrQuotaReset(t *testing.T) {
	sends := 0
	tr, clock := transportFixture(t, func(*http.Request) (*http.Response, error) {
		sends++
		return transportReply(200, `{"jsonrpc":"2.0","id":1,"result":"0x1"}`), nil
	})
	tr.cfg.RPCRequestsPerMinute = 32
	tr.started = clock.Now().Add(-2 * time.Minute)
	tr.MarkInitialized()
	for i := 0; i < 32; i++ {
		if _, e := tr.Do(context.Background(), "rpc", "POST", "https://rpc.example", transportRPCBody, "normal"); e != nil {
			t.Fatal(e)
		}
	}
	before := clock.Now()
	if e := tr.WaitRPCSlots(context.Background(), "https://rpc.example", 20); e != nil {
		t.Fatal(e)
	}
	g, e := tr.gate("rpc", "https://rpc.example")
	if e != nil {
		t.Fatal(e)
	}
	recent := 0
	for _, r := range g.state.Recent {
		if r.At.After(clock.Now().Add(-time.Minute)) {
			recent++
		}
	}
	if sends != 32 || len(g.state.Recent) != 32 || recent != 12 || !clock.Now().After(before) {
		t.Fatal("wait sent HTTP or reset quota", sends, recent, len(g.state.Recent))
	}
	g.state.CooldownUntil = clock.Now().Add(time.Hour)
	before = clock.Now()
	if e := tr.WaitRPCSlots(context.Background(), "https://rpc.example", 20); e != nil || clock.Now() != before {
		t.Fatal("cooldown hidden by capacity wait", e)
	}
	if e := tr.WaitRPCSlots(context.Background(), "https://rpc.example", 33); e == nil {
		t.Fatal("impossible capacity accepted")
	}
}

// Exercise the same fetch/commit/cursor order as the runner: exhausting the
// shared quota on the second piece must retain the first committed coverage.
func TestSecondReceiptPieceBudgetFailureKeepsCommittedFirstPiece(t *testing.T) {
	h, _, recs := receiptFixture(t)
	m, e := LoadManifest("../../config/lst-lido-ethereum.json")
	if e != nil {
		t.Fatal(e)
	}
	calls := 0
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
			json.Unmarshal(call.Params[0], &tag)
			n := uint64(115)
			if tag != "finalized" {
				n, e = q64(tag)
				if e != nil {
					t.Fatal(e)
				}
			}
			result = map[string]any{"number": fmt.Sprintf("0x%x", n), "hash": fmt.Sprintf("0x%064x", n), "parentHash": fmt.Sprintf("0x%064x", n-1), "timestamp": fmt.Sprintf("0x%x", h.Time.Unix()), "logsBloom": Hex(h.LogsBloom), "transactions": h.Transactions}
		case "eth_getBlockReceipts":
			var hash string
			json.Unmarshal(call.Params[0], &hash)
			n := new(big.Int)
			n.SetString(strings.TrimPrefix(hash, "0x"), 16)
			piece := make([]map[string]any, len(recs))
			for i, original := range recs {
				copy := map[string]any{}
				for k, v := range original {
					copy[k] = v
				}
				copy["blockHash"], copy["blockNumber"] = hash, fmt.Sprintf("0x%x", n)
				logs := append([]chainLog(nil), original["logs"].([]chainLog)...)
				for j := range logs {
					logs[j].BlockHash, logs[j].BlockNumber = hash, fmt.Sprintf("0x%x", n)
				}
				copy["logs"] = logs
				piece[i] = copy
			}
			result = piece
		case "eth_call":
			result = fmt.Sprintf("0x%024s%s", "", strings.TrimPrefix(m.Addresses["queue_impl"], "0x"))
		default:
			t.Fatal(call.Method)
		}
		raw, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": call.Id, "result": result})
		if err != nil {
			t.Fatal(err)
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
	for pass := 0; pass < 2; pass++ {
		to := p.Next + 7
		b, err := c.Logs(ctx, p.Next, to, "live")
		if ve := Validate(b); ve != nil {
			t.Fatal(ve, err)
		}
		if ce := c.Commit(context.Background(), b); ce != nil {
			t.Fatal(ce)
		}
		p.result(c, to, err)
		if pass == 0 {
			if err != nil || p.Next != 108 || len(b.Requests) != 8 || b.Capture.Status != "complete" {
				t.Fatal(p, b.Capture, err)
			}
		} else if err == nil || !strings.Contains(err.Error(), "local_gate_budget_exhausted") || p.Next != 108 || p.RangeSize != 4 || b.Capture.Status != "failed" || len(b.Requests) != 0 {
			t.Fatal(p, b.Capture, err)
		}
	}
	if calls != 32 || len(store.batches) != 2 {
		t.Fatal(calls, len(store.batches))
	}
	for _, b := range store.batches {
		if b.Capture.Status == "complete" && (*b.Capture.FromBlock != 100 || *b.Capture.ToBlock != 107 || len(b.Requests) != 8) {
			t.Fatal("first coverage changed")
		}
	}
}

func TestConfiguredStartupGapAndAvailableQuota(t *testing.T) {
	var sent []time.Time
	var clock *transportFakeClock
	tr, c := transportFixture(t, func(*http.Request) (*http.Response, error) {
		sent = append(sent, clock.Now())
		return transportReply(200, `{}`), nil
	})
	clock = c
	tr.cfg.StartupRPCGap = 10 * time.Second
	tr.cfg.RPCRequestsPerMinute = 32
	for i := 0; i < 9; i++ {
		if _, err := tr.Do(context.Background(), "rpc", "POST", "https://rpc.example", transportRPCBody, "normal"); err != nil {
			t.Fatal(err)
		}
	}
	for i := 1; i < len(sent); i++ {
		if sent[i].Sub(sent[i-1]) < 10*time.Second {
			t.Fatal("preflight burst", sent)
		}
	}
	before := clock.Now()
	g, _ := tr.gate("rpc", "https://rpc.example")
	recent := len(g.state.Recent)
	slots, err := tr.AvailableRPCSlots("https://rpc.example")
	if err != nil || slots != 26 || clock.Now() != before || len(g.state.Recent) != recent || len(sent) != 9 {
		t.Fatal("quota inspection sends, waits, or mutates", slots, err, len(sent))
	}
	tr.MarkInitialized()
	if _, err = tr.Do(context.Background(), "rpc", "POST", "https://rpc.example", transportRPCBody, "normal"); err != nil {
		t.Fatal(err)
	}
	if _, err = tr.Do(context.Background(), "rpc", "POST", "https://rpc.example", transportRPCBody, "normal"); err != nil {
		t.Fatal(err)
	}
	if gap := sent[len(sent)-1].Sub(sent[len(sent)-2]); gap != 500*time.Millisecond {
		t.Fatal("startup gap leaked into timed quotes", gap)
	}
	g.state.CooldownUntil = clock.Now().Add(time.Hour)
	if slots, err = tr.AvailableRPCSlots("https://rpc.example"); err != nil || slots != 0 {
		t.Fatal(slots, err)
	}
	for _, gap := range []time.Duration{time.Second, 31 * time.Second} {
		if _, err = NewTransport(TransportConfig{StartupRPCGap: gap}); err == nil {
			t.Fatal("invalid gap accepted", gap)
		}
	}
}

func TestQuoteTimingDiagnosticsPreserveThresholds(t *testing.T) {
	ended := time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC)
	source := ended.Add(-15 * time.Second)
	available := source.Add(15 * time.Second)
	block := source.Add(-30 * time.Second)
	q := Quote{HedgeDepthEventTime: &source, HedgeDepthAvailableAt: &available, MarkSourceTime: &source, MarkAvailableAt: &available}
	p := ProtocolState{StateStatus: "ok"}
	started := ended.Add(-30 * time.Second)
	if reason := quoteTimingReason(q, p, Block{Time: block}, started, ended); reason != "" {
		t.Fatal("inclusive boundary rejected", reason)
	}
	old := source.Add(-time.Millisecond)
	q.HedgeDepthEventTime = &old
	reason := quoteTimingReason(q, p, Block{Time: block}, started, ended)
	for _, cause := range []string{"hedge_depth_source_age_exceeded", "hedge_depth_availability_lag_invalid"} {
		if !strings.Contains(reason, cause) {
			t.Fatal("lost timing cause", reason)
		}
	}
	newer := source.Add(time.Millisecond)
	q.HedgeDepthEventTime = &newer
	if reason = quoteTimingReason(q, p, Block{Time: block}, started, ended); !strings.Contains(reason, "hedge_depth_block_alignment_exceeded value_ms=30001") {
		t.Fatal(reason)
	}
	q.MarkSourceTime = nil
	reason = quoteTimingReason(q, ProtocolState{StateStatus: "unknown"}, Block{Time: ended.Add(-61 * time.Second)}, started.Add(-time.Millisecond), ended)
	for _, cause := range []string{"protocol_unavailable", "round_duration_exceeded", "block_age_exceeded", "mark_time_missing"} {
		if !strings.Contains(reason, cause) {
			t.Fatal("lost timing cause", reason)
		}
	}
	original := "stage=head cause=source_cooldown http_attempted=false"
	if combined := appendReason(original, reason); !strings.HasPrefix(combined, original+"; ") {
		t.Fatal("underlying failure overwritten", combined)
	}
}

func TestMarketWaitsPersistedStartupNextAtBeforeObservation(t *testing.T) {
	tr, clock := transportFixture(t, func(*http.Request) (*http.Response, error) { return transportReply(200, `{}`), nil })
	tr.cfg.StartupRPCGap = 10 * time.Second
	clock.at = tr.started.Add(2 * time.Minute)
	if _, err := tr.Do(context.Background(), "rpc", "POST", "https://rpc.example", transportRPCBody, "normal"); err != nil {
		t.Fatal(err)
	}
	tr.MarkInitialized()
	before := clock.Now()
	g, _ := tr.gate("rpc", "https://rpc.example")
	next := g.state.NextAt
	recent := len(g.state.Recent)
	if err := tr.WaitRPCSlots(context.Background(), "https://rpc.example", 20); err != nil {
		t.Fatal(err)
	}
	if clock.Now() != next || clock.Now().Sub(before) != 10*time.Second || len(g.state.Recent) != recent || g.state.NextAt != next {
		t.Fatal("startup waiting charged to market or erased", clock.Now(), g.state)
	}
}
