package across

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/vphoenix/crypto-market-info/internal/dex"
	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
)

func testEvent(t *testing.T, name string, v map[string]any) (Log, Block) {
	t.Helper()
	ev := ABI.Events[name]
	log := Log{ChainID: 8453, Address: strings.Repeat("a", 20), BlockNumber: 123, BlockHash: strings.Repeat("b", 32), TxHash: strings.Repeat("c", 32), PayloadHash: strings.Repeat("p", 32), Topics: []string{string(ev.ID[:])}}
	values := []any{}
	for _, a := range ev.Inputs {
		if a.Indexed {
			packed, e := (abi.Arguments{a}).Pack(v[a.Name])
			if e != nil {
				t.Fatal(e)
			}
			log.Topics = append(log.Topics, string(packed))
		} else {
			values = append(values, v[a.Name])
		}
	}
	data, e := ev.Inputs.NonIndexed().Pack(values...)
	if e != nil {
		t.Fatal(e)
	}
	log.Data = string(data)
	return log, Block{ChainID: 8453, Number: 123, Hash: log.BlockHash, Time: time.Unix(1000, 0).UTC()}
}
func depositValues(legacy bool) map[string]any {
	a := common.HexToAddress("0x1234567890123456789012345678901234567890")
	var addr any = hashWord(WordAddress(string(a[:])))
	var id any = new(big.Int).Lsh(big.NewInt(1), 180)
	if legacy {
		addr = a
		id = uint32(17)
	}
	return map[string]any{"inputToken": addr, "outputToken": addr, "inputAmount": big.NewInt(100000000), "outputAmount": big.NewInt(99999000), "destinationChainId": big.NewInt(42161), "depositId": id, "quoteTimestamp": uint32(990), "fillDeadline": uint32(1200), "exclusivityDeadline": uint32(1000), "depositor": addr, "recipient": addr, "exclusiveRelayer": addr, "message": []byte{}}
}
func meta() DecodeMeta {
	return DecodeMeta{CaptureID: strings.Repeat("x", 32), ABIRevision: ABIRevision, AvailableAt: time.Unix(1001, 0).UTC()}
}
func TestProtocolDepositStrictABIAndFullIdentity(t *testing.T) {
	l, b := testEvent(t, "FundsDeposited", depositValues(false))
	batch, known, e := DecodeLog(l, b, meta())
	if e != nil || !known || len(batch.Deposits) != 1 {
		t.Fatalf("decode %v %v", known, e)
	}
	d := batch.Deposits[0]
	if d.DepositId.BitLen() != 181 || d.MessageHash != strings.Repeat("\x00", 32) {
		t.Fatal("large ID or empty message changed")
	}
	if d.RelayHash == MessageHash("") {
		t.Fatal("empty relay hash")
	}
	other := d
	other.OutputAmountRaw = big.NewInt(99999001)
	h, e := RelayHash(other)
	if e != nil || h == d.RelayHash {
		t.Fatal("different full terms collided")
	}
	other = d
	other.QuoteTimestamp++
	h, _ = RelayHash(other)
	if h != d.RelayHash {
		t.Fatal("quote timestamp incorrectly hashes")
	}
	l.Data += "\x00"
	if _, _, e := DecodeLog(l, b, meta()); e == nil {
		t.Fatal("accepted trailing data")
	}
	l, b = testEvent(t, "V3FundsDeposited", depositValues(true))
	x, _, e := DecodeLog(l, b, meta())
	if e != nil || x.Deposits[0].DepositId.Uint64() != 17 {
		t.Fatal(e)
	}
	if len(x.Deposits[0].Depositor) != 32 || x.Deposits[0].Depositor[:12] != strings.Repeat("\x00", 12) {
		t.Fatal("legacy address padding")
	}
	l.Topics[2] = string([]byte{1}) + l.Topics[2][1:]
	if _, _, e = DecodeLog(l, b, meta()); e == nil {
		t.Fatal("accepted overflowing uint32 indexed field")
	}
}
func TestProtocolFillActualPaymentAndType(t *testing.T) {
	v := depositValues(false)
	delete(v, "destinationChainId")
	v["originChainId"] = big.NewInt(42161)
	v["repaymentChainId"] = big.NewInt(42161)
	v["relayer"] = hashWord(strings.Repeat("r", 32))
	v["messageHash"] = hashWord(MessageHash(""))
	typ := ABI.Events["FilledRelay"].Inputs.NonIndexed()
	var tuple reflect.Type
	for _, a := range typ {
		if a.Name == "relayExecutionInfo" {
			tuple = a.Type.GetType()
		}
	}
	info := reflect.New(tuple).Elem()
	info.FieldByName("UpdatedRecipient").Set(reflect.ValueOf(hashWord(strings.Repeat("u", 32))))
	info.FieldByName("UpdatedMessageHash").Set(reflect.ValueOf(hashWord(MessageHash(""))))
	info.FieldByName("UpdatedOutputAmount").Set(reflect.ValueOf(big.NewInt(99998000)))
	info.FieldByName("FillType").SetUint(1)
	v["relayExecutionInfo"] = info.Interface()
	l, b := testEvent(t, "FilledRelay", v)
	batch, _, e := DecodeLog(l, b, meta())
	if e != nil {
		t.Fatal(e)
	}
	f := batch.Fills[0]
	if f.UpdatedOutputAmountRaw.Int64() != 99998000 || f.OutputAmountRaw.Int64() != 99999000 || f.FillType != 1 || f.RepaymentAddress != strings.Repeat("r", 32) {
		t.Fatal("actual versus original terms lost")
	}
	info.FieldByName("FillType").SetUint(3)
	v["relayExecutionInfo"] = info.Interface()
	l, b = testEvent(t, "FilledRelay", v)
	if _, _, e = DecodeLog(l, b, meta()); e == nil {
		t.Fatal("accepted unknown fill type")
	}
}
func TestProtocolRefundArraysAndClaimCaller(t *testing.T) {
	a := common.HexToAddress("0x1111111111111111111111111111111111111111")
	z := common.HexToAddress("0x2222222222222222222222222222222222222222")
	v := map[string]any{"amountToReturn": big.NewInt(9), "chainId": big.NewInt(8453), "refundAmounts": []*big.Int{big.NewInt(17)}, "rootBundleId": uint32(2), "leafId": uint32(4), "l2TokenAddress": a, "refundAddresses": []common.Address{z}, "deferredRefunds": true, "caller": a}
	l, b := testEvent(t, "ExecutedRelayerRefundRoot", v)
	batch, _, e := DecodeLog(l, b, meta())
	if e != nil {
		t.Fatal(e)
	}
	d := batch.Refunds[0]
	if d.DeferredRefunds == nil || !*d.DeferredRefunds || d.RefundAmountsRaw[0].Int64() != 17 || d.AmountToReturnRaw.Int64() != 9 {
		t.Fatal("refund semantics lost")
	}
	v["refundAmounts"] = []*big.Int{}
	l, b = testEvent(t, "ExecutedRelayerRefundRoot", v)
	if _, _, e = DecodeLog(l, b, meta()); e == nil {
		t.Fatal("accepted mismatched refund arrays")
	}
	claim := map[string]any{"l2TokenAddress": hashWord(WordAddress(string(a[:]))), "refundAddress": hashWord(WordAddress(string(z[:]))), "amount": big.NewInt(17), "caller": a}
	l, b = testEvent(t, "ClaimedRelayerRefund", claim)
	batch, _, e = DecodeLog(l, b, meta())
	if e != nil {
		t.Fatal(e)
	}
	d = batch.Refunds[0]
	if d.Caller == d.RefundAddresses[0] || d.RootBundleId != nil || d.LeafId != nil || d.EventKind != "deferred_claim" {
		t.Fatal("claim caller/recipient conflated")
	}
}

type protocolTransport func(*http.Request) (*http.Response, error)

func (f protocolTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func mockReader(t *testing.T, chain uint64, reply func(string, json.RawMessage) any) *Reader {
	t.Helper()
	rpc, e := ethereum.NewClient("https://test.invalid", t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	rpc.HTTP.Transport = protocolTransport(func(r *http.Request) (*http.Response, error) {
		var req []struct {
			ID     int             `json:"id"`
			Method string          `json:"method"`
			Params json.RawMessage `json:"params"`
		}
		if e := json.NewDecoder(r.Body).Decode(&req); e != nil {
			return nil, e
		}
		out := []any{}
		for _, q := range req {
			out = append(out, map[string]any{"jsonrpc": "2.0", "id": q.ID, "result": reply(q.Method, q.Params)})
		}
		b, e := json.Marshal(out)
		if e != nil {
			return nil, e
		}
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(b))}, nil
	})
	return NewReader(rpc, ChainConfig{ChainID: chain, SpokePool: strings.Repeat("s", 20)})
}
func TestProtocolRPCMissingAndAnchorChecks(t *testing.T) {
	for _, tc := range []struct {
		name    string
		reply   any
		success bool
	}{{"empty", []any{}, true}, {"null", nil, false}, {"object", map[string]any{}, false}} {
		t.Run(tc.name, func(t *testing.T) {
			r := mockReader(t, 8453, func(string, json.RawMessage) any { return tc.reply })
			v, e := r.Logs(context.Background(), 1, 2)
			if (e == nil) != tc.success {
				t.Fatalf("%v %v", v, e)
			}
		})
	}
	l, b := testEvent(t, "FundsDeposited", depositValues(false))
	b.Hash = strings.Repeat("z", 32)
	if _, _, e := DecodeLog(l, b, meta()); e == nil {
		t.Fatal("accepted orphan log header")
	}
	r := mockReader(t, 8453, func(method string, p json.RawMessage) any {
		switch method {
		case "eth_getStorageAt":
			return Hex(WordAddress(strings.Repeat("a", 20)))
		case "eth_getCode":
			return "0x0102"
		}
		t.Fatal(method)
		return nil
	})
	r.Chain.Implementations = []Implementation{{Address: strings.Repeat("a", 20), CodeHash: string(crypto.Keccak256([]byte{1, 3})), ABIRevision: ABIRevision}}
	if _, e := r.VerifyImplementation(context.Background(), Block{ChainID: 8453, Hash: strings.Repeat("h", 32)}); e == nil {
		t.Fatal("accepted unmatched deployed bytecode")
	}
}
func TestProtocolReceiptFeesAndTransactionAnchors(t *testing.T) {
	for _, tc := range []struct {
		name     string
		chain    uint64
		operator bool
		badHash  bool
		want     string
	}{{"base_unknown_operator", 8453, false, false, ""}, {"base_complete", 8453, true, false, "650"}, {"arb_no_double_l1", 42161, false, false, "600"}, {"mismatched_tx", 42161, false, true, "error"}} {
		t.Run(tc.name, func(t *testing.T) {
			hash := Hex(strings.Repeat("h", 32))
			txh := Hex(strings.Repeat("t", 32))
			from := Hex(strings.Repeat("a", 20))
			to := Hex(strings.Repeat("z", 20))
			r := mockReader(t, tc.chain, func(method string, p json.RawMessage) any {
				switch method {
				case "eth_getTransactionByHash":
					h := hash
					if tc.badHash {
						h = Hex(strings.Repeat("v", 32))
					}
					return map[string]any{"hash": txh, "blockHash": h, "blockNumber": "0x7b", "transactionIndex": "0x0", "from": from, "to": to, "type": "0x2", "input": "0x12345678", "value": "0x0", "chainId": fmt.Sprintf("0x%x", tc.chain)}
				case "eth_getTransactionReceipt":
					v := map[string]any{"transactionHash": txh, "blockHash": hash, "blockNumber": "0x7b", "transactionIndex": "0x0", "from": from, "to": to, "status": "0x1", "gasUsed": "0xc8", "effectiveGasPrice": "0x3", "l1Fee": "0x32", "gasUsedForL1": "0x14", "logs": []any{}}
					if tc.operator {
						v["operatorFee"] = "0x0"
					}
					return v
				case "eth_getBlockByNumber":
					return map[string]any{"number": "0x7b", "hash": hash, "parentHash": Hex(strings.Repeat("p", 32)), "timestamp": "0x3e8"}
				}
				t.Fatal(method)
				return nil
			})
			out, e := r.Receipt(context.Background(), strings.Repeat("t", 32))
			if tc.want == "error" {
				if e == nil {
					t.Fatal("accepted mismatched tx block")
				}
				return
			}
			if e != nil {
				t.Fatal(e)
			}
			if tc.want == "" {
				if out.TotalFeeWei != nil || out.FeeComplete {
					t.Fatal("missing fee became zero")
				}
			} else if out.TotalFeeWei == nil || out.TotalFeeWei.String() != tc.want || !out.FeeComplete {
				t.Fatalf("fee got %+v", out)
			}
		})
	}
}
func TestProtocolManifest(t *testing.T) {
	m, e := LoadManifest("../../config/across-research.json")
	if e != nil {
		t.Fatal(e)
	}
	if m.BinanceURL != "https://api.binance.com" || len(m.Hash) != 32 || len(m.Chains[0].Implementations) != 1 {
		t.Fatal("manifest identity")
	}
}

func TestProtocolQuantityRejectsSignedOrNoncanonicalData(t *testing.T) {
	for _, s := range []string{"0x+1", "0x-0", "0x01", "0x", "1", "0xgg"} {
		if _, e := Quantity(s); e == nil {
			t.Fatalf("accepted %q", s)
		}
	}
	if n, e := Quantity("0xffff"); e != nil || n.Uint64() != 65535 {
		t.Fatal("valid quantity rejected")
	}
}

// Opt-in integration check compares the complete relay identity against deployed
// contract code without submitting a transaction. Default tests never go online.
func TestProtocolLiveRelayHash(t *testing.T) {
	if os.Getenv("ACROSS_LIVE_PROTOCOL_TEST") != "1" {
		t.Skip("set ACROSS_LIVE_PROTOCOL_TEST=1 for public read-only RPC verification")
	}
	m := runnerManifest(t)
	for _, ch := range m.Chains {
		t.Run(fmt.Sprint(ch.ChainID), func(t *testing.T) {
			rpc, e := ethereum.NewClient(ch.Endpoint(), "../../research/2026-10-02-across-implementation/live-protocol-evidence")
			if e != nil {
				t.Fatal(e)
			}
			r := NewReader(rpc, ch)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			b, e := r.Header(ctx, "finalized")
			if e != nil {
				t.Fatal(e)
			}
			if e = r.Preflight(ctx, b); e != nil {
				t.Fatal(e)
			}
			a := WordAddress(ch.USDC)
			d := Deposit{ChainId: 8453, DestinationChainId: ch.ChainID, Depositor: a, Recipient: a, ExclusiveRelayer: strings.Repeat("\x00", 32), InputToken: a, OutputToken: a, InputAmountRaw: big.NewInt(123456789), OutputAmountRaw: big.NewInt(123450000), DepositId: new(big.Int).Lsh(big.NewInt(1), 200), FillDeadline: 1800000000, ExclusivityDeadline: 1799999900, Message: "across collector public identity check"}
			expected, e := RelayHash(d)
			if e != nil {
				t.Fatal(e)
			}
			tuple := reflect.New(ABI.Methods["getV3RelayHash"].Inputs[0].Type.GetType()).Elem()
			for name, v := range map[string]any{"Depositor": hashWord(d.Depositor), "Recipient": hashWord(d.Recipient), "ExclusiveRelayer": hashWord(d.ExclusiveRelayer), "InputToken": hashWord(d.InputToken), "OutputToken": hashWord(d.OutputToken), "InputAmount": d.InputAmountRaw, "OutputAmount": d.OutputAmountRaw, "OriginChainId": new(big.Int).SetUint64(d.ChainId), "DepositId": d.DepositId, "FillDeadline": d.FillDeadline, "ExclusivityDeadline": d.ExclusivityDeadline, "Message": []byte(d.Message)} {
				tuple.FieldByName(name).Set(reflect.ValueOf(v))
			}
			out, e := r.call(ctx, b, ch.SpokePool, "getV3RelayHash", tuple.Interface())
			if e != nil {
				t.Fatal(e)
			}
			actual := word(out[0])
			if actual != expected {
				t.Fatalf("deployed relay hash differs got=%s want=%s", Hex(actual), Hex(expected))
			}
			t.Logf("chain=%d finalized=%d block=%s relay_hash=%s", ch.ChainID, b.Number, Hex(b.Hash), Hex(actual))
		})
	}
}

func TestProtocolHeadersBatchBoundsOrderAndEvidence(t *testing.T) {
	rpc, e := ethereum.NewClient("https://test.invalid", t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	var groups, active, maxActive atomic.Int32
	rpc.HTTP.Transport = protocolTransport(func(req *http.Request) (*http.Response, error) {
		n := active.Add(1)
		defer active.Add(-1)
		for old := maxActive.Load(); n > old && !maxActive.CompareAndSwap(old, n); old = maxActive.Load() {
		}
		groups.Add(1)
		deadline, ok := req.Context().Deadline()
		if !ok || time.Until(deadline) > 5*time.Second {
			t.Error("header group has no bounded deadline")
		}
		var calls []struct {
			ID     int               `json:"id"`
			Params []json.RawMessage `json:"params"`
		}
		if e := json.NewDecoder(req.Body).Decode(&calls); e != nil {
			return nil, e
		}
		if len(calls) > 10 {
			t.Errorf("unbounded RPC group %d", len(calls))
		}
		out := make([]any, len(calls))
		for i, c := range calls {
			var tag string
			if e := json.Unmarshal(c.Params[0], &tag); e != nil {
				return nil, e
			}
			height, e := q64(tag)
			if e != nil {
				return nil, e
			}
			hash, e := ParseHex(fmt.Sprintf("0x%064x", height), 32)
			if e != nil {
				return nil, e
			}
			out[len(calls)-1-i] = map[string]any{"jsonrpc": "2.0", "id": c.ID, "result": protocolHeader(height, hash, time.Unix(1000, 0).UTC())}
		}
		b, e := json.Marshal(out)
		if e != nil {
			return nil, e
		}
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(b))}, nil
	})
	r := NewReader(rpc, ChainConfig{ChainID: 8453})
	heights := make([]uint64, 65)
	for i := range heights {
		heights[i] = 1000 + uint64(64-i)
	}
	headers, e := r.Headers(context.Background(), heights)
	if e != nil {
		t.Fatal(e)
	}
	if len(headers) != 65 || groups.Load() != 7 || maxActive.Load() > 2 || r.Used != 65 || len(r.Members) != 65 {
		t.Fatalf("batch bounds failed groups=%d max_active=%d used=%d refs=%d", groups.Load(), maxActive.Load(), r.Used, len(r.Members))
	}
	for i, h := range headers {
		if h.Number != heights[i] || h.ChainID != 8453 || len(h.PayloadHash) != 32 || h.AvailableAt.IsZero() {
			t.Fatalf("order or evidence lost at %d", i)
		}
		var hash dex.Hash
		copy(hash[:], h.PayloadHash)
		if _, e := rpc.Archive.Get(hash); e != nil {
			t.Fatalf("missing header payload %v", e)
		}
	}
	if _, e = r.Headers(context.Background(), make([]uint64, 513)); e == nil {
		t.Fatal("accepted over-budget headers")
	}
	before := r.Used
	if h, e := r.Headers(context.Background(), nil); e != nil || len(h) != 0 || r.Used != before {
		t.Fatal("empty request made RPC")
	}
}
func TestProtocolHeadersPartialFailureRejectsBatch(t *testing.T) {
	r := mockReader(t, 8453, func(method string, p json.RawMessage) any {
		var args []json.RawMessage
		json.Unmarshal(p, &args)
		var tag string
		json.Unmarshal(args[0], &tag)
		n, _ := q64(tag)
		if n == 2 {
			return protocolHeader(3, strings.Repeat("h", 32), time.Unix(1000, 0).UTC())
		}
		return protocolHeader(n, strings.Repeat("h", 32), time.Unix(1000, 0).UTC())
	})
	headers, e := r.Headers(context.Background(), []uint64{1, 2, 3})
	if e == nil || headers != nil || len(r.Members) != 3 {
		t.Fatal("partial batch became valid coverage or lost evidence")
	}
}
func TestProtocolHeadersCancellationIncludesQueuedGroups(t *testing.T) {
	rpc, e := ethereum.NewClient("https://test.invalid", t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	var called atomic.Int32
	rpc.HTTP.Transport = protocolTransport(func(req *http.Request) (*http.Response, error) {
		called.Add(1)
		<-req.Context().Done()
		return nil, req.Context().Err()
	})
	r := NewReader(rpc, ChainConfig{ChainID: 8453})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	headers, e := r.Headers(ctx, make([]uint64, 65))
	if e == nil || headers != nil || called.Load() > 2 || time.Since(start) > time.Second {
		t.Fatal("queued header groups escaped shared deadline")
	}
}

func TestProtocolLiveHeaders(t *testing.T) {
	if os.Getenv("ACROSS_LIVE_PROTOCOL_TEST") != "1" {
		t.Skip("set ACROSS_LIVE_PROTOCOL_TEST=1 for public read-only RPC verification")
	}
	m := runnerManifest(t)
	for _, ch := range m.Chains {
		t.Run(fmt.Sprint(ch.ChainID), func(t *testing.T) {
			rpc, e := ethereum.NewClient(ch.Endpoint(), "../../research/2026-10-02-across-implementation/live-protocol-evidence")
			if e != nil {
				t.Fatal(e)
			}
			r := NewReader(rpc, ch)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			head, e := r.Header(ctx, "finalized")
			if e != nil {
				t.Fatal(e)
			}
			nums := make([]uint64, 65)
			for i := range nums {
				nums[i] = head.Number - 64 + uint64(i)
			}
			start := time.Now()
			headers, e := r.Headers(ctx, nums)
			elapsed := time.Since(start).Milliseconds()
			if e != nil {
				t.Fatal(e)
			}
			for i, h := range headers {
				if h.Number != nums[i] {
					t.Fatal("wrong header sequence")
				}
				if i > 0 && h.ParentHash != headers[i-1].Hash {
					t.Fatal("headers do not form chain")
				}
			}
			groups := 4
			if ch.ChainID == 8453 {
				groups = 7
			}
			t.Logf("chain=%d headers=%d first=%d last=%d requests=%d max_concurrent=2 elapsed_ms=%d", ch.ChainID, len(headers), nums[0], nums[len(nums)-1], groups, elapsed)
		})
	}
}
