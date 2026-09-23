package ethereum

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/vphoenix/crypto-market-info/internal/dex"
	"io"
	"math/big"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type transportFunc func(*http.Request) (*http.Response, error)

func (f transportFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type failedBody struct{}

func (failedBody) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }
func (failedBody) Close() error             { return nil }

type timeoutBody struct{}

func (timeoutBody) Read([]byte) (int, error) { return 0, context.DeadlineExceeded }
func (timeoutBody) Close() error             { return nil }

func response(s string) *http.Response {
	return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(s)), Header: make(http.Header)}
}
func TestBatchDistinguishesResponseReadFailureFromSizeLimit(t *testing.T) {
	for _, tc := range []struct {
		name string
		body io.ReadCloser
		want string
	}{
		{"truncated response", failedBody{}, "rpc_response_truncated"},
		{"read timeout", timeoutBody{}, "rpc_response_read_timeout"},
		{"oversized response", io.NopCloser(strings.NewReader(strings.Repeat("x", (16<<20)+1))), "rpc_response_too_large"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := testClient(t, func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 200, Body: tc.body, Header: make(http.Header)}, nil
			})
			r := c.One(context.Background(), "eth_chainId", []any{})
			if r.Err == nil || r.Err.Error() != tc.want || r.Payload != (dex.Hash{}) {
				t.Fatalf("result=%+v, want %s without success evidence", r, tc.want)
			}
		})
	}
}
func testClient(t *testing.T, fn transportFunc) *Client {
	t.Helper()
	c, e := NewClient("https://user:secret@example.test/private?key=secret", t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	c.HTTP = &http.Client{Transport: fn}
	return c
}
func TestBatchStrictIDsPartialFailureAndArchive(t *testing.T) {
	for _, body := range []string{`[{"jsonrpc":"2.0","id":1,"result":"0x0"}]`, `[{"jsonrpc":"2.0","id":1,"result":"0x0"},{"jsonrpc":"2.0","id":1,"result":"0x1"}]`, `[{"jsonrpc":"2.0","id":1,"result":"0x0"},{"jsonrpc":"2.0","id":3,"result":"0x1"}]`} {
		c := testClient(t, func(*http.Request) (*http.Response, error) { return response(body), nil })
		rr := c.Batch(context.Background(), []Call{{"x", []any{}}, {"y", []any{}}})
		for _, r := range rr {
			if r.Err == nil || r.Payload == (dex.Hash{}) {
				t.Fatal("incomplete batch accepted")
			}
			raw, e := c.Archive.Get(r.Payload)
			if e != nil || strings.Contains(string(raw), "secret") {
				t.Fatal("missing evidence or leaked endpoint", e)
			}
		}
	}
	c := testClient(t, func(*http.Request) (*http.Response, error) {
		return response(`[{"jsonrpc":"2.0","id":2,"error":{"code":-32000,"message":"revert"}},{"jsonrpc":"2.0","id":1,"result":"0x1234"}]`), nil
	})
	rr := c.Batch(context.Background(), []Call{{"x", nil}, {"y", nil}})
	if rr[0].Err != nil || rr[1].Err == nil {
		t.Fatal("partial method failure wrongly handled")
	}
}
func TestBatchBoundAndCancellation(t *testing.T) {
	var active, maximum atomic.Int32
	var mu sync.Mutex
	counts := []int{}
	c := testClient(t, func(r *http.Request) (*http.Response, error) {
		n := active.Add(1)
		defer active.Add(-1)
		for {
			old := maximum.Load()
			if n <= old || maximum.CompareAndSwap(old, n) {
				break
			}
		}
		var reqs []map[string]json.RawMessage
		json.NewDecoder(r.Body).Decode(&reqs)
		mu.Lock()
		counts = append(counts, len(reqs))
		mu.Unlock()
		time.Sleep(3 * time.Millisecond)
		out := []map[string]any{}
		for _, q := range reqs {
			out = append(out, map[string]any{"jsonrpc": "2.0", "id": q["id"], "result": "0x0"})
		}
		b, _ := json.Marshal(out)
		return response(string(b)), nil
	})
	calls := make([]Call, 61)
	for i := range calls {
		calls[i] = Call{"eth_chainId", []any{}}
	}
	rr := c.Batch(context.Background(), calls)
	for _, r := range rr {
		if r.Err != nil {
			t.Fatal(r.Err)
		}
	}
	if maximum.Load() > 2 {
		t.Fatal("RPC concurrency unbounded")
	}
	for _, n := range counts {
		if n > 20 {
			t.Fatal("batch too large")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, r := range c.Batch(ctx, calls) {
		if r.Err == nil {
			t.Fatal("canceled request accepted")
		}
	}
}
func TestHeaderTagsBatchPreservesTagIdentity(t *testing.T) {
	wantTags := []string{"finalized", "safe", "0x64"}
	wantNumbers := []string{"0x5a", "0x5f", "0x64"}
	c := testClient(t, func(r *http.Request) (*http.Response, error) {
		var requests []struct {
			ID     uint64
			Method string
			Params []json.RawMessage
		}
		if err := json.NewDecoder(r.Body).Decode(&requests); err != nil || len(requests) != len(wantTags) {
			t.Fatalf("requests=%v err=%v", requests, err)
		}
		out := make([]map[string]any, 0, len(requests))
		for i := len(requests) - 1; i >= 0; i-- {
			var tag string
			if err := json.Unmarshal(requests[i].Params[0], &tag); err != nil || requests[i].Method != "eth_getBlockByNumber" || tag != wantTags[i] {
				t.Fatalf("request=%+v tag=%s err=%v", requests[i], tag, err)
			}
			out = append(out, map[string]any{"jsonrpc": "2.0", "id": requests[i].ID, "result": map[string]any{
				"number": wantNumbers[i], "hash": dex.Digest([]byte(wantTags[i])).String(), "parentHash": dex.Digest([]byte("parent")).String(),
				"timestamp": "0x1", "baseFeePerGas": "0x1", "miner": DefaultManifest().Addresses["WETH"].String(),
			}})
		}
		body, _ := json.Marshal(out)
		return response(string(body)), nil
	})
	heads, err := c.HeaderTags(context.Background(), wantTags...)
	if err != nil || len(heads) != 3 || heads[0].Number != 90 || heads[1].Number != 95 || heads[2].Number != 100 {
		t.Fatalf("heads=%v err=%v", heads, err)
	}
}
func TestABIWidthsAndQuoteUsesHash(t *testing.T) {
	anchor := dex.Digest([]byte("block"))
	m := DefaultManifest()
	c := testClient(t, func(r *http.Request) (*http.Response, error) {
		var reqs []struct{ Params []json.RawMessage }
		json.NewDecoder(r.Body).Decode(&reqs)
		for _, req := range reqs {
			var ref map[string]any
			json.Unmarshal(req.Params[1], &ref)
			if ref["blockHash"] != anchor.String() || ref["requireCanonical"] != true {
				t.Fatal("not hash pinned")
			}
			var call map[string]string
			json.Unmarshal(req.Params[0], &call)
			if !strings.HasPrefix(call["data"], "0xbd21704a") {
				t.Fatal("not exact output")
			}
		}
		result := "0x" + word(big.NewInt(42)) + word(big.NewInt(123)) + word(big.NewInt(2)) + word(big.NewInt(9000))
		return response(fmt.Sprintf(`[{"jsonrpc":"2.0","id":1,"result":%q}]`, result)), nil
	})
	q := c.Quotes(context.Background(), anchor, []dex.SwapRequest{{TokenIn: m.Addresses["USDC"], TokenOut: m.Addresses["WETH"], Fee: 500, Amount: big.NewInt(100), ExactOutput: true}})[0]
	if q.Err != nil || q.Amount.Int64() != 42 || q.Ticks != 2 {
		t.Fatal(q)
	}
	for _, s := range []string{"0x00", "0x", "0x-1", "42", "0x100"} {
		if _, e := Quantity(s, 8); e == nil {
			t.Fatal("invalid quantity", s)
		}
	}
	if _, e := ABIWord(Result{Raw: json.RawMessage(`"0x01"`)}, 256); e == nil {
		t.Fatal("short ABI accepted")
	}
	wide := "0x" + word(new(big.Int).Lsh(big.NewInt(1), 160))
	if _, e := ABIWord(Result{Raw: json.RawMessage(fmt.Sprintf("%q", wide))}, 160); e == nil {
		t.Fatal("oversized ABI accepted")
	}
}
func TestReceiptRejectsWrongBlockAndAcceptsEmptyCalldata(t *testing.T) {
	c := testClient(t, nil)
	a := dex.Anchor{ChainID: 1, Number: 7, Hash: dex.Digest([]byte("block")), Manifest: ManifestHash(), Batch: dex.Digest([]byte("batch")), Time: time.Unix(100, 0).UTC()}
	h := dex.Digest([]byte("tx"))
	address := DefaultManifest().Addresses["USDC"].String()
	tx := map[string]any{"hash": h.String(), "blockHash": a.Hash.String(), "blockNumber": "0x7", "transactionIndex": "0x0", "from": address, "to": address, "type": "0x2", "value": "0x0", "input": "0x"}
	receipt := map[string]any{"transactionHash": h.String(), "blockHash": a.Hash.String(), "blockNumber": "0x7", "transactionIndex": "0x0", "from": address, "to": address, "type": "0x2", "status": "0x1", "gasUsed": "0x5208", "effectiveGasPrice": "0x1", "logs": []any{}}
	result := func(v any) Result {
		b, _ := json.Marshal(v)
		return Result{Raw: b, Payload: dex.Digest(b), At: dex.Now()}
	}
	r, e := c.parseReceipt(a, h, 0, result(tx), result(receipt))
	if e != nil || r.Selector != nil || r.LogCount != 0 {
		t.Fatal(r, e)
	}
	receipt["blockHash"] = dex.Digest([]byte("other")).String()
	if _, e = c.parseReceipt(a, h, 0, result(tx), result(receipt)); e == nil {
		t.Fatal("mixed hash receipt accepted")
	}
}
func TestLogGroupsCoverageAndStableRetryIdentity(t *testing.T) {
	m := DefaultManifest()
	a := dex.Anchor{ChainID: 1, Number: 7, Hash: dex.Digest([]byte("block")), Manifest: ManifestHash(), Batch: dex.Digest([]byte("batch")), Time: time.Unix(100, 0).UTC()}
	bad := false
	failGroup := false
	empty := false
	c := testClient(t, func(r *http.Request) (*http.Response, error) {
		var reqs []struct {
			ID     uint64
			Params []struct {
				BlockHash string
				Address   []string
			}
		}
		json.NewDecoder(r.Body).Decode(&reqs)
		out := []map[string]any{}
		for _, req := range reqs {
			if len(req.Params[0].Address) > 6 || req.Params[0].BlockHash != a.Hash.String() {
				t.Fatal("bad log filter")
			}
			row := map[string]any{"jsonrpc": "2.0", "id": req.ID}
			vals := []any{}
			if req.Params[0].Address[0] == m.Pools[0].Address.String() && !empty {
				address := m.Pools[0].Address.String()
				if bad {
					address = m.Addresses["psm"].String()
				}
				vals = append(vals, map[string]any{"blockHash": a.Hash.String(), "blockNumber": "0x7", "transactionHash": dex.Digest([]byte("tx")).String(), "transactionIndex": "0x0", "logIndex": "0x0", "address": address, "data": "0x", "topics": []string{}, "removed": false})
			}
			if failGroup && req.Params[0].Address[0] == m.Addresses["psm"].String() {
				row["error"] = map[string]any{"code": -32000, "message": "limit"}
			} else {
				row["result"] = vals
			}
			out = append(out, row)
		}
		b, _ := json.Marshal(out)
		return response(string(b)), nil
	})
	first, _, e := c.Logs(context.Background(), a)
	if e != nil || len(first) != 1 {
		t.Fatal(first, e)
	}
	second, _, e := c.Logs(context.Background(), a)
	if e != nil || dex.LogDigest(first) != dex.LogDigest(second) {
		t.Fatal("retry changes immutable log payload", e)
	}
	bad = true
	if _, _, e = c.Logs(context.Background(), a); e == nil {
		t.Fatal("wrong filter group emitter accepted")
	}
	bad = false
	failGroup = true
	if logs, _, e := c.Logs(context.Background(), a); e == nil || len(logs) != 0 {
		t.Fatal("partial log groups accepted as complete")
	}
	failGroup = false
	empty = true
	if logs, _, e := c.Logs(context.Background(), a); e != nil || len(logs) != 0 {
		t.Fatal("successful empty log batch rejected", e)
	}
}
