package lst

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRPCArchivedHTTP400ErrorsKeepTransportSemantics(t *testing.T) {
	for _, tc := range []struct {
		name, body, want string
		status           int
		rpcError         bool
	}{
		{"provider_error", `{"jsonrpc":"2.0","id":1,"error":{"code":35,"message":"ranges over 10000 blocks are not supported on free plan"}}`, "source_http_400", 400, true},
		{"wrong_id", `{"jsonrpc":"2.0","id":2,"error":{"code":35,"message":"range"}}`, "source_http_400", 400, false},
		{"wrong_version", `{"jsonrpc":"1.0","id":1,"error":{"code":35,"message":"range"}}`, "source_http_400", 400, false},
		{"trailing_json", `{"jsonrpc":"2.0","id":1,"error":{"code":35,"message":"range"}} {}`, "source_http_400", 400, false},
		{"missing_code", `{"jsonrpc":"2.0","id":1,"error":{"message":"range"}}`, "source_http_400", 400, false},
		{"missing_message", `{"jsonrpc":"2.0","id":1,"error":{"code":35}}`, "source_http_400", 400, false},
		{"result_and_error", `{"jsonrpc":"2.0","id":1,"result":null,"error":{"code":35,"message":"range"}}`, "source_http_400", 400, false},
		{"http_failure_result", `{"jsonrpc":"2.0","id":1,"result":[]}`, "source_http_400", 400, false},
		{"http_rate_limit", `{"jsonrpc":"2.0","id":1,"error":{"code":35,"message":"range"}}`, "source_rate_limited", 429, false},
		{"body_rate_limit", `{"jsonrpc":"2.0","id":1,"error":{"code":35,"message":"rate limit exceeded"}}`, "source_rate_limited", 400, false},
		{"http_forbidden", `{"jsonrpc":"2.0","id":1,"error":{"code":35,"message":"range"}}`, "source_disabled", 403, false},
		{"http_banned", `{"jsonrpc":"2.0","id":1,"error":{"code":35,"message":"range"}}`, "source_disabled", 418, false},
		{"server_failure", `{"jsonrpc":"2.0","id":1,"error":{"code":35,"message":"range"}}`, "source_http_500", 500, false},
		{"successful_error", `{"jsonrpc":"2.0","id":1,"error":{"code":35,"message":"range"}}`, "rpc_error_35", 200, true},
		{"successful_missing_code", `{"jsonrpc":"2.0","id":1,"error":{"message":"range"}}`, "rpc_envelope", 200, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tr, _ := transportFixture(t, func(*http.Request) (*http.Response, error) {
				return transportReply(tc.status, tc.body), nil
			})
			rpc := RPC{Transport: tr, URL: "https://rpc.example"}
			raw, res, err := rpc.Call(context.Background(), "eth_getLogs", []any{}, "logs")
			var rpcErr *RPCError
			if err == nil || !strings.Contains(err.Error(), tc.want) || errors.As(err, &rpcErr) != tc.rpcError || raw != nil {
				t.Fatalf("raw=%s err=%v rpc_error=%v", raw, err, rpcErr)
			}
			if len(res.PayloadHash) != 32 || res.HTTPStatus != tc.status {
				t.Fatal("HTTP failure lost archived evidence", res)
			}
			if tc.rpcError && rpcErr.Code != 35 {
				t.Fatal("wrong provider error", rpcErr)
			}
		})
	}
}

func TestBackfillProviderPlanErrorDoesNotSplitOrAdvance(t *testing.T) {
	store := &gasMemoryStore{}
	calls := 0
	c := runnerReadyCollector(t, store, &calls)
	tr, _ := transportFixture(t, func(req *http.Request) (*http.Response, error) {
		calls++
		var call struct {
			ID     uint64            `json:"id"`
			Method string            `json:"method"`
			Params []json.RawMessage `json:"params"`
		}
		if err := json.NewDecoder(req.Body).Decode(&call); err != nil {
			t.Fatal(err)
		}
		switch call.Method {
		case "eth_getBlockByNumber":
			var tag string
			if err := json.Unmarshal(call.Params[0], &tag); err != nil {
				t.Fatal(err)
			}
			n := uint64(700)
			if tag != "finalized" {
				var err error
				n, err = q64(tag)
				if err != nil {
					t.Fatal(err)
				}
			}
			return transportReply(200, fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"result":{"number":"0x%x","hash":"0x%064x","parentHash":"0x%064x","timestamp":"0x6a05c000"}}`, call.ID, n, n, n-1)), nil
		case "eth_call":
			return transportReply(200, fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"result":"0x%024s%s"}`, call.ID, "", strings.TrimPrefix(c.Manifest.Addresses["queue_impl"], "0x"))), nil
		case "eth_getLogs":
			return transportReply(400, fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"error":{"code":35,"message":"ranges over 10000 blocks are not supported on free plan"}}`, call.ID)), nil
		default:
			t.Fatal("unexpected RPC", call.Method)
			return nil, errors.New("unexpected_rpc")
		}
	})
	c.RPC.Transport = tr
	p := historicalProgress{Manifest: c.Manifest.Hash, Days: 30, From: 100, To: 611, Next: 100, RangeSize: 512, FundingDone: true, FundingFrom: Now().Add(-30 * 24 * time.Hour), FundingTo: Now()}
	file := filepath.Join(c.StateDir, "backfill.gob")
	if err := writeGob(file, p); err != nil {
		t.Fatal(err)
	}
	err := c.Backfill(context.Background(), 30, 0, nil)
	var rpcErr *RPCError
	if !errors.As(err, &rpcErr) || rpcErr.Code != 35 || errors.Is(err, ErrLogRange) || calls != 6 {
		t.Fatalf("provider error must stop one attempt: err=%v calls=%d", err, calls)
	}
	if len(store.batches) != 1 || store.batches[0].Capture.Status != "failed" || store.batches[0].Capture.Canonical || len(store.batches[0].Requests)+len(store.batches[0].Finalizations)+len(store.batches[0].Claims) != 0 {
		t.Fatal("provider error invented complete or current event data", store.batches)
	}
	var after historicalProgress
	if err := readGob(file, &after); err != nil {
		t.Fatal(err)
	}
	if after.Next != p.Next || after.RangeSize != p.RangeSize {
		t.Fatal("provider error advanced or split cursor", after)
	}
}
