package reserve

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"reflect"
	"testing"

	"github.com/vphoenix/crypto-market-info/internal/dex"
	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
)

func TestReviewPollHeadersKeepsLatestSamplingAndBoundsFinalityReads(t *testing.T) {
	for _, finality := range []bool{false, true} {
		rpc, err := ethereum.NewClient("https://example.invalid", t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		rpc.Archive.HashOnly = true
		wantTags := []string{"latest"}
		wantNumbers := []uint64{100}
		if finality {
			wantTags = append(wantTags, "safe", "finalized")
			wantNumbers = append(wantNumbers, 95, 90)
		}
		var gotTags []string
		httpRequests := 0
		rpc.HTTP.Transport = transportFunc(func(req *http.Request) (*http.Response, error) {
			httpRequests++
			var calls []struct {
				ID     uint64
				Method string
				Params []json.RawMessage
			}
			if err := json.NewDecoder(req.Body).Decode(&calls); err != nil {
				return nil, err
			}
			out := make([]map[string]any, len(calls))
			for i, call := range calls {
				var tag string
				if len(call.Params) != 2 || call.Method != "eth_getBlockByNumber" || string(call.Params[1]) != "false" {
					t.Errorf("unexpected block request: %+v", call)
				}
				if err := json.Unmarshal(call.Params[0], &tag); err != nil {
					return nil, err
				}
				gotTags = append(gotTags, tag)
				number := map[string]string{"latest": "0x64", "safe": "0x5f", "finalized": "0x5a"}[tag]
				// Providers may reorder JSON-RPC batch replies.
				out[len(calls)-1-i] = map[string]any{"jsonrpc": "2.0", "id": call.ID, "result": map[string]any{
					"number": number, "hash": dex.Digest([]byte(tag)).String(), "parentHash": dex.Digest([]byte("parent")).String(),
					"timestamp": "0x1", "baseFeePerGas": "0x1", "miner": USDC.String(),
				}}
			}
			raw, err := json.Marshal(out)
			if err != nil {
				return nil, err
			}
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(raw))}, nil
		})
		c := &Collector{RPC: rpc}
		blocks, err := c.pollHeaders(context.Background(), finality)
		if err != nil || httpRequests != 1 || !reflect.DeepEqual(gotTags, wantTags) || len(blocks) != len(wantNumbers) {
			t.Fatalf("finality=%v requests=%d tags=%v blocks=%v error=%v", finality, httpRequests, gotTags, blocks, err)
		}
		for i, want := range wantNumbers {
			if blocks[i].Number != want || blocks[i].Hash != dex.Digest([]byte(wantTags[i])) {
				t.Fatalf("tag identity changed: %+v", blocks[i])
			}
		}
	}
}

func TestReviewOnlyRangeLimitsSplitAndSuccessRestoresCapacity(t *testing.T) {
	for _, tc := range []struct {
		message string
		code    int
		want    string
	}{
		{"Archive requests require a personal token.", -32602, "rpc_archive_auth_required"},
		{"Rate limit exceeded", -32016, "rpc_rate_limited"},
		{"block range extends beyond current head block", -32602, "rpc_remote_error(code=-32602)"},
		{"query returned more than 10000 results", -32005, "rpc_log_range_limit"},
		{"block range is too large", -32602, "rpc_log_range_limit"},
	} {
		reason := SourceError(nil, ethereum.Result{Err: &ethereum.RPCError{Code: tc.code, Message: tc.message}})
		if reason != tc.want {
			t.Fatalf("%q classified as %q; want %q", tc.message, reason, tc.want)
		}
		wantChunk := uint64(512)
		if reason == "rpc_log_range_limit" {
			wantChunk = 256
		}
		if got := nextLogChunk(512, 512, false, reason); got != wantChunk {
			t.Fatalf("%s changed chunk to %d; want %d", reason, got, wantChunk)
		}
	}
	for _, reason := range []string{"deadline_exceeded", "rpc_transport_timeout", "rpc_response_read_failed", "logs_encoding"} {
		if got := nextLogChunk(512, 100, false, reason); got != 512 {
			t.Fatalf("non-range failure %s reduced capacity to %d", reason, got)
		}
	}
	chunk := uint64(1)
	for i := 0; i < 9; i++ {
		chunk = nextLogChunk(chunk, chunk, true, "")
	}
	if chunk != 512 || nextLogChunk(chunk, chunk, true, "") != 512 {
		t.Fatal("successful recovery did not restore bounded batch capacity", chunk)
	}
}

func TestReviewDeferredAuthorizationCannotBecomeCoverage(t *testing.T) {
	first := rangeCap(100, 109)
	afterGap := rangeCap(120, 129)
	denied := rangeCap(110, 119)
	denied.LogCoverage = "missing"
	denied.ReceiptCoverage = "missing"
	denied.Reason = "rpc_archive_auth_required"
	caps := []Capture{first, denied, afterGap}
	if got := ContiguousLiveCursor(caps, 99, false); got != 109 {
		t.Fatal("authorization deferral skipped uncollected logs", got)
	}
	// A differently sized successful retry can fill the gap without erasing
	// the failed attempt or discarding a previously successful later interval.
	caps = append(caps, rangeCap(110, 124))
	if got := ContiguousLiveCursor(caps, 99, false); got != 129 {
		t.Fatal("successful retry was not merged into continuous coverage", got)
	}
	if caps[1].LogCoverage != "missing" || caps[1].Reason != "rpc_archive_auth_required" {
		t.Fatal("earlier failed attempt was rewritten")
	}
}
