package ethereum

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/dex"
)

func TestHashOnlyCollectsAnchoredLogsReceiptsAndQuotesWithoutArchive(t *testing.T) {
	m := DefaultManifest()
	blockHash := dex.Digest([]byte("block"))
	txHash := dex.Digest([]byte("tx"))
	header := map[string]any{
		"number": "0x7", "hash": blockHash.String(), "parentHash": dex.Digest([]byte("parent")).String(),
		"timestamp": "0x64", "baseFeePerGas": "0x1", "miner": m.Addresses["WETH"].String(),
	}
	log := map[string]any{
		"blockNumber": "0x7", "blockHash": blockHash.String(), "transactionHash": txHash.String(),
		"transactionIndex": "0x0", "logIndex": "0x0", "address": m.LogAddresses()[0],
		"data": "0x0102", "topics": []string{dex.Digest([]byte("event")).String()}, "removed": false,
	}
	tx := map[string]any{
		"hash": txHash.String(), "blockNumber": "0x7", "blockHash": blockHash.String(),
		"transactionIndex": "0x0", "from": m.Addresses["USDC"].String(), "to": m.Addresses["WETH"].String(),
		"type": "0x2", "value": "0x1", "input": "0x0102030405",
	}
	receipt := map[string]any{
		"transactionHash": txHash.String(), "blockNumber": "0x7", "blockHash": blockHash.String(),
		"transactionIndex": "0x0", "from": tx["from"], "to": tx["to"], "type": "0x2",
		"status": "0x1", "gasUsed": "0x5208", "effectiveGasPrice": "0x2", "logs": []any{log},
	}
	c := testClient(t, func(r *http.Request) (*http.Response, error) {
		var requests []struct {
			ID     uint64
			Method string
			Params []json.RawMessage
		}
		if err := json.NewDecoder(r.Body).Decode(&requests); err != nil {
			return nil, err
		}
		out := make([]map[string]any, 0, len(requests))
		for _, request := range requests {
			var result any
			switch request.Method {
			case "eth_getBlockByNumber":
				result = header
			case "eth_getLogs":
				var filter struct {
					BlockHash string
					Address   []string
				}
				if err := json.Unmarshal(request.Params[0], &filter); err != nil || filter.BlockHash != blockHash.String() {
					return nil, fmt.Errorf("log request lost block hash")
				}
				result = []any{}
				for _, address := range filter.Address {
					if address == log["address"] {
						result = []any{log}
					}
				}
			case "eth_getTransactionByHash":
				result = tx
			case "eth_getTransactionReceipt":
				result = receipt
			case "eth_call":
				var ref struct {
					BlockHash        string
					RequireCanonical bool
				}
				if err := json.Unmarshal(request.Params[1], &ref); err != nil || ref.BlockHash != blockHash.String() || !ref.RequireCanonical {
					return nil, fmt.Errorf("quote request lost canonical block hash")
				}
				result = "0x" + word(big.NewInt(42)) + word(big.NewInt(123)) + word(big.NewInt(2)) + word(big.NewInt(9000))
			default:
				return nil, fmt.Errorf("unexpected RPC method %s", request.Method)
			}
			out = append(out, map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": result})
		}
		body, err := json.Marshal(out)
		if err != nil {
			return nil, err
		}
		return response(string(body)), nil
	})
	// A regular file cannot host an archive directory. Any accidental write
	// fails even when these tests run with elevated filesystem permissions.
	blocked := filepath.Join(t.TempDir(), "not-a-directory")
	if err := os.WriteFile(blocked, []byte("sentinel"), 0600); err != nil {
		t.Fatal(err)
	}
	c.Archive = Archive{Dir: filepath.Join(blocked, "evidence"), HashOnly: true}
	ctx := context.Background()
	h, err := c.Header(ctx, "latest")
	if err != nil {
		t.Fatal(err)
	}
	b, err := c.Collect(ctx, h, false)
	if err != nil || b.Block.LogCoverage != "complete" || b.Block.ReceiptCoverage != "complete" || len(b.Logs) != 1 || len(b.Receipts) != 1 {
		t.Fatalf("hash-only backfill lost typed facts: block=%+v err=%v", b.Block, err)
	}
	if b.Block.Payload == (dex.Hash{}) || b.Block.Hash != blockHash || !b.Block.Time.Equal(time.Unix(100, 0).UTC()) || b.Block.LogMembers != dex.LogDigest(b.Logs) || b.Block.ReceiptMembers != dex.ReceiptDigest(b.Receipts) {
		t.Fatal("block anchor, UTC time, payload or member digests changed")
	}
	rawReceipt, _ := json.Marshal(receipt)
	r := b.Receipts[0]
	if r.CalldataHash != dex.Digest([]byte{1, 2, 3, 4, 5}) || r.ReceiptHash != dex.Digest(rawReceipt) || r.GasUsed != 21000 || r.GasPrice.Cmp(big.NewInt(2)) != 0 || r.LogCount != 1 || r.AvailableAt.IsZero() {
		t.Fatal("receipt values or source digests changed")
	}
	q := c.Quotes(ctx, blockHash, []dex.SwapRequest{{TokenIn: m.Addresses["USDC"], TokenOut: m.Addresses["WETH"], Fee: 500, Amount: big.NewInt(100)}})[0]
	if q.Err != nil || q.Amount.Cmp(big.NewInt(42)) != 0 || q.Payload == (dex.Hash{}) || q.At.IsZero() {
		t.Fatal("quote values, source digest or time lost", q.Err)
	}
	if got, err := os.ReadFile(blocked); err != nil || string(got) != "sentinel" {
		t.Fatal("collector modified the archive path", err)
	}
}

func TestHashOnlyKeepsDigestsWithoutCreatingResponseFiles(t *testing.T) {
	archive := Archive{Dir: t.TempDir(), HashOnly: true}
	raw := []byte{0xff, 0, 0xfe}
	h, e := archive.Put(raw)
	if e != nil || h != dex.Digest(raw) {
		t.Fatal("source digest changed", h, e)
	}
	object := struct{ Data []byte }{raw}
	b, _ := json.Marshal(object)
	if got, e := archive.PutObject(object); e != nil || got != dex.Digest(b) {
		t.Fatal("proof digest changed", got, e)
	}
	if _, e := archive.Get(h); e == nil || e.Error() != "raw_response_not_retained" {
		t.Fatal("hash-only reader silently retained raw response", e)
	}
	if entries, e := os.ReadDir(archive.Dir); e != nil || len(entries) != 0 {
		t.Fatal("hash-only path wrote files", len(entries), e)
	}
	legacy := Archive{Dir: t.TempDir()}
	if _, e = legacy.Put(raw); e != nil {
		t.Fatal(e)
	}
	if got, e := legacy.Get(h); e != nil || string(got) != string(raw) {
		t.Fatal("default archive behavior changed", e)
	}
}

func TestHashOnlyRPCKeepsSafeDiagnosticsAndPersistentCooldown(t *testing.T) {
	c := testClient(t, func(*http.Request) (*http.Response, error) {
		r := response(`[{"jsonrpc":"2.0","id":1,"error":{"code":-32016,"message":"over rate limit; secret-provider-message"}}]`)
		return r, nil
	})
	c.Archive.HashOnly = true
	var diagnostics []RPCDiagnostic
	c.Diagnostic = func(d RPCDiagnostic) { diagnostics = append(diagnostics, d) }
	if e := c.ConfigurePolicy(Policy{Directory: t.TempDir(), MinInterval: time.Millisecond, PerMember: time.Millisecond, Timeout: time.Second, Cooldown: time.Second, BatchSize: 2}); e != nil {
		t.Fatal(e)
	}
	r := c.One(context.Background(), "eth_call", []any{"secret-request-parameter"})
	if !IsRateLimited(r.Err) || r.Payload == (dex.Hash{}) || len(diagnostics) != 1 {
		t.Fatal("hash-only mode lost source classification", r.Err, len(diagnostics))
	}
	b, _ := json.Marshal(diagnostics)
	if strings.Contains(string(b), "secret-") || diagnostics[0].HTTPStatus != 200 || !diagnostics[0].Sent || len(diagnostics[0].RPCCodes) != 1 {
		t.Fatal("unsafe or missing operational metadata", string(b))
	}
	clone := c.Clone()
	if !clone.Archive.HashOnly || clone.Diagnostic == nil {
		t.Fatal("worker clone restored raw archive retention")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if e := clone.WaitReady(ctx); !errors.Is(e, context.DeadlineExceeded) {
		t.Fatal("discarded raw bodies lost persistent cooldown", e)
	}
	files, e := filepath.Glob(filepath.Join(c.Archive.Dir, "*", "*.json.gz"))
	if e != nil || len(files) != 0 {
		t.Fatal("RPC responses still archived", files, e)
	}
}

func TestHashOnlyDiagnosticFieldsAreBounded(t *testing.T) {
	c := testClient(t, func(*http.Request) (*http.Response, error) {
		errorsInBody := make([]map[string]any, 500)
		for i := range errorsInBody {
			errorsInBody[i] = map[string]any{"jsonrpc": "2.0", "id": i + 1, "error": map[string]any{"code": -32016, "message": "over rate limit"}}
		}
		b, _ := json.Marshal(errorsInBody)
		r := response(string(b))
		r.Header.Set("Content-Type", strings.Repeat("x", 64<<10))
		return r, nil
	})
	c.Archive.HashOnly = true
	var got RPCDiagnostic
	c.Diagnostic = func(d RPCDiagnostic) { got = d }
	if r := c.One(context.Background(), "eth_chainId", []any{}); r.Err == nil {
		t.Fatal("oversized member list was accepted")
	}
	b, _ := json.Marshal(got)
	if len(got.RPCCodes) > 20 || len(got.Methods) > 20 || len(got.Headers["Content-Type"]) > 256 || len(b) > 4096 {
		t.Fatalf("unbounded error sample: codes=%d methods=%d header=%d bytes=%d", len(got.RPCCodes), len(got.Methods), len(got.Headers["Content-Type"]), len(b))
	}
}

func TestHashOnly429TruncationKeepsCooldownWithoutResponseBody(t *testing.T) {
	c := testClient(t, func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 429, Header: http.Header{"Retry-After": []string{"1"}}, Body: brokenBody{}}, nil
	})
	c.Archive.HashOnly = true
	var diagnostic RPCDiagnostic
	c.Diagnostic = func(d RPCDiagnostic) { diagnostic = d }
	if e := c.ConfigurePolicy(Policy{Directory: t.TempDir(), MinInterval: time.Millisecond, PerMember: time.Millisecond, Timeout: time.Second, Cooldown: time.Second, BatchSize: 2}); e != nil {
		t.Fatal(e)
	}
	r := c.One(context.Background(), "eth_chainId", []any{})
	if r.Err == nil || diagnostic.HTTPStatus != 429 || diagnostic.Failure != "rpc_response_truncated" {
		t.Fatal("failure metadata lost with response body", r.Err, diagnostic)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if e := c.WaitReady(ctx); !errors.Is(e, context.DeadlineExceeded) {
		t.Fatal("429 headers did not persist cooldown", e)
	}
	if entries, e := os.ReadDir(c.Archive.Dir); e != nil || len(entries) != 0 {
		t.Fatal("truncated body still archived", e)
	}
}
