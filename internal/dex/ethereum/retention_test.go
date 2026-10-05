package ethereum

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/dex"
)

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
