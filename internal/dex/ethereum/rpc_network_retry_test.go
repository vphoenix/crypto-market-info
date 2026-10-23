package ethereum

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/dex"
)

func TestRetryNetworkFailuresRecoversClientAndClone(t *testing.T) {
	for _, tc := range []struct {
		name, class, code string
		err               error
	}{
		{"DNS not found", "dns_not_found", "rpc_transport_failure", &net.DNSError{Err: "secret", Name: "private", IsNotFound: true}},
		{"DNS error", "dns_error", "rpc_transport_failure", &net.DNSError{Err: "secret", Name: "private"}},
		{"DNS timeout", "dns_timeout", "rpc_transport_timeout", &net.DNSError{Err: "secret", Name: "private", IsTimeout: true}},
		{"connection refused", "connection_refused", "rpc_transport_failure", syscall.ECONNREFUSED},
		{"network unreachable", "network_unreachable", "rpc_transport_failure", syscall.ENETUNREACH},
		{"host unreachable", "host_unreachable", "rpc_transport_failure", syscall.EHOSTUNREACH},
		{"connection reset", "connection_reset", "rpc_transport_failure", syscall.ECONNRESET},
	} {
		t.Run(tc.name, func(t *testing.T) {
			attempts := 0
			c := testClient(t, func(*http.Request) (*http.Response, error) {
				attempts++
				if attempts <= 2 {
					return nil, tc.err
				}
				return response(`[{"jsonrpc":"2.0","id":1,"result":"0x1"}]`), nil
			})
			c.AuditFailures = true
			c.RetryNetworkFailures = true
			var diagnostics []RPCDiagnostic
			c.Diagnostic = func(d RPCDiagnostic) { diagnostics = append(diagnostics, d) }
			clone := c.Clone()
			for i, client := range []*Client{c, clone} {
				r := client.One(context.Background(), "eth_chainId", []any{})
				var transport *TransportError
				if !errors.As(fmt.Errorf("source: %w", r.Err), &transport) || transport.Code != tc.code || transport.Class != tc.class || !IsNetworkFailure(r.Err) {
					t.Fatalf("unclassified failure: %+v", r.Err)
				}
				if attempts != i+1 || r.Err.Error() != tc.code || r.Payload == (dex.Hash{}) || r.At.IsZero() || r.StartedAt.IsZero() || len(r.Raw) != 0 {
					t.Fatalf("failure retried or reported as success: attempts=%d result=%+v", attempts, r)
				}
				if c.ConfirmedNetworkFault() != nil || clone.ConfirmedNetworkFault() != nil {
					t.Fatal("recoverable failure permanently stopped the source")
				}
				raw, err := c.Archive.Get(r.Payload)
				var envelope struct {
					Source, Failure, ErrorClass string
					At, StartedAt               time.Time
					Sent                        bool
					Response                    []byte
				}
				if err != nil || json.Unmarshal(raw, &envelope) != nil || envelope.Source != "example.test" || envelope.Failure != tc.code || envelope.ErrorClass != tc.class || !envelope.Sent || envelope.At.IsZero() || envelope.StartedAt.IsZero() || len(envelope.Response) != 0 {
					t.Fatalf("failure evidence missing: %s err=%v", raw, err)
				}
				if strings.Contains(string(raw), "secret") || strings.Contains(string(raw), "private") || errors.Unwrap(r.Err) != nil {
					t.Fatal("raw transport credentials escaped sanitization")
				}
			}
			for _, client := range []*Client{c, clone} {
				r := client.One(context.Background(), "eth_chainId", []any{})
				if r.Err != nil || string(r.Raw) != `"0x1"` || r.Payload == (dex.Hash{}) {
					t.Fatalf("source did not recover: %+v", r)
				}
			}
			if attempts != 4 || len(diagnostics) != 2 || diagnostics[0].ErrorClass != tc.class || diagnostics[1].ErrorClass != tc.class {
				t.Fatalf("unexpected retries or lost diagnostics: attempts=%d diagnostics=%+v", attempts, diagnostics)
			}
		})
	}
}

func TestRetryNetworkFailuresKeepsCertificateVerification(t *testing.T) {
	requests := 0
	s := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests++
		io.WriteString(w, `[{"jsonrpc":"2.0","id":1,"result":"0x1"}]`)
	}))
	s.Config.ErrorLog = log.New(io.Discard, "", 0)
	s.StartTLS()
	defer s.Close()
	c, err := NewClient(s.URL, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	c.AuditFailures = true
	c.RetryNetworkFailures = true
	for _, client := range []*Client{c, c.Clone()} {
		r := client.One(context.Background(), "eth_chainId", nil)
		var transport *TransportError
		if !errors.As(r.Err, &transport) || transport.Class != "tls_certificate_error" || !IsNetworkFailure(r.Err) || r.Payload == (dex.Hash{}) {
			t.Fatalf("untrusted certificate was accepted or misclassified: %+v", r)
		}
	}
	if requests != 0 || c.ConfirmedNetworkFault() != nil {
		t.Fatal("TLS verification bypassed or TLS failure permanently stopped source")
	}
	// Trust only the server's test certificate; recovery never disables verification.
	c.HTTP = s.Client()
	if r := c.One(context.Background(), "eth_chainId", nil); r.Err != nil || string(r.Raw) != `"0x1"` {
		t.Fatalf("corrected trust did not recover: %+v", r)
	}
}

func TestDefaultNetworkFailureStillStopsSharedSource(t *testing.T) {
	attempts := 0
	c := testClient(t, func(*http.Request) (*http.Response, error) {
		attempts++
		return nil, &net.DNSError{Err: "secret", IsNotFound: true}
	})
	c.AuditFailures = true
	clone := c.Clone()
	for _, client := range []*Client{c, clone} {
		r := client.One(context.Background(), "eth_chainId", nil)
		if r.Err == nil || !strings.HasPrefix(r.Err.Error(), "network_issue_stop:") || client.ConfirmedNetworkFault() == nil {
			t.Fatalf("default source-stop policy changed: %+v", r)
		}
	}
	if attempts != 1 {
		t.Fatalf("default source-stop policy sent %d requests", attempts)
	}
}

type retryReadErrorBody struct{ err error }

func (b retryReadErrorBody) Read([]byte) (int, error) { return 0, b.err }
func (retryReadErrorBody) Close() error               { return nil }

func TestNetworkFailureClassificationExcludesLocalAndProtocolFailures(t *testing.T) {
	for _, tc := range []struct {
		name, code string
		network    bool
		transport  error
		read       error
		status     int
		body       string
	}{
		{name: "transport timeout", transport: context.DeadlineExceeded, code: "rpc_transport_timeout", network: true},
		{name: "transport cancellation", transport: context.Canceled, code: "rpc_request_canceled"},
		{name: "truncated body", read: io.ErrUnexpectedEOF, code: "rpc_response_truncated", network: true},
		{name: "body timeout", read: context.DeadlineExceeded, code: "rpc_response_read_timeout", network: true},
		{name: "body I/O failure", read: syscall.ECONNRESET, code: "rpc_response_read_failed", network: true},
		{name: "body cancellation", read: context.Canceled, code: "rpc_response_read_failed"},
		{name: "HTTP rate limit", status: 429, body: "rate limited", code: "rpc_http_429"},
		{name: "rate limited truncated body", status: 429, read: io.ErrUnexpectedEOF, code: "rpc_response_truncated"},
		{name: "HTTP server error", status: 503, body: "unavailable", code: "rpc_http_503"},
		{name: "invalid JSON", body: "not JSON", code: "rpc_batch_incomplete"},
		{name: "remote error", body: `[{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"revert"}}]`, code: "rpc_remote_error(code=-32000)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := testClient(t, func(*http.Request) (*http.Response, error) {
				if tc.transport != nil {
					return nil, tc.transport
				}
				r := response(tc.body)
				if tc.status != 0 {
					r.StatusCode = tc.status
				}
				if tc.read != nil {
					r.Body = retryReadErrorBody{tc.read}
				}
				return r, nil
			})
			c.RetryNetworkFailures = true
			var after error
			c.AfterRequest = func(_ int, err error) { after = err }
			r := c.One(context.Background(), "eth_chainId", nil)
			if r.Err == nil || r.Err.Error() != tc.code || IsNetworkFailure(r.Err) != tc.network {
				t.Fatalf("result=%+v want code=%s network=%v", r, tc.code, tc.network)
			}
			if IsNetworkFailure(after) != tc.network || tc.status == 429 && !IsRateLimited(after) {
				t.Fatalf("quota callback misclassified failure: %v", after)
			}
		})
	}
	for _, err := range []error{nil, context.Canceled, context.DeadlineExceeded, errors.New("rpc_transport_failure"), errors.New("rpc_source_wait_exceeds_deadline"), &HTTPError{Status: 429}} {
		if IsNetworkFailure(err) {
			t.Fatalf("untyped/local failure treated as network: %v", err)
		}
	}
}

func TestNetworkFailureClassificationExcludesOperationCancellationAndBudget(t *testing.T) {
	for _, budget := range []bool{false, true} {
		t.Run(fmt.Sprint("budget=", budget), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			if budget {
				cancel()
				ctx, cancel = context.WithTimeout(context.Background(), 10*time.Millisecond)
			}
			defer cancel()
			c := testClient(t, func(*http.Request) (*http.Response, error) {
				if budget {
					<-ctx.Done()
				} else {
					cancel()
				}
				return nil, ctx.Err()
			})
			c.AuditFailures = true
			c.RetryNetworkFailures = true
			var after error
			c.AfterRequest = func(_ int, err error) { after = err }
			r := c.One(ctx, "eth_chainId", nil)
			want := "rpc_operation_canceled"
			if budget {
				want = "rpc_operation_budget_exhausted"
			}
			if r.Err == nil || r.Err.Error() != want || IsNetworkFailure(r.Err) || IsNetworkFailure(after) || r.Payload == (dex.Hash{}) || c.ConfirmedNetworkFault() != nil {
				t.Fatalf("local operation failure became a network failure: %+v", r)
			}
		})
	}
}
