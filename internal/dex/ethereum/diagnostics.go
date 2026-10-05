package ethereum

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/vphoenix/crypto-market-info/internal/dex"
	"net"
	"net/http"
	"net/http/httptrace"
	"sync"
	"syscall"
	"time"
)

type networkFault struct {
	mu    sync.Mutex
	class string
}

func (c *Client) rememberNetworkFault(e error) {
	class := safeNetworkError(e)
	switch class {
	case "dns_not_found", "dns_error", "tls_certificate_error", "connection_refused", "network_unreachable", "host_unreachable":
	default:
		return
	}
	if c.networkFault == nil {
		return
	}
	c.networkFault.mu.Lock()
	defer c.networkFault.mu.Unlock()
	c.networkFault.class = class
}

// Timeouts and resets alone do not prove a route/DNS/TLS problem. Only specific
// connection diagnostics stop the opt-in collector rather than auto-retrying.
func (c *Client) ConfirmedNetworkFault() error {
	if c.networkFault == nil {
		return nil
	}
	c.networkFault.mu.Lock()
	defer c.networkFault.mu.Unlock()
	if c.networkFault.class == "" {
		return nil
	}
	return fmt.Errorf("network_issue_stop: source=%s class=%s", c.SourceID, c.networkFault.class)
}

type rpcTrace struct {
	mu     sync.Mutex
	events map[string]time.Time
}

// Small diagnostic fields only. No response body, provider message, URL or
// request parameters are retained by a HashOnly client's operational logging.
type RPCDiagnostic struct {
	Source              string
	At                  time.Time
	HTTPStatus          int
	Sent                bool
	Failure, ErrorClass string
	Methods             []string
	RPCCodes            []int
	Headers             map[string]string
	Trace               map[string]time.Time
}

func newRPCTrace() *rpcTrace         { return &rpcTrace{events: map[string]time.Time{}} }
func (t *rpcTrace) mark(name string) { t.mu.Lock(); t.events[name] = dex.Now(); t.mu.Unlock() }
func (t *rpcTrace) context(ctx context.Context) context.Context {
	return httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		DNSStart: func(httptrace.DNSStartInfo) { t.mark("dns_start") }, DNSDone: func(httptrace.DNSDoneInfo) { t.mark("dns_done") },
		ConnectStart: func(string, string) { t.mark("connect_start") }, ConnectDone: func(string, string, error) { t.mark("connect_done") },
		TLSHandshakeStart: func() { t.mark("tls_start") }, TLSHandshakeDone: func(tls.ConnectionState, error) { t.mark("tls_done") },
		GotConn: func(httptrace.GotConnInfo) { t.mark("got_connection") }, GotFirstResponseByte: func() { t.mark("first_response_byte") },
		WroteRequest: func(httptrace.WroteRequestInfo) { t.mark("wrote_request") },
	})
}
func safeNetworkError(e error) string {
	if e == nil {
		return ""
	}
	if errors.Is(e, context.DeadlineExceeded) {
		return "deadline"
	}
	if errors.Is(e, context.Canceled) {
		return "cancelled"
	}
	var dns *net.DNSError
	if errors.As(e, &dns) {
		if dns.IsNotFound {
			return "dns_not_found"
		}
		if dns.IsTimeout {
			return "dns_timeout"
		}
		return "dns_error"
	}
	var cert x509.UnknownAuthorityError
	var host x509.HostnameError
	var invalid x509.CertificateInvalidError
	if errors.As(e, &cert) || errors.As(e, &host) || errors.As(e, &invalid) {
		return "tls_certificate_error"
	}
	for _, v := range []struct {
		err  error
		name string
	}{{syscall.ECONNREFUSED, "connection_refused"}, {syscall.ECONNRESET, "connection_reset"}, {syscall.ENETUNREACH, "network_unreachable"}, {syscall.EHOSTUNREACH, "host_unreachable"}} {
		if errors.Is(e, v.err) {
			return v.name
		}
	}
	var n net.Error
	if errors.As(e, &n) && n.Timeout() {
		return "network_timeout"
	}
	return "unclassified_io_error"
}
func (c *Client) archiveRPC(body []byte, start time.Time, status int, headers http.Header, raw []byte, failure string, err error, t *rpcTrace) (dex.Hash, error) {
	h := map[string]string{}
	for _, name := range []string{"Retry-After", "Date", "Content-Type", "RateLimit-Limit", "RateLimit-Remaining", "RateLimit-Reset", "X-RateLimit-Limit", "X-RateLimit-Remaining", "X-RateLimit-Reset"} {
		if value := headers.Get(name); value != "" {
			h[name] = value[:min(len(value), 256)]
		}
	}
	t.mu.Lock()
	events := map[string]time.Time{}
	for k, v := range t.events {
		events[k] = v
	}
	t.mu.Unlock()
	if c.Diagnostic != nil {
		var replies []struct{ Error *RPCError }
		codes := []int{}
		if json.Unmarshal(raw, &replies) == nil {
			for _, reply := range replies {
				if reply.Error != nil && len(codes) < 20 {
					codes = append(codes, reply.Error.Code)
				}
			}
		}
		if failure != "" || status != 200 || len(codes) > 0 {
			var calls []Call
			_ = json.Unmarshal(body, &calls)
			methods := make([]string, min(len(calls), 20))
			for i := range methods {
				methods[i] = calls[i].Method[:min(len(calls[i].Method), 64)]
			}
			c.Diagnostic(RPCDiagnostic{c.SourceID, dex.Now(), status, true, failure, safeNetworkError(err), methods, codes, h, events})
		}
	}
	return c.Archive.PutObject(struct {
		Source              string
		At, StartedAt       time.Time
		Requests            json.RawMessage
		HTTPStatus          int
		Response            []byte
		Sent                bool
		Failure, ErrorClass string
		Headers             map[string]string
		Trace               map[string]time.Time
	}{c.SourceID, dex.Now(), start, body, status, raw, true, failure, safeNetworkError(err), h, events})
}

// Planned local failures have an explicit Sent=false envelope. These requests
// must not be counted as provider traffic or interpreted as network failures.
func (c *Client) archiveUnsent(calls []Call, reason string) (dex.Hash, error) {
	type request struct {
		JSONRPC string `json:"jsonrpc"`
		ID      uint64 `json:"id"`
		Call
	}
	r := make([]request, len(calls))
	for i, v := range calls {
		r[i] = request{"2.0", uint64(i + 1), v}
	}
	return c.Archive.PutObject(struct {
		Source     string
		At         time.Time
		Requests   []request
		HTTPStatus int
		Response   []byte
		Sent       bool
		Failure    string
	}{c.SourceID, dex.Now(), r, 0, nil, false, reason})
}
