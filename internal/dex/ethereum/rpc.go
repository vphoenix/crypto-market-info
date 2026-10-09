package ethereum

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/vphoenix/crypto-market-info/internal/dex"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Call struct {
	Method string `json:"method"`
	Params any    `json:"params"`
}
type Result struct {
	Raw       json.RawMessage
	Payload   dex.Hash
	Err       error
	At        time.Time
	StartedAt time.Time
}

// RPCError preserves protocol classification without putting provider messages
// (which can contain credentials) in operational logs. Provider text remains in
// memory; archive retention is controlled separately by Archive.HashOnly.
type RPCError struct {
	Code    int
	Message string
}

func (e *RPCError) Error() string {
	if e.RateLimited() {
		return fmt.Sprintf("rpc_rate_limited(code=%d)", e.Code)
	}
	if e.Code == -32601 {
		return "rpc_method_not_found"
	}
	return fmt.Sprintf("rpc_remote_error(code=%d)", e.Code)
}
func (e *RPCError) RateLimited() bool {
	s := strings.ToLower(e.Message)
	return e.Code == -32016 || e.Code == -32005 && strings.Contains(s, "rate") || e.Code == -32011 && strings.Contains(s, "request limit reached") || strings.Contains(s, "over rate limit") || strings.Contains(s, "too many requests")
}

type HTTPError struct {
	Status     int
	RetryAfter time.Duration
	RPCCode    int
	RPCMessage string // memory only; Error() never exposes provider text.
}

func (e *HTTPError) Error() string { return fmt.Sprintf("rpc_http_%d", e.Status) }
func IsRateLimited(err error) bool {
	var r *RPCError
	var h *HTTPError
	return errors.As(err, &r) && r.RateLimited() || errors.As(err, &h) && h.Status == 429
}

type Client struct {
	url, SourceID string
	HTTP          *http.Client
	Archive       Archive
	slots         chan struct{}
	// Optional source-wide coordination. Queue time precedes the HTTP timeout.
	BeforeRequest func(context.Context, int) error
	AfterRequest  func(int, error)
	AuditFailures bool
	Diagnostic    func(RPCDiagnostic)
	// Optional policies used by other collectors in this shared workspace.
	BatchSize     int
	Timeout       time.Duration
	WaitForSource func(context.Context) error
	networkFault  *networkFault
}

func NewClient(endpoint, dir string) (*Client, error) {
	u, e := url.Parse(endpoint)
	if e != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, errors.New("invalid DEX RPC URL")
	}
	return &Client{url: endpoint, SourceID: strings.ToLower(u.Hostname()), HTTP: &http.Client{Timeout: 5 * time.Second}, Archive: Archive{Dir: dir}, slots: make(chan struct{}, 2), networkFault: &networkFault{}}, nil
}

// Clone reserves HTTP slots for a worker while sharing transport and quota.
// Maintenance quota waiters cannot occupy every slot of a timed probe worker.
func (c *Client) Clone() *Client {
	return &Client{url: c.url, SourceID: c.SourceID, HTTP: c.HTTP, Archive: c.Archive, slots: make(chan struct{}, 2), BeforeRequest: c.BeforeRequest, AfterRequest: c.AfterRequest, AuditFailures: c.AuditFailures, Diagnostic: c.Diagnostic, BatchSize: c.BatchSize, Timeout: c.Timeout, WaitForSource: c.WaitForSource, networkFault: c.networkFault}
}
func (c *Client) Batch(ctx context.Context, calls []Call) []Result {
	out := make([]Result, len(calls))
	var wg sync.WaitGroup
	size := c.BatchSize
	if size <= 0 || size > 20 {
		size = 20
	}
	for start := 0; start < len(calls); start += size {
		end := min(start+size, len(calls))
		wg.Add(1)
		go func(start, end int) { defer wg.Done(); copy(out[start:end], c.group(ctx, calls[start:end])) }(start, end)
	}
	wg.Wait()
	return out
}
func (c *Client) group(ctx context.Context, calls []Call) []Result {
	out := make([]Result, len(calls))
	var started time.Time
	rateAlreadyReported := false
	if c.AfterRequest != nil {
		defer func() {
			if rateAlreadyReported {
				return
			}
			for _, r := range out {
				if IsRateLimited(r.Err) {
					c.AfterRequest(len(calls), r.Err)
					return
				}
			}
			for _, r := range out {
				if r.Err != nil {
					c.AfterRequest(len(calls), r.Err)
					return
				}
			}
			c.AfterRequest(len(calls), nil)
		}()
	}
	fail := func(e error, h dex.Hash) []Result {
		if h == (dex.Hash{}) && c.AuditFailures && e.Error() != "evidence_write_failed" && started.IsZero() {
			var err error
			h, err = c.archiveUnsent(calls, e.Error())
			if err != nil {
				e = errors.New("evidence_write_failed")
			}
		}
		for i := range out {
			out[i] = Result{Err: e, Payload: h, At: dex.Now(), StartedAt: started}
		}
		return out
	}
	if ctx.Err() != nil {
		return fail(errors.New("deadline_exceeded"), dex.Hash{})
	}
	select {
	case c.slots <- struct{}{}:
		defer func() { <-c.slots }()
	case <-ctx.Done():
		return fail(errors.New("deadline_exceeded"), dex.Hash{})
	}
	if c.BeforeRequest != nil {
		if e := c.BeforeRequest(ctx, len(calls)); e != nil {
			return fail(e, dex.Hash{})
		}
	}
	if e := c.ConfirmedNetworkFault(); e != nil {
		return fail(e, dex.Hash{})
	}
	timeout := c.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	operationCtx := ctx
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if ctx.Err() != nil {
		return fail(errors.New("deadline_exceeded"), dex.Hash{})
	}
	type request struct {
		JSONRPC string `json:"jsonrpc"`
		ID      uint64 `json:"id"`
		Call
	}
	reqs := make([]request, len(calls))
	for i, v := range calls {
		reqs[i] = request{"2.0", uint64(i + 1), v}
	}
	body, e := json.Marshal(reqs)
	if e != nil {
		return fail(errors.New("rpc_request_encoding"), dex.Hash{})
	}
	req, e := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(body))
	if e != nil {
		return fail(errors.New("rpc_request_invalid"), dex.Hash{})
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "crypto-market-info/1.0")
	trace := newRPCTrace()
	req = req.WithContext(trace.context(req.Context()))
	started = dex.Now()
	var responseHeaders http.Header
	var diagnosticError error
	archiveFailure := func(code string, status int, raw []byte) []Result {
		if !c.AuditFailures {
			return fail(errors.New(code), dex.Hash{})
		}
		c.rememberNetworkFault(diagnosticError)
		h, err := c.archiveRPC(body, started, status, responseHeaders, raw, code, diagnosticError, trace)
		if err != nil {
			return fail(errors.New("evidence_write_failed"), dex.Hash{})
		}
		if e := c.ConfirmedNetworkFault(); e != nil {
			return fail(e, h)
		}
		return fail(errors.New(code), h)
	}
	resp, e := c.HTTP.Do(req)
	if e != nil {
		diagnosticError = e
		code := "rpc_transport_failure"
		var networkError net.Error
		if errors.Is(e, context.Canceled) {
			code = "rpc_request_canceled"
		} else if errors.Is(e, context.DeadlineExceeded) || errors.As(e, &networkError) && networkError.Timeout() {
			code = "rpc_transport_timeout"
		}
		if operationCtx.Err() != nil {
			if errors.Is(operationCtx.Err(), context.Canceled) {
				code = "rpc_operation_canceled"
			} else {
				code = "rpc_operation_budget_exhausted"
			}
		}
		return archiveFailure(code, 0, nil)
	}
	defer resp.Body.Close()
	responseHeaders = resp.Header
	if resp.StatusCode == 429 && c.AfterRequest != nil {
		var retry time.Duration
		if n, e := strconv.ParseUint(resp.Header.Get("Retry-After"), 10, 32); e == nil {
			retry = time.Duration(n) * time.Second
		} else if at, e := http.ParseTime(resp.Header.Get("Retry-After")); e == nil {
			retry = max(time.Until(at), time.Duration(0))
		}
		c.AfterRequest(len(calls), &HTTPError{Status: 429, RetryAfter: retry})
		rateAlreadyReported = true
	}
	raw, e := io.ReadAll(io.LimitReader(resp.Body, (16<<20)+1))
	if e != nil {
		diagnosticError = e
		code := "rpc_response_read_failed"
		var networkError net.Error
		if errors.Is(e, context.DeadlineExceeded) || errors.As(e, &networkError) && networkError.Timeout() {
			code = "rpc_response_read_timeout"
		} else if errors.Is(e, io.ErrUnexpectedEOF) {
			code = "rpc_response_truncated"
		}
		if operationCtx.Err() != nil {
			if errors.Is(operationCtx.Err(), context.Canceled) {
				code = "rpc_operation_canceled"
			} else {
				code = "rpc_operation_budget_exhausted"
			}
		}
		return archiveFailure(code, resp.StatusCode, raw)
	}
	if len(raw) > 16<<20 {
		return archiveFailure("rpc_response_too_large", resp.StatusCode, raw[:16<<20])
	}
	at := dex.Now()
	h, e := c.archiveRPC(body, started, resp.StatusCode, resp.Header, raw, "", nil, trace)
	if e != nil {
		return fail(errors.New("evidence_write_failed"), dex.Hash{})
	}
	if resp.StatusCode != 200 {
		var retry time.Duration
		if seconds, err := strconv.ParseUint(resp.Header.Get("Retry-After"), 10, 32); err == nil {
			retry = time.Duration(seconds) * time.Second
		} else if at, err := http.ParseTime(resp.Header.Get("Retry-After")); err == nil {
			retry = max(time.Until(at), time.Duration(0))
		}
		httpError := &HTTPError{Status: resp.StatusCode, RetryAfter: retry}
		var errorsInBody []struct{ Error *RPCError }
		if json.Unmarshal(raw, &errorsInBody) == nil {
			for _, v := range errorsInBody {
				if v.Error != nil {
					httpError.RPCCode, httpError.RPCMessage = v.Error.Code, v.Error.Message
					break
				}
			}
		}
		return fail(httpError, h)
	}
	type response struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      uint64          `json:"id"`
		Result  json.RawMessage `json:"result"`
		Error   json.RawMessage `json:"error"`
	}
	var rr []response
	if json.Unmarshal(raw, &rr) != nil || len(rr) != len(calls) {
		return fail(errors.New("rpc_batch_incomplete"), h)
	}
	seen := map[uint64]bool{}
	for _, r := range rr {
		hasErr := len(r.Error) > 0 && string(r.Error) != "null"
		hasResult := len(r.Result) > 0
		if r.JSONRPC != "2.0" || r.ID == 0 || r.ID > uint64(len(calls)) || seen[r.ID] || hasErr == hasResult {
			return fail(errors.New("rpc_batch_invalid_ids_or_envelope"), h)
		}
		seen[r.ID] = true
		o := Result{Raw: r.Result, Payload: h, At: at, StartedAt: started}
		if hasErr {
			var remote RPCError
			if json.Unmarshal(r.Error, &remote) != nil || remote.Code == 0 || remote.Message == "" {
				o.Err = errors.New("rpc_invalid_error_envelope")
			} else {
				o.Err = &remote
			}
		}
		out[r.ID-1] = o
	}
	return out
}
func (c *Client) One(ctx context.Context, method string, params any) Result {
	return c.Batch(ctx, []Call{{method, params}})[0]
}
func BlockRef(h dex.Hash) map[string]any {
	return map[string]any{"blockHash": h.String(), "requireCanonical": true}
}
func EthCall(to dex.Address, data string, h dex.Hash) Call {
	return Call{"eth_call", []any{map[string]string{"to": to.String(), "data": data}, BlockRef(h)}}
}
