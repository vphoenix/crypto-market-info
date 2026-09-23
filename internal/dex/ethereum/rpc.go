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
	"sync"
	"time"
)

type Call struct {
	Method string `json:"method"`
	Params any    `json:"params"`
}
type Result struct {
	Raw     json.RawMessage
	Payload dex.Hash
	Err     error
	At      time.Time
}
type Client struct {
	url, SourceID string
	HTTP          *http.Client
	Archive       Archive
	slots         chan struct{}
}

func NewClient(endpoint, dir string) (*Client, error) {
	u, e := url.Parse(endpoint)
	if e != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return nil, errors.New("invalid DEX RPC URL")
	}
	return &Client{url: endpoint, SourceID: u.Hostname(), HTTP: &http.Client{Timeout: 5 * time.Second}, Archive: Archive{dir}, slots: make(chan struct{}, 2)}, nil
}
func (c *Client) Batch(ctx context.Context, calls []Call) []Result {
	out := make([]Result, len(calls))
	var wg sync.WaitGroup
	for start := 0; start < len(calls); start += 20 {
		end := min(start+20, len(calls))
		wg.Add(1)
		go func(start, end int) { defer wg.Done(); copy(out[start:end], c.group(ctx, calls[start:end])) }(start, end)
	}
	wg.Wait()
	return out
}
func (c *Client) group(ctx context.Context, calls []Call) []Result {
	out := make([]Result, len(calls))
	fail := func(e error, h dex.Hash) []Result {
		for i := range out {
			out[i] = Result{Err: e, Payload: h, At: dex.Now()}
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
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
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
	resp, e := c.HTTP.Do(req)
	if e != nil {
		return fail(errors.New("rpc_transport_or_timeout"), dex.Hash{})
	}
	defer resp.Body.Close()
	raw, e := io.ReadAll(io.LimitReader(resp.Body, (16<<20)+1))
	if e != nil {
		code := "rpc_response_read_failed"
		var networkError net.Error
		if errors.Is(e, context.DeadlineExceeded) || errors.As(e, &networkError) && networkError.Timeout() {
			code = "rpc_response_read_timeout"
		} else if errors.Is(e, io.ErrUnexpectedEOF) {
			code = "rpc_response_truncated"
		}
		return fail(errors.New(code), dex.Hash{})
	}
	if len(raw) > 16<<20 {
		return fail(errors.New("rpc_response_too_large"), dex.Hash{})
	}
	at := dex.Now()
	h, e := c.Archive.PutObject(struct {
		Source     string
		At         time.Time
		Requests   json.RawMessage
		HTTPStatus int
		Response   []byte
	}{c.SourceID, at, body, resp.StatusCode, raw})
	if e != nil {
		return fail(errors.New("evidence_write_failed"), dex.Hash{})
	}
	if resp.StatusCode != 200 {
		return fail(fmt.Errorf("rpc_http_%d", resp.StatusCode), h)
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
		o := Result{Raw: r.Result, Payload: h, At: at}
		if hasErr {
			o.Err = errors.New("rpc_method_error")
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
