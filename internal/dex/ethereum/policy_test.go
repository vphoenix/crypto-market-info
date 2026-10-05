package ethereum

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestPolicyCoordinatesClientsMembersAndPersistsCooldown(t *testing.T) {
	var mu sync.Mutex
	var arrivals []time.Time
	limited := false
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		arrivals = append(arrivals, time.Now())
		isLimited := limited
		mu.Unlock()
		if isLimited {
			w.Header().Set("Retry-After", "1")
			w.WriteHeader(429)
			io.WriteString(w, "Rate limit exceeded")
			return
		}
		var reqs []struct{ ID int }
		json.NewDecoder(r.Body).Decode(&reqs)
		out := []map[string]any{}
		for _, v := range reqs {
			out = append(out, map[string]any{"jsonrpc": "2.0", "id": v.ID, "result": "0x1"})
		}
		json.NewEncoder(w).Encode(out)
	}))
	defer s.Close()
	dir := t.TempDir()
	p := Policy{Directory: dir, MinInterval: 100 * time.Millisecond, PerMember: 300 * time.Millisecond, Timeout: time.Second, Cooldown: 40 * time.Millisecond, BatchSize: 2}
	makeClient := func() *Client {
		c, e := NewClient(s.URL+"/secret-token?key=hidden", t.TempDir())
		if e != nil {
			t.Fatal(e)
		}
		if e = c.ConfigurePolicy(p); e != nil {
			t.Fatal(e)
		}
		return c
	}
	c1, c2 := makeClient(), makeClient()
	call := Call{"eth_chainId", []any{}}
	for _, r := range c1.Batch(context.Background(), []Call{call, call}) {
		if r.Err != nil {
			t.Fatal(r.Err)
		}
	}
	if r := c2.One(context.Background(), call.Method, call.Params); r.Err != nil {
		t.Fatal(r.Err)
	}
	mu.Lock()
	gap := arrivals[1].Sub(arrivals[0])
	limited = true
	mu.Unlock()
	if gap < 200*time.Millisecond {
		t.Fatalf("member pacing missing: %s", gap)
	}
	r := c1.One(context.Background(), call.Method, call.Params)
	if !IsRateLimited(r.Err) {
		t.Fatal(r.Err)
	}
	raw, e := c1.Archive.Get(r.Payload)
	if e != nil {
		t.Fatal(e)
	}
	if strings.Contains(string(raw), "secret-token") || strings.Contains(string(raw), "hidden") {
		t.Fatal("endpoint credential leaked")
	}
	c3 := makeClient()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	if r = c3.One(ctx, call.Method, call.Params); r.Err == nil {
		t.Fatal("cooldown not restored")
	}
	mu.Lock()
	count := len(arrivals)
	mu.Unlock()
	if count != 3 {
		t.Fatalf("unsent request reached provider: %d", count)
	}
	raw, e = c3.Archive.Get(r.Payload)
	if e != nil {
		t.Fatal(e)
	}
	var local struct {
		Sent    bool
		Failure string
	}
	if json.Unmarshal(raw, &local) != nil || local.Sent || local.Failure != "rpc_source_wait_exceeds_deadline" {
		t.Fatal(string(raw))
	}
}
func TestMixedMemberRateLimitStillCoolsSource(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `[{"jsonrpc":"2.0","id":1,"error":{"code":3,"message":"execution reverted"}},{"jsonrpc":"2.0","id":2,"error":{"code":-32016,"message":"Rate limit exceeded"}}]`)
	}))
	defer s.Close()
	c, e := NewClient(s.URL, t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	if e = c.ConfigurePolicy(Policy{Directory: t.TempDir(), MinInterval: time.Millisecond, PerMember: time.Millisecond, Timeout: time.Second, Cooldown: 3 * time.Second, BatchSize: 2}); e != nil {
		t.Fatal(e)
	}
	c.Batch(context.Background(), []Call{{"a", []any{}}, {"b", []any{}}})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if e = c.WaitReady(ctx); !errors.Is(e, context.DeadlineExceeded) {
		t.Fatalf("mixed batch masked cooldown: %v", e)
	}
}
func TestNetworkDiagnosticsAreSafeAndSpecific(t *testing.T) {
	if safeNetworkError(&net.DNSError{Err: "secret credentials", Name: "private", IsNotFound: true}) != "dns_not_found" {
		t.Fatal("DNS classification")
	}
	if safeNetworkError(context.DeadlineExceeded) != "deadline" {
		t.Fatal("deadline classification")
	}
	c, _ := NewClient("https://example.invalid/credential?key=secret", t.TempDir())
	c.AuditFailures = true
	c.HTTP.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, &net.DNSError{Err: "secret credentials", Name: "example.invalid", IsNotFound: true}
	})
	r := c.One(context.Background(), "eth_chainId", []any{})
	raw, e := c.Archive.Get(r.Payload)
	if e != nil {
		t.Fatal(e)
	}
	if strings.Contains(string(raw), "credential") || strings.Contains(string(raw), "secret") {
		t.Fatal("network error leaked credentials")
	}
	if !strings.Contains(string(raw), "dns_not_found") {
		t.Fatal(string(raw))
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type brokenBody struct{}

func (brokenBody) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }
func (brokenBody) Close() error             { return nil }
func TestRateLimitHeadersCoolEvenWhenBodyReadFails(t *testing.T) {
	c, _ := NewClient("https://example.invalid", t.TempDir())
	if e := c.ConfigurePolicy(Policy{Directory: t.TempDir(), MinInterval: time.Millisecond, PerMember: time.Millisecond, Timeout: time.Second, Cooldown: 3 * time.Second, BatchSize: 2}); e != nil {
		t.Fatal(e)
	}
	c.HTTP.Transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 429, Header: http.Header{"Retry-After": []string{"1"}}, Body: brokenBody{}}, nil
	})
	r := c.One(context.Background(), "eth_chainId", []any{})
	if r.Err == nil {
		t.Fatal("truncation lost")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if e := c.WaitReady(ctx); !errors.Is(e, context.DeadlineExceeded) {
		t.Fatal("429 headers lost cooldown", e)
	}
	raw, e := c.Archive.Get(r.Payload)
	if e != nil {
		t.Fatal(e)
	}
	if !strings.Contains(string(raw), "rpc_response_truncated") {
		t.Fatal(string(raw))
	}
}
