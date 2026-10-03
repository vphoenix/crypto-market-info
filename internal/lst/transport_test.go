package lst

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/dex"
)

type transportFakeClock struct {
	mu sync.Mutex
	at time.Time
}

func (c *transportFakeClock) Now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.at }
func (c *transportFakeClock) Sleep(ctx context.Context, d time.Duration) error {
	if e := ctx.Err(); e != nil {
		return e
	}
	c.mu.Lock()
	c.at = c.at.Add(d)
	c.mu.Unlock()
	return nil
}

type transportRoundTrip func(*http.Request) (*http.Response, error)

func (f transportRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func transportReply(code int, raw string) *http.Response {
	return &http.Response{StatusCode: code, Body: io.NopCloser(strings.NewReader(raw)), Header: http.Header{}}
}
func transportFixture(t *testing.T, round transportRoundTrip) (*Transport, *transportFakeClock) {
	t.Helper()
	clock := &transportFakeClock{at: time.Date(2026, 10, 2, 0, 0, 0, 0, time.UTC)}
	tr, e := NewTransport(TransportConfig{StateDir: filepath.Join(t.TempDir(), "state"), ArchiveDir: filepath.Join(t.TempDir(), "archive"), Clock: clock, HTTP: &http.Client{Transport: round}, Jitter: func() time.Duration { return 0 }})
	if e != nil {
		t.Fatal(e)
	}
	return tr, clock
}

var transportRPCBody = []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}`)

func TestTransportColdStartSteadyNoIdleBurst(t *testing.T) {
	var times []time.Time
	var clock *transportFakeClock
	tr, c := transportFixture(t, func(*http.Request) (*http.Response, error) {
		times = append(times, clock.Now())
		return transportReply(200, `{"jsonrpc":"2.0","id":1,"result":"0x1"}`), nil
	})
	clock = c
	tr.MarkInitialized()
	for i := 0; i < 70; i++ {
		if _, e := tr.Do(context.Background(), "rpc", "POST", "https://rpc.example/secret-key", transportRPCBody, "normal"); e != nil {
			t.Fatal(e)
		}
	}
	for i := 1; i < len(times); i++ {
		want := 500 * time.Millisecond
		if times[i-1].Before(tr.started.Add(time.Minute)) {
			want = 2 * time.Second
		}
		if times[i].Sub(times[i-1]) < want {
			t.Fatalf("send %d gap %v < %v", i, times[i].Sub(times[i-1]), want)
		}
	}
	first := 0
	for _, at := range times {
		if at.Before(tr.started.Add(time.Minute)) {
			first++
		}
	}
	if first > 30 {
		t.Fatalf("cold minute: %d", first)
	}
	clock.Sleep(context.Background(), time.Hour)
	for range 2 {
		tr.Do(context.Background(), "rpc", "POST", "https://rpc.example", transportRPCBody, "normal")
	}
	if times[len(times)-1].Sub(times[len(times)-2]) < 500*time.Millisecond {
		t.Fatal("idle saved up burst tokens")
	}
}

func TestTransportFundingBudgetsAndLogBudget(t *testing.T) {
	var sends []time.Time
	var clock *transportFakeClock
	tr, c := transportFixture(t, func(*http.Request) (*http.Response, error) {
		sends = append(sends, clock.Now())
		return transportReply(200, `[]`), nil
	})
	clock = c
	for range 12 {
		if _, e := tr.Do(context.Background(), "binance", "GET", "https://fapi.example/fapi/v1/fundingInfo", nil, "funding"); e != nil {
			t.Fatal(e)
		}
	}
	for i, at := range sends {
		if i > 0 && at.Sub(sends[i-1]) < 5*time.Second {
			t.Fatal("REST cadence")
		}
		for _, window := range []struct {
			d   time.Duration
			cap int
		}{{time.Minute, 2}, {5 * time.Minute, 10}} {
			count := 0
			for _, other := range sends {
				if other.After(at.Add(-window.d)) && !other.After(at) {
					count++
				}
			}
			if count > window.cap {
				t.Fatalf("%s cap: %d", window.d, count)
			}
		}
	}
	sends = nil
	logs := []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_getLogs","params":[{}]}`)
	for range 5 {
		if _, e := tr.Do(context.Background(), "rpc", "POST", "https://rpc.example", logs, "logs"); e != nil {
			t.Fatal(e)
		}
	}
	if sends[4].Sub(sends[0]) < 5*time.Minute {
		t.Fatal("watch logs exceeded 4 / 5 minutes")
	}
}

func TestTransportCooldownPersistsAndBanStops(t *testing.T) {
	calls := 0
	tr, clock := transportFixture(t, func(*http.Request) (*http.Response, error) {
		calls++
		if calls == 1 {
			r := transportReply(429, `{"msg":"rate limit"}`)
			r.Header.Set("Retry-After", "900")
			return r, nil
		}
		return transportReply(418, `{}`), nil
	})
	first, e := tr.Do(context.Background(), "rpc", "POST", "https://rpc.example/credential", transportRPCBody, "normal")
	if e == nil || first.PayloadHash == "" {
		t.Fatal("429 missing error/evidence")
	}
	at := clock.Now()
	restart, e := NewTransport(tr.cfg)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = restart.Do(context.Background(), "rpc", "POST", "https://rpc.example/credential", transportRPCBody, "normal"); e == nil {
		t.Fatal("418 not rejected")
	}
	if clock.Now().Sub(at) < 15*time.Minute {
		t.Fatal("restart lost retry-after")
	}
	again, _ := NewTransport(tr.cfg)
	if _, e = again.Do(context.Background(), "rpc", "POST", "https://rpc.example", transportRPCBody, "normal"); e == nil || calls != 2 {
		t.Fatal("ban restarted/probed", e, calls)
	}
}

func TestTransportRPCBodyLimitAndNetworkBackoff(t *testing.T) {
	calls := 0
	tr, clock := transportFixture(t, func(*http.Request) (*http.Response, error) {
		calls++
		if calls == 1 {
			return transportReply(200, `{"jsonrpc":"2.0","id":1,"error":{"code":-32005,"message":"rate limit exceeded"}}`), nil
		}
		return nil, errors.New("credential in URL must never escape")
	})
	start := clock.Now()
	if _, e := tr.Do(context.Background(), "rpc", "POST", "https://user:password@rpc.example/token?key=secret", transportRPCBody, "normal"); e == nil {
		t.Fatal("RPC200 limit not recognized")
	}
	if _, e := tr.Do(context.Background(), "rpc", "POST", "https://rpc.example", transportRPCBody, "normal"); e == nil || strings.Contains(e.Error(), "credential") {
		t.Fatal("raw network error escaped")
	}
	if clock.Now().Sub(start) < 5*time.Minute {
		t.Fatal("RPC200 cooldown missing")
	}
	before := clock.Now()
	tr.Do(context.Background(), "rpc", "POST", "https://rpc.example", transportRPCBody, "normal")
	if clock.Now().Sub(before) < 15*time.Second {
		t.Fatal("network backoff missing")
	}
}

func TestTransportReadOnlySingleRPCAndEvidenceRedaction(t *testing.T) {
	calls := 0
	tr, _ := transportFixture(t, func(*http.Request) (*http.Response, error) {
		calls++
		return transportReply(200, `{"result":"0x1"}`), nil
	})
	for _, body := range []string{`[{"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}]`, `{"jsonrpc":"2.0","id":1,"method":"eth_sendRawTransaction","params":[]}`} {
		if _, e := tr.Do(context.Background(), "rpc", "POST", "https://rpc.example", []byte(body), "normal"); e == nil {
			t.Fatal("unsafe rpc accepted")
		}
	}
	if calls != 0 {
		t.Fatal("invalid request was sent")
	}
	r, e := tr.Do(context.Background(), "rpc", "POST", "https://name:password@rpc.example/private-token?api-key=secret", transportRPCBody, "normal")
	if e != nil {
		t.Fatal(e)
	}
	h, e := dex.ParseHash(r.RequestHash)
	if e != nil {
		t.Fatal(e)
	}
	raw, e := tr.archive.Get(h)
	if e != nil {
		t.Fatal(e)
	}
	for _, secret := range []string{"password", "private-token", "api-key", "secret"} {
		if strings.Contains(string(raw), secret) {
			t.Fatalf("secret %s in evidence", secret)
		}
	}
}

func TestTransportCorruptStateFailsClosedAndRedirectsNotFollowed(t *testing.T) {
	calls := 0
	tr, _ := transportFixture(t, func(*http.Request) (*http.Response, error) {
		calls++
		r := transportReply(302, `{}`)
		r.Header.Set("Location", "https://other.example")
		return r, nil
	})
	if _, e := tr.Do(context.Background(), "rpc", "POST", "https://rpc.example", transportRPCBody, "normal"); e == nil || calls != 1 {
		t.Fatal("redirect followed")
	}
	g, _ := tr.gate("rpc", "https://rpc.example")
	if e := os.WriteFile(g.file, []byte("broken"), 0600); e != nil {
		t.Fatal(e)
	}
	restart, _ := NewTransport(tr.cfg)
	if _, e := restart.Do(context.Background(), "rpc", "POST", "https://rpc.example", transportRPCBody, "normal"); e == nil || calls != 1 {
		t.Fatal("corrupt state permitted send")
	}
}

func TestTransportDynamicWeightThresholdAndRangeError(t *testing.T) {
	tr, clock := transportFixture(t, func(*http.Request) (*http.Response, error) {
		r := transportReply(200, `{}`)
		r.Header.Set("X-Mbx-Used-Weight-1m", "500")
		return r, nil
	})
	if e := tr.SetBinanceWeightLimit("https://fapi.example", 1000); e != nil {
		t.Fatal(e)
	}
	before := clock.Now()
	if _, e := tr.Do(context.Background(), "binance", "GET", "https://fapi.example/fapi/v1/exchangeInfo", nil, "normal"); e != nil {
		t.Fatal(e)
	}
	ready, e := tr.ReadyAt("binance", "https://fapi.example")
	if e != nil || ready.Sub(before) < time.Minute {
		t.Fatal("dynamic 50% threshold ignored")
	}
	for _, raw := range []string{`{"error":{"code":-32005,"message":"query returned more than 10000 results"}}`, `{"error":{"code":-32005,"message":"block range exceeds limit"}}`} {
		limited, disabled := responseLimit([]byte(raw))
		if limited || disabled {
			t.Fatal("range error classified as rate limit")
		}
	}
}

func TestTransportSlowResponseStartsFreshGap(t *testing.T) {
	var clock *transportFakeClock
	var sent []time.Time
	tr, c := transportFixture(t, func(*http.Request) (*http.Response, error) {
		sent = append(sent, clock.Now())
		clock.Sleep(context.Background(), 3*time.Second)
		return transportReply(200, `{"jsonrpc":"2.0","id":1,"result":"0x1"}`), nil
	})
	clock = c
	for range 2 {
		if _, e := tr.Do(context.Background(), "rpc", "POST", "https://rpc.example", transportRPCBody, "normal"); e != nil {
			t.Fatal(e)
		}
	}
	if sent[1].Sub(sent[0]) < 5*time.Second {
		t.Fatal("slow response consumed the next send gap")
	}
	restart, e := NewTransport(tr.cfg)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = restart.Do(context.Background(), "rpc", "POST", "https://rpc.example", transportRPCBody, "normal"); e != nil {
		t.Fatal(e)
	}
	if sent[2].Sub(sent[1]) < 5*time.Second {
		t.Fatal("restart lost post-response gap")
	}
}

func TestTransportRestartInFlightWaitsStartupGap(t *testing.T) {
	for _, tc := range []struct {
		source, method, endpoint string
		body                     []byte
		gap                      time.Duration
	}{
		{"rpc", "POST", "https://rpc.example", transportRPCBody, 2 * time.Second},
		{"binance", "GET", "https://fapi.example/fapi/v1/exchangeInfo", nil, 5 * time.Second},
	} {
		t.Run(tc.source, func(t *testing.T) {
			var clock *transportFakeClock
			var sent time.Time
			tr, c := transportFixture(t, func(*http.Request) (*http.Response, error) { sent = clock.Now(); return transportReply(200, `{}`), nil })
			clock = c
			g, e := tr.gate(tc.source, tc.endpoint)
			if e != nil {
				t.Fatal(e)
			}
			// Simulate a crashed process whose durable reservation is about to
			// expire while its actual HTTP request could still be in flight.
			g.state.NextAt = clock.Now().Add(time.Millisecond)
			if e = saveGate(g); e != nil {
				t.Fatal(e)
			}
			restart, e := NewTransport(tr.cfg)
			if e != nil {
				t.Fatal(e)
			}
			if _, e = restart.Do(context.Background(), tc.source, tc.method, tc.endpoint, tc.body, "normal"); e != nil {
				t.Fatal(e)
			}
			if sent.Sub(restart.started) < tc.gap {
				t.Fatalf("restart send gap %v < %v", sent.Sub(restart.started), tc.gap)
			}
		})
	}
}

func TestTransportNearDeadlineDoesNotSendOrPenalize(t *testing.T) {
	calls := 0
	tr, _ := transportFixture(t, func(*http.Request) (*http.Response, error) { calls++; return transportReply(200, `{}`), nil })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, e := tr.Do(ctx, "rpc", "POST", "https://rpc.example", transportRPCBody, "normal"); e == nil || e.Error() != "round_budget_exhausted" {
		t.Fatal("near deadline not rejected", e)
	}
	g, e := tr.gate("rpc", "https://rpc.example")
	if e != nil {
		t.Fatal(e)
	}
	if calls != 0 || g.state.Failures != 0 || !g.state.CooldownUntil.IsZero() {
		t.Fatal("near deadline sent or cooled source", calls, g.state.Failures)
	}
}
