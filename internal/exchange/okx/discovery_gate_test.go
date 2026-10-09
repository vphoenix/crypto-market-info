package okx

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/exchange"
)

func TestDiscoveryDiscountConcurrentWorkersAndRetryStayPaced(t *testing.T) {
	var mu sync.Mutex
	var sends []time.Time
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		sends = append(sends, time.Now())
		n := len(sends)
		mu.Unlock()
		if n == 1 {
			http.Error(w, "transient", http.StatusServiceUnavailable)
			return
		}
		fmt.Fprint(w, `{"code":"0","data":[]}`)
	}))
	defer server.Close()
	c := NewClient()
	c.BaseURL = server.URL
	c.Retry.BaseDelay = time.Millisecond
	c.Retry.Jitter = func(d time.Duration) time.Duration { return d }
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	errs := make(chan error, 3)
	for n := 0; n < 3; n++ {
		go func() {
			_, err := c.DiscoveryPublic(ctx, "/api/v5/public/discount-rate-interest-free-quota", nil)
			errs <- err
		}()
	}
	for n := 0; n < 3; n++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(sends) != 4 {
		t.Fatalf("physical sends=%d, expected three workers plus one retry", len(sends))
	}
	for n := 1; n < len(sends); n++ {
		if gap := sends[n].Sub(sends[n-1]); gap < time.Second {
			t.Fatalf("concurrent request or retry bypassed discount pacing: %s", gap)
		}
	}
}

type discoveryClockGate struct {
	last atomic.Int64
}

func (g *discoveryClockGate) Wait(ctx context.Context) error {
	if !exchange.Wait(ctx, 5*time.Millisecond) {
		return ctx.Err()
	}
	g.last.Store(time.Now().UTC().UnixMilli())
	return nil
}

func TestDiscoveryRetrySourceClockFollowsPhysicalGate(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			http.Error(w, "retry", http.StatusServiceUnavailable)
			return
		}
		fmt.Fprint(w, `{"code":"0","data":[]}`)
	}))
	defer server.Close()
	c := NewClient()
	c.BaseURL = server.URL
	g := &discoveryClockGate{}
	c.riskTierGate = g
	var hooks atomic.Int32
	c.Retry.BeforeRequest = func(context.Context) error { hooks.Add(1); return nil }
	got, err := c.DiscoveryPublic(context.Background(), "/api/v5/public/position-tiers", nil)
	if err != nil {
		t.Fatal(err)
	}
	if requests.Load() != 2 || hooks.Load() != 2 || got.Source.SourceMS < g.last.Load() || got.Source.ObservedMS < got.Source.SourceMS {
		t.Fatalf("wrong retry hook or provenance time: %+v requests=%d hooks=%d", got.Source, requests.Load(), hooks.Load())
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = c.DiscoveryPublic(cancelled, "/api/v5/public/position-tiers", nil); err == nil || requests.Load() != 2 {
		t.Fatal("cancelled wait sent another request")
	}
}
