package okx

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/exchange"
)

const sharedDiscountFixture = `{"code":"0","data":[{"ccy":"USDT","collateralRestrict":false,"details":[{"minAmt":"0","maxAmt":"1000000","discountRate":"1","liqPenaltyRate":"0.01"}]}]}`

func TestSharedDiscountConcurrentCallersKeepOriginalClockAndOwnBytes(t *testing.T) {
	var requests atomic.Int32
	started, release := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			close(started)
		}
		<-release
		fmt.Fprint(w, sharedDiscountFixture)
	}))
	defer server.Close()
	c := NewClient()
	c.BaseURL = server.URL
	c.discountGate = exchange.NewRequestGate(time.Millisecond)
	var wg sync.WaitGroup
	results := make([]PublicResponse, 8)
	errs := make([]error, 8)
	for i := range results {
		wg.Add(1)
		go func(i int) { defer wg.Done(); results[i], errs[i] = c.discoveryDiscount(context.Background()) }(i)
	}
	<-started
	close(release)
	wg.Wait()
	if requests.Load() != 1 {
		t.Fatal("shared request duplicated", requests.Load())
	}
	for i := range results {
		if errs[i] != nil || results[i].Source != results[0].Source {
			t.Fatal("source identity changed", errs[i])
		}
	}
	results[0].Data[0] = '!'
	later, err := c.discoveryDiscount(context.Background())
	if err != nil || later.Data[0] == '!' || later.Source != results[1].Source || requests.Load() != 1 {
		t.Fatal("cache alias or clock refresh", err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.discoveryDiscount(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled cache hit succeeded", err)
	}
}
func TestSharedDiscountExpiryFailureAndInvalidPayload(t *testing.T) {
	var requests atomic.Int32
	var failed atomic.Bool
	var malformed atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if failed.Load() {
			http.Error(w, "unavailable", 503)
			return
		}
		if malformed.Load() {
			fmt.Fprint(w, `{"code":"0","data":[]}`)
			return
		}
		fmt.Fprint(w, sharedDiscountFixture)
	}))
	defer server.Close()
	c := NewClient()
	c.BaseURL = server.URL
	c.discountGate = exchange.NewRequestGate(time.Millisecond)
	c.Retry.MaxAttempts = 1
	if _, err := c.discoveryDiscount(context.Background()); err != nil {
		t.Fatal(err)
	}
	c.discountResponses.mu.Lock()
	c.discountResponses.value.Source.SourceMS = time.Now().Add(-2 * time.Minute).UnixMilli()
	c.discountResponses.mu.Unlock()
	failed.Store(true)
	if r, err := c.discoveryDiscount(context.Background()); err == nil || len(r.Data) != 0 {
		t.Fatal("expired source served after failure")
	}
	failed.Store(false)
	malformed.Store(true)
	if _, err := c.discoveryDiscount(context.Background()); err == nil {
		t.Fatal("empty discount catalog shared")
	}
	malformed.Store(false)
	if _, err := c.discoveryDiscount(context.Background()); err != nil || requests.Load() != 4 {
		t.Fatal("source failed to recover", err, requests.Load())
	}
}
func TestSharedDiscountWaiterCancellationDoesNotCancelOwner(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-release
		fmt.Fprint(w, sharedDiscountFixture)
	}))
	defer server.Close()
	c := NewClient()
	c.BaseURL = server.URL
	owner := make(chan error, 1)
	go func() { _, e := c.discoveryDiscount(context.Background()); owner <- e }()
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := c.discoveryDiscount(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	close(release)
	if err := <-owner; err != nil {
		t.Fatal("waiter cancelled shared owner", err)
	}
}
