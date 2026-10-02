package across

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
)

func throttleReader(t *testing.T, interval time.Duration, roundTrip func(*http.Request) (*http.Response, error)) *Reader {
	t.Helper()
	rpc, e := ethereum.NewClient("https://test.invalid", t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	rpc.HTTP.Transport = protocolTransport(roundTrip)
	r := NewReader(rpc, ChainConfig{ChainID: 8453})
	r.RPCMinInterval = interval
	return r
}
func throttleHeaderResponse(req *http.Request) (*http.Response, error) {
	var calls []struct {
		ID     uint64            `json:"id"`
		Params []json.RawMessage `json:"params"`
	}
	if e := json.NewDecoder(req.Body).Decode(&calls); e != nil {
		return nil, e
	}
	if len(calls) != 1 {
		return nil, errors.New("throttle_sent_batch")
	}
	var tag string
	if e := json.Unmarshal(calls[0].Params[0], &tag); e != nil {
		return nil, e
	}
	n, e := q64(tag)
	if e != nil {
		return nil, e
	}
	raw, e := json.Marshal([]any{map[string]any{"jsonrpc": "2.0", "id": calls[0].ID, "result": protocolHeader(n, strings.Repeat("h", 32), time.Unix(1000, 0).UTC())}})
	if e != nil {
		return nil, e
	}
	return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(bytes.NewReader(raw))}, nil
}
func TestRPCThrottleCoordinatesConcurrentHeadersAndEvidence(t *testing.T) {
	interval := 15 * time.Millisecond
	var mu sync.Mutex
	starts := []time.Time{}
	var active, maxActive atomic.Int32
	r := throttleReader(t, interval, func(req *http.Request) (*http.Response, error) {
		n := active.Add(1)
		defer active.Add(-1)
		for old := maxActive.Load(); n > old && !maxActive.CompareAndSwap(old, n); old = maxActive.Load() {
		}
		mu.Lock()
		starts = append(starts, time.Now())
		mu.Unlock()
		return throttleHeaderResponse(req)
	})
	var wg sync.WaitGroup
	errs := make(chan error, 3)
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			heads, e := r.Headers(context.Background(), []uint64{uint64(10*i + 1), uint64(10*i + 2), uint64(10*i + 3)})
			if e == nil && len(heads) != 3 {
				e = errors.New("lost_headers")
			}
			errs <- e
		}(i)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
	if len(starts) != 9 || r.Used != 9 || len(r.Members) != 9 || maxActive.Load() > 2 {
		t.Fatalf("requests=%d used=%d evidence=%d max_active=%d", len(starts), r.Used, len(r.Members), maxActive.Load())
	}
	for i := 1; i < len(starts); i++ {
		if gap := starts[i].Sub(starts[i-1]); gap < interval {
			t.Fatalf("request %d gap %s below %s", i, gap, interval)
		}
	}
	if _, e := r.proof(); e != nil {
		t.Fatal(e)
	}
}
func TestRPCThrottleCancellationDoesNotSend(t *testing.T) {
	var sent atomic.Int32
	r := throttleReader(t, 500*time.Millisecond, func(req *http.Request) (*http.Response, error) { sent.Add(1); return throttleHeaderResponse(req) })
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, e := r.Header(ctx, "0x1"); !errors.Is(e, context.Canceled) {
		t.Fatalf("got %v", e)
	}
	if sent.Load() != 0 || r.Used != 0 {
		t.Fatal("pre-canceled request was sent")
	}
	if _, e := r.Header(context.Background(), "0x1"); e != nil {
		t.Fatal(e)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, e := r.Headers(ctx, []uint64{2, 3}); !errors.Is(e, context.DeadlineExceeded) {
		t.Fatalf("got %v", e)
	}
	if elapsed := time.Since(start); elapsed > 250*time.Millisecond {
		t.Fatalf("cancellation waited for cooldown %s", elapsed)
	}
	if sent.Load() != 1 || r.Used != 1 || len(r.Members) != 1 {
		t.Fatal("canceled cooldown issued a request or phantom evidence")
	}
}
func TestRPCThrottleCancellationWhileAnotherRequestActive(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	var sent atomic.Int32
	r := throttleReader(t, time.Millisecond, func(req *http.Request) (*http.Response, error) {
		sent.Add(1)
		once.Do(func() { close(started) })
		select {
		case <-release:
		case <-req.Context().Done():
			return nil, req.Context().Err()
		}
		return throttleHeaderResponse(req)
	})
	done := make(chan error, 1)
	go func() { _, e := r.Header(context.Background(), "0x1"); done <- e }()
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, e := r.Headers(ctx, []uint64{2, 3})
	elapsed := time.Since(start)
	close(release)
	firstErr := <-done
	if !errors.Is(e, context.DeadlineExceeded) || firstErr != nil {
		t.Fatalf("waiting=%v active=%v", e, firstErr)
	}
	if elapsed > 250*time.Millisecond || sent.Load() != 1 || r.Used != 1 {
		t.Fatalf("blocked cancellation or extra request: elapsed=%s sent=%d used=%d", elapsed, sent.Load(), r.Used)
	}
}
func TestRPCThrottleAbortsBatchAfterSourceFailure(t *testing.T) {
	for _, body := range []string{`[{"jsonrpc":"2.0","id":1,"error":{"code":-32016,"message":"over rate limit"}}]`, `[{"jsonrpc":"2.0","id":1,"result":null}]`} {
		t.Run(body, func(t *testing.T) {
			var sent atomic.Int32
			r := throttleReader(t, 500*time.Millisecond, func(req *http.Request) (*http.Response, error) {
				sent.Add(1)
				return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(body))}, nil
			})
			start := time.Now()
			heads, e := r.Headers(context.Background(), make([]uint64, 40))
			if e == nil || heads != nil || sent.Load() != 1 || r.Used != 1 || len(r.Members) != 1 {
				t.Fatalf("batch continued after failure: sent=%d used=%d evidence=%d err=%v", sent.Load(), r.Used, len(r.Members), e)
			}
			if time.Since(start) > 250*time.Millisecond {
				t.Fatal("failed batch waited for skipped members")
			}
		})
	}
}
func TestRPCThrottleHeaderBudget(t *testing.T) {
	r := NewReader(nil, ChainConfig{})
	if r.headerBudget(65) != 5*time.Second {
		t.Fatal("default budget changed")
	}
	r.RPCMinInterval = 500 * time.Millisecond
	if got := r.headerBudget(65); got != 357500*time.Millisecond {
		t.Fatalf("throttled budget %s", got)
	}
	r.RPCMinInterval = time.Duration(1<<63 - 1)
	if got := r.headerBudget(512); got != time.Duration(1<<63-1) {
		t.Fatal("duration overflow")
	}
}
