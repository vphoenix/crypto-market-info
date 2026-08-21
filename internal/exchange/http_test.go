package exchange

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestRateLimitResponseStartsSharedCooldownWithoutRetry(t *testing.T) {
	for _, status := range []int{http.StatusTooManyRequests, http.StatusTeapot} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				requests.Add(1)
				w.Header().Set("Retry-After", "1")
				w.WriteHeader(status)
			}))
			defer server.Close()

			cfg := DefaultHTTPRetryConfig()
			if _, err := Get(context.Background(), server.Client(), server.URL, cfg); err == nil {
				t.Fatal("rate-limit response unexpectedly succeeded")
			}
			if got := requests.Load(); got != 1 {
				t.Fatalf("rate-limit response was retried: requests=%d", got)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			if _, err := Get(ctx, server.Client(), server.URL, cfg); err != context.DeadlineExceeded {
				t.Fatalf("second request error=%v, want deadline exceeded", err)
			}
			if got := requests.Load(); got != 1 {
				t.Fatalf("shared cooldown allowed another request: requests=%d", got)
			}
		})
	}
}

func TestServerErrorRetryUsesConfiguredJitter(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if requests.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ok"))
	}))
	defer server.Close()

	var jitterCalls atomic.Int32
	cfg := DefaultHTTPRetryConfig()
	cfg.MaxAttempts = 2
	cfg.BaseDelay = time.Millisecond
	cfg.Jitter = func(delay time.Duration) time.Duration {
		jitterCalls.Add(1)
		return delay
	}
	payload, err := Get(context.Background(), server.Client(), server.URL, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if string(payload) != "ok" || requests.Load() != 2 || jitterCalls.Load() != 1 {
		t.Fatalf("payload=%q requests=%d jitter_calls=%d", payload, requests.Load(), jitterCalls.Load())
	}
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, time.August, 20, 0, 0, 0, 0, time.UTC)
	if delay, valid := parseRetryAfter("0", now); !valid || delay != 0 {
		t.Fatalf("zero delay=(%s,%t)", delay, valid)
	}
	if delay, valid := parseRetryAfter("3", now); !valid || delay != 3*time.Second {
		t.Fatalf("seconds delay=(%s,%t)", delay, valid)
	}
	date := now.Add(5 * time.Second).Format(http.TimeFormat)
	if delay, valid := parseRetryAfter(date, now); !valid || delay != 5*time.Second {
		t.Fatalf("date delay=(%s,%t)", delay, valid)
	}
	if _, valid := parseRetryAfter("not-a-delay", now); valid {
		t.Fatal("invalid Retry-After was accepted")
	}
}
