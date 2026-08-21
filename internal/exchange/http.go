package exchange

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type HTTPRetryConfig struct {
	MaxAttempts       int
	BaseDelay         time.Duration
	MaxDelay          time.Duration
	Statuses          map[int]struct{}
	Cooldown          *RequestGate
	RateLimitFallback time.Duration
	Jitter            func(time.Duration) time.Duration
}

func DefaultHTTPRetryConfig() HTTPRetryConfig {
	return HTTPRetryConfig{MaxAttempts: 4, BaseDelay: 250 * time.Millisecond, MaxDelay: 2 * time.Second,
		Statuses: map[int]struct{}{http.StatusInternalServerError: {}, http.StatusBadGateway: {}, http.StatusServiceUnavailable: {}, http.StatusGatewayTimeout: {}},
		Cooldown: NewRequestGate(0), RateLimitFallback: time.Minute, Jitter: AddJitter}
}

func Get(ctx context.Context, client *http.Client, rawURL string, cfg HTTPRetryConfig) ([]byte, error) {
	return doHTTP(ctx, client, http.MethodGet, rawURL, nil, cfg)
}

func PostJSON(ctx context.Context, client *http.Client, rawURL string, body []byte, cfg HTTPRetryConfig) ([]byte, error) {
	return doHTTP(ctx, client, http.MethodPost, rawURL, body, cfg)
}

func doHTTP(ctx context.Context, client *http.Client, method, rawURL string, body []byte, cfg HTTPRetryConfig) ([]byte, error) {
	if client == nil {
		client = http.DefaultClient
	}
	attempts := cfg.MaxAttempts
	if attempts < 1 {
		attempts = 1
	}
	for attempt := 1; attempt <= attempts; attempt++ {
		if err := cfg.Cooldown.Wait(ctx); err != nil {
			return nil, err
		}
		req, err := http.NewRequestWithContext(ctx, method, rawURL, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		if method == http.MethodPost {
			req.Header.Set("Content-Type", "application/json")
		}
		response, err := client.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, err
		}
		payload, readErr := func() ([]byte, error) { defer response.Body.Close(); return io.ReadAll(response.Body) }()
		if readErr != nil {
			return nil, readErr
		}
		if response.StatusCode >= 200 && response.StatusCode < 300 {
			return payload, nil
		}
		if response.StatusCode == http.StatusTooManyRequests || response.StatusCode == http.StatusTeapot {
			delay, valid := parseRetryAfter(response.Header.Get("Retry-After"), time.Now())
			if !valid {
				delay = cfg.RateLimitFallback
				if delay <= 0 {
					delay = time.Minute
				}
			}
			cfg.Cooldown.Cooldown(delay)
			return nil, fmt.Errorf("%s %s returned %s after %d attempt(s); cooling down for %s: %s", method, rawURL, response.Status, attempt, delay, truncate(payload, 512))
		}
		_, retry := cfg.Statuses[response.StatusCode]
		if !retry || attempt == attempts {
			return nil, fmt.Errorf("%s %s returned %s after %d attempt(s): %s", method, rawURL, response.Status, attempt, truncate(payload, 512))
		}
		delay := cfg.BaseDelay
		for power := 1; power < attempt; power++ {
			delay *= 2
		}
		if cfg.MaxDelay > 0 && delay > cfg.MaxDelay {
			delay = cfg.MaxDelay
		}
		jitter := cfg.Jitter
		if jitter == nil {
			jitter = AddJitter
		}
		delay = jitter(delay)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	return nil, fmt.Errorf("%s %s exhausted retries", method, rawURL)
}

func parseRetryAfter(value string, now time.Time) (time.Duration, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, false
	}
	if seconds, err := strconv.ParseInt(value, 10, 64); err == nil {
		if seconds < 0 {
			return 0, false
		}
		return time.Duration(seconds) * time.Second, true
	}
	when, err := http.ParseTime(value)
	if err != nil {
		return 0, false
	}
	if !when.After(now) {
		return 0, true
	}
	return when.Sub(now), true
}

func truncate(payload []byte, limit int) string {
	if len(payload) > limit {
		payload = payload[:limit]
	}
	return string(payload)
}
