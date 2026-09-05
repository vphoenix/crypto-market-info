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
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
	Statuses    map[int]struct{}
	// RateLimitStatuses maps status codes to a minimum cooldown. A zero value
	// uses Retry-After when valid and RateLimitFallback otherwise.
	RateLimitStatuses map[int]time.Duration
	Cooldown          *RequestGate
	RateLimitFallback time.Duration
	Jitter            func(time.Duration) time.Duration
}

func DefaultHTTPRetryConfig() HTTPRetryConfig {
	return HTTPRetryConfig{MaxAttempts: 4, BaseDelay: 250 * time.Millisecond, MaxDelay: 2 * time.Second,
		Statuses:          map[int]struct{}{http.StatusInternalServerError: {}, http.StatusBadGateway: {}, http.StatusServiceUnavailable: {}, http.StatusGatewayTimeout: {}},
		RateLimitStatuses: map[int]time.Duration{http.StatusTooManyRequests: 0, http.StatusTeapot: 0},
		Cooldown:          NewRequestGate(0), RateLimitFallback: time.Minute, Jitter: AddJitter}
}

type HTTPResponse struct {
	Payload    []byte
	StatusCode int
	Header     http.Header
}

type RateLimitError struct {
	Method     string
	URL        string
	StatusCode int
	Code       int64
	Message    string
	RetryAt    time.Time
}

func (e *RateLimitError) Error() string {
	if e == nil {
		return "rate limited"
	}
	identity := http.StatusText(e.StatusCode)
	if e.Code != 0 {
		identity = fmt.Sprintf("code %d", e.Code)
	} else if identity == "" {
		identity = fmt.Sprintf("HTTP %d", e.StatusCode)
	}
	message := strings.TrimSpace(e.Message)
	if message != "" {
		identity += ": " + message
	}
	return fmt.Sprintf("%s %s rate limited (%s); retry at %s", e.Method, e.URL, identity, e.RetryAt.UTC().Format(time.RFC3339Nano))
}

func Get(ctx context.Context, client *http.Client, rawURL string, cfg HTTPRetryConfig) ([]byte, error) {
	response, err := GetResponse(ctx, client, rawURL, cfg)
	if err != nil {
		return nil, err
	}
	return response.Payload, nil
}

func GetResponse(ctx context.Context, client *http.Client, rawURL string, cfg HTTPRetryConfig) (HTTPResponse, error) {
	return doHTTP(ctx, client, http.MethodGet, rawURL, nil, cfg)
}

func PostJSON(ctx context.Context, client *http.Client, rawURL string, body []byte, cfg HTTPRetryConfig) ([]byte, error) {
	response, err := doHTTP(ctx, client, http.MethodPost, rawURL, body, cfg)
	if err != nil {
		return nil, err
	}
	return response.Payload, nil
}

func doHTTP(ctx context.Context, client *http.Client, method, rawURL string, body []byte, cfg HTTPRetryConfig) (HTTPResponse, error) {
	if client == nil {
		client = http.DefaultClient
	}
	attempts := cfg.MaxAttempts
	if attempts < 1 {
		attempts = 1
	}
	for attempt := 1; attempt <= attempts; attempt++ {
		if cfg.Cooldown != nil {
			if err := cfg.Cooldown.Wait(ctx); err != nil {
				return HTTPResponse{}, err
			}
		}
		req, err := http.NewRequestWithContext(ctx, method, rawURL, bytes.NewReader(body))
		if err != nil {
			return HTTPResponse{}, err
		}
		if method == http.MethodPost {
			req.Header.Set("Content-Type", "application/json")
		}
		response, err := client.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return HTTPResponse{}, ctx.Err()
			}
			return HTTPResponse{}, err
		}
		payload, readErr := func() ([]byte, error) { defer response.Body.Close(); return io.ReadAll(response.Body) }()
		if readErr != nil {
			return HTTPResponse{}, readErr
		}
		result := HTTPResponse{Payload: payload, StatusCode: response.StatusCode, Header: response.Header.Clone()}
		if response.StatusCode >= 200 && response.StatusCode < 300 {
			return result, nil
		}
		minimum, limited := cfg.RateLimitStatuses[response.StatusCode]
		if cfg.RateLimitStatuses == nil && (response.StatusCode == http.StatusTooManyRequests || response.StatusCode == http.StatusTeapot) {
			limited = true
		}
		if limited {
			now := time.Now()
			delay, valid := parseRetryAfter(response.Header.Get("Retry-After"), now)
			if !valid {
				delay = cfg.RateLimitFallback
				if delay <= 0 {
					delay = time.Minute
				}
			}
			if delay < minimum {
				delay = minimum
			}
			if cfg.Cooldown != nil {
				cfg.Cooldown.Cooldown(delay)
			}
			return result, &RateLimitError{Method: method, URL: rawURL, StatusCode: response.StatusCode, Message: truncate(payload, 512), RetryAt: now.Add(delay).UTC()}
		}
		_, retry := cfg.Statuses[response.StatusCode]
		if !retry || attempt == attempts {
			return result, fmt.Errorf("%s %s returned %s after %d attempt(s): %s", method, rawURL, response.Status, attempt, truncate(payload, 512))
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
			return HTTPResponse{}, ctx.Err()
		case <-timer.C:
		}
	}
	return HTTPResponse{}, fmt.Errorf("%s %s exhausted retries", method, rawURL)
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
