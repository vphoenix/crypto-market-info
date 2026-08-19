package exchange

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

type HTTPRetryConfig struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
	Statuses    map[int]struct{}
}

func DefaultHTTPRetryConfig() HTTPRetryConfig {
	return HTTPRetryConfig{MaxAttempts: 4, BaseDelay: 250 * time.Millisecond, MaxDelay: 2 * time.Second,
		Statuses: map[int]struct{}{http.StatusTooManyRequests: {}, http.StatusInternalServerError: {}, http.StatusBadGateway: {}, http.StatusServiceUnavailable: {}, http.StatusGatewayTimeout: {}}}
}

func Get(ctx context.Context, client *http.Client, rawURL string, cfg HTTPRetryConfig) ([]byte, error) {
	if client == nil {
		client = http.DefaultClient
	}
	attempts := cfg.MaxAttempts
	if attempts < 1 {
		attempts = 1
	}
	for attempt := 1; attempt <= attempts; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
		if err != nil {
			return nil, err
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
		_, retry := cfg.Statuses[response.StatusCode]
		if !retry || attempt == attempts {
			return nil, fmt.Errorf("GET %s returned %s after %d attempt(s): %s", rawURL, response.Status, attempt, truncate(payload, 512))
		}
		delay := cfg.BaseDelay
		for power := 1; power < attempt; power++ {
			delay *= 2
		}
		if cfg.MaxDelay > 0 && delay > cfg.MaxDelay {
			delay = cfg.MaxDelay
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
	return nil, fmt.Errorf("GET %s exhausted retries", rawURL)
}

func truncate(payload []byte, limit int) string {
	if len(payload) > limit {
		payload = payload[:limit]
	}
	return string(payload)
}
