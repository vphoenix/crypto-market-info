package bybit

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/exchange"
)

const (
	defaultRESTURL               = "https://api.bybit.com"
	defaultBodyRateLimitFallback = time.Minute
	defaultForbiddenCooldown     = 10 * time.Minute
)

type Client struct {
	HTTP                  *http.Client
	BaseURL               string
	Retry                 exchange.HTTPRetryConfig
	BodyRateLimitFallback time.Duration
	ForbiddenCooldown     time.Duration
	Logger                *slog.Logger
}

type apiEnvelope struct {
	RetCode *int64          `json:"retCode"`
	RetMsg  string          `json:"retMsg"`
	Result  json.RawMessage `json:"result"`
}

func NewClient() *Client {
	retry := exchange.DefaultHTTPRetryConfig()
	return &Client{
		HTTP:                  &http.Client{Timeout: 30 * time.Second},
		BaseURL:               defaultRESTURL,
		Retry:                 retry,
		BodyRateLimitFallback: defaultBodyRateLimitFallback,
		ForbiddenCooldown:     defaultForbiddenCooldown,
		Logger:                slog.Default(),
	}
}

func (c *Client) apiGet(ctx context.Context, path string, values url.Values, retry exchange.HTTPRetryConfig) ([]byte, error) {
	if c.HTTP == nil {
		c.HTTP = http.DefaultClient
	}
	base := c.BaseURL
	if base == "" {
		base = defaultRESTURL
	}
	endpoint := strings.TrimRight(base, "/") + path
	if len(values) > 0 {
		endpoint += "?" + values.Encode()
	}
	response, err := exchange.GetResponse(ctx, c.HTTP, endpoint, retry)
	if err != nil {
		if response.StatusCode == http.StatusForbidden && isAccessTooFrequent(response.Payload) {
			return nil, c.forbiddenRateLimitError(endpoint, response)
		}
		return nil, err
	}
	var envelope apiEnvelope
	if err = json.Unmarshal(response.Payload, &envelope); err != nil {
		return nil, fmt.Errorf("Bybit %s JSON: %w", path, err)
	}
	if envelope.RetCode == nil {
		return nil, fmt.Errorf("Bybit %s retCode is required", path)
	}
	if *envelope.RetCode == 10006 {
		return nil, c.bodyRateLimitError(endpoint, response, envelope.RetMsg)
	}
	if *envelope.RetCode != 0 {
		return nil, fmt.Errorf("Bybit %s code=%d msg=%q", path, *envelope.RetCode, envelope.RetMsg)
	}
	return response.Payload, nil
}

func isAccessTooFrequent(payload []byte) bool {
	return strings.Contains(strings.ToLower(string(bytes.TrimSpace(payload))), "access too frequent")
}

func (c *Client) forbiddenRateLimitError(endpoint string, response exchange.HTTPResponse) error {
	cooldown := c.ForbiddenCooldown
	if cooldown <= 0 {
		cooldown = defaultForbiddenCooldown
	}
	retryAt := time.Now().Add(cooldown).UTC()
	if c.Retry.Cooldown != nil {
		c.Retry.Cooldown.Cooldown(time.Until(retryAt))
	}
	message := strings.TrimSpace(string(response.Payload))
	if len(message) > 512 {
		message = message[:512] + "..."
	}
	return &exchange.RateLimitError{
		Method:     http.MethodGet,
		URL:        endpoint,
		StatusCode: response.StatusCode,
		Message:    message,
		RetryAt:    retryAt,
	}
}

func (c *Client) bodyRateLimitError(endpoint string, response exchange.HTTPResponse, message string) error {
	now := time.Now()
	fallback := c.BodyRateLimitFallback
	if fallback <= 0 {
		fallback = defaultBodyRateLimitFallback
	}
	retryAt := now.Add(fallback)
	if raw := strings.TrimSpace(response.Header.Get("X-Bapi-Limit-Reset-Timestamp")); raw != "" {
		if milliseconds, err := strconv.ParseInt(raw, 10, 64); err == nil {
			candidate := time.UnixMilli(milliseconds)
			if candidate.After(now) {
				retryAt = candidate
			}
		}
	}
	if c.Retry.Cooldown != nil {
		c.Retry.Cooldown.Cooldown(time.Until(retryAt))
	}
	return &exchange.RateLimitError{Method: http.MethodGet, URL: endpoint, StatusCode: response.StatusCode, Code: 10006, Message: message, RetryAt: retryAt.UTC()}
}

func (c *Client) metadataGet(ctx context.Context, values url.Values) ([]byte, error) {
	logger := c.Logger
	if logger == nil {
		logger = slog.Default()
	}
	for {
		payload, err := c.apiGet(ctx, "/v5/market/instruments-info", values, c.Retry)
		if err == nil {
			return payload, nil
		}
		var limited *exchange.RateLimitError
		if !errors.As(err, &limited) {
			return nil, err
		}
		logger.Warn("Bybit metadata rate limited; waiting before retry", "retry_at", limited.RetryAt, "error", err)
		if c.Retry.Cooldown != nil {
			if waitErr := c.Retry.Cooldown.Wait(ctx); waitErr != nil {
				return nil, waitErr
			}
			continue
		}
		delay := time.Until(limited.RetryAt)
		if delay > 0 && !exchange.Wait(ctx, delay) {
			return nil, ctx.Err()
		}
	}
}

func decodeResult(payload []byte, endpoint string) (json.RawMessage, error) {
	var envelope apiEnvelope
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return nil, fmt.Errorf("Bybit %s JSON: %w", endpoint, err)
	}
	if envelope.RetCode == nil {
		return nil, fmt.Errorf("Bybit %s retCode is required", endpoint)
	}
	if *envelope.RetCode != 0 {
		return nil, fmt.Errorf("Bybit %s code=%d msg=%q", endpoint, *envelope.RetCode, envelope.RetMsg)
	}
	trimmed := bytes.TrimSpace(envelope.Result)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil, fmt.Errorf("Bybit %s result is required", endpoint)
	}
	return envelope.Result, nil
}
