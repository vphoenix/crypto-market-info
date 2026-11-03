// Package hyperliquid reads Hyperliquid public info endpoints. It only parses
// the source protocol; analysis rules belong elsewhere.
package hyperliquid

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/exchange"
	"github.com/vphoenix/crypto-market-info/internal/perpcontext"
	marketyield "github.com/vphoenix/crypto-market-info/internal/yield"
)

const DefaultInfoURL = "https://api.hyperliquid.xyz/info"

type Client struct {
	InfoURL string
	HTTP    *http.Client
	Retry   exchange.HTTPRetryConfig
}

func NewClient(infoURL string) *Client {
	if strings.TrimSpace(infoURL) == "" {
		infoURL = DefaultInfoURL
	}
	return &Client{InfoURL: infoURL, HTTP: &http.Client{Timeout: 20 * time.Second}, Retry: exchange.DefaultHTTPRetryConfig()}
}

// ContextCollector reads metaAndAssetCtxs for one dex ("" is the default
// validator-operated dex). The source carries no timestamp or block, so
// observation_time is the local time the complete response was received.
type ContextCollector struct {
	Client *Client
	Dex    string
	Now    func() time.Time
}

func (c *ContextCollector) Collect(ctx context.Context) (perpcontext.Batch, error) {
	if c == nil || c.Client == nil || c.Client.InfoURL == "" {
		return perpcontext.Batch{}, fmt.Errorf("Hyperliquid context collector requires a client")
	}
	body, err := json.Marshal(map[string]string{"type": "metaAndAssetCtxs", "dex": c.Dex})
	if err != nil {
		return perpcontext.Batch{}, err
	}
	raw, err := exchange.PostJSON(ctx, c.Client.HTTP, c.Client.InfoURL, body, c.Client.Retry)
	if err != nil {
		return perpcontext.Batch{}, fmt.Errorf("Hyperliquid metaAndAssetCtxs dex %q: %w", c.Dex, err)
	}
	now := c.Now
	if now == nil {
		now = time.Now
	}
	return ParseAssetContexts(raw, c.Dex, time.UnixMilli(now().UnixMilli()).UTC())
}

// ParseAssetContexts converts one complete response body: a two-element array
// of meta (with universe) and per-asset contexts in the same order. Delisted
// assets are counted in UniverseCount but not stored. Unknown extra fields are
// covered by the payload hash but not stored.
func ParseAssetContexts(raw []byte, dex string, collected time.Time) (perpcontext.Batch, error) {
	var envelope []json.RawMessage
	if err := marketyield.DecodeJSON(raw, &envelope); err != nil {
		return perpcontext.Batch{}, fmt.Errorf("decode metaAndAssetCtxs: %w", err)
	}
	if len(envelope) != 2 {
		return perpcontext.Batch{}, fmt.Errorf("metaAndAssetCtxs has %d elements, expected 2", len(envelope))
	}
	var meta struct {
		Universe []map[string]json.RawMessage `json:"universe"`
	}
	var contexts []map[string]json.RawMessage
	if err := marketyield.DecodeJSON(envelope[0], &meta); err != nil {
		return perpcontext.Batch{}, fmt.Errorf("decode meta: %w", err)
	}
	if err := marketyield.DecodeJSON(envelope[1], &contexts); err != nil {
		return perpcontext.Batch{}, fmt.Errorf("decode asset contexts: %w", err)
	}
	if len(meta.Universe) == 0 || len(meta.Universe) > 65535 || len(meta.Universe) != len(contexts) {
		return perpcontext.Batch{}, fmt.Errorf("universe has %d assets and contexts %d", len(meta.Universe), len(contexts))
	}
	digest := sha256.Sum256(raw)
	batch := perpcontext.Batch{Dex: dex, CollectedAt: collected, UniverseCount: uint16(len(meta.Universe))}
	for index, asset := range meta.Universe {
		delisted, err := optionalBool(asset, "isDelisted")
		if err != nil {
			return perpcontext.Batch{}, fmt.Errorf("universe[%d]: %w", index, err)
		}
		if delisted {
			continue
		}
		o, err := parseAsset(asset, contexts[index])
		if err != nil {
			return perpcontext.Batch{}, fmt.Errorf("universe[%d]: %w", index, err)
		}
		o.Dex, o.ObservationTime, o.UniverseCount, o.PayloadHash = dex, collected, batch.UniverseCount, string(digest[:])
		batch.Assets = append(batch.Assets, o)
	}
	for i := range batch.Assets {
		batch.Assets[i].BatchAssetCount = uint16(len(batch.Assets))
	}
	return batch, batch.Validate()
}

func parseAsset(asset, ctx map[string]json.RawMessage) (perpcontext.Observation, error) {
	var o perpcontext.Observation
	var err error
	if o.Coin, err = jsonString(asset, "name"); err != nil {
		return o, err
	}
	leverage, err := jsonInt(asset, "maxLeverage", 65535)
	if err != nil {
		return o, err
	}
	decimals, err := jsonInt(asset, "szDecimals", 18)
	if err != nil {
		return o, err
	}
	isolated, err := optionalBool(asset, "onlyIsolated")
	if err != nil {
		return o, err
	}
	o.MaxLeverage, o.SzDecimals = uint16(leverage), uint8(decimals)
	if isolated {
		o.OnlyIsolated = 1
	}
	for _, f := range []struct {
		key    string
		target *decimal.Decimal
	}{
		{"markPx", &o.MarkPx}, {"oraclePx", &o.OraclePx}, {"prevDayPx", &o.PrevDayPx},
		{"funding", &o.FundingRateHourly}, {"openInterest", &o.OpenInterest},
		{"dayNtlVlm", &o.DayNotionalVolume}, {"dayBaseVlm", &o.DayBaseVolume},
	} {
		if *f.target, err = jsonDecimal(ctx, f.key); err != nil {
			return o, err
		}
	}
	// The book fields are all present or all null; a mix is malformed.
	nulls := 0
	for _, key := range []string{"midPx", "premium", "impactPxs"} {
		value, ok := ctx[key]
		if !ok {
			return o, fmt.Errorf("missing %s", key)
		}
		if string(value) == "null" {
			nulls++
		}
	}
	switch nulls {
	case 3:
		return o, nil
	case 0:
	default:
		return o, fmt.Errorf("midPx, premium and impactPxs must be all present or all null")
	}
	if o.MidPx, err = jsonDecimal(ctx, "midPx"); err != nil {
		return o, err
	}
	if o.Premium, err = jsonDecimal(ctx, "premium"); err != nil {
		return o, err
	}
	var impact []string
	if err = json.Unmarshal(ctx["impactPxs"], &impact); err != nil || len(impact) != 2 {
		return o, fmt.Errorf("impactPxs must be two decimal strings")
	}
	if o.ImpactBidPx, err = parseDecimal("impactPxs[0]", impact[0]); err != nil {
		return o, err
	}
	if o.ImpactAskPx, err = parseDecimal("impactPxs[1]", impact[1]); err != nil {
		return o, err
	}
	o.BookValid = 1
	return o, nil
}

func jsonString(item map[string]json.RawMessage, key string) (string, error) {
	raw, ok := item[key]
	if !ok {
		return "", fmt.Errorf("missing %s", key)
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", fmt.Errorf("%s must be a string", key)
	}
	return value, nil
}

func jsonDecimal(item map[string]json.RawMessage, key string) (decimal.Decimal, error) {
	value, err := jsonString(item, key)
	if err != nil {
		return decimal.Decimal{}, err
	}
	return parseDecimal(key, value)
}

// parseDecimal accepts only plain decimal notation; exponents, signs other
// than a leading minus and empty strings are rejected rather than coerced.
func parseDecimal(key, value string) (decimal.Decimal, error) {
	digits := strings.TrimPrefix(value, "-")
	whole, frac, _ := strings.Cut(digits, ".")
	if whole == "" || strings.Trim(whole, "0123456789") != "" || strings.Trim(frac, "0123456789") != "" || strings.HasSuffix(value, ".") {
		return decimal.Decimal{}, fmt.Errorf("%s %q is not a plain decimal", key, value)
	}
	return decimal.RequireFromString(value), nil
}

func jsonInt(item map[string]json.RawMessage, key string, max int64) (int64, error) {
	raw, ok := item[key]
	if !ok {
		return 0, fmt.Errorf("missing %s", key)
	}
	value, err := strconv.ParseInt(string(raw), 10, 64)
	if err != nil || value < 0 || value > max {
		return 0, fmt.Errorf("%s must be an integer in [0,%d]", key, max)
	}
	return value, nil
}

func optionalBool(item map[string]json.RawMessage, key string) (bool, error) {
	raw, ok := item[key]
	if !ok {
		return false, nil
	}
	var value bool
	if err := json.Unmarshal(raw, &value); err != nil {
		return false, fmt.Errorf("%s must be a boolean", key)
	}
	return value, nil
}
