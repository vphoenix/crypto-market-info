package okxearn

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/exchange"
	"github.com/vphoenix/crypto-market-info/internal/model"
	marketyield "github.com/vphoenix/crypto-market-info/internal/yield"
)

const (
	DefaultBaseURL = "https://www.okx.com"
	historyPath    = "/api/v5/finance/savings/lending-rate-history"
	assetKey       = "cex:okx:AVAX"
)

type Collector struct {
	BaseURL    string
	HTTPClient *http.Client
	Retry      exchange.HTTPRetryConfig
	Now        func() time.Time
}

func NewCollector(baseURL string) *Collector {
	if strings.TrimSpace(baseURL) == "" {
		baseURL = DefaultBaseURL
	}
	retry := exchange.DefaultHTTPRetryConfig()
	retry.Cooldown = exchange.NewRequestGate(time.Second)
	return &Collector{BaseURL: strings.TrimRight(baseURL, "/"), HTTPClient: &http.Client{Timeout: 20 * time.Second}, Retry: retry}
}

type historyResponse struct {
	Code string         `json:"code"`
	Data []historyPoint `json:"data"`
}

type historyPoint struct {
	Currency    string `json:"ccy"`
	Timestamp   string `json:"ts"`
	LendingRate string `json:"lendingRate"`
}

func (c *Collector) Collect(ctx context.Context) (marketyield.Batch, error) {
	if c == nil || strings.TrimSpace(c.BaseURL) == "" {
		return marketyield.Batch{}, fmt.Errorf("OKX earn collector is not configured")
	}
	now := c.Now
	if now == nil {
		now = time.Now
	}
	collected := now().UTC().Truncate(time.Millisecond)
	lower, upper := collected.Add(-7*24*time.Hour).UnixMilli(), collected.UnixMilli()
	future := collected.Add(5 * time.Minute).UnixMilli()
	// Keep the source precision until all duplicates (including page boundaries)
	// have been checked. Truncating first could hide contradictory responses.
	rates := make(map[int64]decimal.Decimal)
	var payloads []marketyield.Payload
	var after int64
	var boundaryRate decimal.Decimal
	complete := false
	for page := 1; page <= 8; page++ {
		query := url.Values{"ccy": {"AVAX"}, "limit": {"100"}}
		if after != 0 {
			query.Set("after", strconv.FormatInt(after, 10))
		}
		requestURL := c.BaseURL + historyPath + "?" + query.Encode()
		body, err := exchange.Get(ctx, c.HTTPClient, requestURL, c.Retry)
		if err != nil {
			return marketyield.Batch{}, fmt.Errorf("OKX earn page %d: %w", page, err)
		}
		payloads = append(payloads,
			marketyield.Payload{Name: fmt.Sprintf("page-%d-request", page), Body: []byte(requestURL)},
			marketyield.Payload{Name: fmt.Sprintf("page-%d-response", page), Body: body})
		var response historyResponse
		if err = marketyield.DecodeJSON(body, &response); err != nil {
			return marketyield.Batch{}, fmt.Errorf("OKX earn page %d: %w", page, err)
		}
		if response.Code != "0" || response.Data == nil || len(response.Data) > 100 {
			return marketyield.Batch{}, fmt.Errorf("OKX earn page %d has invalid code or data", page)
		}
		if len(response.Data) == 0 {
			complete = true
			break
		}
		var minimum int64
		var minimumPoint historyPoint
		for index, point := range response.Data {
			ts, err := positiveTimestamp(point.Timestamp)
			if err != nil || point.Currency != "AVAX" || ts > future {
				return marketyield.Batch{}, fmt.Errorf("OKX earn page %d point %d has invalid currency or timestamp", page, index)
			}
			// Check every row before filtering; an ignored cursor can otherwise
			// look successful merely because the response contains one old row.
			if after != 0 && ts > after {
				return marketyield.Batch{}, fmt.Errorf("OKX earn page %d contains timestamp beyond after", page)
			}
			if minimum == 0 || ts < minimum {
				minimum, minimumPoint = ts, point
			}
			inWindow := ts >= lower && ts <= upper
			if !inWindow && ts != after {
				continue // Old records may predate the lendingRate field.
			}
			rate, err := parseRate(point.LendingRate)
			if err != nil {
				return marketyield.Batch{}, fmt.Errorf("OKX earn page %d point %d: %w", page, index, err)
			}
			if ts == after && !rate.Equal(boundaryRate) {
				return marketyield.Batch{}, fmt.Errorf("OKX earn page %d has conflicting boundary rate", page)
			}
			if !inWindow {
				continue
			}
			if previous, exists := rates[ts]; exists && !previous.Equal(rate) {
				return marketyield.Batch{}, fmt.Errorf("OKX earn has conflicting rates at %d", ts)
			}
			rates[ts] = rate
		}
		if minimum < lower {
			complete = true
			break
		}
		if after != 0 && minimum >= after {
			return marketyield.Batch{}, fmt.Errorf("OKX earn pagination did not advance")
		}
		boundaryRate, err = parseRate(minimumPoint.LendingRate)
		if err != nil {
			return marketyield.Batch{}, fmt.Errorf("OKX earn page %d boundary: %w", page, err)
		}
		after = minimum
	}
	if !complete {
		return marketyield.Batch{}, fmt.Errorf("OKX earn history exceeded 8 pages without completing window")
	}
	if len(rates) == 0 {
		return marketyield.Batch{}, fmt.Errorf("OKX earn history has no points in window")
	}
	timestamps := make([]int64, 0, len(rates))
	for ts := range rates {
		timestamps = append(timestamps, ts)
	}
	sort.Slice(timestamps, func(i, j int) bool { return timestamps[i] < timestamps[j] })
	if timestamps[len(timestamps)-1] < collected.Add(-6*time.Hour).UnixMilli() {
		return marketyield.Batch{}, fmt.Errorf("OKX earn latest history point is stale")
	}
	hash := marketyield.HashPayloads(payloads...)
	route := route(c.BaseURL + historyPath + "?ccy=AVAX")
	items := make([]marketyield.CollectedYield, 0, len(timestamps))
	for _, ts := range timestamps {
		rate := rates[ts].Truncate(18)
		items = append(items, marketyield.CollectedYield{Route: route, Observation: marketyield.YieldObservation{
			ObservationTime: time.UnixMilli(ts).UTC(), CollectedAt: collected,
			TierNo: 1, TierMinAmount: decimal.Zero, TierMode: "unknown", Rate: &rate,
			RateKind: "apr", RateOrigin: "reported", RateMode: "variable",
			RewardAssetKeys: []string{assetKey}, RewardComponentRates: []*decimal.Decimal{&rate},
			RulePrincipalLossMode: "unknown", RuleEligibility: "unknown", EligibilityReason: marketyield.Ptr("public_lending_rate_only"),
			ExposureRatio: marketyield.Ptr(decimal.NewFromInt(1)), Availability: "unknown", SourcePayloadHash: &hash,
		}})
	}
	return marketyield.Batch{Source: "okx-avax-flexible", CollectedAt: collected, Items: items}, nil
}

func positiveTimestamp(text string) (int64, error) {
	if text == "" {
		return 0, fmt.Errorf("timestamp is missing")
	}
	for _, digit := range text {
		if digit < '0' || digit > '9' {
			return 0, fmt.Errorf("timestamp is not an integer")
		}
	}
	value, err := strconv.ParseInt(text, 10, 64)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("timestamp is not a positive int64")
	}
	return value, nil
}

func parseRate(text string) (decimal.Decimal, error) {
	value, err := model.ParseStrictDecimal(text, "lendingRate")
	if err != nil || value.IsNegative() || value.GreaterThanOrEqual(decimal.New(1, 20)) {
		return decimal.Decimal{}, fmt.Errorf("lendingRate is missing, negative or outside Decimal(38,18)")
	}
	return value, nil
}

func route(sourceURL string) marketyield.YieldRouteDefinition {
	return marketyield.YieldRouteDefinition{
		ProviderType: "cex", Provider: "OKX", ProductCode: "simple-earn-flexible-avax", ProductName: "OKX AVAX 活期公开历史 APR", YieldType: "cex_earn",
		DepositAssetKey: assetKey, PositionAssetKey: "cex:okx:earn-flexible:AVAX", RedeemAssetKey: assetKey,
		PriceExposureAsset: marketyield.Ptr("AVAX"), IncomeSource: "borrow_interest", SourceURL: sourceURL, CollectionEnabled: true,
	}
}
