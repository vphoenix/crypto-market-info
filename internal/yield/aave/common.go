package aave

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"time"

	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/exchange"
	"github.com/vphoenix/crypto-market-info/internal/model"
	marketyield "github.com/vphoenix/crypto-market-info/internal/yield"
)

const (
	chainID    = 43114
	wavax      = "0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7"
	wavaxAsset = "eip155:43114:erc20:" + wavax
)

func query(ctx context.Context, client *http.Client, endpoint, query string, retry exchange.HTTPRetryConfig, result any) (string, error) {
	request, err := json.Marshal(struct {
		Query string `json:"query"`
	}{Query: query})
	if err != nil {
		return "", err
	}
	response, err := exchange.PostJSON(ctx, client, endpoint, request, retry)
	if err != nil {
		return "", err
	}
	if err = marketyield.DecodeJSON(response, result); err != nil {
		return "", err
	}
	return marketyield.HashPayloads(marketyield.Payload{Name: "request", Body: request}, marketyield.Payload{Name: "response", Body: response}), nil
}

// Only the unit conversion is shared. V3 must separately validate its integer
// ray input; V4 is a percentage string. Negative values are rejected BEFORE
// truncation so a tiny negative cannot silently become a valid zero.
func scaledRate(text string, shift int32) (decimal.Decimal, error) {
	value, err := model.ParseStrictDecimal(text, "rate")
	if err != nil || value.IsNegative() {
		return decimal.Decimal{}, fmt.Errorf("rate is missing, invalid or negative")
	}
	value = value.Shift(shift)
	if value.IsNegative() || value.GreaterThanOrEqual(decimal.New(1, 20)) {
		return decimal.Decimal{}, fmt.Errorf("rate exceeds Decimal(38,18)")
	}
	return value.Truncate(18), nil
}

type ratePoint struct {
	at   time.Time
	rate decimal.Decimal
}

func historyBatch(source string, route marketyield.YieldRouteDefinition, collected time.Time, points []ratePoint, hash string, exposure *decimal.Decimal) (marketyield.Batch, error) {
	if len(points) == 0 || len(points) > 512 {
		return marketyield.Batch{}, fmt.Errorf("%s history has invalid point count", source)
	}
	seen := make(map[int64]bool, len(points))
	for i := range points {
		at := points[i].at
		if at.IsZero() || at.Before(collected.Add(-14*24*time.Hour)) || at.After(collected.Add(5*time.Minute)) {
			return marketyield.Batch{}, fmt.Errorf("%s point %d has invalid timestamp", source, i)
		}
		points[i].at = at.UTC().Truncate(time.Millisecond)
		key := points[i].at.UnixMilli()
		if seen[key] {
			return marketyield.Batch{}, fmt.Errorf("%s history has duplicate timestamp", source)
		}
		seen[key] = true
	}
	sort.Slice(points, func(i, j int) bool { return points[i].at.Before(points[j].at) })
	if points[len(points)-1].at.Before(collected.Add(-6 * time.Hour)) {
		return marketyield.Batch{}, fmt.Errorf("%s latest history point is stale", source)
	}
	items := make([]marketyield.CollectedYield, 0, len(points))
	for _, point := range points {
		rate := point.rate
		items = append(items, marketyield.CollectedYield{Route: route, Observation: marketyield.YieldObservation{
			ObservationTime: point.at, CollectedAt: collected,
			TierNo: 1, TierMinAmount: decimal.Zero, TierMode: "none", Rate: &rate,
			RateKind: "apy", RateOrigin: "reported", RateMode: "variable",
			RewardAssetKeys: []string{wavaxAsset}, RewardComponentRates: []*decimal.Decimal{&rate},
			RulePrincipalLossMode: "none", RuleEligibility: "candidate", ExposureRatio: exposure,
			Availability: "unknown", SourcePayloadHash: &hash,
		}})
	}
	return marketyield.Batch{Source: source, CollectedAt: collected, Items: items}, nil
}

func route(product, name, position, contract, sourceURL string) marketyield.YieldRouteDefinition {
	return marketyield.YieldRouteDefinition{
		ProviderType: "protocol", Provider: "Aave", ProductCode: product, ProductName: name, YieldType: "lending",
		DepositAssetKey: wavaxAsset, PositionAssetKey: position, RedeemAssetKey: wavaxAsset,
		Network: marketyield.Ptr("avalanche-c-mainnet"), ContractAddress: &contract,
		PriceExposureAsset: marketyield.Ptr("AVAX"), IncomeSource: "borrow_interest", SourceURL: sourceURL, CollectionEnabled: true,
	}
}
