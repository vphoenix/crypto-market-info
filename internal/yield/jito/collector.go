package jito

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/exchange"
	marketyield "github.com/vphoenix/crypto-market-info/internal/yield"
	"github.com/vphoenix/crypto-market-info/internal/yield/solana"
)

const DefaultBaseURL = "https://kobe.mainnet.jito.network"

type Collector struct {
	BaseURL    string
	HTTPClient *http.Client
	Retry      exchange.HTTPRetryConfig
	Reader     solana.PoolReader
	Now        func() time.Time
}

func NewCollector(baseURL string, reader solana.PoolReader) *Collector {
	if strings.TrimSpace(baseURL) == "" {
		baseURL = DefaultBaseURL
	}
	return &Collector{BaseURL: strings.TrimRight(baseURL, "/"), HTTPClient: &http.Client{Timeout: 20 * time.Second}, Retry: exchange.DefaultHTTPRetryConfig(), Reader: reader}
}

type point struct {
	Data json.Number `json:"data"`
	Date string      `json:"date"`
}
type statsResponse struct {
	APY []point `json:"apy"`
	TVL []point `json:"tvl"`
}
type ratioResponse struct {
	Ratios []point `json:"ratios"`
}
type parsedPoint struct {
	at    time.Time
	value decimal.Decimal
}

func (c *Collector) Collect(ctx context.Context) (marketyield.Batch, error) {
	if c == nil || c.Reader == nil || strings.TrimSpace(c.BaseURL) == "" {
		return marketyield.Batch{}, fmt.Errorf("Jito collector is not configured")
	}
	now := c.Now
	if now == nil {
		now = time.Now
	}
	if _, err := c.Reader.Read(ctx, solana.PoolConfig{Program: solana.StakePoolProgram, Address: solana.JitoPoolAddress, Mint: solana.JitoMintAddress}); err != nil {
		return marketyield.Batch{}, fmt.Errorf("JitoSOL identity: %w", err)
	}
	statsBody, err := exchange.Get(ctx, c.HTTPClient, c.BaseURL+"/api/v1/stake_pool_stats", c.Retry)
	if err != nil {
		return marketyield.Batch{}, err
	}
	ratioBody, err := exchange.Get(ctx, c.HTTPClient, c.BaseURL+"/api/v1/jitosol_sol_ratio", c.Retry)
	if err != nil {
		return marketyield.Batch{}, err
	}
	collected := time.UnixMilli(now().UTC().UnixMilli()).UTC()
	var stats statsResponse
	var ratios ratioResponse
	if err = marketyield.DecodeJSON(statsBody, &stats); err != nil {
		return marketyield.Batch{}, fmt.Errorf("Jito stats: %w", err)
	}
	if err = marketyield.DecodeJSON(ratioBody, &ratios); err != nil {
		return marketyield.Batch{}, fmt.Errorf("Jito ratio: %w", err)
	}
	apy, err := parsePoints(stats.APY, "apy", false, 128)
	if err != nil {
		return marketyield.Batch{}, err
	}
	tvl, err := parsePoints(stats.TVL, "tvl", true, 128)
	if err != nil {
		return marketyield.Batch{}, err
	}
	ratio, err := parsePoints(ratios.Ratios, "ratios", false, 128)
	if err != nil {
		return marketyield.Batch{}, err
	}
	dates := make([]string, 0, len(apy))
	for date := range apy {
		if _, ok := tvl[date]; ok {
			if _, ok = ratio[date]; ok {
				dates = append(dates, date)
			}
		}
	}
	if len(dates) == 0 || len(dates) > 128 {
		return marketyield.Batch{}, fmt.Errorf("Jito common history point count %d is invalid", len(dates))
	}
	sort.Slice(dates, func(i, j int) bool { return apy[dates[i]].at.Before(apy[dates[j]].at) })
	latest := apy[dates[len(dates)-1]].at
	if latest.After(collected.Add(5*time.Minute)) || collected.Sub(latest) > 72*time.Hour {
		return marketyield.Batch{}, fmt.Errorf("Jito latest point %s is outside freshness bounds", latest)
	}
	hash := marketyield.HashPayloads(marketyield.Payload{Name: "stake_pool_stats", Body: statsBody}, marketyield.Payload{Name: "jitosol_sol_ratio", Body: ratioBody})
	network, address, exposure := "solana-mainnet", solana.JitoPoolAddress, solana.SOLAsset
	route := marketyield.YieldRouteDefinition{ProviderType: "protocol", Provider: "Jito", ProductCode: "jitosol", ProductName: "JitoSOL", YieldType: "liquid_staking",
		DepositAssetKey: solana.SOLAsset, PositionAssetKey: solana.JitoSOLAsset, RedeemAssetKey: solana.SOLAsset, Network: &network, ContractAddress: &address, PriceExposureAsset: &exposure,
		IncomeSource: "combined", SourceURL: c.BaseURL + "/api/v1/stake_pool_stats", CollectionEnabled: true}
	items := make([]marketyield.CollectedYield, 0, len(dates))
	for _, date := range dates {
		a, t, r := apy[date], tvl[date], ratio[date]
		if a.value.IsNegative() || !a.value.LessThan(decimal.NewFromInt(1)) || !r.value.IsPositive() || t.value.IsNegative() {
			return marketyield.Batch{}, fmt.Errorf("Jito point %s has invalid values", date)
		}
		tvlSOL := t.value.Div(decimal.NewFromInt(1_000_000_000))
		rate, exchangeRatio := a.value, r.value
		observation := marketyield.YieldObservation{ObservationTime: a.at, CollectedAt: collected, TierNo: 1, TierMinAmount: decimal.Zero, TierMode: "none", Rate: &rate,
			RateKind: "apy", RateOrigin: "reported", RateMode: "variable", RewardAssetKeys: []string{solana.SOLAsset}, RewardComponentRates: []*decimal.Decimal{&rate},
			RulePrincipalLossMode: "none", RuleEligibility: "candidate", ExposureRatio: &exchangeRatio, TVL: &tvlSOL, Availability: "unknown", SourcePayloadHash: &hash}
		items = append(items, marketyield.CollectedYield{Route: route, Observation: observation})
	}
	return marketyield.Batch{Source: "jitosol", CollectedAt: collected, Items: items}, nil
}

func parsePoints(points []point, name string, integer bool, limit int) (map[string]parsedPoint, error) {
	if len(points) == 0 || len(points) > limit {
		return nil, fmt.Errorf("Jito %s point count %d is invalid", name, len(points))
	}
	out := make(map[string]parsedPoint, len(points))
	for index, item := range points {
		at, err := time.Parse(time.RFC3339Nano, item.Date)
		if err != nil {
			return nil, fmt.Errorf("Jito %s point %d date: %w", name, index, err)
		}
		at = time.UnixMilli(at.UTC().UnixMilli()).UTC()
		if _, exists := out[item.Date]; exists {
			return nil, fmt.Errorf("Jito %s contains duplicate date %s", name, item.Date)
		}
		raw := item.Data.String()
		if raw == "" {
			return nil, fmt.Errorf("Jito %s point %d has missing data", name, index)
		}
		if integer {
			value, ok := new(big.Int).SetString(raw, 10)
			if !ok || value.Sign() < 0 {
				return nil, fmt.Errorf("Jito %s point %d is not an unsigned integer", name, index)
			}
		}
		value, err := decimal.NewFromString(raw)
		if err != nil {
			return nil, fmt.Errorf("Jito %s point %d: %w", name, index, err)
		}
		out[item.Date] = parsedPoint{at: at, value: value}
	}
	return out, nil
}
