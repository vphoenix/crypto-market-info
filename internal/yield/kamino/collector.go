package kamino

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/exchange"
	marketyield "github.com/vphoenix/crypto-market-info/internal/yield"
	"github.com/vphoenix/crypto-market-info/internal/yield/solana"
)

const (
	DefaultBaseURL = "https://api.kamino.finance"
	Program        = "KLend2g3cP87fffoy8q1mQqGKjrxjC8boSyAYavgmjD"
	Market         = "7u3HeHxYDLhnCoErrtycNokbQYbWGzLs6JSDqGAv5PfF"
	Reserve        = "d4A2prbA2whesmvHaL88BH6Ewn5N4bTSU2Ze8P6Bc4Q"
	PositionAsset  = "solana:mainnet:protocol-position:kamino:" + Reserve
)

type AccountReader interface {
	AccountInfo(context.Context, string) (solana.Account, error)
}

type Collector struct {
	BaseURL    string
	HTTPClient *http.Client
	Retry      exchange.HTTPRetryConfig
	Accounts   AccountReader
	Now        func() time.Time
}

func NewCollector(baseURL string, accounts AccountReader) *Collector {
	if strings.TrimSpace(baseURL) == "" {
		baseURL = DefaultBaseURL
	}
	return &Collector{BaseURL: strings.TrimRight(baseURL, "/"), HTTPClient: &http.Client{Timeout: 20 * time.Second}, Retry: exchange.DefaultHTTPRetryConfig(), Accounts: accounts}
}

type historyResponse struct {
	Reserve string         `json:"reserve"`
	History []historyPoint `json:"history"`
}

type historyPoint struct {
	Timestamp time.Time      `json:"timestamp"`
	Metrics   historyMetrics `json:"metrics"`
}

type historyMetrics struct {
	Status              string          `json:"status"`
	Symbol              string          `json:"symbol"`
	Decimals            json.Number     `json:"decimals"`
	MintAddress         string          `json:"mintAddress"`
	TotalSupply         json.RawMessage `json:"totalSupply"`
	ExchangeRate        json.RawMessage `json:"exchangeRate"`
	IsUIDeprecated      *bool           `json:"isUIDeprecated"`
	SupplyInterestAPY   json.RawMessage `json:"supplyInterestAPY"`
	ReserveDepositLimit json.RawMessage `json:"reserveDepositLimit"`
}

func (c *Collector) Collect(ctx context.Context) (marketyield.Batch, error) {
	if c == nil || strings.TrimSpace(c.BaseURL) == "" || c.Accounts == nil {
		return marketyield.Batch{}, fmt.Errorf("Kamino collector is not configured")
	}
	now := c.Now
	if now == nil {
		now = time.Now
	}
	collected := time.UnixMilli(now().UTC().UnixMilli()).UTC()
	account, err := c.Accounts.AccountInfo(ctx, Reserve)
	if err != nil {
		return marketyield.Batch{}, fmt.Errorf("Kamino reserve identity: %w", err)
	}
	if account.Owner != Program {
		return marketyield.Batch{}, fmt.Errorf("Kamino reserve owner %q does not match program", account.Owner)
	}
	path := "/kamino-market/" + Market + "/reserves/" + Reserve + "/metrics/history"
	query := url.Values{}
	query.Set("env", "mainnet-beta")
	query.Set("start", collected.Add(-30*24*time.Hour).Format(time.RFC3339))
	query.Set("end", collected.Format(time.RFC3339))
	query.Set("frequency", "hour")
	body, err := exchange.Get(ctx, c.HTTPClient, c.BaseURL+path+"?"+query.Encode(), c.Retry)
	if err != nil {
		return marketyield.Batch{}, err
	}
	var response historyResponse
	if err = marketyield.DecodeJSON(body, &response); err != nil {
		return marketyield.Batch{}, fmt.Errorf("Kamino history: %w", err)
	}
	if response.Reserve != Reserve || len(response.History) == 0 || len(response.History) > 1024 {
		return marketyield.Batch{}, fmt.Errorf("Kamino history has invalid reserve or point count")
	}
	hash := marketyield.HashPayloads(marketyield.Payload{Name: "history", Body: body})
	route := route(c.BaseURL + path)
	items := make([]marketyield.CollectedYield, 0, len(response.History))
	var previous time.Time
	for index, point := range response.History {
		at := time.UnixMilli(point.Timestamp.UTC().UnixMilli()).UTC()
		if at.IsZero() || (!previous.IsZero() && !at.After(previous)) || at.After(collected.Add(5*time.Minute)) {
			return marketyield.Batch{}, fmt.Errorf("Kamino point %d has invalid timestamp ordering", index)
		}
		previous = at
		m := point.Metrics
		decimals, parseErr := unsignedNumber(m.Decimals, "decimals")
		rate, rateErr := rawDecimal(m.SupplyInterestAPY, "supplyInterestAPY")
		exchangeRate, exchangeErr := rawDecimal(m.ExchangeRate, "exchangeRate")
		tvl, tvlErr := rawDecimal(m.TotalSupply, "totalSupply")
		depositLimit, limitErr := rawDecimal(m.ReserveDepositLimit, "reserveDepositLimit")
		if parseErr != nil || rateErr != nil || exchangeErr != nil || tvlErr != nil || limitErr != nil || decimals != 9 || m.Symbol != "SOL" || m.MintAddress != solana.WSOLMintAddress || rate.IsNegative() || !exchangeRate.IsPositive() || tvl.IsNegative() || depositLimit.IsNegative() {
			return marketyield.Batch{}, fmt.Errorf("Kamino point %d has invalid metrics", index)
		}
		exposure := decimal.NewFromInt(1).Div(exchangeRate)
		capacity := depositLimit.Div(decimal.NewFromInt(1_000_000_000))
		remaining := capacity.Sub(tvl)
		if remaining.IsNegative() {
			remaining = decimal.Zero
		}
		availability := "unknown"
		if m.Status == "Active" && m.IsUIDeprecated != nil && !*m.IsUIDeprecated {
			availability = "available"
		} else if m.Status == "Inactive" {
			availability = "unavailable"
		}
		rateCopy, exposureCopy, tvlCopy, capacityCopy, remainingCopy := rate, exposure, tvl, capacity, remaining
		unbonding := uint64(0)
		observation := marketyield.YieldObservation{ObservationTime: at, CollectedAt: collected, TierNo: 1, TierMinAmount: decimal.Zero, TierMode: "none", Rate: &rateCopy,
			RateKind: "apy", RateOrigin: "reported", RateMode: "variable", RewardAssetKeys: []string{solana.WSOLAsset}, RewardComponentRates: []*decimal.Decimal{&rateCopy},
			UnbondingSeconds: &unbonding, RulePrincipalLossMode: "none", RuleEligibility: "candidate", ExposureRatio: &exposureCopy, Capacity: &capacityCopy,
			RemainingCapacity: &remainingCopy, TVL: &tvlCopy, Availability: availability, SourcePayloadHash: &hash}
		items = append(items, marketyield.CollectedYield{Route: route, Observation: observation})
	}
	if collected.Sub(items[len(items)-1].Observation.ObservationTime) > 72*time.Hour {
		return marketyield.Batch{}, fmt.Errorf("Kamino latest history point is stale")
	}
	return marketyield.Batch{Source: "kamino-main-sol", CollectedAt: collected, Items: items}, nil
}

func route(sourceURL string) marketyield.YieldRouteDefinition {
	network, address, exposure := "solana-mainnet", Reserve, solana.SOLAsset
	return marketyield.YieldRouteDefinition{ProviderType: "protocol", Provider: "Kamino", ProductCode: "main-sol", ProductName: "Kamino Main SOL lending", YieldType: "lending",
		DepositAssetKey: solana.WSOLAsset, PositionAssetKey: PositionAsset, RedeemAssetKey: solana.WSOLAsset, Network: &network, ContractAddress: &address,
		PriceExposureAsset: &exposure, IncomeSource: "borrow_interest", SourceURL: sourceURL, CollectionEnabled: true}
}

func unsignedNumber(number json.Number, name string) (uint64, error) {
	if number.String() == "" {
		return 0, fmt.Errorf("%s is missing", name)
	}
	value, err := decimal.NewFromString(number.String())
	if err != nil || value.IsNegative() || !value.Equal(value.Truncate(0)) || value.GreaterThan(decimal.NewFromInt(1<<53)) {
		return 0, fmt.Errorf("%s is not an unsigned integer", name)
	}
	return uint64(value.IntPart()), nil
}

func rawDecimal(raw json.RawMessage, name string) (decimal.Decimal, error) {
	text := strings.TrimSpace(string(raw))
	if len(text) > 1 && text[0] == '"' {
		var value string
		if err := json.Unmarshal(raw, &value); err != nil {
			return decimal.Decimal{}, fmt.Errorf("%s: %w", name, err)
		}
		text = value
	}
	if text == "" || text == "null" {
		return decimal.Decimal{}, fmt.Errorf("%s is missing", name)
	}
	value, err := decimal.NewFromString(text)
	if err != nil {
		return decimal.Decimal{}, fmt.Errorf("%s: %w", name, err)
	}
	return value, nil
}
