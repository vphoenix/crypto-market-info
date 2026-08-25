package save

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
	DefaultBaseURL  = "https://api.solend.fi"
	Program         = "So1endDq2YkqhipRh3WViPa8hdiSpxWy6z3Z6tMCpAo"
	Market          = "4UpD2fh7xH3VP9QQaXtsS1YY3bxzWhtfpks7FatyKvdY"
	Reserve         = "8PbodeaosQP19SjYFx855UMqWxH2HynZLdBXmsrbac36"
	CollateralMint  = "5h6ssFpeDeRbzsEHDbTQNH7nVGgsKrZydxdSTnLm6QdV"
	CollateralAsset = "solana:mainnet:spl:" + CollateralMint
)

type RPC interface {
	AccountInfo(context.Context, string) (solana.Account, error)
	Block(context.Context, uint64) (solana.BlockAnchor, error)
}

type Collector struct {
	BaseURL    string
	HTTPClient *http.Client
	Retry      exchange.HTTPRetryConfig
	RPC        RPC
	Now        func() time.Time
}

func NewCollector(baseURL string, rpc RPC) *Collector {
	if strings.TrimSpace(baseURL) == "" {
		baseURL = DefaultBaseURL
	}
	return &Collector{BaseURL: strings.TrimRight(baseURL, "/"), HTTPClient: &http.Client{Timeout: 20 * time.Second}, Retry: exchange.DefaultHTTPRetryConfig(), RPC: rpc}
}

type currentResult struct {
	Reserve            currentReserve  `json:"reserve"`
	CTokenExchangeRate json.RawMessage `json:"cTokenExchangeRate"`
	Rates              struct {
		SupplyInterest json.RawMessage `json:"supplyInterest"`
	} `json:"rates"`
}

type currentReserve struct {
	Pubkey        string `json:"pubkey"`
	Address       string `json:"address"`
	LendingMarket string `json:"lendingMarket"`
	LastUpdate    struct {
		Slot  json.RawMessage `json:"slot"`
		Stale json.RawMessage `json:"stale"`
	} `json:"lastUpdate"`
	Liquidity struct {
		MintPubkey                  string          `json:"mintPubkey"`
		MintDecimals                json.RawMessage `json:"mintDecimals"`
		AvailableAmount             json.RawMessage `json:"availableAmount"`
		BorrowedAmountWads          json.RawMessage `json:"borrowedAmountWads"`
		AccumulatedProtocolFeesWads json.RawMessage `json:"accumulatedProtocolFeesWads"`
	} `json:"liquidity"`
	Collateral struct {
		MintPubkey      string          `json:"mintPubkey"`
		MintTotalSupply json.RawMessage `json:"mintTotalSupply"`
	} `json:"collateral"`
	Config struct {
		DepositLimit json.RawMessage `json:"depositLimit"`
	} `json:"config"`
}

type historyPoint struct {
	SupplyAPY          json.RawMessage `json:"supplyAPY"`
	ReserveID          string          `json:"reserveID"`
	CTokenExchangeRate json.RawMessage `json:"cTokenExchangeRate"`
	Timestamp          json.RawMessage `json:"timestamp"`
}

func (c *Collector) Collect(ctx context.Context) (marketyield.Batch, error) {
	if c == nil || strings.TrimSpace(c.BaseURL) == "" || c.RPC == nil {
		return marketyield.Batch{}, fmt.Errorf("Save collector is not configured")
	}
	now := c.Now
	if now == nil {
		now = time.Now
	}
	collected := time.UnixMilli(now().UTC().UnixMilli()).UTC()
	currentURL := c.BaseURL + "/v1/reserves?" + url.Values{"scope": {"reserve"}, "ids": {Reserve}}.Encode()
	currentBody, err := exchange.Get(ctx, c.HTTPClient, currentURL, c.Retry)
	if err != nil {
		return marketyield.Batch{}, err
	}
	var currentEnvelope map[string]json.RawMessage
	if err = marketyield.DecodeJSON(currentBody, &currentEnvelope); err != nil {
		return marketyield.Batch{}, fmt.Errorf("Save current reserves: %w", err)
	}
	resultsBody, hasResults := currentEnvelope["results"]
	nextBody, hasNext := currentEnvelope["next"]
	if len(currentEnvelope) != 2 || !hasResults || !hasNext || strings.TrimSpace(string(nextBody)) != "null" {
		return marketyield.Batch{}, fmt.Errorf("Save current response has invalid results or next")
	}
	var results []currentResult
	if err = marketyield.DecodeJSON(resultsBody, &results); err != nil || len(results) != 1 {
		return marketyield.Batch{}, fmt.Errorf("Save current response has invalid results")
	}
	value := results[0]
	reserve := value.Reserve
	if reserve.Pubkey != Reserve || reserve.Address != Reserve || reserve.LendingMarket != Market || reserve.Liquidity.MintPubkey != solana.WSOLMintAddress || reserve.Collateral.MintPubkey != CollateralMint {
		return marketyield.Batch{}, fmt.Errorf("Save current response has invalid fixed identity")
	}
	decimals, decimalsErr := rawUint(reserve.Liquidity.MintDecimals, "mintDecimals")
	slot, slotErr := rawUint(reserve.LastUpdate.Slot, "lastUpdate.slot")
	stale, staleErr := rawUint(reserve.LastUpdate.Stale, "lastUpdate.stale")
	if decimalsErr != nil || slotErr != nil || staleErr != nil || decimals != 9 || slot == 0 || stale > 1 {
		return marketyield.Batch{}, fmt.Errorf("Save current response has invalid decimals, slot, or stale")
	}
	account, err := c.RPC.AccountInfo(ctx, Reserve)
	if err != nil {
		return marketyield.Batch{}, fmt.Errorf("Save reserve identity: %w", err)
	}
	if account.Owner != Program {
		return marketyield.Batch{}, fmt.Errorf("Save reserve owner %q does not match program", account.Owner)
	}
	block, err := c.RPC.Block(ctx, slot)
	if err != nil {
		return marketyield.Batch{}, fmt.Errorf("Save source block: %w", err)
	}
	currentItem, err := c.currentItem(value, collected, block, currentBody)
	if err != nil {
		return marketyield.Batch{}, err
	}
	historyURL := c.BaseURL + "/v1/reserves/historical-interest-rates?" + url.Values{"ids": {Reserve}, "span": {"30d"}}.Encode()
	historyBody, err := exchange.Get(ctx, c.HTTPClient, historyURL, c.Retry)
	if err != nil {
		return marketyield.Batch{}, err
	}
	var history map[string][]historyPoint
	if err = marketyield.DecodeJSON(historyBody, &history); err != nil {
		return marketyield.Batch{}, fmt.Errorf("Save rate history: %w", err)
	}
	points, ok := history[Reserve]
	if !ok || len(history) != 1 || len(points) == 0 || len(points) > 64 {
		return marketyield.Batch{}, fmt.Errorf("Save history has invalid reserve key or point count")
	}
	historyHash := marketyield.HashPayloads(marketyield.Payload{Name: "history", Body: historyBody})
	route := c.route()
	items := make([]marketyield.CollectedYield, 0, len(points)+1)
	var previous int64
	for index, point := range points {
		if point.ReserveID != Reserve {
			return marketyield.Batch{}, fmt.Errorf("Save history point %d has invalid reserve", index)
		}
		seconds, secondsErr := rawUint(point.Timestamp, "timestamp")
		rate, rateErr := rawDecimal(point.SupplyAPY, "supplyAPY")
		exposure, exposureErr := rawDecimal(point.CTokenExchangeRate, "cTokenExchangeRate")
		if secondsErr != nil || seconds > uint64(^uint64(0)>>1) || rateErr != nil || exposureErr != nil || rate.IsNegative() || !exposure.IsPositive() || (index > 0 && int64(seconds) <= previous) {
			return marketyield.Batch{}, fmt.Errorf("Save history point %d is invalid", index)
		}
		previous = int64(seconds)
		at := time.Unix(int64(seconds), 0).UTC()
		if at.After(collected.Add(5 * time.Minute)) {
			return marketyield.Batch{}, fmt.Errorf("Save history point %d is in the future", index)
		}
		if at.Equal(currentItem.Observation.ObservationTime) {
			continue
		}
		rateCopy, exposureCopy := rate, exposure
		unbonding := uint64(0)
		observation := marketyield.YieldObservation{ObservationTime: at, CollectedAt: collected, TierNo: 1, TierMinAmount: decimal.Zero, TierMode: "none", Rate: &rateCopy,
			RateKind: "apy", RateOrigin: "reported", RateMode: "variable", RewardAssetKeys: []string{solana.WSOLAsset}, RewardComponentRates: []*decimal.Decimal{&rateCopy},
			UnbondingSeconds: &unbonding, RulePrincipalLossMode: "none", RuleEligibility: "candidate", ExposureRatio: &exposureCopy, Availability: "unknown", SourcePayloadHash: &historyHash}
		items = append(items, marketyield.CollectedYield{Route: route, Observation: observation})
	}
	if collected.Sub(time.Unix(previous, 0).UTC()) > 72*time.Hour {
		return marketyield.Batch{}, fmt.Errorf("Save latest history point is stale")
	}
	items = append(items, currentItem)
	return marketyield.Batch{Source: "save-main-sol", CollectedAt: collected, Items: items}, nil
}

func (c *Collector) currentItem(value currentResult, collected time.Time, block solana.BlockAnchor, responseBody []byte) (marketyield.CollectedYield, error) {
	ratePercent, rateErr := rawDecimal(value.Rates.SupplyInterest, "supplyInterest")
	exposure, exposureErr := rawDecimal(value.CTokenExchangeRate, "cTokenExchangeRate")
	available, availableErr := rawDecimal(value.Reserve.Liquidity.AvailableAmount, "availableAmount")
	borrowedWads, borrowedErr := rawDecimal(value.Reserve.Liquidity.BorrowedAmountWads, "borrowedAmountWads")
	feesWads, feesErr := rawDecimal(value.Reserve.Liquidity.AccumulatedProtocolFeesWads, "accumulatedProtocolFeesWads")
	depositLimit, limitErr := rawDecimal(value.Reserve.Config.DepositLimit, "depositLimit")
	cTokenSupply, supplyErr := rawDecimal(value.Reserve.Collateral.MintTotalSupply, "mintTotalSupply")
	if rateErr != nil || exposureErr != nil || availableErr != nil || borrowedErr != nil || feesErr != nil || limitErr != nil || supplyErr != nil || ratePercent.IsNegative() || !exposure.IsPositive() || available.IsNegative() || borrowedWads.IsNegative() || feesWads.IsNegative() || depositLimit.IsNegative() || cTokenSupply.IsNegative() {
		return marketyield.CollectedYield{}, fmt.Errorf("Save current response has invalid decimal metrics")
	}
	oneE18 := decimal.New(1, 18)
	oneE9 := decimal.New(1, 9)
	netLamports := available.Add(borrowedWads.Div(oneE18)).Sub(feesWads.Div(oneE18))
	if netLamports.IsNegative() {
		return marketyield.CollectedYield{}, fmt.Errorf("Save current net deposits are negative")
	}
	tvl := netLamports.Div(oneE9)
	derivedTVL := cTokenSupply.Mul(exposure).Div(oneE9)
	if tvl.Sub(derivedTVL).Abs().GreaterThan(decimal.New(1, -9)) {
		return marketyield.CollectedYield{}, fmt.Errorf("Save current TVL invariant differs by more than one lamport")
	}
	capacity := depositLimit.Div(oneE9)
	remaining := capacity.Sub(tvl)
	if remaining.IsNegative() {
		remaining = decimal.Zero
	}
	rate := ratePercent.Div(decimal.NewFromInt(100))
	at := time.Unix(block.Time, 0).UTC()
	if at.After(collected.Add(5 * time.Minute)) {
		return marketyield.CollectedYield{}, fmt.Errorf("Save current source block is in the future")
	}
	hash := marketyield.HashPayloads(marketyield.Payload{Name: "reserves", Body: responseBody}, marketyield.Payload{Name: "block", Body: block.Payload})
	height, blockHash, finality := block.Height, block.Hash, "finalized_anchor"
	rateCopy, exposureCopy, tvlCopy, capacityCopy, remainingCopy := rate, exposure, tvl, capacity, remaining
	unbonding := uint64(0)
	observation := marketyield.YieldObservation{ObservationTime: at, CollectedAt: collected, TierNo: 1, TierMinAmount: decimal.Zero, TierMode: "none", Rate: &rateCopy,
		RateKind: "apy", RateOrigin: "reported", RateMode: "variable", RewardAssetKeys: []string{solana.WSOLAsset}, RewardComponentRates: []*decimal.Decimal{&rateCopy},
		UnbondingSeconds: &unbonding, RulePrincipalLossMode: "none", RuleEligibility: "candidate", ExposureRatio: &exposureCopy, Capacity: &capacityCopy,
		RemainingCapacity: &remainingCopy, TVL: &tvlCopy, Availability: "unknown", BlockHeight: &height, BlockHash: &blockHash, Finality: &finality, SourcePayloadHash: &hash}
	return marketyield.CollectedYield{Route: c.route(), Observation: observation}, nil
}

func (c *Collector) route() marketyield.YieldRouteDefinition {
	network, address, exposure := "solana-mainnet", Reserve, solana.SOLAsset
	return marketyield.YieldRouteDefinition{ProviderType: "protocol", Provider: "Save", ProductCode: "main-sol", ProductName: "Save Main SOL lending", YieldType: "lending",
		DepositAssetKey: solana.WSOLAsset, PositionAssetKey: CollateralAsset, RedeemAssetKey: solana.WSOLAsset, Network: &network, ContractAddress: &address,
		PriceExposureAsset: &exposure, IncomeSource: "borrow_interest", SourceURL: c.BaseURL + "/v1/reserves", CollectionEnabled: true}
}

func rawUint(raw json.RawMessage, name string) (uint64, error) {
	value, err := rawDecimal(raw, name)
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
