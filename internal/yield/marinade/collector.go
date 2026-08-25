package marinade

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/exchange"
	marketyield "github.com/vphoenix/crypto-market-info/internal/yield"
	"github.com/vphoenix/crypto-market-info/internal/yield/solana"
)

const (
	DefaultAPYBaseURL = "https://apy.marinade.finance"
	NativeAuthority   = "stWirqFCf2Uts1JBL1Jsd3r6VBWhgnpdPxCTe1MFjrq"
)

type MSOLIdentityValidator interface{ ValidateMSOLIdentity(context.Context) error }

type product uint8

const (
	productMSOL product = iota + 1
	productNative
)

type Collector struct {
	BaseURL    string
	HTTPClient *http.Client
	Retry      exchange.HTTPRetryConfig
	Identity   MSOLIdentityValidator
	Now        func() time.Time
	product    product
}

func NewMSOLCollector(baseURL string, identity MSOLIdentityValidator) *Collector {
	return newCollector(baseURL, productMSOL, identity)
}
func NewNativeCollector(baseURL string) *Collector { return newCollector(baseURL, productNative, nil) }
func newCollector(baseURL string, kind product, identity MSOLIdentityValidator) *Collector {
	if strings.TrimSpace(baseURL) == "" {
		baseURL = DefaultAPYBaseURL
	}
	return &Collector{BaseURL: strings.TrimRight(baseURL, "/"), HTTPClient: &http.Client{Timeout: 20 * time.Second}, Retry: exchange.DefaultHTTPRetryConfig(), Identity: identity, product: kind}
}

type rollingResponse struct {
	Times  []json.Number `json:"times"`
	Values []json.Number `json:"values"`
	Labels []struct {
		LowerTime  json.Number `json:"lowerTime"`
		UpperTime  json.Number `json:"upperTime"`
		LowerPrice json.Number `json:"lowerPrice"`
		UpperPrice json.Number `json:"upperPrice"`
	} `json:"labels"`
}

func (c *Collector) Collect(ctx context.Context) (marketyield.Batch, error) {
	if c == nil || strings.TrimSpace(c.BaseURL) == "" || (c.product != productMSOL && c.product != productNative) {
		return marketyield.Batch{}, fmt.Errorf("Marinade collector is not configured")
	}
	now := c.Now
	if now == nil {
		now = time.Now
	}
	path := "/v1/rolling-apy/marinade-native"
	if c.product == productMSOL {
		if c.Identity == nil {
			return marketyield.Batch{}, fmt.Errorf("mSOL identity validator is not configured")
		}
		if err := c.Identity.ValidateMSOLIdentity(ctx); err != nil {
			return marketyield.Batch{}, fmt.Errorf("mSOL identity: %w", err)
		}
		path = "/v1/rolling-apy/liquid-pool-token/" + solana.MarinadeProgram
	}
	requestTime := time.UnixMilli(now().UTC().UnixMilli()).UTC()
	query := url.Values{}
	query.Set("window", "1209600")
	query.Set("from", strconv.FormatInt(requestTime.Add(-30*24*time.Hour).Unix(), 10))
	query.Set("to", strconv.FormatInt(requestTime.Unix(), 10))
	rawURL := c.BaseURL + path + "?" + query.Encode()
	body, err := exchange.Get(ctx, c.HTTPClient, rawURL, c.Retry)
	if err != nil {
		return marketyield.Batch{}, err
	}
	collected := time.UnixMilli(now().UTC().UnixMilli()).UTC()
	var response rollingResponse
	if err = marketyield.DecodeJSON(body, &response); err != nil {
		return marketyield.Batch{}, fmt.Errorf("Marinade rolling APY: %w", err)
	}
	count := len(response.Times)
	if count == 0 || count > 64 || len(response.Values) != count {
		return marketyield.Batch{}, fmt.Errorf("Marinade rolling APY arrays have invalid lengths")
	}
	if c.product == productMSOL && len(response.Labels) != count {
		return marketyield.Batch{}, fmt.Errorf("mSOL rolling APY labels differ in length")
	}
	hash := marketyield.HashPayloads(marketyield.Payload{Name: "rolling_apy", Body: body})
	route := c.route(rawURL)
	items := make([]marketyield.CollectedYield, 0, count)
	var previous uint64
	for index := 0; index < count; index++ {
		seconds, err := numberUint(response.Times[index], "time")
		if err != nil {
			return marketyield.Batch{}, fmt.Errorf("Marinade point %d: %w", index, err)
		}
		if index > 0 && seconds <= previous {
			return marketyield.Batch{}, fmt.Errorf("Marinade times are not strictly increasing")
		}
		previous = seconds
		at := time.Unix(int64(seconds), 0).UTC()
		if at.After(collected.Add(5 * time.Minute)) {
			return marketyield.Batch{}, fmt.Errorf("Marinade point %d is in the future", index)
		}
		rate, err := numberDecimal(response.Values[index], "APY")
		if err != nil || rate.IsNegative() || !rate.LessThan(decimal.NewFromInt(1)) {
			return marketyield.Batch{}, fmt.Errorf("Marinade point %d has invalid APY", index)
		}
		exposure := decimal.NewFromInt(1)
		if c.product == productMSOL {
			label := response.Labels[index]
			lower, lowErr := numberUint(label.LowerTime, "lowerTime")
			upper, upErr := numberUint(label.UpperTime, "upperTime")
			lowerPrice, lpErr := numberDecimal(label.LowerPrice, "lowerPrice")
			upperPrice, upErr2 := numberDecimal(label.UpperPrice, "upperPrice")
			if lowErr != nil || upErr != nil || lpErr != nil || upErr2 != nil || upper != seconds || lower >= upper || !lowerPrice.IsPositive() || !upperPrice.IsPositive() {
				return marketyield.Batch{}, fmt.Errorf("mSOL point %d has invalid label", index)
			}
			exposure = upperPrice
		}
		rateCopy, exposureCopy := rate, exposure
		observation := marketyield.YieldObservation{ObservationTime: at, CollectedAt: collected, TierNo: 1, TierMinAmount: decimal.Zero, TierMode: "none", Rate: &rateCopy,
			RateKind: "apy", RateOrigin: "reported", RateMode: "variable", RewardAssetKeys: []string{solana.SOLAsset}, RewardComponentRates: []*decimal.Decimal{&rateCopy},
			RulePrincipalLossMode: "none", RuleEligibility: "candidate", ExposureRatio: &exposureCopy, Availability: "unknown", SourcePayloadHash: &hash}
		items = append(items, marketyield.CollectedYield{Route: route, Observation: observation})
	}
	latest := items[len(items)-1].Observation.ObservationTime
	if collected.Sub(latest) > 72*time.Hour {
		return marketyield.Batch{}, fmt.Errorf("Marinade latest point %s is stale", latest)
	}
	source := "marinade-native"
	if c.product == productMSOL {
		source = "marinade-msol"
	}
	return marketyield.Batch{Source: source, CollectedAt: collected, Items: items}, nil
}

func (c *Collector) route(sourceURL string) marketyield.YieldRouteDefinition {
	network, exposure := "solana-mainnet", solana.SOLAsset
	if c.product == productMSOL {
		address := solana.MarinadeState
		return marketyield.YieldRouteDefinition{ProviderType: "protocol", Provider: "Marinade", ProductCode: "msol", ProductName: "Marinade mSOL", YieldType: "liquid_staking",
			DepositAssetKey: solana.SOLAsset, PositionAssetKey: solana.MSOLAsset, RedeemAssetKey: solana.SOLAsset, Network: &network, ContractAddress: &address, PriceExposureAsset: &exposure,
			IncomeSource: "combined", SourceURL: strings.Split(sourceURL, "?")[0], CollectionEnabled: true}
	}
	address := NativeAuthority
	return marketyield.YieldRouteDefinition{ProviderType: "protocol", Provider: "Marinade", ProductCode: "marinade-native", ProductName: "Marinade Native", YieldType: "native_staking",
		DepositAssetKey: solana.SOLAsset, PositionAssetKey: solana.SOLAsset, RedeemAssetKey: solana.SOLAsset, Network: &network, ContractAddress: &address, PriceExposureAsset: &exposure,
		IncomeSource: "combined", SourceURL: strings.Split(sourceURL, "?")[0], CollectionEnabled: true}
}

func numberUint(number json.Number, name string) (uint64, error) {
	raw := number.String()
	value, ok := new(big.Int).SetString(raw, 10)
	if !ok || value.Sign() < 0 || !value.IsUint64() {
		return 0, fmt.Errorf("%s is not an unsigned integer", name)
	}
	return value.Uint64(), nil
}
func numberDecimal(number json.Number, name string) (decimal.Decimal, error) {
	if number.String() == "" {
		return decimal.Decimal{}, fmt.Errorf("%s is missing", name)
	}
	value, err := decimal.NewFromString(number.String())
	if err != nil {
		return decimal.Decimal{}, fmt.Errorf("%s: %w", name, err)
	}
	return value, nil
}
