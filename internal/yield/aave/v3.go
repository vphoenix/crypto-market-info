package aave

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/exchange"
	marketyield "github.com/vphoenix/crypto-market-info/internal/yield"
)

const (
	DefaultV3Endpoint = "https://api.v3.aave.com/graphql"
	v3Pool            = "0x794a61358d6845594f94dc1db02a252b5b4814ad"
	v3AToken          = "0x6d80113e533a2c0fe82eabd35f1875dcea89ea97"
	v3Query           = `query {
  market(request: {address: "0x794a61358D6845594F94dc1DB02A252b5b4814aD", chainId: 43114}) {
    address chain { chainId }
    reserves { underlyingToken { address decimals } aToken { address decimals } }
  }
  supplyAPYHistory(request: {
    chainId: 43114, underlyingToken: "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7",
    market: "0x794a61358D6845594F94dc1DB02A252b5b4814aD", window: LAST_WEEK
  }) { date avgRate { raw decimals } }
}`
)

type V3Collector struct {
	Endpoint   string
	HTTPClient *http.Client
	Retry      exchange.HTTPRetryConfig
	Now        func() time.Time
}

func NewV3Collector(endpoint string) *V3Collector {
	if strings.TrimSpace(endpoint) == "" {
		endpoint = DefaultV3Endpoint
	}
	retry := exchange.DefaultHTTPRetryConfig()
	retry.Cooldown = exchange.NewRequestGate(time.Second)
	return &V3Collector{Endpoint: endpoint, HTTPClient: &http.Client{Timeout: 20 * time.Second}, Retry: retry}
}

type v3Response struct {
	Errors []json.RawMessage `json:"errors"`
	Data   *struct {
		Market *struct {
			Address string `json:"address"`
			Chain   struct {
				ChainID int `json:"chainId"`
			} `json:"chain"`
			Reserves []struct {
				UnderlyingToken v3Token `json:"underlyingToken"`
				AToken          v3Token `json:"aToken"`
			} `json:"reserves"`
		} `json:"market"`
		History []struct {
			Date    time.Time `json:"date"`
			AvgRate struct {
				Raw      string `json:"raw"`
				Decimals int    `json:"decimals"`
			} `json:"avgRate"`
		} `json:"supplyAPYHistory"`
	} `json:"data"`
}

type v3Token struct {
	Address  string `json:"address"`
	Decimals int    `json:"decimals"`
}

func (c *V3Collector) Collect(ctx context.Context) (marketyield.Batch, error) {
	if c == nil || strings.TrimSpace(c.Endpoint) == "" {
		return marketyield.Batch{}, fmt.Errorf("Aave V3 collector is not configured")
	}
	now := c.Now
	if now == nil {
		now = time.Now
	}
	collected := now().UTC().Truncate(time.Millisecond)
	var response v3Response
	hash, err := query(ctx, c.HTTPClient, c.Endpoint, v3Query, c.Retry, &response)
	if err != nil {
		return marketyield.Batch{}, fmt.Errorf("Aave V3 history: %w", err)
	}
	if len(response.Errors) != 0 || response.Data == nil || response.Data.Market == nil {
		return marketyield.Batch{}, fmt.Errorf("Aave V3 response has GraphQL errors or missing data/market")
	}
	market := response.Data.Market
	if !strings.EqualFold(market.Address, v3Pool) || market.Chain.ChainID != chainID {
		return marketyield.Batch{}, fmt.Errorf("Aave V3 market identity mismatch")
	}
	matches := 0
	for _, reserve := range market.Reserves {
		if !strings.EqualFold(reserve.UnderlyingToken.Address, wavax) {
			continue
		}
		matches++
		if reserve.UnderlyingToken.Decimals != 18 || reserve.AToken.Decimals != 18 || !strings.EqualFold(reserve.AToken.Address, v3AToken) {
			return marketyield.Batch{}, fmt.Errorf("Aave V3 WAVAX token identity mismatch")
		}
	}
	if matches != 1 {
		return marketyield.Batch{}, fmt.Errorf("Aave V3 expected exactly one WAVAX reserve, got %d", matches)
	}
	points := make([]ratePoint, 0, len(response.Data.History))
	for i, point := range response.Data.History {
		if point.AvgRate.Decimals != 27 || !unsignedInteger(point.AvgRate.Raw) {
			return marketyield.Batch{}, fmt.Errorf("Aave V3 point %d has invalid raw/decimals", i)
		}
		rate, err := scaledRate(point.AvgRate.Raw, -27)
		if err != nil {
			return marketyield.Batch{}, fmt.Errorf("Aave V3 point %d: %w", i, err)
		}
		points = append(points, ratePoint{at: point.Date, rate: rate})
	}
	definition := route("avalanche-v3-wavax-supply", "Aave V3 WAVAX 历史基础 APY", "eip155:43114:erc20:"+v3AToken, v3Pool, c.Endpoint)
	return historyBatch("aave-v3-avax", definition, collected, points, hash, marketyield.Ptr(decimal.NewFromInt(1)))
}

func unsignedInteger(text string) bool {
	if text == "" {
		return false
	}
	for _, digit := range text {
		if digit < '0' || digit > '9' {
			return false
		}
	}
	return true
}
