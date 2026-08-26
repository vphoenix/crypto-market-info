package aave

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/exchange"
	marketyield "github.com/vphoenix/crypto-market-info/internal/yield"
)

const (
	DefaultV4Endpoint = "https://api.v4.aave.com/graphql"
	v4Spoke           = "0x435272ceff93a1e657e8abfdf0a13e95900a3a56"
	v4ReserveID       = "NDMxMTQ6OjB4NDM1MjcyQ2VmRjkzYTFFNjU3RThBQmZkZjBBMTNlOTU5MDBBM2E1Njo6MA=="
	v4Query           = `query {
  reserve(request: {query: {reserveInput: {
    chainId: 43114, spoke: "0x435272CefF93a1E657E8ABfdf0A13e95900A3a56", onChainId: "0"
  }}}) {
    id onChainId chain { chainId } spoke { address }
    asset { underlying { address info { decimals } } }
  }
  supplyApyHistory(request: {
    reserve: "NDMxMTQ6OjB4NDM1MjcyQ2VmRjkzYTFFNjU3RThBQmZkZjBBMTNlOTU5MDBBM2E1Njo6MA==",
    window: LAST_WEEK, includeRewards: false
  }) { date avgRate { normalized } }
}`
)

type V4Collector struct {
	Endpoint   string
	HTTPClient *http.Client
	Retry      exchange.HTTPRetryConfig
	Now        func() time.Time
}

func NewV4Collector(endpoint string) *V4Collector {
	if strings.TrimSpace(endpoint) == "" {
		endpoint = DefaultV4Endpoint
	}
	retry := exchange.DefaultHTTPRetryConfig()
	retry.Cooldown = exchange.NewRequestGate(time.Second)
	return &V4Collector{Endpoint: endpoint, HTTPClient: &http.Client{Timeout: 20 * time.Second}, Retry: retry}
}

type v4Response struct {
	Errors []json.RawMessage `json:"errors"`
	Data   *struct {
		Reserve *struct {
			ID        string `json:"id"`
			OnChainID string `json:"onChainId"`
			Chain     struct {
				ChainID int `json:"chainId"`
			} `json:"chain"`
			Spoke struct {
				Address string `json:"address"`
			} `json:"spoke"`
			Asset struct {
				Underlying struct {
					Address string `json:"address"`
					Info    struct {
						Decimals int `json:"decimals"`
					} `json:"info"`
				} `json:"underlying"`
			} `json:"asset"`
		} `json:"reserve"`
		History []struct {
			Date    time.Time `json:"date"`
			AvgRate struct {
				Normalized string `json:"normalized"`
			} `json:"avgRate"`
		} `json:"supplyApyHistory"`
	} `json:"data"`
}

func (c *V4Collector) Collect(ctx context.Context) (marketyield.Batch, error) {
	if c == nil || strings.TrimSpace(c.Endpoint) == "" {
		return marketyield.Batch{}, fmt.Errorf("Aave V4 collector is not configured")
	}
	now := c.Now
	if now == nil {
		now = time.Now
	}
	collected := now().UTC().Truncate(time.Millisecond)
	var response v4Response
	hash, err := query(ctx, c.HTTPClient, c.Endpoint, v4Query, c.Retry, &response)
	if err != nil {
		return marketyield.Batch{}, fmt.Errorf("Aave V4 history: %w", err)
	}
	if len(response.Errors) != 0 || response.Data == nil || response.Data.Reserve == nil {
		return marketyield.Batch{}, fmt.Errorf("Aave V4 response has GraphQL errors or missing data/reserve")
	}
	reserve := response.Data.Reserve
	if reserve.ID != v4ReserveID || reserve.OnChainID != "0" || reserve.Chain.ChainID != chainID || !strings.EqualFold(reserve.Spoke.Address, v4Spoke) ||
		!strings.EqualFold(reserve.Asset.Underlying.Address, wavax) || reserve.Asset.Underlying.Info.Decimals != 18 {
		return marketyield.Batch{}, fmt.Errorf("Aave V4 reserve identity mismatch")
	}
	points := make([]ratePoint, 0, len(response.Data.History))
	for i, point := range response.Data.History {
		rate, err := scaledRate(point.AvgRate.Normalized, -2)
		if err != nil {
			return marketyield.Batch{}, fmt.Errorf("Aave V4 point %d: %w", i, err)
		}
		points = append(points, ratePoint{at: point.Date, rate: rate})
	}
	definition := route("avalanche-v4-main-wavax-supply", "Aave V4 Main WAVAX 历史基础 APY", "eip155:43114:protocol-position:aave-v4:"+v4Spoke+":0", v4Spoke, c.Endpoint)
	return historyBatch("aave-v4-avax", definition, collected, points, hash, nil)
}
