package okx

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/exchange"
	"github.com/vphoenix/crypto-market-info/internal/model"
)

type Client struct {
	HTTP    *http.Client
	BaseURL string
	Retry   exchange.HTTPRetryConfig
}

func NewClient() *Client {
	return &Client{HTTP: &http.Client{Timeout: 15 * time.Second}, BaseURL: "https://www.okx.com", Retry: exchange.DefaultHTTPRetryConfig()}
}

func (c *Client) Instruments(ctx context.Context, marketType model.MarketType) ([]model.Instrument, error) {
	instType := "SPOT"
	if marketType == model.MarketPerpetual {
		instType = "SWAP"
	} else if marketType != model.MarketSpot {
		return nil, fmt.Errorf("unsupported OKX market type %q", marketType)
	}
	values := url.Values{"instType": []string{instType}}
	payload, err := exchange.Get(ctx, c.HTTP, strings.TrimRight(c.BaseURL, "/")+"/api/v5/public/instruments?"+values.Encode(), c.Retry)
	if err != nil {
		return nil, err
	}
	return ParseInstruments(payload, marketType)
}
