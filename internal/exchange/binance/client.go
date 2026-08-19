package binance

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/exchange"
	"github.com/vphoenix/crypto-market-info/internal/model"
)

type Client struct {
	HTTP           *http.Client
	SpotBaseURL    string
	FuturesBaseURL string
	Retry          exchange.HTTPRetryConfig
}

func NewClient() *Client {
	return &Client{HTTP: &http.Client{Timeout: 15 * time.Second}, SpotBaseURL: "https://api.binance.com", FuturesBaseURL: "https://fapi.binance.com", Retry: exchange.DefaultHTTPRetryConfig()}
}

func (c *Client) Instruments(ctx context.Context, marketType model.MarketType) ([]model.Instrument, error) {
	path, base := "", ""
	switch marketType {
	case model.MarketSpot:
		path = "/api/v3/exchangeInfo"
		base = c.SpotBaseURL
	case model.MarketPerpetual:
		path = "/fapi/v1/exchangeInfo"
		base = c.FuturesBaseURL
	default:
		return nil, fmt.Errorf("unsupported Binance market type %q", marketType)
	}
	payload, err := exchange.Get(ctx, c.HTTP, strings.TrimRight(base, "/")+path, c.Retry)
	if err != nil {
		return nil, err
	}
	return ParseExchangeInfo(payload, marketType)
}

func (c *Client) DepthSnapshot(ctx context.Context, instrument model.Instrument) (model.BookSnapshot, error) {
	path, base := "/api/v3/depth", c.SpotBaseURL
	if instrument.MarketType == model.MarketPerpetual {
		path, base = "/fapi/v1/depth", c.FuturesBaseURL
	}
	values := url.Values{"symbol": []string{instrument.ExchangeSymbol}, "limit": []string{strconv.Itoa(1000)}}
	payload, err := exchange.Get(ctx, c.HTTP, strings.TrimRight(base, "/")+path+"?"+values.Encode(), c.Retry)
	if err != nil {
		return model.BookSnapshot{}, err
	}
	return ParseDepthSnapshot(payload, instrument, time.Now().UTC())
}
