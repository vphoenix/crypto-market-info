package binance

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/exchange"
	"github.com/vphoenix/crypto-market-info/internal/model"
)

type Client struct {
	HTTP           *http.Client
	SpotBaseURL    string
	FuturesBaseURL string
	Retry          exchange.HTTPRetryConfig
	// SnapshotInterval is shared by every symbol using this client. Binance gives
	// depth snapshots a high request weight, so starts are serialized globally.
	SnapshotInterval time.Duration
	snapshotOnce     sync.Once
	snapshotGate     exchange.WaitGate
}

func NewClient() *Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSHandshakeTimeout = 30 * time.Second
	return &Client{
		HTTP:             &http.Client{Transport: transport, Timeout: 60 * time.Second},
		SpotBaseURL:      "https://api.binance.com",
		FuturesBaseURL:   "https://fapi.binance.com",
		Retry:            exchange.DefaultHTTPRetryConfig(),
		SnapshotInterval: time.Second,
	}
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
	if err := c.waitSnapshot(ctx); err != nil {
		return model.BookSnapshot{}, err
	}
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

func (c *Client) waitSnapshot(ctx context.Context) error {
	c.snapshotOnce.Do(func() {
		interval := c.SnapshotInterval
		if interval <= 0 {
			interval = time.Second
		}
		c.snapshotGate = exchange.NewRequestGate(interval)
	})
	return c.snapshotGate.Wait(ctx)
}
