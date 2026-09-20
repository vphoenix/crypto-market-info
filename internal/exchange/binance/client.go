package binance

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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
	SnapshotInterval   time.Duration
	snapshotOnce       sync.Once
	snapshotGate       exchange.WaitGate
	snapshotMu         sync.Mutex
	lastSnapshotStart  time.Time
	ConnectionInterval time.Duration
	connectionOnce     sync.Once
	connectionGate     exchange.WaitGate
	requestID          atomic.Int64
}

func NewClient() *Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSHandshakeTimeout = 30 * time.Second
	return &Client{
		HTTP:               &http.Client{Transport: transport, Timeout: 60 * time.Second},
		SpotBaseURL:        "https://api.binance.com",
		FuturesBaseURL:     "https://fapi.binance.com",
		Retry:              exchange.DefaultHTTPRetryConfig(),
		SnapshotInterval:   time.Second,
		ConnectionInterval: time.Second,
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
	return c.depthSnapshot(ctx, instrument, nil)
}

// depthSnapshot serializes all symbols. A manager supplies arm to begin its
// generation only after both HTTP cooldown and snapshot pacing have passed.
func (c *Client) depthSnapshot(ctx context.Context, instrument model.Instrument, arm func() error) (model.BookSnapshot, error) {
	c.snapshotMu.Lock()
	defer c.snapshotMu.Unlock()
	path, base := "/api/v3/depth", c.SpotBaseURL
	if instrument.MarketType == model.MarketPerpetual {
		path, base = "/fapi/v1/depth", c.FuturesBaseURL
	}
	values := url.Values{"symbol": []string{instrument.ExchangeSymbol}, "limit": []string{strconv.Itoa(1000)}}
	retry := c.Retry
	if arm != nil {
		retry.MaxAttempts = 1 // Retries return to the manager's fair target queue.
	}
	retry.BeforeRequest = func(ctx context.Context) error {
		if err := c.waitSnapshot(ctx); err != nil {
			return err
		}
		if c.Retry.Cooldown != nil {
			if err := c.Retry.Cooldown.Wait(ctx); err != nil {
				return err
			}
		}
		if c.Retry.BeforeRequest != nil {
			if err := c.Retry.BeforeRequest(ctx); err != nil {
				return err
			}
		}
		// Pacing is measured at the actual send boundary too: a long cooldown
		// may have consumed a previously reserved request-gate slot.
		interval := c.SnapshotInterval
		if interval <= 0 {
			interval = time.Second
		}
		if !exchange.Wait(ctx, time.Until(c.lastSnapshotStart.Add(interval))) {
			return ctx.Err()
		}
		if c.Retry.Cooldown != nil {
			if err := c.Retry.Cooldown.Wait(ctx); err != nil {
				return err
			}
		}
		if arm != nil {
			if err := arm(); err != nil {
				return err
			}
		}
		c.lastSnapshotStart = time.Now()
		return ctx.Err()
	}
	payload, err := exchange.Get(ctx, c.HTTP, strings.TrimRight(base, "/")+path+"?"+values.Encode(), retry)
	if err != nil {
		return model.BookSnapshot{}, err
	}
	return ParseDepthSnapshot(payload, instrument, time.Now().UTC())
}

func (c *Client) WebsocketConnectGate() exchange.WaitGate {
	c.connectionOnce.Do(func() {
		interval := c.ConnectionInterval
		if interval <= 0 {
			interval = time.Second
		}
		c.connectionGate = exchange.NewRequestGate(interval)
	})
	return c.connectionGate
}

func (c *Client) waitWebsocket(ctx context.Context) error {
	// A positive independent jitter spreads reconnect waves across processes.
	if !exchange.Wait(ctx, exchange.AddJitter(100*time.Millisecond)) {
		return ctx.Err()
	}
	return c.WebsocketConnectGate().Wait(ctx)
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
