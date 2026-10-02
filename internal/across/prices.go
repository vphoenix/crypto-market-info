package across

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"time"

	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
)

type BBO struct {
	Symbol                   string
	Bid, Ask, BidQty, AskQty decimal.Decimal
	RequestedAt, AvailableAt time.Time
	PayloadHash              string
}
type Prices struct {
	URL       string
	HTTP      *http.Client
	Archive   ethereum.Archive
	ETH, USDC *BBO
	next      time.Time
}

var pricePattern = regexp.MustCompile(`^(0|[1-9][0-9]*)(\.[0-9]{1,18})?$`)

func parsePrice(s string) (decimal.Decimal, error) {
	if !pricePattern.MatchString(s) {
		return decimal.Zero, errors.New("invalid_decimal_price")
	}
	d, e := decimal.NewFromString(s)
	if e != nil || !d.IsPositive() || d.GreaterThanOrEqual(decimal.New(1, 20)) {
		return decimal.Zero, errors.New("invalid_decimal_price")
	}
	return d, nil
}
func ParseBBO(raw []byte, symbol string) (BBO, error) {
	var v struct{ Symbol, BidPrice, AskPrice, BidQty, AskQty string }
	var out BBO
	if e := json.Unmarshal(raw, &v); e != nil || v.Symbol != symbol {
		return out, errors.New("invalid_bbo_symbol_or_response")
	}
	var e error
	out.Symbol = symbol
	for _, x := range []struct {
		s string
		p *decimal.Decimal
	}{{v.BidPrice, &out.Bid}, {v.AskPrice, &out.Ask}, {v.BidQty, &out.BidQty}, {v.AskQty, &out.AskQty}} {
		*x.p, e = parsePrice(x.s)
		if e != nil {
			return out, e
		}
	}
	if out.Bid.GreaterThan(out.Ask) {
		return out, errors.New("crossed_bbo")
	}
	return out, nil
}
func (p *Prices) fetch(ctx context.Context, symbol string) (*BBO, error) {
	base := p.URL
	if base == "" {
		base = "https://api.binance.com"
	}
	u, e := url.Parse(base)
	if e != nil || u.Scheme != "https" || u.Host == "" || u.User != nil || u.RawQuery != "" {
		return nil, errors.New("invalid_public_price_url")
	}
	u.Path = "/api/v3/ticker/bookTicker"
	u.RawQuery = url.Values{"symbol": []string{symbol}}.Encode()
	at := Now()
	req, e := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if e != nil {
		return nil, errors.New("price_request")
	}
	req.Header.Set("User-Agent", "crypto-market-info/1.0")
	client := p.HTTP
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	resp, e := client.Do(req)
	if e != nil {
		return nil, errors.New("price_transport")
	}
	defer resp.Body.Close()
	raw, e := io.ReadAll(io.LimitReader(resp.Body, 65537))
	if e != nil || len(raw) > 65536 {
		return nil, errors.New("price_response_size_or_read")
	}
	available := Now()
	h, e := p.Archive.PutObject(struct {
		Source, Symbol           string
		RequestedAt, AvailableAt time.Time
		HTTPStatus               int
		Response                 []byte
	}{u.Hostname(), symbol, at, available, resp.StatusCode, raw})
	if e != nil {
		return nil, e
	}
	if resp.StatusCode != 200 {
		return nil, errors.New("price_http_failure")
	}
	b, e := ParseBBO(raw, symbol)
	if e != nil {
		return nil, e
	}
	b.RequestedAt = at
	b.AvailableAt = available
	b.PayloadHash = string(h[:])
	return &b, nil
}
func (p *Prices) Refresh(ctx context.Context) {
	if Now().Before(p.next) {
		return
	}
	p.next = Now().Add(time.Minute)
	// Independent sources: a failed refresh never changes a cached observation time.
	if b, e := p.fetch(ctx, "ETHUSDT"); e == nil {
		p.ETH = b
	}
	if b, e := p.fetch(ctx, "USDCUSDT"); e == nil {
		p.USDC = b
	}
}
func (p *Prices) Attach(v *OrderProbe) {
	fresh := func(b *BBO) bool {
		return b != nil && !v.AvailableAt.Before(b.AvailableAt) && v.AvailableAt.Sub(b.AvailableAt) <= time.Minute
	}
	if fresh(p.ETH) {
		b := p.ETH
		v.EthUsdtAsk = Ptr(b.Ask)
		v.EthUsdtAskQty = Ptr(b.AskQty)
		v.EthPriceRequestedAt = Ptr(b.RequestedAt)
		v.EthPriceAvailableAt = Ptr(b.AvailableAt)
		v.EthPricePayloadHash = Ptr(b.PayloadHash)
	}
	if fresh(p.USDC) {
		b := p.USDC
		v.UsdcUsdtBid = Ptr(b.Bid)
		v.UsdcUsdtAsk = Ptr(b.Ask)
		v.UsdcUsdtBidQty = Ptr(b.BidQty)
		v.UsdcUsdtAskQty = Ptr(b.AskQty)
		v.UsdcPriceRequestedAt = Ptr(b.RequestedAt)
		v.UsdcPriceAvailableAt = Ptr(b.AvailableAt)
		v.UsdcPricePayloadHash = Ptr(b.PayloadHash)
	}
}
