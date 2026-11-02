package justlend

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/exchange"
	"github.com/vphoenix/crypto-market-info/internal/lendingmarket"
	marketyield "github.com/vphoenix/crypto-market-info/internal/yield"
)

const jusdtAddress = "TXJgMdjVX5dKiQaUi9QobwNxtSQaFqccvd"

// A response missing any of these markets is treated as incomplete, never as
// a smaller market list.
var requiredMarkets = map[string]string{jtrxAddress: "jTRX", jstrxAddress: "jsTRX", jusdtAddress: "jUSDT"}

var sourceInteger = regexp.MustCompile(`^[0-9]+$`)

// MarketCollector reads the full /lend/jtoken market list, borrow side
// included. The source carries no timestamp or block, so observation_time is
// the local time the complete response was received.
type MarketCollector struct {
	Client *Client
	Now    func() time.Time
}

func (c *MarketCollector) Collect(ctx context.Context) (lendingmarket.Batch, error) {
	if c == nil || c.Client == nil || c.Client.BaseURL == "" {
		return lendingmarket.Batch{}, fmt.Errorf("JustLend market collector requires a client")
	}
	raw, err := exchange.Get(ctx, c.Client.HTTP, c.Client.BaseURL+"/lend/jtoken", c.Client.Retry)
	if err != nil {
		return lendingmarket.Batch{}, fmt.Errorf("JustLend GET /lend/jtoken: %w", err)
	}
	now := c.Now
	if now == nil {
		now = time.Now
	}
	return ParseMarkets(raw, time.UnixMilli(now().UnixMilli()).UTC())
}

// ParseMarkets converts one complete response body. Every market must carry
// all sixteen fields with the expected JSON types; unknown extra fields are
// covered by the payload hash but not stored.
func ParseMarkets(raw []byte, collected time.Time) (lendingmarket.Batch, error) {
	var envelope struct {
		Code    *int   `json:"code"`
		Message string `json:"message"`
		Data    *struct {
			TokenList []map[string]json.RawMessage `json:"tokenList"`
		} `json:"data"`
	}
	if err := marketyield.DecodeJSON(raw, &envelope); err != nil {
		return lendingmarket.Batch{}, fmt.Errorf("decode /lend/jtoken: %w", err)
	}
	if envelope.Code == nil {
		return lendingmarket.Batch{}, fmt.Errorf("/lend/jtoken missing business code")
	}
	if *envelope.Code != 0 {
		return lendingmarket.Batch{}, fmt.Errorf("/lend/jtoken business code %d: %s", *envelope.Code, envelope.Message)
	}
	if envelope.Data == nil || envelope.Data.TokenList == nil {
		return lendingmarket.Batch{}, fmt.Errorf("/lend/jtoken missing tokenList")
	}
	list := envelope.Data.TokenList
	if len(list) == 0 || len(list) > 65535 {
		return lendingmarket.Batch{}, fmt.Errorf("/lend/jtoken tokenList has %d markets", len(list))
	}
	digest := sha256.Sum256(raw)
	batch := lendingmarket.Batch{CollectedAt: collected, Markets: make([]lendingmarket.Observation, 0, len(list))}
	found := map[string]bool{}
	for index, item := range list {
		m, err := parseMarket(item)
		if err != nil {
			return lendingmarket.Batch{}, fmt.Errorf("tokenList[%d]: %w", index, err)
		}
		if symbol, ok := requiredMarkets[m.MarketAddress]; ok {
			if m.MarketSymbol != symbol {
				return lendingmarket.Batch{}, fmt.Errorf("market %s symbol %q, expected %q", m.MarketAddress, m.MarketSymbol, symbol)
			}
			found[m.MarketAddress] = true
		}
		m.ObservationTime = collected
		m.BatchMarketCount = uint16(len(list))
		m.PayloadHash = string(digest[:])
		batch.Markets = append(batch.Markets, m)
	}
	for address, symbol := range requiredMarkets {
		if !found[address] {
			return lendingmarket.Batch{}, fmt.Errorf("/lend/jtoken missing required market %s (%s)", symbol, address)
		}
	}
	return batch, batch.Validate()
}

func parseMarket(item map[string]json.RawMessage) (lendingmarket.Observation, error) {
	var m lendingmarket.Observation
	for _, f := range []struct {
		key    string
		target *string
	}{
		{"address", &m.MarketAddress}, {"symbol", &m.MarketSymbol},
		{"underlyingAddress", &m.UnderlyingAddress}, {"underlyingSymbol", &m.UnderlyingSymbol},
	} {
		value, err := jsonString(item, f.key)
		if err != nil {
			return m, err
		}
		*f.target = value
	}
	rawDecimals, ok := item["underlyingDecimal"]
	if !ok {
		return m, fmt.Errorf("missing underlyingDecimal")
	}
	decimals, err := strconv.ParseUint(string(rawDecimals), 10, 8)
	if err != nil {
		return m, fmt.Errorf("underlyingDecimal %s is not a JSON integer 0-255", rawDecimals)
	}
	m.UnderlyingDecimals = uint8(decimals)
	for _, f := range []struct {
		key    string
		target *decimal.Decimal
	}{
		{"supplyRate", &m.SupplyRateAPR}, {"borrowRate", &m.BorrowRateAPR}, {"cash", &m.Cash},
		{"totalBorrows", &m.TotalBorrows}, {"reserves", &m.Reserves}, {"totalSupply", &m.TotalSupply},
		{"exchangeRate", &m.ExchangeRate}, {"collateralFactor", &m.CollateralFactor},
		{"reserveFactor", &m.ReserveFactor}, {"underlyingPriceInTrx", &m.UnderlyingPriceTRX},
	} {
		value, err := jsonString(item, f.key)
		if err != nil {
			return m, err
		}
		if *f.target, err = parseDecimal(value); err != nil {
			return m, fmt.Errorf("%s: %w", f.key, err)
		}
	}
	// borrowIndex is the raw 1e18-scaled integer; store it divided by 1e18,
	// which is exact and keeps it inside DECIMAL(38,18).
	index, err := jsonString(item, "borrowIndex")
	if err != nil {
		return m, err
	}
	if !sourceInteger.MatchString(index) {
		return m, fmt.Errorf("borrowIndex %q is not a non-negative integer", index)
	}
	raw, err := decimal.NewFromString(index)
	if err != nil {
		return m, fmt.Errorf("borrowIndex %q: %w", index, err)
	}
	m.BorrowIndex = raw.Shift(-18)
	return m, nil
}

func jsonString(item map[string]json.RawMessage, key string) (string, error) {
	raw, ok := item[key]
	if !ok || string(raw) == "null" {
		return "", fmt.Errorf("missing %s", key)
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return "", fmt.Errorf("%s must be a JSON string", key)
	}
	return value, nil
}
