package universe

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/vphoenix/crypto-market-info/internal/model"
)

var basePattern = regexp.MustCompile(`^[A-Z0-9][A-Z0-9._]{0,31}$`)

type PerpetualMarketKey struct {
	BaseAsset   string           `json:"base_asset"`
	QuoteAsset  string           `json:"quote_asset"`
	SettleAsset string           `json:"settle_asset"`
	MarketType  model.MarketType `json:"market_type"`
}

func ParsePerpetualMarketKey(raw string) (PerpetualMarketKey, error) {
	base, ok := strings.CutSuffix(raw, "-USDT-PERP")
	if !ok || !basePattern.MatchString(base) {
		return PerpetualMarketKey{}, fmt.Errorf("invalid canonical perpetual key %q", raw)
	}
	return PerpetualMarketKey{base, "USDT", "USDT", model.MarketPerpetual}, nil
}

func (k PerpetualMarketKey) String() string { return k.BaseAsset + "-USDT-PERP" }

func Eligible(i model.Instrument) bool {
	return i.MarketType == model.MarketPerpetual && i.QuoteAsset == "USDT" && i.SettleAsset != nil && *i.SettleAsset == "USDT" && i.ExpiryTime == nil
}
