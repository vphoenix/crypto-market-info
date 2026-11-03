// Package perpcontext keeps typed per-asset perpetual market context: mark,
// oracle and mid prices, premium, the current hourly funding rate, open
// interest and impact prices, for every listed asset of one venue dex at one
// collection time. It is a low-frequency observation, not an order book: it
// records the oracle-versus-mark relationship and premium dynamics that
// realized funding history (backfillable from the venue) does not keep.
//
// Version 1 covers Hyperliquid metaAndAssetCtxs: one complete response for
// one dex becomes one batch with one row per listed (not delisted) asset.
package perpcontext

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"
	"time"

	"github.com/shopspring/decimal"
)

const TableName = "hyperliquid_asset_context"

// Observation is one listed asset at one collection time. Prices are in the
// dex collateral (USDC) per whole coin, sizes in whole coins and volumes in
// collateral. FundingRateHourly is the source's current per-hour rate, not a
// realized settlement. When BookValid is 0 the source returned no mid price,
// premium or impact prices and those four columns are zero; a zero is then
// "absent", never a price.
type Observation struct {
	Dex               string          `ch:"dex"`
	Coin              string          `ch:"coin"`
	ObservationTime   time.Time       `ch:"observation_time"`
	UniverseCount     uint16          `ch:"universe_count"`
	BatchAssetCount   uint16          `ch:"batch_asset_count"`
	MaxLeverage       uint16          `ch:"max_leverage"`
	SzDecimals        uint8           `ch:"sz_decimals"`
	OnlyIsolated      uint8           `ch:"only_isolated"`
	MarkPx            decimal.Decimal `ch:"mark_px"`
	OraclePx          decimal.Decimal `ch:"oracle_px"`
	PrevDayPx         decimal.Decimal `ch:"prev_day_px"`
	FundingRateHourly decimal.Decimal `ch:"funding_rate_hourly"`
	OpenInterest      decimal.Decimal `ch:"open_interest"`
	DayNotionalVolume decimal.Decimal `ch:"day_ntl_vlm"`
	DayBaseVolume     decimal.Decimal `ch:"day_base_vlm"`
	BookValid         uint8           `ch:"book_valid"`
	MidPx             decimal.Decimal `ch:"mid_px"`
	Premium           decimal.Decimal `ch:"premium"`
	ImpactBidPx       decimal.Decimal `ch:"impact_bid_px"`
	ImpactAskPx       decimal.Decimal `ch:"impact_ask_px"`
	// PayloadHash is the raw SHA-256 of the complete response body shared by
	// every row of the batch.
	PayloadHash string `ch:"payload_hash"`
}

// Batch is one complete dex response. UniverseCount includes delisted assets
// that are not stored, so a reader can tell a filtered row from a lost one.
type Batch struct {
	Dex           string
	CollectedAt   time.Time
	UniverseCount uint16
	Assets        []Observation
}

type Collector interface {
	Collect(context.Context) (Batch, error)
}

type Sink interface {
	CheckHyperliquidContextSchema(context.Context) error
	WriteHyperliquidContextBatch(context.Context, Batch) error
}

var decimalLimit = decimal.New(1, 20)

// Validate checks a batch is complete, internally consistent and exactly
// representable as DECIMAL(38,18). It never rounds: a value with more than 18
// fractional digits rejects the whole batch instead of storing a different
// number.
func (b Batch) Validate() error {
	if !ValidDex(b.Dex) {
		return fmt.Errorf("invalid dex %q", b.Dex)
	}
	if b.CollectedAt.IsZero() || !b.CollectedAt.Equal(time.UnixMilli(b.CollectedAt.UnixMilli()).UTC()) || b.CollectedAt.Location() != time.UTC {
		return fmt.Errorf("collected_at must be a non-zero UTC millisecond time")
	}
	if len(b.Assets) == 0 || len(b.Assets) > int(b.UniverseCount) {
		return fmt.Errorf("batch requires 1 to universe_count (%d) assets, got %d", b.UniverseCount, len(b.Assets))
	}
	hash := b.Assets[0].PayloadHash
	if len(hash) != sha256.Size || hash == string(make([]byte, sha256.Size)) {
		return fmt.Errorf("payload_hash must be a non-zero 32-byte SHA-256")
	}
	seen := make(map[string]struct{}, len(b.Assets))
	for index, a := range b.Assets {
		if err := a.validate(b, hash); err != nil {
			return fmt.Errorf("asset %d (%s): %w", index, a.Coin, err)
		}
		if _, ok := seen[a.Coin]; ok {
			return fmt.Errorf("asset %d duplicates coin %s", index, a.Coin)
		}
		seen[a.Coin] = struct{}{}
	}
	return nil
}

func (a Observation) validate(b Batch, hash string) error {
	if a.Dex != b.Dex {
		return fmt.Errorf("dex %q differs from batch dex %q", a.Dex, b.Dex)
	}
	if !a.ObservationTime.Equal(b.CollectedAt) || a.ObservationTime.Location() != time.UTC {
		return fmt.Errorf("observation_time differs from batch collected_at")
	}
	if a.UniverseCount != b.UniverseCount || a.BatchAssetCount != uint16(len(b.Assets)) {
		return fmt.Errorf("universe_count %d / batch_asset_count %d differ from batch %d / %d", a.UniverseCount, a.BatchAssetCount, b.UniverseCount, len(b.Assets))
	}
	if a.PayloadHash != hash {
		return fmt.Errorf("payload_hash differs within batch")
	}
	if !ValidCoin(b.Dex, a.Coin) {
		return fmt.Errorf("invalid coin name for dex %q", b.Dex)
	}
	if a.MaxLeverage == 0 || a.SzDecimals > 18 || a.OnlyIsolated > 1 || a.BookValid > 1 {
		return fmt.Errorf("invalid max_leverage %d, sz_decimals %d, only_isolated %d or book_valid %d", a.MaxLeverage, a.SzDecimals, a.OnlyIsolated, a.BookValid)
	}
	for _, f := range []struct {
		name   string
		value  decimal.Decimal
		signed bool
	}{
		{"mark_px", a.MarkPx, false}, {"oracle_px", a.OraclePx, false}, {"prev_day_px", a.PrevDayPx, false},
		{"funding_rate_hourly", a.FundingRateHourly, true}, {"open_interest", a.OpenInterest, false},
		{"day_ntl_vlm", a.DayNotionalVolume, false}, {"day_base_vlm", a.DayBaseVolume, false},
		{"mid_px", a.MidPx, false}, {"premium", a.Premium, true},
		{"impact_bid_px", a.ImpactBidPx, false}, {"impact_ask_px", a.ImpactAskPx, false},
	} {
		if err := exactStorable(f.value, f.signed); err != nil {
			return fmt.Errorf("%s: %w", f.name, err)
		}
	}
	if !a.MarkPx.IsPositive() || !a.OraclePx.IsPositive() {
		return fmt.Errorf("mark_px and oracle_px must be positive")
	}
	if a.BookValid == 1 {
		if !a.MidPx.IsPositive() || !a.ImpactBidPx.IsPositive() || !a.ImpactAskPx.IsPositive() {
			return fmt.Errorf("book_valid row requires positive mid and impact prices")
		}
	} else if !a.MidPx.IsZero() || !a.Premium.IsZero() || !a.ImpactBidPx.IsZero() || !a.ImpactAskPx.IsZero() {
		return fmt.Errorf("absent book fields must be stored as zero")
	}
	return nil
}

func exactStorable(d decimal.Decimal, signed bool) error {
	if d.IsNegative() && !signed {
		return fmt.Errorf("negative value %s", d)
	}
	if d.Abs().GreaterThanOrEqual(decimalLimit) {
		return fmt.Errorf("value %s exceeds DECIMAL(38,18)", d)
	}
	if !d.Equal(d.Truncate(18)) {
		return fmt.Errorf("value %s has more than 18 fractional digits", d)
	}
	return nil
}

// ValidDex accepts "" (the validator-operated default dex) or a deployer dex
// name of 1-16 lowercase ASCII letters or digits.
func ValidDex(dex string) bool {
	if len(dex) > 16 {
		return false
	}
	for i := 0; i < len(dex); i++ {
		if !(dex[i] >= 'a' && dex[i] <= 'z' || dex[i] >= '0' && dex[i] <= '9') {
			return false
		}
	}
	return true
}

// ValidCoin requires printable ASCII without spaces, 1-64 bytes. Default-dex
// coins carry no prefix; deployer-dex coins are "<dex>:<name>" as the source
// returns them.
func ValidCoin(dex, coin string) bool {
	if len(coin) == 0 || len(coin) > 64 {
		return false
	}
	for i := 0; i < len(coin); i++ {
		if coin[i] < 0x21 || coin[i] > 0x7e {
			return false
		}
	}
	if dex == "" {
		return !strings.Contains(coin, ":")
	}
	name, ok := strings.CutPrefix(coin, dex+":")
	return ok && name != "" && !strings.Contains(name, ":")
}
