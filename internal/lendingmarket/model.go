// Package lendingmarket keeps typed lending-market state, including the borrow
// side. A borrow rate is a cost of a position, not a deposit yield route, so it
// is stored apart from yield_observation and joined only at analysis time.
//
// Version 1 covers JustLend (TRON) jToken markets: one complete /lend/jtoken
// response becomes one batch with one row per market at the collection time.
package lendingmarket

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/shopspring/decimal"
)

const TableName = "justlend_market_observation"

// Observation is one jToken market at one collection time. Amounts are in
// whole underlying units (cash, total_borrows, reserves) or whole jToken units
// (total_supply). Rates are simple APR: per-block rate × 10,512,000 blocks.
type Observation struct {
	MarketAddress      string          `ch:"market_address"`
	ObservationTime    time.Time       `ch:"observation_time"`
	BatchMarketCount   uint16          `ch:"batch_market_count"`
	MarketSymbol       string          `ch:"market_symbol"`
	UnderlyingAddress  string          `ch:"underlying_address"`
	UnderlyingSymbol   string          `ch:"underlying_symbol"`
	UnderlyingDecimals uint8           `ch:"underlying_decimals"`
	SupplyRateAPR      decimal.Decimal `ch:"supply_rate_apr"`
	BorrowRateAPR      decimal.Decimal `ch:"borrow_rate_apr"`
	Cash               decimal.Decimal `ch:"cash"`
	TotalBorrows       decimal.Decimal `ch:"total_borrows"`
	Reserves           decimal.Decimal `ch:"reserves"`
	TotalSupply        decimal.Decimal `ch:"total_supply"`
	ExchangeRate       decimal.Decimal `ch:"exchange_rate"`
	BorrowIndex        decimal.Decimal `ch:"borrow_index"`
	CollateralFactor   decimal.Decimal `ch:"collateral_factor"`
	ReserveFactor      decimal.Decimal `ch:"reserve_factor"`
	UnderlyingPriceTRX decimal.Decimal `ch:"underlying_price_trx"`
	// PayloadHash is the raw SHA-256 of the complete response body shared by
	// every row of the batch.
	PayloadHash string `ch:"payload_hash"`
}

type Batch struct {
	CollectedAt time.Time
	Markets     []Observation
}

type Collector interface {
	Collect(context.Context) (Batch, error)
}

type Sink interface {
	CheckJustLendMarketSchema(context.Context) error
	WriteJustLendMarketBatch(context.Context, Batch) error
}

var decimalLimit = decimal.New(1, 20)

// Validate checks a batch is complete, internally consistent and exactly
// representable as DECIMAL(38,18). It never rounds: a value with more than 18
// fractional digits rejects the whole batch instead of storing a different
// number.
func (b Batch) Validate() error {
	if b.CollectedAt.IsZero() || !b.CollectedAt.Equal(time.UnixMilli(b.CollectedAt.UnixMilli()).UTC()) || b.CollectedAt.Location() != time.UTC {
		return fmt.Errorf("collected_at must be a non-zero UTC millisecond time")
	}
	if len(b.Markets) == 0 || len(b.Markets) > 65535 {
		return fmt.Errorf("batch requires 1 to 65535 markets, got %d", len(b.Markets))
	}
	hash := b.Markets[0].PayloadHash
	if len(hash) != sha256.Size || hash == string(make([]byte, sha256.Size)) {
		return fmt.Errorf("payload_hash must be a non-zero 32-byte SHA-256")
	}
	seen := make(map[string]struct{}, len(b.Markets))
	for index, m := range b.Markets {
		if err := m.validate(b.CollectedAt, uint16(len(b.Markets)), hash); err != nil {
			return fmt.Errorf("market %d (%s): %w", index, m.MarketAddress, err)
		}
		if _, ok := seen[m.MarketAddress]; ok {
			return fmt.Errorf("market %d duplicates address %s", index, m.MarketAddress)
		}
		seen[m.MarketAddress] = struct{}{}
	}
	return nil
}

func (m Observation) validate(collected time.Time, count uint16, hash string) error {
	if !m.ObservationTime.Equal(collected) || m.ObservationTime.Location() != time.UTC {
		return fmt.Errorf("observation_time differs from batch collected_at")
	}
	if m.BatchMarketCount != count {
		return fmt.Errorf("batch_market_count %d differs from batch size %d", m.BatchMarketCount, count)
	}
	if m.PayloadHash != hash {
		return fmt.Errorf("payload_hash differs within batch")
	}
	if !ValidTronAddress(m.MarketAddress) || !ValidTronAddress(m.UnderlyingAddress) {
		return fmt.Errorf("invalid TRON Base58Check address")
	}
	if !validSymbol(m.MarketSymbol) || !validSymbol(m.UnderlyingSymbol) {
		return fmt.Errorf("symbol must be 1-32 printable ASCII characters")
	}
	if m.UnderlyingDecimals > 36 {
		return fmt.Errorf("underlying_decimals %d exceeds 36", m.UnderlyingDecimals)
	}
	for _, f := range []struct {
		name  string
		value decimal.Decimal
	}{
		{"supply_rate_apr", m.SupplyRateAPR}, {"borrow_rate_apr", m.BorrowRateAPR}, {"cash", m.Cash},
		{"total_borrows", m.TotalBorrows}, {"reserves", m.Reserves}, {"total_supply", m.TotalSupply},
		{"exchange_rate", m.ExchangeRate}, {"borrow_index", m.BorrowIndex}, {"collateral_factor", m.CollateralFactor},
		{"reserve_factor", m.ReserveFactor}, {"underlying_price_trx", m.UnderlyingPriceTRX},
	} {
		if err := exactStorable(f.value); err != nil {
			return fmt.Errorf("%s: %w", f.name, err)
		}
	}
	for _, f := range []struct {
		name  string
		value decimal.Decimal
	}{{"collateral_factor", m.CollateralFactor}, {"reserve_factor", m.ReserveFactor}} {
		if f.value.GreaterThan(decimal.NewFromInt(1)) {
			return fmt.Errorf("%s %s exceeds 1", f.name, f.value)
		}
	}
	return nil
}

func exactStorable(d decimal.Decimal) error {
	if d.IsNegative() {
		return fmt.Errorf("negative value %s", d)
	}
	if d.GreaterThanOrEqual(decimalLimit) {
		return fmt.Errorf("value %s exceeds DECIMAL(38,18)", d)
	}
	if !d.Equal(d.Truncate(18)) {
		return fmt.Errorf("value %s has more than 18 fractional digits", d)
	}
	return nil
}

func validSymbol(s string) bool {
	if len(s) == 0 || len(s) > 32 {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < 0x21 || s[i] > 0x7e {
			return false
		}
	}
	return true
}

// ValidTronAddress checks the 34-character Base58Check form with the 0x41
// mainnet prefix and the double-SHA-256 checksum.
func ValidTronAddress(value string) bool {
	if len(value) != 34 || value[0] != 'T' {
		return false
	}
	const alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
	number := new(big.Int)
	base := big.NewInt(58)
	for _, char := range value {
		index := strings.IndexRune(alphabet, char)
		if index < 0 {
			return false
		}
		number.Mul(number, base)
		number.Add(number, big.NewInt(int64(index)))
	}
	decoded := number.Bytes()
	if len(decoded) != 25 || decoded[0] != 0x41 {
		return false
	}
	first := sha256.Sum256(decoded[:21])
	second := sha256.Sum256(first[:])
	return bytes.Equal(decoded[21:], second[:4])
}
