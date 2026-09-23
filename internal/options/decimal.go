// Package options contains the typed, offline-testable derivatives foundation.
// It does not start network tasks or execute trades.
package options

import (
	"fmt"
	"math/big"
	"regexp"
	"strings"

	"github.com/shopspring/decimal"
)

const NormalizationVersion = "deribit-jsonrpc-v1"

var jsonNumber = regexp.MustCompile(`^-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?$`)

func StorageUnit() decimal.Decimal { return decimal.New(1, -8) }

// ParseNumber accepts JSON's exponent notation without ever passing through a
// binary float. Bounds apply before any large power-of-ten allocation.
func ParseNumber(raw string) (decimal.Decimal, error) {
	if len(raw) == 0 || len(raw) > 128 || !jsonNumber.MatchString(raw) {
		return decimal.Zero, fmt.Errorf("invalid or oversized JSON decimal")
	}
	if n := strings.IndexAny(raw, "eE"); n >= 0 {
		exp, ok := new(big.Int).SetString(raw[n+1:], 10)
		if !ok || !exp.IsInt64() || exp.Int64() < -128 || exp.Int64() > 128 {
			return decimal.Zero, fmt.Errorf("decimal exponent out of bounds")
		}
	}
	v, err := decimal.NewFromString(raw)
	if err != nil || v.Exponent() < -256 || v.Exponent() > 128 {
		return decimal.Zero, fmt.Errorf("decimal outside parser bounds")
	}
	return v, nil
}

func decimalInteger(v decimal.Decimal, scale int32) (*big.Int, error) {
	shift := int64(v.Exponent()) + int64(scale)
	if shift < -256 || shift > 256 {
		return nil, fmt.Errorf("decimal scale out of bounds")
	}
	n := v.Coefficient()
	power := new(big.Int).Exp(big.NewInt(10), big.NewInt(abs(shift)), nil)
	if shift >= 0 {
		return n.Mul(n, power), nil
	}
	q, r := new(big.Int), new(big.Int)
	q.QuoRem(n, power, r)
	if r.Sign() != 0 {
		return nil, fmt.Errorf("decimal is not exactly representable")
	}
	return q, nil
}

func abs(v int64) int64 {
	if v < 0 {
		return -v
	}
	return v
}

func Price(raw string, signed bool) (int64, error) {
	v, err := ParseNumber(raw)
	if err != nil {
		return 0, err
	}
	n, err := decimalInteger(v, 8)
	if err != nil {
		return 0, err
	}
	if !n.IsInt64() || (!signed && n.Sign() <= 0) {
		return 0, fmt.Errorf("price outside allowed Int64 domain")
	}
	return n.Int64(), nil
}

func Quantity(raw string) (uint64, error) {
	v, err := ParseNumber(raw)
	if err != nil {
		return 0, err
	}
	n, err := decimalInteger(v, 8)
	if err != nil {
		return 0, err
	}
	if !n.IsUint64() {
		return 0, fmt.Errorf("quantity outside UInt64 domain")
	}
	return n.Uint64(), nil
}

func ValidateDecimal(v decimal.Decimal) error {
	n, err := decimalInteger(v, 18)
	if err != nil {
		return err
	}
	if len(new(big.Int).Abs(n).String()) > 38 {
		return fmt.Errorf("decimal exceeds Decimal(38,18)")
	}
	return nil
}
