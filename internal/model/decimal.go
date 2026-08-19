package model

import (
	"fmt"
	"math/big"
	"strings"

	"github.com/shopspring/decimal"
)

func ParseStrictDecimal(raw, field string) (decimal.Decimal, error) {
	if raw == "" || strings.TrimSpace(raw) != raw || !plainDecimal(raw) {
		return decimal.Zero, fmt.Errorf("%s must be a plain decimal string", field)
	}
	value, err := decimal.NewFromString(raw)
	if err != nil {
		return decimal.Zero, fmt.Errorf("%s: %w", field, err)
	}
	return value, nil
}

func ParsePositiveDecimal(raw, field string) (decimal.Decimal, error) {
	value, err := ParseStrictDecimal(raw, field)
	if err != nil {
		return decimal.Zero, err
	}
	if !value.IsPositive() {
		return decimal.Zero, fmt.Errorf("%s must be positive", field)
	}
	return value, nil
}

func PriceTick(raw string, step decimal.Decimal) (int64, error) {
	value, err := ParsePositiveDecimal(raw, "price")
	if err != nil {
		return 0, err
	}
	units, err := decimalUnits(value, step)
	if err != nil {
		return 0, fmt.Errorf("price: %w", err)
	}
	if !units.IsInt64() || units.Sign() <= 0 {
		return 0, fmt.Errorf("price tick is outside positive Int64 range")
	}
	return units.Int64(), nil
}

func QuantityLot(raw string, step decimal.Decimal) (uint64, error) {
	value, err := ParseStrictDecimal(raw, "quantity")
	if err != nil {
		return 0, err
	}
	if value.IsNegative() {
		return 0, fmt.Errorf("quantity must be non-negative")
	}
	units, err := decimalUnits(value, step)
	if err != nil {
		return 0, fmt.Errorf("quantity: %w", err)
	}
	if !units.IsUint64() {
		return 0, fmt.Errorf("quantity lot is outside UInt64 range")
	}
	return units.Uint64(), nil
}

func decimalUnits(value, step decimal.Decimal) (*big.Int, error) {
	if !step.IsPositive() {
		return nil, fmt.Errorf("step must be positive")
	}
	commonExponent := value.Exponent()
	if step.Exponent() < commonExponent {
		commonExponent = step.Exponent()
	}
	valueInteger := scaledCoefficient(value, commonExponent)
	stepInteger := scaledCoefficient(step, commonExponent)
	quotient, remainder := new(big.Int), new(big.Int)
	quotient.QuoRem(valueInteger, stepInteger, remainder)
	if remainder.Sign() != 0 {
		return nil, fmt.Errorf("value %s is not exactly divisible by step %s", value, step)
	}
	return quotient, nil
}

func scaledCoefficient(value decimal.Decimal, exponent int32) *big.Int {
	coefficient := new(big.Int).Set(value.Coefficient())
	shift := value.Exponent() - exponent
	if shift <= 0 {
		return coefficient
	}
	power := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(shift)), nil)
	return coefficient.Mul(coefficient, power)
}

func plainDecimal(value string) bool {
	start := 0
	if value[0] == '-' {
		if len(value) == 1 {
			return false
		}
		start = 1
	}
	digits, dots := 0, 0
	for index := start; index < len(value); index++ {
		switch value[index] {
		case '.':
			dots++
			if dots > 1 {
				return false
			}
		default:
			if value[index] < '0' || value[index] > '9' {
				return false
			}
			digits++
		}
	}
	return digits > 0
}
