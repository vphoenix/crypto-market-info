package yield

import (
	"fmt"

	"github.com/shopspring/decimal"
)

// normalizeDecimals applies the documented Decimal(38,18) storage quantum
// using decimal arithmetic, after validating the unrounded source values.
// Evidence hashes continue to identify the original source response.
func (o *YieldObservation) normalizeDecimals() error {
	normalize := func(name string, d decimal.Decimal) (decimal.Decimal, error) {
		if d.Exponent() < -4096 || d.Exponent() > 38 || d.Coefficient().BitLen() > 13600 || d.Abs().GreaterThanOrEqual(decimal.New(1, 20)) {
			return decimal.Zero, fmt.Errorf("%s exceeds Decimal(38,18) range", name)
		}
		return d.Truncate(18), nil
	}
	var err error
	o.TierMinAmount, err = normalize("tier_min_amount", o.TierMinAmount)
	if err != nil {
		return err
	}
	for _, field := range []struct {
		name  string
		value **decimal.Decimal
	}{
		{"tier_max_amount", &o.TierMaxAmount}, {"rate", &o.Rate},
		{"entry_fee_rate", &o.EntryFeeRate}, {"exit_fee_rate", &o.ExitFeeRate},
		{"fixed_penalty_rate", &o.FixedPenaltyRate}, {"performance_fee_rate", &o.PerformanceFeeRate},
		{"entry_fee_amount", &o.EntryFeeAmount}, {"exit_fee_amount", &o.ExitFeeAmount},
		{"fixed_principal_loss_rate", &o.FixedPrincipalLossRate}, {"exposure_ratio", &o.ExposureRatio},
		{"capacity", &o.Capacity}, {"remaining_capacity", &o.RemainingCapacity},
		{"tvl", &o.TVL}, {"pool_cash", &o.PoolCash},
	} {
		if *field.value == nil {
			continue
		}
		d, e := normalize(field.name, **field.value)
		if e != nil {
			return e
		}
		*field.value = &d // Do not mutate collector-owned or shared decimal pointers.
	}
	rates := make([]*decimal.Decimal, len(o.RewardComponentRates))
	for i, rate := range o.RewardComponentRates {
		if rate == nil {
			continue
		}
		d, e := normalize("reward_component_rate", *rate)
		if e != nil {
			return e
		}
		rates[i] = &d
	}
	if o.RewardComponentRates != nil {
		o.RewardComponentRates = rates
	}
	return nil
}
