package model

import (
	"fmt"

	"github.com/shopspring/decimal"
)

// This codec changes the physical representation only. The publication still
// commits to DiscoveryHash(CapitalParameters.Body()), including observation time.
const CapitalVersionsCodec = "discovery-capital-versions-v2"
const capitalRuleSemantics = "okx-cross-base-quantity-v1"

// Version identities exclude observation clocks and transport hashes. Those
// belong to the observation and its independently committed provenance.
type DiscountRuleVersion struct {
	ID       string
	Currency string
	Tiers    []DiscountTier
}

type RiskRuleVersion struct {
	ID           string
	InstrumentID uint32
	Base, Role   string
	Tiers        []RiskTier
}

type CapitalVersionObservation struct {
	Parameters                                            CapitalParameters // Arrays and Sources are omitted in storage.
	QuoteVersion, BaseVersion, PerpVersion, BorrowVersion string
	Hash                                                  string
}

type CapitalVersions struct {
	Quote, Base  DiscountRuleVersion
	Perp, Borrow RiskRuleVersion
	Observation  CapitalVersionObservation
}

func (v DiscountRuleVersion) Body() map[string]any {
	lo, hi, rate, penalty := []decimal.Decimal{}, []decimal.Decimal{}, []decimal.Decimal{}, []decimal.Decimal{}
	for _, t := range v.Tiers {
		lo = append(lo, t.Minimum)
		hi = append(hi, t.Maximum)
		rate = append(rate, t.Rate)
		penalty = append(penalty, t.Penalty)
	}
	return map[string]any{"exchange": "OKX", "semantics": capitalRuleSemantics, "currency": v.Currency, "amount_unit": v.Currency, "minimum": lo, "maximum": hi, "rate": rate, "penalty": penalty}
}
func (v RiskRuleVersion) Body() map[string]any {
	lo, hi, initial, maintenance, leverage := []decimal.Decimal{}, []decimal.Decimal{}, []decimal.Decimal{}, []decimal.Decimal{}, []decimal.Decimal{}
	for _, t := range v.Tiers {
		lo = append(lo, t.Minimum)
		hi = append(hi, t.Maximum)
		initial = append(initial, t.Initial)
		maintenance = append(maintenance, t.Maintenance)
		leverage = append(leverage, t.MaximumLeverage)
	}
	return map[string]any{"exchange": "OKX", "semantics": capitalRuleSemantics, "mode": "cross", "instrument_id": v.InstrumentID, "base": v.Base, "amount_unit": v.Base, "role": v.Role, "minimum": lo, "maximum": hi, "initial": initial, "maintenance": maintenance, "maximum_leverage": leverage}
}
func (v DiscountRuleVersion) Validate() error {
	if v.Currency == "" || !ValidDigest(v.ID) || len(v.Tiers) == 0 {
		return fmt.Errorf("invalid discount version identity")
	}
	end, previous := decimal.Zero, decimal.NewFromInt(1)
	for _, t := range v.Tiers {
		for _, n := range []decimal.Decimal{t.Minimum, t.Maximum, t.Rate, t.Penalty} {
			if !ReferenceDecimal(n) {
				return fmt.Errorf("discount tier outside Decimal(38,18)")
			}
		}
		if !t.Minimum.Equal(end) || !t.Maximum.GreaterThan(end) || t.Rate.IsNegative() || t.Rate.GreaterThan(previous) || t.Penalty.IsNegative() || t.Penalty.GreaterThan(decimal.NewFromInt(1)) {
			return fmt.Errorf("invalid discount version tiers")
		}
		end, previous = t.Maximum, t.Rate
	}
	if DiscoveryHash(v.Body()) != v.ID {
		return fmt.Errorf("discount version content mismatch")
	}
	return nil
}
func (v RiskRuleVersion) Validate() error {
	if v.InstrumentID == 0 || v.Base == "" || (v.Role != "SWAP" && v.Role != "MARGIN") || !ValidDigest(v.ID) || len(v.Tiers) == 0 {
		return fmt.Errorf("invalid risk version identity")
	}
	end := decimal.Zero
	for _, t := range v.Tiers {
		for _, n := range []decimal.Decimal{t.Minimum, t.Maximum, t.Initial, t.Maintenance, t.MaximumLeverage} {
			if !ReferenceDecimal(n) {
				return fmt.Errorf("risk tier outside Decimal(38,18)")
			}
		}
		if t.Minimum.LessThan(end) || !t.Maximum.GreaterThan(end) || !t.Initial.IsPositive() || t.Maintenance.IsNegative() || t.Maintenance.GreaterThan(t.Initial) || !t.MaximumLeverage.IsPositive() {
			return fmt.Errorf("invalid risk version tiers")
		}
		end = t.Maximum
	}
	if DiscoveryHash(v.Body()) != v.ID {
		return fmt.Errorf("risk version content mismatch")
	}
	return nil
}

func SplitCapitalVersions(p CapitalParameters) (CapitalVersions, error) {
	var v CapitalVersions
	if err := p.Validate(); err != nil {
		return v, err
	}
	if !ReferenceDecimal(p.BorrowLeverage) {
		return v, fmt.Errorf("capital leverage outside Decimal(38,18)")
	}
	v.Quote = DiscountRuleVersion{Currency: "USDT", Tiers: append([]DiscountTier(nil), p.QuoteDiscount...)}
	v.Base = DiscountRuleVersion{Currency: p.Base, Tiers: append([]DiscountTier(nil), p.BaseDiscount...)}
	v.Perp = RiskRuleVersion{InstrumentID: p.InstrumentID, Base: p.Base, Role: "SWAP", Tiers: append([]RiskTier(nil), p.PerpTiers...)}
	v.Borrow = RiskRuleVersion{InstrumentID: p.InstrumentID, Base: p.Base, Role: "MARGIN", Tiers: append([]RiskTier(nil), p.BorrowTiers...)}
	v.Quote.ID = DiscoveryHash(v.Quote.Body())
	v.Base.ID = DiscoveryHash(v.Base.Body())
	v.Perp.ID = DiscoveryHash(v.Perp.Body())
	v.Borrow.ID = DiscoveryHash(v.Borrow.Body())
	for _, err := range []error{v.Quote.Validate(), v.Base.Validate(), v.Perp.Validate(), v.Borrow.Validate()} {
		if err != nil {
			return CapitalVersions{}, err
		}
	}
	v.Observation = CapitalVersionObservation{Parameters: p, QuoteVersion: v.Quote.ID, BaseVersion: v.Base.ID, PerpVersion: v.Perp.ID, BorrowVersion: v.Borrow.ID, Hash: DiscoveryHash(p.Body())}
	v.Observation.Parameters.QuoteDiscount = nil
	v.Observation.Parameters.BaseDiscount = nil
	v.Observation.Parameters.PerpTiers = nil
	v.Observation.Parameters.BorrowTiers = nil
	v.Observation.Parameters.Sources = nil
	return v, nil
}

// Rebuild verifies every reference and scope before exposing the old logical
// model. A missing or corrupt version is never replaced with a prior observation.
func (v CapitalVersions) Rebuild(sources []PublicSource) (CapitalParameters, error) {
	o := v.Observation
	p := o.Parameters
	for _, err := range []error{v.Quote.Validate(), v.Base.Validate(), v.Perp.Validate(), v.Borrow.Validate()} {
		if err != nil {
			return CapitalParameters{}, err
		}
	}
	if v.Quote.ID != o.QuoteVersion || v.Base.ID != o.BaseVersion || v.Perp.ID != o.PerpVersion || v.Borrow.ID != o.BorrowVersion || v.Quote.Currency != "USDT" || v.Base.Currency != p.Base || v.Perp.Base != p.Base || v.Borrow.Base != p.Base || v.Perp.InstrumentID != p.InstrumentID || v.Borrow.InstrumentID != p.InstrumentID || v.Perp.Role != "SWAP" || v.Borrow.Role != "MARGIN" {
		return CapitalParameters{}, fmt.Errorf("capital version reference/scope mismatch")
	}
	p.QuoteDiscount = append([]DiscountTier(nil), v.Quote.Tiers...)
	p.BaseDiscount = append([]DiscountTier(nil), v.Base.Tiers...)
	p.PerpTiers = append([]RiskTier(nil), v.Perp.Tiers...)
	p.BorrowTiers = append([]RiskTier(nil), v.Borrow.Tiers...)
	p.Sources = append([]PublicSource(nil), sources...)
	if err := p.Validate(); err != nil {
		return CapitalParameters{}, err
	}
	if !ReferenceDecimal(p.BorrowLeverage) || DiscoveryHash(p.Body()) != o.Hash {
		return CapitalParameters{}, fmt.Errorf("capital observation content mismatch")
	}
	return p, nil
}
