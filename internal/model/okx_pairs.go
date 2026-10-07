package model

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"strings"
	"time"
)

type OKXBasicLoan struct {
	Currency               string
	DailyRate, PublicQuota decimal.Decimal
}
type OKXCurrencyRate struct {
	Currency  string
	DailyRate decimal.Decimal
}
type OKXLoanLevel struct {
	Family, Level, Currency, StrategyType     string
	Quota, InterestDiscount, QuotaCoefficient *decimal.Decimal
}
type OKXLoanPolicy struct {
	ID                      uuid.UUID
	RequestedAt, ObservedAt time.Time
	SourceURL, PayloadHash  string
	Basic                   []OKXBasicLoan
	Overrides               []OKXCurrencyRate
	Levels                  []OKXLoanLevel
}
type OKXPairMember struct {
	Base                          string
	Spot, Perpetual               Instrument
	SpotMinSize, PerpetualMinSize decimal.Decimal
}
type OKXPairObservation struct {
	ID                       uuid.UUID
	ObservedAt               time.Time
	EffectiveMinute          time.Time
	LoanPolicyID             uuid.UUID
	SourceURLs, SourceHashes []string
	RequestedAt, ReceivedAt  []time.Time
	RawCounts                []uint32
	Pairs                    []OKXPairMember
}

func (p OKXLoanPolicy) Validate() error {
	if p.ID == uuid.Nil || !referenceTimes(p.RequestedAt, p.ObservedAt) || p.SourceURL == "" || !ValidDigest(p.PayloadHash) || len(p.Basic) == 0 {
		return fmt.Errorf("invalid public loan observation")
	}
	return p.ValidateTerms()
}

// ValidateTerms does not fabricate transport provenance for a decoded response.
func (p OKXLoanPolicy) ValidateTerms() error {
	if len(p.Basic) == 0 {
		return fmt.Errorf("empty public loan terms")
	}
	seen := map[string]bool{}
	for _, b := range p.Basic {
		if b.Currency == "" || strings.TrimSpace(b.Currency) != b.Currency || seen[b.Currency] || !ReferenceDecimal(b.DailyRate) || !ReferenceDecimal(b.PublicQuota) {
			return fmt.Errorf("invalid basic loan reference")
		}
		seen[b.Currency] = true
	}
	seen = map[string]bool{}
	for _, r := range p.Overrides {
		if r.Currency == "" || strings.TrimSpace(r.Currency) != r.Currency || seen[r.Currency] || !ReferenceDecimal(r.DailyRate) {
			return fmt.Errorf("invalid currency override")
		}
		seen[r.Currency] = true
	}
	seen = map[string]bool{}
	for _, l := range p.Levels {
		key := l.Family + "/" + l.Level + "/" + l.Currency + "/" + l.StrategyType
		if l.Level == "" || strings.TrimSpace(l.Level) != l.Level || strings.TrimSpace(l.Currency) != l.Currency || strings.TrimSpace(l.StrategyType) != l.StrategyType || (l.Family != "vip" && l.Family != "regular" && l.Family != "config") || seen[key] {
			return fmt.Errorf("invalid loan level identity")
		}
		seen[key] = true
		for _, v := range []*decimal.Decimal{l.Quota, l.InterestDiscount, l.QuotaCoefficient} {
			if v != nil && !ReferenceDecimal(*v) {
				return fmt.Errorf("invalid optional loan term")
			}
		}
	}
	return nil
}
func ReferenceDecimal(v decimal.Decimal) bool {
	return !v.IsNegative() && v.Exponent() >= -18 && v.LessThan(decimal.New(1, 20))
}
func referenceTimes(a, b time.Time) bool {
	return !a.IsZero() && !b.Before(a) && a.Equal(a.Truncate(time.Microsecond)) && b.Equal(b.Truncate(time.Microsecond))
}
func (o OKXPairObservation) Validate() error {
	if o.EffectiveMinute.IsZero() || !o.EffectiveMinute.Equal(o.EffectiveMinute.Truncate(time.Minute)) || o.EffectiveMinute.Before(o.ObservedAt) {
		return fmt.Errorf("invalid pair activation minute")
	}
	if o.ID == uuid.Nil || o.LoanPolicyID == uuid.Nil || o.ObservedAt.IsZero() || !o.ObservedAt.Equal(o.ObservedAt.Truncate(time.Microsecond)) || len(o.Pairs) == 0 || len(o.SourceURLs) != 4 || len(o.SourceHashes) != 4 || len(o.RequestedAt) != 4 || len(o.ReceivedAt) != 4 || len(o.RawCounts) != 4 {
		return fmt.Errorf("invalid paired catalog observation")
	}
	for n := range o.SourceURLs {
		if o.SourceURLs[n] == "" || !ValidDigest(o.SourceHashes[n]) || !referenceTimes(o.RequestedAt[n], o.ReceivedAt[n]) || o.ReceivedAt[n].After(o.ObservedAt) {
			return fmt.Errorf("invalid catalog source evidence")
		}
	}
	seen := map[string]bool{}
	for _, p := range o.Pairs {
		if p.Base == "" || seen[p.Base] || p.Spot.ID == 0 || p.Perpetual.ID == 0 || p.Spot.Exchange != "OKX" || p.Perpetual.Exchange != "OKX" || p.Spot.MarketType != MarketSpot || p.Perpetual.MarketType != MarketPerpetual || p.Spot.BaseAsset != p.Base || p.Perpetual.BaseAsset != p.Base || p.Spot.QuoteAsset != "USDT" || p.Perpetual.QuoteAsset != "USDT" || p.Perpetual.SettleAsset == nil || *p.Perpetual.SettleAsset != "USDT" || !p.SpotMinSize.IsPositive() || !p.PerpetualMinSize.IsPositive() || !ReferenceDecimal(p.SpotMinSize) || !ReferenceDecimal(p.PerpetualMinSize) {
			return fmt.Errorf("invalid registered pair")
		}
		if err := p.Spot.Validate(); err != nil {
			return err
		}
		if err := p.Perpetual.Validate(); err != nil {
			return err
		}
		seen[p.Base] = true
	}
	return nil
}
func ReferenceHash(v any) string {
	data, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}
