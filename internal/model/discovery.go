package model

// Discovery facts are independent observations, never reconstructions of a
// previous settlement or retrospective claims about a completed publication.
import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/shopspring/decimal"
	"strconv"
	"strings"
	"time"
)

const DiscoveryCodec = "discovery-row-v1"
const DiscoveryMinuteCodec = "discovery-minute-v1"
const OKXCapitalVersion = "okx-multicurrency-cross-v1"
const OKXReferenceProfile = "okx-multicurrency-usdt-reference-v1"
const OKXInterestVersion = "okx-hourly-base-debt-v1"

// Source timestamps are exchange timestamps, or explicitly identified request
// observations for source APIs that do not expose an observation clock.
type PublicSource struct {
	URL         string
	PayloadHash string
	TimeBasis   string
	SourceMS    int64
	ObservedMS  int64
}

func (s PublicSource) Validate() error {
	if !strings.HasPrefix(s.URL, "https://") || !ValidDigest(s.PayloadHash) || s.SourceMS <= 0 || s.ObservedMS < s.SourceMS {
		return fmt.Errorf("invalid public source identity/time")
	}
	if s.TimeBasis != "exchange_ts" && s.TimeBasis != "request_without_source_clock" && s.TimeBasis != "reviewed_static_rule" {
		return fmt.Errorf("unsupported source time basis")
	}
	return nil
}

type FundingPrediction struct {
	ID                                                 string
	InstrumentID                                       uint32
	ObservedMS, SourceMS, FundingMS                    int64
	Rate, Mark, BaseIndex, BaseUSDIndex, QuoteUSDIndex decimal.Decimal
	MarkMS, BaseIndexMS, QuoteIndexMS                  int64
	Sources                                            []PublicSource
}

func (f FundingPrediction) Kind() string { return "funding_forecast_observation" }
func (f FundingPrediction) Body() map[string]any {
	return map[string]any{"observation_id": f.ID, "instrument_id": f.InstrumentID, "observed_ms": f.ObservedMS, "source_ms": f.SourceMS, "funding_ms": f.FundingMS, "rate": f.Rate, "mark": f.Mark, "mark_ms": f.MarkMS, "base_index": f.BaseIndex, "base_index_ms": f.BaseIndexMS, "base_usd_index": f.BaseUSDIndex, "quote_usd_index": f.QuoteUSDIndex, "quote_index_ms": f.QuoteIndexMS}
}
func (f FundingPrediction) Validate() error {
	if f.ID == "" || f.InstrumentID == 0 || f.SourceMS <= 0 || f.SourceMS > f.ObservedMS || f.FundingMS <= f.SourceMS || f.MarkMS <= 0 || f.MarkMS > f.ObservedMS || !f.Mark.IsPositive() || !ReferenceDecimal(f.Rate.Abs()) || !ReferenceDecimal(f.Mark) || !f.BaseIndex.IsPositive() || !ReferenceDecimal(f.BaseIndex) || f.BaseIndexMS <= 0 || f.BaseIndexMS > f.ObservedMS || !f.BaseUSDIndex.IsPositive() || !f.QuoteUSDIndex.IsPositive() || f.QuoteIndexMS <= 0 || f.QuoteIndexMS > f.ObservedMS || !f.BaseIndex.Equal(f.BaseUSDIndex.DivRound(f.QuoteUSDIndex, 36).RoundCeil(18)) {
		return fmt.Errorf("invalid independent next funding prediction")
	}
	return validatePublicSources(f.Sources)
}

type TradeBar struct {
	ID                             string
	InstrumentID                   uint32
	ObservedMS, SourceMS, MinuteMS int64
	BaseVolume                     decimal.Decimal
	Final                          bool
	ConversionHash                 string
	Sources                        []PublicSource
}

func (f TradeBar) Kind() string { return "cex_trade_bar_1m" }
func (f TradeBar) Body() map[string]any {
	final := 0
	if f.Final {
		final = 1
	}
	return map[string]any{"observation_id": f.ID, "instrument_id": f.InstrumentID, "observed_ms": f.ObservedMS, "source_ms": f.SourceMS, "minute_ms": f.MinuteMS, "base_volume": f.BaseVolume, "is_final": final, "conversion_hash": f.ConversionHash}
}
func (f TradeBar) Validate() error {
	if f.ID == "" || f.InstrumentID == 0 || f.MinuteMS%60000 != 0 || !f.Final || f.SourceMS < f.MinuteMS+60000 || f.SourceMS > f.ObservedMS || f.BaseVolume.IsNegative() || !ReferenceDecimal(f.BaseVolume) || !ValidDigest(f.ConversionHash) {
		return fmt.Errorf("invalid final base-volume minute")
	}
	return validatePublicSources(f.Sources)
}

type ReferenceFee struct {
	ID                                              string
	InstrumentID                                    uint32
	ObservedMS, SourceMS, EffectiveMS, ValidUntilMS int64
	PerpMaker, PerpTaker, SpotTaker                 decimal.Decimal
	BuyFeeCurrency, SellFeeCurrency                 string
	SpotMaximum, PerpMaximum, PositionLimit         decimal.Decimal
	Profile, InterestVersion                        string
	Sources                                         []PublicSource
}

func (f ReferenceFee) Kind() string { return "cex_fee_schedule_rule" }
func (f ReferenceFee) Body() map[string]any {
	return map[string]any{"observation_id": f.ID, "instrument_id": f.InstrumentID, "observed_ms": f.ObservedMS, "source_ms": f.SourceMS, "effective_ms": f.EffectiveMS, "valid_until_ms": f.ValidUntilMS, "perp_maker": f.PerpMaker, "perp_taker": f.PerpTaker, "spot_taker": f.SpotTaker, "buy_fee_currency": f.BuyFeeCurrency, "sell_fee_currency": f.SellFeeCurrency, "spot_maximum": f.SpotMaximum, "perp_maximum": f.PerpMaximum, "position_limit": f.PositionLimit, "reference_profile": f.Profile, "interest_rule_version": f.InterestVersion}
}
func (f ReferenceFee) Validate() error {
	if f.ID == "" || f.InstrumentID == 0 || f.SourceMS <= 0 || f.SourceMS > f.ObservedMS || f.EffectiveMS > f.ObservedMS || f.ValidUntilMS <= f.ObservedMS || f.Profile == "" || f.InterestVersion == "" || f.BuyFeeCurrency == "" || f.SellFeeCurrency == "" {
		return fmt.Errorf("invalid public fee rule identity/time")
	}
	for _, v := range []decimal.Decimal{f.PerpMaker, f.PerpTaker, f.SpotTaker} {
		if v.IsNegative() || v.GreaterThanOrEqual(decimal.NewFromInt(1)) || !ReferenceDecimal(v) {
			return fmt.Errorf("invalid public fee cost")
		}
	}
	for _, v := range []decimal.Decimal{f.SpotMaximum, f.PerpMaximum, f.PositionLimit} {
		if !v.IsPositive() || !ReferenceDecimal(v) {
			return fmt.Errorf("invalid public product bound")
		}
	}
	return validatePublicSources(f.Sources)
}

type DiscountTier struct{ Minimum, Maximum, Rate, Penalty decimal.Decimal }
type RiskTier struct{ Minimum, Maximum, Initial, Maintenance, MaximumLeverage decimal.Decimal }
type CapitalParameters struct {
	ID                                              string
	InstrumentID                                    uint32
	ObservedMS, SourceMS, EffectiveMS, ValidUntilMS int64
	Base                                            string
	QuoteDiscount, BaseDiscount                     []DiscountTier
	PerpTiers, BorrowTiers                          []RiskTier
	BorrowLeverage                                  decimal.Decimal
	ModelHash                                       string
	Sources                                         []PublicSource
}

func (f CapitalParameters) Kind() string { return "okx_public_capital_parameters" }
func (f CapitalParameters) Body() map[string]any {
	body := map[string]any{"observation_id": f.ID, "instrument_id": f.InstrumentID, "observed_ms": f.ObservedMS, "source_ms": f.SourceMS, "effective_ms": f.EffectiveMS, "valid_until_ms": f.ValidUntilMS, "base": f.Base, "borrow_leverage": f.BorrowLeverage, "model_hash": f.ModelHash}
	for name, tiers := range map[string][]DiscountTier{"quote_discount": f.QuoteDiscount, "base_discount": f.BaseDiscount} {
		lower, upper, rates, penalties := []decimal.Decimal{}, []decimal.Decimal{}, []decimal.Decimal{}, []decimal.Decimal{}
		for _, t := range tiers {
			lower = append(lower, t.Minimum)
			upper = append(upper, t.Maximum)
			rates = append(rates, t.Rate)
			penalties = append(penalties, t.Penalty)
		}
		body[name+"_minimum"] = lower
		body[name+"_maximum"] = upper
		body[name+"_rate"] = rates
		body[name+"_liquidation_penalty"] = penalties
	}
	for name, tiers := range map[string][]RiskTier{"perp": f.PerpTiers, "borrow": f.BorrowTiers} {
		lower, upper, imr, mmr, lever := []decimal.Decimal{}, []decimal.Decimal{}, []decimal.Decimal{}, []decimal.Decimal{}, []decimal.Decimal{}
		for _, t := range tiers {
			lower = append(lower, t.Minimum)
			upper = append(upper, t.Maximum)
			imr = append(imr, t.Initial)
			mmr = append(mmr, t.Maintenance)
			lever = append(lever, t.MaximumLeverage)
		}
		body[name+"_minimum"] = lower
		body[name+"_maximum"] = upper
		body[name+"_imr"] = imr
		body[name+"_mmr"] = mmr
		body[name+"_maximum_leverage"] = lever
	}
	return body
}
func (f CapitalParameters) Validate() error {
	if f.ID == "" || f.InstrumentID == 0 || f.Base == "" || !f.BorrowLeverage.IsPositive() || !ValidDigest(f.ModelHash) || f.SourceMS <= 0 || f.SourceMS > f.ObservedMS || f.EffectiveMS > f.ObservedMS || f.ValidUntilMS <= f.ObservedMS {
		return fmt.Errorf("invalid public capital parameter identity/time")
	}
	for _, tiers := range [][]DiscountTier{f.QuoteDiscount, f.BaseDiscount} {
		if len(tiers) == 0 {
			return fmt.Errorf("discount tiers missing")
		}
		end := decimal.Zero
		previous := decimal.NewFromInt(1)
		for _, t := range tiers {
			if !t.Minimum.Equal(end) || !t.Maximum.GreaterThan(end) || t.Rate.IsNegative() || t.Rate.GreaterThan(previous) || t.Penalty.IsNegative() || t.Penalty.GreaterThan(decimal.NewFromInt(1)) {
				return fmt.Errorf("invalid discount tier coverage")
			}
			end = t.Maximum
			previous = t.Rate
		}
	}
	for _, tiers := range [][]RiskTier{f.PerpTiers, f.BorrowTiers} {
		if len(tiers) == 0 {
			return fmt.Errorf("risk tiers missing")
		}
		end := decimal.Zero
		for _, t := range tiers {
			if t.Minimum.LessThan(end) || !t.Maximum.GreaterThan(end) || !t.Initial.IsPositive() || t.Maintenance.IsNegative() || t.Maintenance.GreaterThan(t.Initial) || !t.MaximumLeverage.IsPositive() {
				return fmt.Errorf("invalid risk tier coverage")
			}
			end = t.Maximum
		}
	}
	return validatePublicSources(f.Sources)
}

type CapitalReference struct{ Parameters CapitalParameters }

func (f CapitalReference) Kind() string { return "cex_collateral_risk_rule" }
func (f CapitalReference) Body() map[string]any {
	p := f.Parameters
	return map[string]any{"observation_id": p.ID, "instrument_id": p.InstrumentID, "observed_ms": p.ObservedMS, "source_ms": p.SourceMS, "effective_ms": p.EffectiveMS, "valid_until_ms": p.ValidUntilMS, "model_version": OKXCapitalVersion, "reference_profile": OKXReferenceProfile, "model_hash": p.ModelHash}
}
func (f CapitalReference) Validate() error { return f.Parameters.Validate() }

func validatePublicSources(sources []PublicSource) error {
	if len(sources) == 0 {
		return fmt.Errorf("public provenance missing")
	}
	for _, s := range sources {
		if err := s.Validate(); err != nil {
			return err
		}
	}
	return nil
}

// Decimal fields are hashed in their physical Decimal(38,18) representation,
// exactly matching Python Decimal after reading quoted ClickHouse decimals.
func PythonStoredDecimal(v decimal.Decimal) string {
	s := v.StringFixed(18)
	negative := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(s, "-")
	digits := strings.TrimLeft(strings.ReplaceAll(s, ".", ""), "0")
	if digits == "" {
		return "0E-18"
	}
	adjusted := len(digits) - 19
	if adjusted < -6 {
		mantissa := digits[:1]
		if len(digits) > 1 {
			mantissa += "." + digits[1:]
		}
		s = mantissa + "E" + strconv.Itoa(adjusted)
	}
	if negative {
		s = "-" + s
	}
	return s
}
func hashJSONValue(v any) any {
	switch x := v.(type) {
	case decimal.Decimal:
		return PythonStoredDecimal(x)
	case []decimal.Decimal:
		out := make([]any, len(x))
		for n, t := range x {
			out[n] = PythonStoredDecimal(t)
		}
		return out
	case map[string]any:
		out := map[string]any{}
		for k, t := range x {
			out[k] = hashJSONValue(t)
		}
		return out
	default:
		return v
	}
}
func DiscoveryHash(value any) string {
	var compact bytes.Buffer
	encoder := json.NewEncoder(&compact)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(hashJSONValue(value)); err != nil {
		return ""
	}
	raw := bytes.TrimSpace(compact.Bytes())
	var formatted bytes.Buffer
	quoted, escape := false, false
	for _, b := range raw {
		formatted.WriteByte(b)
		if quoted {
			if escape {
				escape = false
			} else if b == '\\' {
				escape = true
			} else if b == '"' {
				quoted = false
			}
			continue
		}
		if b == '"' {
			quoted = true
		} else if b == ',' || b == ':' {
			formatted.WriteByte(' ')
		}
	}
	sum := sha256.Sum256(formatted.Bytes())
	return hex.EncodeToString(sum[:])
}
func TradingMinuteBody(batch MinuteBatch) map[string]any {
	sides := func(levels [LegacyBookDepth]Level) [][2]any {
		result := make([][2]any, len(levels))
		for n, l := range levels {
			result[n] = [2]any{l.PriceTick, l.QtyLot}
		}
		return result
	}
	deltas := make([]map[string]any, 0, len(batch.Deltas))
	for _, d := range batch.Deltas {
		bids, asks := make([][2]any, len(d.BidChangePrice)), make([][2]any, len(d.AskChangePrice))
		for n, p := range d.BidChangePrice {
			bids[n] = [2]any{p, d.BidChangeQty[n]}
		}
		for n, p := range d.AskChangePrice {
			asks[n] = [2]any{p, d.AskChangeQty[n]}
		}
		deltas = append(deltas, map[string]any{"second": d.SecondOffset, "bids": bids, "asks": asks})
	}
	m := batch.Minute
	return map[string]any{"id": strconv.FormatUint(m.ID, 10), "instrument_id": m.InstrumentID, "minute_ms": m.MinuteTime.UnixMilli(), "stored_depth": m.StoredDepth, "valid_bitmap": m.ValidBitmap, "bids": sides(m.Bids), "asks": sides(m.Asks), "deltas": deltas}
}
func CeilMilliseconds(t time.Time) int64 {
	return (t.UnixNano() + int64(time.Millisecond) - 1) / int64(time.Millisecond)
}
