package options

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/model"
)

type ComboLeg struct {
	InstrumentID   uint32
	SignedRatio    int32
	AmountPerCombo decimal.Decimal
}

// ContractSpec is immutable. EvidenceHash is the FIRST definition evidence;
// refreshed responses and mutable tick/min-amount rules must not overwrite it.
type ContractSpec struct {
	Instrument                     model.Instrument
	NativeID                       uint64
	CreatedAt                      time.Time
	SourceInstrumentType           string // linear or reversed, including futures
	NativeAmountKind               string // base, usd, combo
	NativeAmountCurrency           string
	ContractSize                   decimal.Decimal
	IndexID, SettlementSemanticsID string
	OptionType                     string
	Strike                         decimal.Decimal
	StrikeCurrency                 string
	Legs                           []ComboLeg
	EvidenceHash                   string
}

func (s ContractSpec) DefinitionHash() string {
	// Metadata-only canonical encoding: no maps, binary floats or evidence/
	// mutable trading fields. Decimal.MarshalJSON preserves exact decimals.
	i := s.Instrument
	v := struct {
		NativeID                                                                                             uint64
		CreatedMS                                                                                            int64
		Exchange, Kind, Symbol, Base, Quote, Settle                                                          string
		Expiry                                                                                               int64
		SourceType, AmountKind, AmountCurrency, Index, Settlement, OptionType, StrikeCurrency, Normalization string
		ContractSize, Strike, PriceUnit, QtyUnit                                                             decimal.Decimal
		Legs                                                                                                 []ComboLeg
	}{NativeID: s.NativeID, CreatedMS: s.CreatedAt.UnixMilli(), Exchange: i.Exchange, Kind: string(i.MarketType), Symbol: i.ExchangeSymbol, Base: i.BaseAsset, Quote: i.QuoteAsset,
		SourceType: s.SourceInstrumentType, AmountKind: s.NativeAmountKind, AmountCurrency: s.NativeAmountCurrency, Index: s.IndexID, Settlement: s.SettlementSemanticsID,
		OptionType: s.OptionType, StrikeCurrency: s.StrikeCurrency, Normalization: NormalizationVersion, ContractSize: s.ContractSize, Strike: s.Strike, PriceUnit: i.PriceTickSize, QtyUnit: i.QuantityStepSize, Legs: s.Legs}
	if i.SettleAsset != nil {
		v.Settle = *i.SettleAsset
	}
	if i.ExpiryTime != nil {
		v.Expiry = i.ExpiryTime.UnixMilli()
	}
	if len(v.Legs) == 0 {
		v.Legs = nil
	}
	body, _ := json.Marshal(v)
	return PayloadHash(body)
}

func (s ContractSpec) Version() string {
	return fmt.Sprintf("deribit:%d:%d:%s", s.NativeID, s.CreatedAt.UnixMilli(), s.DefinitionHash())
}

func (s ContractSpec) Validate() error {
	i := s.Instrument
	if err := i.ValidateDefinition(); err != nil {
		return err
	}
	if i.Exchange != "Deribit" || (i.MarketType != model.MarketOption && i.MarketType != model.MarketOptionCombo && i.MarketType != model.MarketDelivery) {
		return fmt.Errorf("unsupported derivative spec")
	}
	if s.NativeID == 0 || s.CreatedAt.IsZero() || !s.CreatedAt.Equal(s.CreatedAt.Truncate(time.Millisecond)) || !model.ValidDigest(s.EvidenceHash) {
		return fmt.Errorf("invalid definition identity/evidence")
	}
	if !i.PriceTickSize.Equal(StorageUnit()) || !i.QuantityStepSize.Equal(StorageUnit()) || !i.ContractMultiplier.Equal(decimal.NewFromInt(1)) {
		return fmt.Errorf("invalid native-amount normalization")
	}
	if i.VenueContractVersion != s.Version() {
		return fmt.Errorf("economic definition/version mismatch")
	}
	if s.SourceInstrumentType != "linear" && s.SourceInstrumentType != "reversed" {
		return fmt.Errorf("unknown source instrument type")
	}
	for _, v := range []string{s.NativeAmountCurrency, s.IndexID, s.SettlementSemanticsID} {
		if v == "" || strings.TrimSpace(v) != v {
			return fmt.Errorf("missing spec semantics")
		}
	}
	if !s.ContractSize.IsPositive() {
		return fmt.Errorf("contract size must be positive")
	}
	if err := ValidateDecimal(s.ContractSize); err != nil {
		return err
	}
	if err := ValidateDecimal(s.Strike); err != nil {
		return err
	}
	if i.MarketType == model.MarketOptionCombo {
		if s.NativeAmountKind != "combo" || len(s.Legs) < 2 || len(s.Legs) > 255 || s.OptionType != "" || !s.Strike.IsZero() {
			return fmt.Errorf("invalid combo definition")
		}
		last := uint32(0)
		for _, l := range s.Legs {
			if l.InstrumentID <= last || l.InstrumentID == i.ID || l.SignedRatio == 0 || !l.AmountPerCombo.IsPositive() {
				return fmt.Errorf("combo legs must be sorted, distinct and nonzero")
			}
			if err := ValidateDecimal(l.AmountPerCombo); err != nil {
				return err
			}
			last = l.InstrumentID
		}
	} else {
		if len(s.Legs) != 0 || i.ExpiryTime == nil || !i.ExpiryTime.After(s.CreatedAt) || !i.ExpiryTime.Equal(i.ExpiryTime.Truncate(time.Second)) {
			return fmt.Errorf("invalid single-contract expiry/legs")
		}
		wantKind, wantCurrency := "base", i.BaseAsset
		if i.MarketType == model.MarketDelivery && s.SourceInstrumentType == "reversed" {
			wantKind, wantCurrency = "usd", "USD"
		}
		if s.NativeAmountKind != wantKind || s.NativeAmountCurrency != wantCurrency {
			return fmt.Errorf("native quantity semantics mismatch")
		}
		if i.MarketType == model.MarketOption {
			if (s.OptionType != "call" && s.OptionType != "put") || !s.Strike.IsPositive() || s.StrikeCurrency == "" {
				return fmt.Errorf("incomplete option payoff")
			}
		} else if s.OptionType != "" || !s.Strike.IsZero() || s.StrikeCurrency != "" {
			return fmt.Errorf("future contains option payoff fields")
		}
	}
	return nil
}

type TickBand struct{ AbovePrice, TickSize decimal.Decimal }
type TradingRule struct {
	InstrumentID                         uint32
	SourceTick, MinAmount, AmountStep    decimal.Decimal
	Bands                                []TickBand
	ObservedAt, KnownFrom, EffectiveFrom time.Time
	SourceURL, PayloadHash               string
	EffectiveTimeBasis                   string
}

func (r TradingRule) Validate() error {
	if r.InstrumentID == 0 || r.ObservedAt.IsZero() || r.KnownFrom.Before(r.ObservedAt) || r.EffectiveFrom.IsZero() || !model.ValidDigest(r.PayloadHash) || (!strings.HasPrefix(r.SourceURL, "https://") && !strings.HasPrefix(r.SourceURL, "wss://") && !strings.HasPrefix(r.SourceURL, "ws://127.0.0.1:") && !strings.HasPrefix(r.SourceURL, "http://127.0.0.1:")) {
		return fmt.Errorf("invalid trading rule evidence")
	}
	if (r.EffectiveTimeBasis != "first_observed" && r.EffectiveTimeBasis != "published") || (r.EffectiveTimeBasis == "first_observed" && !r.EffectiveFrom.Equal(r.ObservedAt)) {
		return fmt.Errorf("invalid effective-time basis")
	}
	for _, t := range []time.Time{r.ObservedAt, r.KnownFrom, r.EffectiveFrom} {
		if !t.Equal(t.Truncate(time.Microsecond)) {
			return fmt.Errorf("rule time exceeds precision")
		}
	}
	for _, v := range []decimal.Decimal{r.SourceTick, r.MinAmount, r.AmountStep} {
		if !v.IsPositive() {
			return fmt.Errorf("trading constraints must be positive")
		}
		if err := ValidateDecimal(v); err != nil {
			return err
		}
	}
	last := decimal.NewFromInt(-1)
	for _, b := range r.Bands {
		if b.AbovePrice.IsNegative() || !b.AbovePrice.GreaterThan(last) || !b.TickSize.IsPositive() {
			return fmt.Errorf("unordered or invalid tick bands")
		}
		if err := ValidateDecimal(b.AbovePrice); err != nil {
			return err
		}
		if err := ValidateDecimal(b.TickSize); err != nil {
			return err
		}
		last = b.AbovePrice
	}
	return nil
}

func (r TradingRule) ID() string {
	r.ObservedAt, r.KnownFrom, r.EffectiveFrom = r.ObservedAt.UTC(), r.KnownFrom.UTC(), r.EffectiveFrom.UTC()
	if len(r.Bands) == 0 {
		r.Bands = nil
	}
	b, _ := json.Marshal(r)
	return PayloadHash(b)
}

func PayloadHash(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
