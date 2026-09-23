package deribit

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/options"
)

type ParsedInstrument struct {
	Spec   options.ContractSpec
	Rule   options.TradingRule
	State  string
	Active bool
}
type ExcludedInstrument struct{ Symbol, Kind, Classification, Reason, PayloadHash string }

// DecodeInstruments is a single-scope decoder, NOT a complete catalog. A live
// collector must verify every requested scope; R4 does not require combo scopes.
// Option combos remain unsupported until their metadata AND leg units are
// independently verified; this decoder never guesses their quantity semantics.
func DecodeInstruments(raw []byte, currency, kind string, at time.Time) ([]ParsedInstrument, []ExcludedInstrument, error) {
	if (currency != "BTC" && currency != "ETH" && currency != "USDC") || (kind != "option" && kind != "future" && kind != "option_combo") || at.IsZero() {
		return nil, nil, fmt.Errorf("invalid metadata scope/time")
	}
	var e struct {
		JSONRPC string             `json:"jsonrpc"`
		Result  *[]json.RawMessage `json:"result"`
		Error   json.RawMessage    `json:"error"`
	}
	if err := decode(raw, &e); err != nil {
		return nil, nil, err
	}
	if e.JSONRPC != "2.0" || e.Result == nil || (len(e.Error) > 0 && string(e.Error) != "null") {
		return nil, nil, fmt.Errorf("incomplete or failed metadata response")
	}
	hash := options.PayloadHash(raw)
	seenIDs := map[uint64]bool{}
	seenNames := map[string]bool{}
	var parsed []ParsedInstrument
	var excluded []ExcludedInstrument
	for _, row := range *e.Result {
		var m metadata
		if err := decode(row, &m); err != nil {
			return nil, nil, err
		}
		if m.ID == nil || *m.ID == 0 || m.Name == "" || m.Kind != kind || seenIDs[*m.ID] || seenNames[m.Name] {
			return nil, nil, fmt.Errorf("conflicting/incomplete metadata identity")
		}
		seenIDs[*m.ID], seenNames[m.Name] = true, true
		if m.Base == "" || m.Settle == "" || m.SourceType == "" {
			return nil, nil, fmt.Errorf("metadata scope classification is unknown")
		}
		if m.Period != "day" && m.Period != "week" && m.Period != "month" && m.Period != "perpetual" {
			return nil, nil, fmt.Errorf("missing/unknown settlement period")
		}
		if m.Active == nil || (m.State != "open" && m.State != "closed" && m.State != "settlement" && m.State != "delivered" && m.State != "inactive") {
			return nil, nil, fmt.Errorf("missing/unknown instrument state")
		}
		classification, reason := "", ""
		if (m.Base != "BTC" && m.Base != "ETH") || m.Period == "perpetual" {
			classification, reason = "out_of_scope", "asset_or_perpetual"
		}
		if classification == "" && kind == "option_combo" {
			classification, reason = "unsupported", "combo_units_and_legs_unverified"
		}
		if classification != "" {
			excluded = append(excluded, ExcludedInstrument{m.Name, m.Kind, classification, reason, hash})
			continue
		}
		p, err := normalizeMetadata(m, at.UTC().Truncate(time.Microsecond), hash)
		if err != nil {
			return nil, nil, fmt.Errorf("%s: %w", m.Name, err)
		}
		if (currency == "USDC" && m.Settle != "USDC") || (currency != "USDC" && m.Settle != currency) {
			return nil, nil, fmt.Errorf("metadata outside requested currency scope")
		}
		parsed = append(parsed, p)
	}
	return parsed, excluded, nil
}

type metadata struct {
	ID           *uint64         `json:"instrument_id"`
	Name         string          `json:"instrument_name"`
	Kind         string          `json:"kind"`
	Base         string          `json:"base_currency"`
	Quote        string          `json:"quote_currency"`
	Settle       string          `json:"settlement_currency"`
	Counter      string          `json:"counter_currency"`
	Index        string          `json:"price_index"`
	SourceType   string          `json:"instrument_type"`
	OptionType   string          `json:"option_type"`
	Period       string          `json:"settlement_period"`
	State        string          `json:"state"`
	Active       *bool           `json:"is_active"`
	Created      *int64          `json:"creation_timestamp"`
	Expires      *int64          `json:"expiration_timestamp"`
	ContractSize json.RawMessage `json:"contract_size"`
	Strike       json.RawMessage `json:"strike"`
	Tick         json.RawMessage `json:"tick_size"`
	MinAmount    json.RawMessage `json:"min_trade_amount"`
	Steps        []struct {
		Above json.RawMessage `json:"above_price"`
		Tick  json.RawMessage `json:"tick_size"`
	} `json:"tick_size_steps"`
}

func normalizeMetadata(m metadata, at time.Time, hash string) (ParsedInstrument, error) {
	var p ParsedInstrument
	if m.Created == nil || *m.Created <= 0 || m.Expires == nil || m.Active == nil || m.State == "" {
		return p, fmt.Errorf("missing metadata timestamps/state")
	}
	if m.SourceType != "linear" && m.SourceType != "reversed" {
		return p, fmt.Errorf("unknown payoff")
	}
	if m.SourceType == "linear" && (m.Settle != "USDC" || m.Quote != "USDC" || m.Counter != "USDC") {
		return p, fmt.Errorf("unsupported linear currency semantics")
	}
	if m.SourceType == "reversed" && (m.Settle != m.Base || m.Counter != "USD") {
		return p, fmt.Errorf("unsupported inverse currency semantics")
	}
	i := model.Instrument{Exchange: "Deribit", MarketType: model.MarketDelivery, ExchangeSymbol: m.Name, BaseAsset: m.Base, QuoteAsset: m.Quote, SettleAsset: &m.Settle,
		ContractMultiplier: decimal.NewFromInt(1), PriceTickSize: options.StorageUnit(), QuantityStepSize: options.StorageUnit()}
	expiry := time.UnixMilli(*m.Expires).UTC()
	i.ExpiryTime = &expiry
	s := options.ContractSpec{NativeID: *m.ID, CreatedAt: time.UnixMilli(*m.Created).UTC(), SourceInstrumentType: m.SourceType, NativeAmountKind: "base", NativeAmountCurrency: m.Base, IndexID: m.Index, EvidenceHash: hash}
	var err error
	s.ContractSize, err = options.ParseNumber(string(m.ContractSize))
	if err != nil {
		return p, err
	}
	if m.Kind == "option" {
		i.MarketType = model.MarketOption
		s.OptionType = m.OptionType
		s.StrikeCurrency = m.Counter
		s.Strike, err = options.ParseNumber(string(m.Strike))
		if err != nil {
			return p, err
		}
		if m.SourceType == "reversed" && m.Quote != m.Base {
			return p, fmt.Errorf("inverse premium is not in base currency")
		}
	} else if m.SourceType == "reversed" {
		if m.Quote != "USD" {
			return p, fmt.Errorf("inverse future must quote USD")
		}
		s.NativeAmountKind, s.NativeAmountCurrency = "usd", "USD"
	}
	// This identifies the economic family; it does not assert that fee/window
	// rule pages have been fetched or verified by this metadata-only decoder.
	s.SettlementSemanticsID = "deribit-" + string(i.MarketType) + "-" + m.SourceType + "-v1"
	s.Instrument = i
	s.Instrument.VenueContractVersion = s.Version()
	if err = s.Validate(); err != nil {
		return p, err
	}
	r := options.TradingRule{ObservedAt: at, KnownFrom: at, EffectiveFrom: at, EffectiveTimeBasis: "first_observed", SourceURL: "https://www.deribit.com/api/v2/public/get_instruments", PayloadHash: hash}
	r.SourceTick, err = options.ParseNumber(string(m.Tick))
	if err != nil {
		return p, err
	}
	r.MinAmount, err = options.ParseNumber(string(m.MinAmount))
	if err != nil {
		return p, err
	}
	r.AmountStep = r.MinAmount
	for _, b := range m.Steps {
		above, err := options.ParseNumber(string(b.Above))
		if err != nil {
			return p, err
		}
		tick, err := options.ParseNumber(string(b.Tick))
		if err != nil {
			return p, err
		}
		r.Bands = append(r.Bands, options.TickBand{AbovePrice: above, TickSize: tick})
	}
	// Validate before registration using a temporary ID; callers bind the actual
	// registry ID and only then compute/persist the rule ID.
	r.InstrumentID = 1
	if err = r.Validate(); err != nil {
		return p, err
	}
	r.InstrumentID = 0
	if strings.TrimSpace(m.State) != m.State {
		return p, fmt.Errorf("invalid market state")
	}
	return ParsedInstrument{Spec: s, Rule: r, State: m.State, Active: *m.Active}, nil
}
