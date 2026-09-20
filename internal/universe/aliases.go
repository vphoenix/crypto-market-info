package universe

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/model"
)

type Alias struct {
	Exchange                           string           `json:"exchange"`
	MarketType                         model.MarketType `json:"market_type"`
	ExchangeSymbol                     string           `json:"exchange_symbol"`
	VenueContractVersion               string           `json:"venue_contract_version"`
	ExpectedBaseAsset                  string           `json:"expected_base_asset"`
	ExpectedQuoteAsset                 string           `json:"expected_quote_asset"`
	ExpectedSettleAsset                string           `json:"expected_settle_asset"`
	CanonicalBaseAsset                 string           `json:"canonical_base_asset"`
	CanonicalBaseUnitsPerVenueBaseUnit string           `json:"canonical_base_units_per_venue_base_unit"`
	Reason                             string           `json:"reason"`
}

type AliasFile struct {
	SchemaVersion int     `json:"schema_version"`
	Aliases       []Alias `json:"aliases"`
}

type aliasKey struct {
	exchange        string
	market          model.MarketType
	symbol, version string
}

type Aliases struct {
	File     AliasFile
	Revision string
	entries  map[aliasKey]Alias
}

func LoadAliases(path string) (*Aliases, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read perpetual aliases: %w", err)
	}
	return ParseAliases(b)
}

func ParseAliases(b []byte) (*Aliases, error) {
	if err := rejectDuplicateJSONKeys(json.NewDecoder(bytes.NewReader(b))); err != nil {
		return nil, err
	}
	var file AliasFile
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err := d.Decode(&file); err != nil {
		return nil, err
	}
	if file.SchemaVersion != 1 || file.Aliases == nil {
		return nil, fmt.Errorf("aliases requires schema_version 1 and an aliases array")
	}
	a := &Aliases{File: file, entries: make(map[aliasKey]Alias)}
	for _, e := range file.Aliases {
		for name, value := range map[string]string{"exchange": e.Exchange, "market_type": string(e.MarketType), "exchange_symbol": e.ExchangeSymbol, "venue_contract_version": e.VenueContractVersion, "expected_base_asset": e.ExpectedBaseAsset, "expected_quote_asset": e.ExpectedQuoteAsset, "expected_settle_asset": e.ExpectedSettleAsset, "canonical_base_asset": e.CanonicalBaseAsset, "reason": e.Reason} {
			if value == "" || strings.TrimSpace(value) != value {
				return nil, fmt.Errorf("alias %s must be nonempty and exact", name)
			}
		}
		if e.MarketType != model.MarketPerpetual || e.ExpectedQuoteAsset != "USDT" || e.ExpectedSettleAsset != "USDT" || !basePattern.MatchString(e.CanonicalBaseAsset) {
			return nil, fmt.Errorf("ineligible alias %s %s", e.Exchange, e.ExchangeSymbol)
		}
		factor, err := model.ParsePositiveDecimal(e.CanonicalBaseUnitsPerVenueBaseUnit, "alias factor")
		if err != nil {
			return nil, err
		}
		if !factor.Equal(factor.Truncate(18)) || factor.GreaterThanOrEqual(decimal.New(1, 20)) {
			return nil, fmt.Errorf("alias factor exceeds Decimal(38,18)")
		}
		key := aliasKey{e.Exchange, e.MarketType, e.ExchangeSymbol, e.VenueContractVersion}
		if _, exists := a.entries[key]; exists {
			return nil, fmt.Errorf("duplicate alias key: %+v", key)
		}
		a.entries[key] = e
	}
	sort.Slice(a.File.Aliases, func(i, j int) bool {
		a, b := a.File.Aliases[i], a.File.Aliases[j]
		return compareIdentity(a.Exchange, a.MarketType, a.ExchangeSymbol, a.VenueContractVersion, b.Exchange, b.MarketType, b.ExchangeSymbol, b.VenueContractVersion)
	})
	encoded, err := json.Marshal(a.File)
	if err != nil {
		return nil, err
	}
	a.Revision = Hash("perpetual-canonical-v1", encoded)
	return a, nil
}

func compareIdentity(a string, am model.MarketType, as, av, b string, bm model.MarketType, bs, bv string) bool {
	if a != b {
		return a < b
	}
	if am != bm {
		return am < bm
	}
	if as != bs {
		return as < bs
	}
	return av < bv
}

func Hash(prefix string, b []byte) string {
	sum := sha256.Sum256(append([]byte(prefix+"\n"), b...))
	return hex.EncodeToString(sum[:])
}

type SelectedInstrument struct {
	Instrument                         model.Instrument   `json:"instrument"`
	MarketKey                          PerpetualMarketKey `json:"market_key"`
	CanonicalBaseUnitsPerVenueBaseUnit decimal.Decimal    `json:"canonical_base_units_per_venue_base_unit"`
	MappingKind                        string             `json:"mapping_kind"`
}

func (a *Aliases) Resolve(i model.Instrument) (SelectedInstrument, error) {
	if !Eligible(i) {
		return SelectedInstrument{}, fmt.Errorf("ineligible perpetual %s %s", i.Exchange, i.ExchangeSymbol)
	}
	base, factor, kind := i.BaseAsset, decimal.NewFromInt(1), "identity"
	if e, ok := a.entries[aliasKey{i.Exchange, i.MarketType, i.ExchangeSymbol, i.VenueContractVersion}]; ok {
		if e.ExpectedBaseAsset != i.BaseAsset || e.ExpectedQuoteAsset != i.QuoteAsset || i.SettleAsset == nil || e.ExpectedSettleAsset != *i.SettleAsset {
			return SelectedInstrument{}, fmt.Errorf("alias assertions differ: %s %s version %s", i.Exchange, i.ExchangeSymbol, i.VenueContractVersion)
		}
		base = e.CanonicalBaseAsset
		factor, _ = model.ParsePositiveDecimal(e.CanonicalBaseUnitsPerVenueBaseUnit, "alias factor")
		kind = "alias"
	}
	k, err := ParsePerpetualMarketKey(base + "-USDT-PERP")
	if err != nil {
		return SelectedInstrument{}, err
	}
	return SelectedInstrument{i, k, factor, kind}, nil
}

// Decode tokens first: encoding/json normally accepts duplicate object keys.
func rejectDuplicateJSONKeys(d *json.Decoder) error {
	var walk func() error
	walk = func() error {
		t, err := d.Token()
		if err != nil {
			return err
		}
		switch t {
		case json.Delim('{'):
			seen := map[string]bool{}
			for d.More() {
				key, err := d.Token()
				if err != nil {
					return err
				}
				s, ok := key.(string)
				if !ok || seen[s] {
					return fmt.Errorf("duplicate/invalid JSON key %v", key)
				}
				// Struct decoding is case-insensitive even with DisallowUnknownFields.
				// Schema keys are all lowercase; reject casing aliases before decode.
				if s != strings.ToLower(s) {
					return fmt.Errorf("noncanonical alias JSON key %q", s)
				}
				seen[s] = true
				if err = walk(); err != nil {
					return err
				}
			}
			_, err = d.Token()
			return err
		case json.Delim('['):
			for d.More() {
				if err = walk(); err != nil {
					return err
				}
			}
			_, err = d.Token()
			return err
		}
		return nil
	}
	if err := walk(); err != nil {
		return err
	}
	if _, err := d.Token(); err != io.EOF {
		return fmt.Errorf("trailing alias JSON")
	}
	return nil
}
