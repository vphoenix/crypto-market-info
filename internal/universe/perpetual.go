package universe

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/vphoenix/crypto-market-info/internal/model"
)

type SelectionMode string

const (
	Disabled SelectionMode = "disabled"
	Auto     SelectionMode = "auto"
	Explicit SelectionMode = "explicit"
)

type VenueSelectionConfig struct {
	Venue               string        `json:"venue"`
	Mode                SelectionMode `json:"mode"`
	Include             []string      `json:"include"`
	Exclude             []string      `json:"exclude"`
	TopicsPerConnection int           `json:"topics_per_connection"`
	MaxInstruments      int           `json:"max_instruments"`
}

type VenueCatalog struct {
	Venue       string
	Instruments []model.Instrument
}
type SelectionConfig struct {
	Venues  []VenueSelectionConfig `json:"venues"`
	Include []string               `json:"canonical_include"`
	Exclude []string               `json:"canonical_exclude"`
}
type VenueSummary struct {
	Venue              string        `json:"venue"`
	Mode               SelectionMode `json:"mode"`
	Candidates         int           `json:"candidates"`
	FilteredCandidates int           `json:"filtered_candidates"`
	Selected           int           `json:"selected"`
}
type PotentialAlias struct {
	Left  model.Instrument `json:"left"`
	Right model.Instrument `json:"right"`
}
type Result struct {
	MappingRevision          string               `json:"mapping_revision"`
	Selected                 []SelectedInstrument `json:"membership"`
	Venues                   []VenueSummary       `json:"venues"`
	GroupCount               int                  `json:"canonical_group_count"`
	IgnoredSingleVenueGroups int                  `json:"ignored_single_venue_groups"`
	IdentityCount            int                  `json:"identity_count"`
	AliasCount               int                  `json:"alias_count"`
	Warnings                 []string             `json:"warnings"`
	PotentialAliases         []PotentialAlias     `json:"potential_aliases"`
}

func ParseList(raw string) ([]string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" || raw == "-" {
		return []string{}, nil
	}
	parts := strings.Split(raw, ",")
	seen := map[string]bool{}
	for i, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" || p == "-" || seen[p] {
			return nil, fmt.Errorf("empty, reserved or duplicate list item %q", p)
		}
		parts[i] = p
		seen[p] = true
	}
	sort.Strings(parts)
	return parts, nil
}

func ParseVenueSelection(venue, raw string) (VenueSelectionConfig, error) {
	s := VenueSelectionConfig{Venue: venue, Include: []string{}, Exclude: []string{}}
	switch strings.TrimSpace(raw) {
	case "":
		return s, fmt.Errorf("%s perpetual symbols must be auto, - or explicit symbols", venue)
	case "auto":
		s.Mode = Auto
	case "-":
		s.Mode = Disabled
	default:
		s.Mode = Explicit
		var err error
		s.Include, err = ParseList(raw)
		if err != nil {
			return s, err
		}
		for _, v := range s.Include {
			if v == "auto" || strings.ToUpper(v) != v || strings.ContainsAny(v, " \t\r\n") {
				return s, fmt.Errorf("invalid exact %s symbol %q", venue, v)
			}
		}
	}
	return s, nil
}

func normalizeConfig(c SelectionConfig) (SelectionConfig, error) {
	c.Venues = append([]VenueSelectionConfig(nil), c.Venues...)
	sort.Slice(c.Venues, func(i, j int) bool { return c.Venues[i].Venue < c.Venues[j].Venue })
	seen := map[string]bool{}
	enabled := 0
	for i, v := range c.Venues {
		if v.Venue == "" || seen[v.Venue] {
			return c, fmt.Errorf("empty/duplicate venue %q", v.Venue)
		}
		seen[v.Venue] = true
		if v.Mode != Disabled && v.Mode != Auto && v.Mode != Explicit {
			return c, fmt.Errorf("invalid %s selection mode", v.Venue)
		}
		if v.Mode != Disabled {
			enabled++
		}
		if (v.Mode == Explicit) != (len(v.Include) > 0) {
			return c, fmt.Errorf("%s explicit mode requires symbols; other modes forbid include", v.Venue)
		}
		var err error
		v.Include, err = normalizedList(v.Include, false)
		if err != nil {
			return c, err
		}
		v.Exclude, err = normalizedList(v.Exclude, false)
		if err != nil {
			return c, err
		}
		if overlap(v.Include, v.Exclude) {
			return c, fmt.Errorf("%s raw include/exclude conflict", v.Venue)
		}
		c.Venues[i] = v
	}
	if enabled == 1 {
		return c, fmt.Errorf("at least two perpetual venues must be enabled, or disable all")
	}
	var err error
	c.Include, err = normalizedList(c.Include, true)
	if err != nil {
		return c, err
	}
	c.Exclude, err = normalizedList(c.Exclude, true)
	if err != nil {
		return c, err
	}
	if overlap(c.Include, c.Exclude) {
		return c, fmt.Errorf("canonical include/exclude conflict")
	}
	return c, nil
}
func NormalizeConfig(c SelectionConfig) (SelectionConfig, error) { return normalizeConfig(c) }
func normalizedList(v []string, canonical bool) ([]string, error) {
	out := append([]string{}, v...)
	sort.Strings(out)
	for i, s := range out {
		if s == "" || strings.TrimSpace(s) != s || (i > 0 && s == out[i-1]) {
			return nil, fmt.Errorf("invalid/duplicate list value %q", s)
		}
		if canonical {
			if _, err := ParsePerpetualMarketKey(s); err != nil {
				return nil, err
			}
		}
	}
	return out, nil
}
func set(v []string) map[string]bool {
	m := map[string]bool{}
	for _, s := range v {
		m[s] = true
	}
	return m
}
func overlap(a, b []string) bool {
	m := set(a)
	for _, s := range b {
		if m[s] {
			return true
		}
	}
	return false
}

func BuildCommonPerpetualUniverse(catalogs []VenueCatalog, cfg SelectionConfig, aliases *Aliases) (Result, error) {
	r := Result{Selected: []SelectedInstrument{}, Venues: []VenueSummary{}, Warnings: []string{}, PotentialAliases: []PotentialAlias{}}
	if aliases == nil {
		return r, fmt.Errorf("alias dictionary required")
	}
	r.MappingRevision = aliases.Revision
	cfg, err := normalizeConfig(cfg)
	if err != nil {
		return r, err
	}
	byVenue := map[string][]model.Instrument{}
	for _, c := range catalogs {
		if _, exists := byVenue[c.Venue]; exists {
			return r, fmt.Errorf("duplicate catalog %s", c.Venue)
		}
		byVenue[c.Venue] = c.Instruments
	}
	groups := map[string][]SelectedInstrument{}
	hitAliases := map[aliasKey]bool{}
	var all []SelectedInstrument
	rawRequired := map[string]map[string]bool{}
	for _, v := range cfg.Venues {
		summary := VenueSummary{Venue: v.Venue, Mode: v.Mode}
		if v.Mode == Disabled {
			r.Venues = append(r.Venues, summary)
			continue
		}
		items, exists := byVenue[v.Venue]
		if !exists {
			return r, fmt.Errorf("missing complete catalog %s", v.Venue)
		}
		includes, excludes := set(v.Include), set(v.Exclude)
		seen := map[string]bool{}
		canonicalSeen := map[string]bool{}
		if v.Mode == Explicit {
			rawRequired[v.Venue] = set(v.Include)
		}
		for _, i := range items {
			if i.Exchange != v.Venue || !Eligible(i) || i.VenueContractVersion == "" {
				return r, fmt.Errorf("invalid catalog identity %s %s", v.Venue, i.ExchangeSymbol)
			}
			if err = i.ValidateDefinition(); err != nil {
				return r, err
			}
			if seen[i.ExchangeSymbol] {
				return r, fmt.Errorf("duplicate live symbol %s %s", v.Venue, i.ExchangeSymbol)
			}
			seen[i.ExchangeSymbol] = true
			summary.Candidates++
			s, err := aliases.Resolve(i)
			if err != nil {
				return r, err
			}
			if s.MappingKind == "alias" {
				hitAliases[aliasKey{i.Exchange, i.MarketType, i.ExchangeSymbol, i.VenueContractVersion}] = true
			}
			all = append(all, s)
			if (v.Mode == Explicit && !includes[i.ExchangeSymbol]) || excludes[i.ExchangeSymbol] {
				continue
			}
			key := s.MarketKey.String()
			if canonicalSeen[key] {
				return r, fmt.Errorf("canonical collision at %s for %s; exclude one raw symbol or fix aliases", v.Venue, key)
			}
			canonicalSeen[key] = true
			groups[key] = append(groups[key], s)
			summary.FilteredCandidates++
		}
		for _, s := range v.Include {
			if !seen[s] {
				return r, fmt.Errorf("%s explicit symbol missing or ineligible: %s", v.Venue, s)
			}
		}
		for _, s := range v.Exclude {
			if !seen[s] {
				r.Warnings = append(r.Warnings, fmt.Sprintf("stale venue exclude: %s %s", v.Venue, s))
			}
		}
		r.Venues = append(r.Venues, summary)
	}
	includes, excludes := set(cfg.Include), set(cfg.Exclude)
	matched := map[string]bool{}
	for key, g := range groups {
		if len(g) < 2 {
			r.IgnoredSingleVenueGroups++
			continue
		}
		matched[key] = true
		if (len(includes) > 0 && !includes[key]) || excludes[key] {
			continue
		}
		r.GroupCount++
		r.Selected = append(r.Selected, g...)
	}
	for _, key := range cfg.Include {
		if !matched[key] {
			return r, fmt.Errorf("canonical include %s is not shared by at least two enabled venues", key)
		}
	}
	for _, key := range cfg.Exclude {
		if !matched[key] {
			r.Warnings = append(r.Warnings, "stale canonical exclude: "+key)
		}
	}
	sort.Slice(r.Selected, func(i, j int) bool {
		a, b := r.Selected[i], r.Selected[j]
		if a.MarketKey.String() != b.MarketKey.String() {
			return a.MarketKey.String() < b.MarketKey.String()
		}
		return compareIdentity(a.Instrument.Exchange, a.Instrument.MarketType, a.Instrument.ExchangeSymbol, a.Instrument.VenueContractVersion, b.Instrument.Exchange, b.Instrument.MarketType, b.Instrument.ExchangeSymbol, b.Instrument.VenueContractVersion)
	})
	counts := map[string]int{}
	for _, s := range r.Selected {
		counts[s.Instrument.Exchange]++
		delete(rawRequired[s.Instrument.Exchange], s.Instrument.ExchangeSymbol)
		if s.MappingKind == "alias" {
			r.AliasCount++
		} else {
			r.IdentityCount++
		}
	}
	for venue, required := range rawRequired {
		if len(required) > 0 {
			return r, fmt.Errorf("%s explicit symbols not in final shared universe: %v", venue, sortedKeys(required))
		}
	}
	for i := range r.Venues {
		r.Venues[i].Selected = counts[r.Venues[i].Venue]
	}
	for key := range aliases.entries {
		if !hitAliases[key] {
			r.Warnings = append(r.Warnings, fmt.Sprintf("unmatched historical alias: %s %s version %s", key.exchange, key.symbol, key.version))
		}
	}
	sort.Strings(r.Warnings)
	sort.Slice(all, func(i, j int) bool {
		a, b := all[i].Instrument, all[j].Instrument
		return compareIdentity(a.Exchange, a.MarketType, a.ExchangeSymbol, a.VenueContractVersion, b.Exchange, b.MarketType, b.ExchangeSymbol, b.VenueContractVersion)
	})
	for i, a := range all {
		for _, b := range all[i+1:] {
			if a.Instrument.Exchange == b.Instrument.Exchange || a.MarketKey == b.MarketKey {
				continue
			}
			ab := strings.TrimLeft(a.Instrument.BaseAsset, "0123456789")
			bb := strings.TrimLeft(b.Instrument.BaseAsset, "0123456789")
			if ab != "" && ab == bb {
				r.PotentialAliases = append(r.PotentialAliases, PotentialAlias{a.Instrument, b.Instrument})
			}
		}
	}
	return r, nil
}

func sortedKeys(m map[string]bool) []string {
	v := make([]string, 0, len(m))
	for k := range m {
		v = append(v, k)
	}
	sort.Strings(v)
	return v
}

func SelectionRevision(cfg SelectionConfig, mappingRevision string, selected []SelectedInstrument, capacity any) (string, string, error) {
	cfg, err := normalizeConfig(cfg)
	if err != nil {
		return "", "", err
	}
	configBytes, err := json.Marshal(struct {
		SelectionConfig
		Capacity any `json:"capacity"`
	}{cfg, capacity})
	if err != nil {
		return "", "", err
	}
	type identity struct{ Key, Exchange, Symbol, Version string }
	identities := make([]identity, 0, len(selected))
	for _, s := range selected {
		identities = append(identities, identity{s.MarketKey.String(), s.Instrument.Exchange, s.Instrument.ExchangeSymbol, s.Instrument.VenueContractVersion})
	}
	sort.Slice(identities, func(i, j int) bool {
		a, b := identities[i], identities[j]
		if a.Key != b.Key {
			return a.Key < b.Key
		}
		return compareIdentity(a.Exchange, model.MarketPerpetual, a.Symbol, a.Version, b.Exchange, model.MarketPerpetual, b.Symbol, b.Version)
	})
	b, err := json.Marshal(struct {
		MappingRevision string          `json:"mapping_revision"`
		Config          json.RawMessage `json:"config"`
		Members         []identity      `json:"members"`
	}{mappingRevision, configBytes, identities})
	if err != nil {
		return "", "", err
	}
	return Hash("perpetual-universe-v1", b), string(configBytes), nil
}
