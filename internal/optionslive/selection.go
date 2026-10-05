package optionslive

import (
	"context"
	"fmt"
	"net/url"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/exchange/deribit"
	"github.com/vphoenix/crypto-market-info/internal/options"
)

type Config struct {
	Enabled                                                        bool
	RESTURL, WSURL                                                 string
	Symbols                                                        []string
	MaxBooks, MaxConnections, ChannelsPerConnection, MaxBookLevels int
	MaxTotalLevels, MaxIngressBytes                                int64
	EvidenceDir                                                    string
}

func (c Config) Validate() error {
	if err := c.validateCatalog(); err != nil {
		return err
	}
	for _, raw := range []string{c.RESTURL, c.WSURL} {
		u, e := url.Parse(raw)
		if e != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return fmt.Errorf("invalid public Deribit URL")
		}
	}
	rest, _ := url.Parse(c.RESTURL)
	ws, _ := url.Parse(c.WSURL)
	if (rest.Scheme != "http" && rest.Scheme != "https") || (ws.Scheme != "ws" && ws.Scheme != "wss") {
		return fmt.Errorf("invalid Deribit URL scheme")
	}
	if len(c.Symbols) > options.MaxLiveBooks {
		return fmt.Errorf("too many options symbols")
	}
	seen := map[string]bool{}
	for _, s := range c.Symbols {
		if _, e := symbolScope(s); e != nil {
			return e
		}
		if seen[s] {
			return fmt.Errorf("duplicate options symbol %s", s)
		}
		seen[s] = true
	}
	return nil
}
func symbolScope(symbol string) (deribit.Scope, error) {
	parts := strings.Split(symbol, "-")
	if len(parts) != 2 && len(parts) != 4 {
		return deribit.Scope{}, fmt.Errorf("unsupported options symbol %q", symbol)
	}
	currency := parts[0]
	if currency == "BTC_USDC" || currency == "ETH_USDC" {
		currency = "USDC"
	}
	if currency != "BTC" && currency != "ETH" && currency != "USDC" {
		return deribit.Scope{}, fmt.Errorf("unsupported options family %q", symbol)
	}
	kind := "future"
	if len(parts) == 4 {
		kind = "option"
		if parts[3] != "C" && parts[3] != "P" {
			return deribit.Scope{}, fmt.Errorf("invalid option type")
		}
	}
	for _, p := range parts {
		if p == "" {
			return deribit.Scope{}, fmt.Errorf("empty symbol component")
		}
	}
	return deribit.Scope{Currency: currency, Kind: kind}, nil
}

type Plan struct {
	Selected   []deribit.ParsedInstrument
	Scopes     []deribit.ScopeResult
	References []options.SelectionReference
	Selection  string
}

func Discover(ctx context.Context, c *deribit.Client, cfg Config) (Plan, error) {
	var plan Plan
	if err := cfg.Validate(); err != nil {
		return plan, err
	}
	wanted := map[string]deribit.Scope{}
	if len(cfg.Symbols) == 0 {
		for _, currency := range []string{"BTC", "ETH", "USDC"} {
			for _, kind := range []string{"option", "future"} {
				s := deribit.Scope{Currency: currency, Kind: kind}
				wanted[s.String()] = s
			}
		}
	} else {
		for _, symbol := range cfg.Symbols {
			s, _ := symbolScope(symbol)
			wanted[s.String()] = s
		}
	}
	var keys []string
	for key := range wanted {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	var all []deribit.ParsedInstrument
	for _, key := range keys {
		r, err := c.FetchScope(ctx, wanted[key])
		if err != nil {
			return plan, fmt.Errorf("catalog %s: %w", key, err)
		}
		plan.Scopes = append(plan.Scopes, r)
		all = append(all, r.Instruments...)
	}
	now := time.Now().UTC()
	bySymbol := map[string]deribit.ParsedInstrument{}
	for _, p := range all {
		if _, ok := bySymbol[p.Spec.Instrument.ExchangeSymbol]; ok {
			return plan, fmt.Errorf("duplicate cross-scope identity")
		}
		bySymbol[p.Spec.Instrument.ExchangeSymbol] = p
	}
	if len(cfg.Symbols) > 0 {
		plan.Selection = "explicit"
		for _, name := range cfg.Symbols {
			p, ok := bySymbol[name]
			if !ok || !p.Active || p.State != "open" || !p.Spec.Instrument.ExpiryTime.After(now) {
				return plan, fmt.Errorf("configured member not live: %s", name)
			}
			plan.Selected = append(plan.Selected, p)
		}
	} else {
		plan.Selection = options.CatalogSelection
		for _, p := range all {
			if p.Active && p.State == "open" && p.Spec.Instrument.ExpiryTime.After(now) {
				plan.Selected = append(plan.Selected, p)
			}
		}
	}
	sort.Slice(plan.Selected, func(i, j int) bool {
		return plan.Selected[i].Spec.Instrument.ExchangeSymbol < plan.Selected[j].Spec.Instrument.ExchangeSymbol
	})
	specs := make([]options.ContractSpec, len(plan.Selected))
	for n, p := range plan.Selected {
		specs[n] = p.Spec
	}
	if len(cfg.Symbols) > 0 {
		if err := options.ValidateSelection(specs); err != nil {
			return plan, err
		}
	}
	return plan, nil
}
