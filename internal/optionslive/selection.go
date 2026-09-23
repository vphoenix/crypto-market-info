package optionslive

import (
	"context"
	"fmt"
	"net/url"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/exchange/deribit"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/options"
)

type Config struct {
	Enabled        bool
	RESTURL, WSURL string
	Symbols        []string
}

func (c Config) Validate() error {
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
		plan.Selection = "near_atm_v1"
		for _, family := range []string{"btc_usd", "eth_usd", "btc_usdc", "eth_usdc"} {
			var futures []deribit.ParsedInstrument
			for _, p := range all {
				i := p.Spec.Instrument
				if i.MarketType == model.MarketDelivery && p.Spec.IndexID == family && p.Active && p.State == "open" {
					dte := i.ExpiryTime.Sub(now)
					if dte >= 48*time.Hour && dte <= 45*24*time.Hour {
						futures = append(futures, p)
					}
				}
			}
			sort.Slice(futures, func(i, j int) bool {
				return futures[i].Spec.Instrument.ExpiryTime.Before(*futures[j].Spec.Instrument.ExpiryTime)
			})
			found := false
			for _, f := range futures {
				calls, puts := map[string]deribit.ParsedInstrument{}, map[string]deribit.ParsedInstrument{}
				for _, p := range all {
					i := p.Spec.Instrument
					if i.MarketType == model.MarketOption && p.Spec.IndexID == family && i.ExpiryTime.Equal(*f.Spec.Instrument.ExpiryTime) && p.Active && p.State == "open" {
						if p.Spec.OptionType == "call" {
							calls[p.Spec.Strike.String()] = p
						} else {
							puts[p.Spec.Strike.String()] = p
						}
					}
				}
				var strikes []decimal.Decimal
				for k, p := range calls {
					if _, ok := puts[k]; ok {
						strikes = append(strikes, p.Spec.Strike)
					}
				}
				if len(strikes) < 3 {
					continue
				}
				sort.Slice(strikes, func(i, j int) bool { return strikes[i].LessThan(strikes[j]) })
				ref, err := c.Reference(ctx, f.Spec.Instrument.ExchangeSymbol)
				if err != nil {
					continue
				}
				mid := decimal.NewFromInt(ref.Bid).Add(decimal.NewFromInt(ref.Ask)).Mul(options.StorageUnit()).Div(decimal.NewFromInt(2))
				center := 0
				for n := 1; n < len(strikes); n++ {
					if strikes[n].Sub(mid).Abs().LessThan(strikes[center].Sub(mid).Abs()) {
						center = n
					}
				}
				if center == 0 || center == len(strikes)-1 {
					continue
				}
				for _, k := range strikes[center-1 : center+2] {
					plan.Selected = append(plan.Selected, calls[k.String()], puts[k.String()])
				}
				plan.Selected = append(plan.Selected, f)
				plan.References = append(plan.References, ref)
				found = true
				break
			}
			if !found {
				return plan, fmt.Errorf("no complete fresh C/P/future group for %s; configure explicit symbols", family)
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
	if err := options.ValidateSelection(specs); err != nil {
		return plan, err
	}
	return plan, nil
}
