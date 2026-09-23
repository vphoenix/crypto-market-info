package app

import (
	"context"
	"encoding/json"
	"github.com/vphoenix/crypto-market-info/internal/options"
	"io"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/config"
	"github.com/vphoenix/crypto-market-info/internal/exchange/deribit"
	"github.com/vphoenix/crypto-market-info/internal/optionslive"
)

// PrintOptionsPlan uses public REST only; it does not connect to ClickHouse or WS.
func PrintOptionsPlan(ctx context.Context, cfg config.Config, out io.Writer) error {
	p, err := optionslive.Discover(ctx, deribit.NewClient(cfg.Options.RESTURL, cfg.Options.WSURL), cfg.Options)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	type member struct {
		Symbol, Kind, Strike, Index string
		Expiry                      *time.Time
	}
	type source struct {
		Scope, URL, Hash string
		ObservedAt       time.Time
		Count            int
	}
	result := struct {
		Selection  string
		Members    []member
		Sources    []source
		References []options.SelectionReference
	}{Selection: p.Selection, References: p.References}
	for _, m := range p.Selected {
		result.Members = append(result.Members, member{m.Spec.Instrument.ExchangeSymbol, string(m.Spec.Instrument.MarketType), m.Spec.Strike.String(), m.Spec.IndexID, m.Spec.Instrument.ExpiryTime})
	}
	for _, s := range p.Scopes {
		result.Sources = append(result.Sources, source{s.Scope.String(), s.URL, s.PayloadHash, s.ObservedAt, len(s.Instruments)})
	}
	return enc.Encode(result)
}
