package clickhouse

import (
	"context"
	"fmt"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"net/url"
)

func (c *Client) validateDiscoveryScope(ctx context.Context, f discoveryFact) error {
	body := f.Body()
	sources := factSources(f)
	if err := model.ValidateDiscoverySources(f.Kind(), sources); err != nil {
		return err
	}
	ids, err := c.instrumentsWhere(ctx, "WHERE instrument_id=?", []any{body["instrument_id"]})
	if err != nil {
		return err
	}
	if len(ids) != 1 {
		return fmt.Errorf("public fact instrument is not registered")
	}
	i := ids[0]
	if i.MarketType != model.MarketPerpetual || i.Exchange != "OKX" || i.QuoteAsset != "USDT" {
		return fmt.Errorf("unsupported discovery source instrument")
	}
	if base, ok := body["base"]; ok && base != i.BaseAsset {
		return fmt.Errorf("capital base identity differs")
	}
	var roles []map[string]string
	switch f.Kind() {
	case "funding_forecast_observation":
		roles = []map[string]string{{"instId": i.ExchangeSymbol}, {"instId": i.ExchangeSymbol, "instType": "SWAP"}, {"instId": i.BaseAsset + "-USD"}, {"instId": "USDT-USD"}}
		for n, k := range []string{"source_ms", "mark_ms", "base_index_ms", "quote_index_ms"} {
			if sources[n].SourceMS != body[k] {
				return fmt.Errorf("native source clock differs from typed fact")
			}
		}
	case "cex_trade_bar_1m":
		roles = []map[string]string{{"instId": i.ExchangeSymbol, "bar": "1m"}}
		if sources[0].SourceMS != body["source_ms"] {
			return fmt.Errorf("bar source clock differs")
		}
	case "cex_fee_schedule_rule":
		roles = []map[string]string{{}, {"instType": "SPOT", "instId": i.BaseAsset + "-USDT"}, {"instType": "SWAP", "instId": i.ExchangeSymbol}}
	case "cex_collateral_risk_rule", "okx_public_capital_parameters":
		roles = []map[string]string{{}, {"instType": "SWAP", "tdMode": "cross", "instFamily": i.BaseAsset + "-USDT"}, {"instType": "MARGIN", "tdMode": "cross", "ccy": i.BaseAsset}}
	}
	first, last := sources[0].SourceMS, sources[0].ObservedMS
	for n, s := range sources {
		u, err := url.Parse(s.URL)
		if err != nil {
			return err
		}
		q := u.Query()
		for key, want := range roles[n] {
			if values := q[key]; len(values) != 1 || values[0] != want {
				return fmt.Errorf("public source instrument differs from registered definition")
			}
		}
		first = min(first, s.SourceMS)
		last = max(last, s.ObservedMS)
	}
	if f.Kind() != "funding_forecast_observation" && (first != body["source_ms"] || last != body["observed_ms"]) {
		return fmt.Errorf("source clocks differ from typed observation")
	}
	return nil
}
