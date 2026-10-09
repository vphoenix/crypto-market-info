package model

import (
	"fmt"
	"net/url"
	"strings"
)

// Members are ordered semantic roles, committed separately from the fact body.
// This binds the response hash and its native/request clock, without raw JSON.
func DiscoverySourcesHash(sources []PublicSource) string {
	rows := make([]map[string]any, 0, len(sources))
	for n, s := range sources {
		rows = append(rows, map[string]any{"source_order": n, "source_url": s.URL, "payload_hash": s.PayloadHash, "time_basis": s.TimeBasis, "source_ms": s.SourceMS, "observed_ms": s.ObservedMS})
	}
	return DiscoveryHash(rows)
}

func ValidateDiscoverySources(kind string, sources []PublicSource) error {
	paths := map[string][]string{
		"funding_forecast_observation":  {"/api/v5/public/funding-rate", "/api/v5/public/mark-price", "/api/v5/market/index-tickers", "/api/v5/market/index-tickers"},
		"cex_trade_bar_1m":              {"/api/v5/market/candles"},
		"cex_fee_schedule_rule":         {"/help/", "/api/v5/public/instruments", "/api/v5/public/instruments"},
		"cex_collateral_risk_rule":      {"/api/v5/public/discount-rate-interest-free-quota", "/api/v5/public/position-tiers", "/api/v5/public/position-tiers"},
		"okx_public_capital_parameters": {"/api/v5/public/discount-rate-interest-free-quota", "/api/v5/public/position-tiers", "/api/v5/public/position-tiers"},
	}
	expected, ok := paths[kind]
	if !ok || len(sources) != len(expected) {
		return fmt.Errorf("incomplete public source roles")
	}
	for n, s := range sources {
		if err := s.Validate(); err != nil {
			return err
		}
		u, err := url.Parse(s.URL)
		if err != nil {
			return err
		}
		if expected[n] == "/help/" {
			if s.URL != "https://www.okx.com/en-gb/help/advance-notice-spot-and-futures-trading-fee-adjustment" {
				return fmt.Errorf("unreviewed fee source URL")
			}
			if !strings.Contains(u.Path, "/help/") || s.TimeBasis != "reviewed_static_rule" {
				return fmt.Errorf("fee rule source missing")
			}
		} else if u.Path != expected[n] {
			return fmt.Errorf("public source role mismatch")
		}
		if (u.Hostname() != "www.okx.com" && u.Hostname() != "openapi.okx.com") || u.User != nil || u.Port() != "" {
			return fmt.Errorf("unreviewed public source origin")
		}
		q := u.Query()
		if kind != "funding_forecast_observation" && expected[n] != "/help/" && s.TimeBasis != "request_without_source_clock" {
			return fmt.Errorf("request-observation clock required")
		}
		if kind == "funding_forecast_observation" {
			if s.TimeBasis != "exchange_ts" {
				return fmt.Errorf("native source clock missing")
			}
			if n == 2 && (!strings.HasSuffix(q.Get("instId"), "-USD") || q.Get("instId") == "USDT-USD") {
				return fmt.Errorf("base index source missing")
			}
			if n == 3 && q.Get("instId") != "USDT-USD" {
				return fmt.Errorf("quote index source missing")
			}
		}
		if (kind == "okx_public_capital_parameters" || kind == "cex_collateral_risk_rule") && n > 0 {
			if q.Get("tdMode") != "cross" || (n == 1 && (q.Get("instType") != "SWAP" || q.Get("instFamily") == "")) || (n == 2 && (q.Get("instType") != "MARGIN" || q.Get("ccy") == "" || q.Get("instId") != "")) {
				return fmt.Errorf("risk source scope mismatch")
			}
		}
	}
	return nil
}
