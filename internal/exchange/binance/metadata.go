package binance

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/model"
)

type exchangeInfo struct {
	Symbols json.RawMessage `json:"symbols"`
}

type symbolInfo struct {
	Symbol       string                       `json:"symbol"`
	Status       string                       `json:"status"`
	ContractType string                       `json:"contractType"`
	BaseAsset    string                       `json:"baseAsset"`
	QuoteAsset   string                       `json:"quoteAsset"`
	MarginAsset  string                       `json:"marginAsset"`
	Filters      []map[string]json.RawMessage `json:"filters"`
}

func ParseExchangeInfo(payload []byte, marketType model.MarketType) ([]model.Instrument, error) {
	if marketType != model.MarketSpot && marketType != model.MarketPerpetual {
		return nil, fmt.Errorf("Binance first version supports spot and perpetual metadata")
	}
	var envelope exchangeInfo
	// Binance adds top-level fields over time, so strictness is applied to every field we consume,
	// while unknown top-level fields remain forward compatible.
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return nil, fmt.Errorf("Binance exchangeInfo JSON: %w", err)
	}
	if len(bytes.TrimSpace(envelope.Symbols)) == 0 || bytes.Equal(bytes.TrimSpace(envelope.Symbols), []byte("null")) {
		return nil, fmt.Errorf("Binance exchangeInfo symbols must be a non-null array")
	}
	var symbols []symbolInfo
	if err := json.Unmarshal(envelope.Symbols, &symbols); err != nil {
		return nil, fmt.Errorf("Binance exchangeInfo symbols: %w", err)
	}
	out := make([]model.Instrument, 0, len(symbols))
	seen := make(map[string]struct{})
	for _, symbol := range symbols {
		if symbol.Status != "TRADING" {
			continue
		}
		if marketType == model.MarketPerpetual && (symbol.ContractType != "PERPETUAL" || symbol.MarginAsset != "USDT") {
			continue
		}
		instrument, err := parseSymbol(symbol, marketType)
		if err != nil {
			return nil, err
		}
		if _, exists := seen[instrument.ExchangeSymbol]; exists {
			return nil, fmt.Errorf("Binance duplicate active symbol %q", instrument.ExchangeSymbol)
		}
		seen[instrument.ExchangeSymbol] = struct{}{}
		out = append(out, instrument)
	}
	return out, nil
}

func parseSymbol(symbol symbolInfo, marketType model.MarketType) (model.Instrument, error) {
	for name, value := range map[string]string{"symbol": symbol.Symbol, "baseAsset": symbol.BaseAsset, "quoteAsset": symbol.QuoteAsset} {
		if value == "" || strings.TrimSpace(value) != value {
			return model.Instrument{}, fmt.Errorf("Binance %s requires an exact value", name)
		}
	}
	tickRaw, err := filterValue(symbol.Filters, "PRICE_FILTER", "tickSize")
	if err != nil {
		return model.Instrument{}, fmt.Errorf("Binance %s: %w", symbol.Symbol, err)
	}
	stepRaw, err := filterValue(symbol.Filters, "LOT_SIZE", "stepSize")
	if err != nil {
		return model.Instrument{}, fmt.Errorf("Binance %s: %w", symbol.Symbol, err)
	}
	tick, err := model.ParsePositiveDecimal(tickRaw, "tickSize")
	if err != nil {
		return model.Instrument{}, fmt.Errorf("Binance %s: %w", symbol.Symbol, err)
	}
	step, err := model.ParsePositiveDecimal(stepRaw, "stepSize")
	if err != nil {
		return model.Instrument{}, fmt.Errorf("Binance %s: %w", symbol.Symbol, err)
	}
	instrument := model.Instrument{Exchange: "Binance", MarketType: marketType, ExchangeSymbol: symbol.Symbol,
		BaseAsset: symbol.BaseAsset, QuoteAsset: symbol.QuoteAsset, ContractMultiplier: decimal.NewFromInt(1), PriceTickSize: tick, QuantityStepSize: step}
	if marketType == model.MarketPerpetual {
		settle := symbol.MarginAsset
		instrument.SettleAsset = &settle
	}
	if err := instrument.ValidateDefinition(); err != nil {
		return model.Instrument{}, fmt.Errorf("Binance %s: %w", symbol.Symbol, err)
	}
	return instrument, nil
}

func filterValue(filters []map[string]json.RawMessage, filterType, field string) (string, error) {
	found := ""
	for _, filter := range filters {
		var kind string
		if raw, exists := filter["filterType"]; exists {
			_ = json.Unmarshal(raw, &kind)
		}
		if kind != filterType {
			continue
		}
		if found != "" {
			return "", fmt.Errorf("duplicate %s filter", filterType)
		}
		raw, exists := filter[field]
		if !exists || json.Unmarshal(raw, &found) != nil || found == "" {
			return "", fmt.Errorf("%s.%s must be a decimal string", filterType, field)
		}
	}
	if found == "" {
		return "", fmt.Errorf("missing %s.%s", filterType, field)
	}
	return found, nil
}
