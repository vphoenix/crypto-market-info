package bybit

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"

	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/model"
)

type instrumentsResult struct {
	Category       string          `json:"category"`
	NextPageCursor string          `json:"nextPageCursor"`
	List           json.RawMessage `json:"list"`
}

type instrumentWire struct {
	Symbol          string  `json:"symbol"`
	SymbolID        *int64  `json:"symbolId"`
	ContractType    string  `json:"contractType"`
	Status          string  `json:"status"`
	BaseCoin        string  `json:"baseCoin"`
	QuoteCoin       string  `json:"quoteCoin"`
	SettleCoin      string  `json:"settleCoin"`
	LaunchTime      *string `json:"launchTime"`
	FundingInterval *int64  `json:"fundingInterval"`
	IsPreListing    *bool   `json:"isPreListing"`
	PriceFilter     *struct {
		TickSize string `json:"tickSize"`
	} `json:"priceFilter"`
	LotSizeFilter *struct {
		QtyStep string `json:"qtyStep"`
	} `json:"lotSizeFilter"`
}

func ParseInstrumentsPage(payload []byte) ([]model.Instrument, string, error) {
	raw, err := decodeResult(payload, "instruments-info")
	if err != nil {
		return nil, "", err
	}
	var result instrumentsResult
	if err = json.Unmarshal(raw, &result); err != nil {
		return nil, "", fmt.Errorf("Bybit instruments-info result: %w", err)
	}
	if result.Category != "linear" {
		return nil, "", fmt.Errorf("Bybit instruments-info category=%q, want linear", result.Category)
	}
	trimmed := bytes.TrimSpace(result.List)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil, "", fmt.Errorf("Bybit instruments-info list must be a non-null array")
	}
	var rows []instrumentWire
	if err = json.Unmarshal(trimmed, &rows); err != nil {
		return nil, "", fmt.Errorf("Bybit instruments-info list: %w", err)
	}
	if len(rows) == 0 {
		return nil, "", fmt.Errorf("Bybit instruments-info list must not be empty")
	}
	out := make([]model.Instrument, 0, len(rows))
	seen := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		for name, value := range map[string]string{"symbol": row.Symbol, "contractType": row.ContractType, "status": row.Status, "baseCoin": row.BaseCoin, "quoteCoin": row.QuoteCoin, "settleCoin": row.SettleCoin} {
			if value == "" || strings.TrimSpace(value) != value {
				return nil, "", fmt.Errorf("Bybit instruments-info %s requires an exact value", name)
			}
		}
		if _, exists := seen[row.Symbol]; exists {
			return nil, "", fmt.Errorf("Bybit instruments-info duplicate symbol %q", row.Symbol)
		}
		seen[row.Symbol] = struct{}{}
		if row.IsPreListing == nil {
			return nil, "", fmt.Errorf("Bybit instruments-info %s isPreListing is required", row.Symbol)
		}
		if row.ContractType != "LinearPerpetual" || row.Status != "Trading" || row.QuoteCoin != "USDT" || row.SettleCoin != "USDT" || *row.IsPreListing {
			continue
		}
		instrument, mapErr := mapInstrument(row)
		if mapErr != nil {
			return nil, "", mapErr
		}
		out = append(out, instrument)
	}
	return out, result.NextPageCursor, nil
}

func mapInstrument(row instrumentWire) (model.Instrument, error) {
	if row.SymbolID == nil || *row.SymbolID <= 0 || row.LaunchTime == nil {
		return model.Instrument{}, fmt.Errorf("Bybit %s symbolId and launchTime must be present positive integers", row.Symbol)
	}
	launchTime, err := strconv.ParseInt(*row.LaunchTime, 10, 64)
	if err != nil || launchTime <= 0 {
		return model.Instrument{}, fmt.Errorf("Bybit %s launchTime must be a positive integer string", row.Symbol)
	}
	if row.FundingInterval == nil || *row.FundingInterval <= 0 || *row.FundingInterval%60 != 0 {
		return model.Instrument{}, fmt.Errorf("Bybit %s fundingInterval must be a positive whole number of hours in minutes", row.Symbol)
	}
	if row.PriceFilter == nil || row.LotSizeFilter == nil {
		return model.Instrument{}, fmt.Errorf("Bybit %s priceFilter and lotSizeFilter are required", row.Symbol)
	}
	tick, err := model.ParsePositiveDecimal(row.PriceFilter.TickSize, "tickSize")
	if err != nil {
		return model.Instrument{}, fmt.Errorf("Bybit %s: %w", row.Symbol, err)
	}
	step, err := model.ParsePositiveDecimal(row.LotSizeFilter.QtyStep, "qtyStep")
	if err != nil {
		return model.Instrument{}, fmt.Errorf("Bybit %s: %w", row.Symbol, err)
	}
	settle := row.SettleCoin
	instrument := model.Instrument{
		Exchange:             "Bybit",
		MarketType:           model.MarketPerpetual,
		ExchangeSymbol:       row.Symbol,
		VenueContractVersion: fmt.Sprintf("%d:%d", *row.SymbolID, launchTime),
		BaseAsset:            row.BaseCoin,
		QuoteAsset:           row.QuoteCoin,
		SettleAsset:          &settle,
		ContractMultiplier:   decimal.NewFromInt(1),
		PriceTickSize:        tick,
		QuantityStepSize:     step,
	}
	if err = instrument.ValidateDefinition(); err != nil {
		return model.Instrument{}, fmt.Errorf("Bybit %s: %w", row.Symbol, err)
	}
	return instrument, nil
}

func (c *Client) Instruments(ctx context.Context, marketType model.MarketType) ([]model.Instrument, error) {
	if marketType != model.MarketPerpetual {
		return nil, fmt.Errorf("unsupported Bybit market type %q", marketType)
	}
	var out []model.Instrument
	seenSymbols := make(map[string]struct{})
	seenCursors := make(map[string]struct{})
	cursor := ""
	for {
		values := url.Values{"category": []string{"linear"}, "status": []string{"Trading"}, "limit": []string{"1000"}}
		if cursor != "" {
			values.Set("cursor", cursor)
		}
		payload, err := c.metadataGet(ctx, values)
		if err != nil {
			return nil, err
		}
		page, next, err := ParseInstrumentsPage(payload)
		if err != nil {
			return nil, err
		}
		for _, instrument := range page {
			if _, exists := seenSymbols[instrument.ExchangeSymbol]; exists {
				return nil, fmt.Errorf("Bybit instruments-info duplicate paginated symbol %q", instrument.ExchangeSymbol)
			}
			seenSymbols[instrument.ExchangeSymbol] = struct{}{}
			out = append(out, instrument)
		}
		if next == "" {
			break
		}
		decoded, err := url.QueryUnescape(next)
		if err != nil {
			return nil, fmt.Errorf("Bybit instruments-info cursor: %w", err)
		}
		if decoded == "" {
			return nil, fmt.Errorf("Bybit instruments-info cursor decoded to empty")
		}
		if _, exists := seenCursors[decoded]; exists {
			return nil, fmt.Errorf("Bybit instruments-info repeated cursor %q", decoded)
		}
		seenCursors[decoded] = struct{}{}
		cursor = decoded
	}
	return out, nil
}
