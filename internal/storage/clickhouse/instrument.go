package clickhouse

import (
	"context"
	"fmt"

	"github.com/vphoenix/crypto-market-info/internal/model"
)

func (c *Client) Instruments(ctx context.Context) ([]model.Instrument, error) {
	rows, err := c.conn.Query(ctx, `SELECT instrument_id, exchange, market_type, exchange_symbol, base_asset, quote_asset, settle_asset,
contract_multiplier, price_tick_size, quantity_step_size, expiry_time FROM `+c.table("instrument")+` FINAL ORDER BY instrument_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.Instrument
	for rows.Next() {
		var item model.Instrument
		// clickhouse-go cannot scan String directly into a named string type.
		var marketType string
		if err = rows.Scan(&item.ID, &item.Exchange, &marketType, &item.ExchangeSymbol, &item.BaseAsset, &item.QuoteAsset, &item.SettleAsset,
			&item.ContractMultiplier, &item.PriceTickSize, &item.QuantityStepSize, &item.ExpiryTime); err != nil {
			return nil, err
		}
		item.MarketType = model.MarketType(marketType)
		if err = item.Validate(); err != nil {
			return nil, fmt.Errorf("invalid stored instrument %d: %w", item.ID, err)
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (c *Client) RegisterInstruments(ctx context.Context, definitions []model.Instrument) ([]model.Instrument, error) {
	existing, err := c.Instruments(ctx)
	if err != nil {
		return nil, err
	}
	maxID := uint32(0)
	for _, item := range existing {
		if item.ID > maxID {
			maxID = item.ID
		}
	}
	result := make([]model.Instrument, 0, len(definitions))
	for _, definition := range definitions {
		definition.ID = 0
		if err = definition.ValidateDefinition(); err != nil {
			return nil, err
		}
		found := false
		for _, stored := range existing {
			if definition.SameDefinition(stored) {
				result = append(result, stored)
				found = true
				break
			}
		}
		if found {
			continue
		}
		if maxID == ^uint32(0) {
			return nil, fmt.Errorf("instrument_id space exhausted")
		}
		maxID++
		definition.ID = maxID
		if err = definition.Validate(); err != nil {
			return nil, err
		}
		if err = c.insertInstrument(ctx, definition); err != nil {
			return nil, err
		}
		existing = append(existing, definition)
		result = append(result, definition)
	}
	return result, nil
}

func (c *Client) insertInstrument(ctx context.Context, item model.Instrument) error {
	query := `INSERT INTO ` + c.table("instrument") + ` (instrument_id, exchange, market_type, exchange_symbol, base_asset, quote_asset, settle_asset,
contract_multiplier, price_tick_size, quantity_step_size, expiry_time) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	return c.retryWrite(ctx, func(writeCtx context.Context) error {
		return c.conn.Exec(writeCtx, query, item.ID, item.Exchange, string(item.MarketType), item.ExchangeSymbol,
			item.BaseAsset, item.QuoteAsset, item.SettleAsset, item.ContractMultiplier, item.PriceTickSize, item.QuantityStepSize, item.ExpiryTime)
	})
}
