package clickhouse

import (
	"context"
	"fmt"
	"sync"

	"github.com/vphoenix/crypto-market-info/internal/model"
)

func (c *Client) Instruments(ctx context.Context) ([]model.Instrument, error) {
	return c.instrumentsWhere(ctx, "", nil)
}
func (c *Client) instrumentsWhere(ctx context.Context, where string, args []any) ([]model.Instrument, error) {
	rows, err := c.conn.Query(ctx, `SELECT instrument_id, exchange, market_type, exchange_symbol, venue_contract_version, base_asset, quote_asset, settle_asset,
contract_multiplier, price_tick_size, quantity_step_size, expiry_time FROM `+c.table("instrument")+` FINAL `+where+` ORDER BY instrument_id`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.Instrument
	for rows.Next() {
		var item model.Instrument
		// clickhouse-go cannot scan String directly into a named string type.
		var marketType string
		if err = rows.Scan(&item.ID, &item.Exchange, &marketType, &item.ExchangeSymbol, &item.VenueContractVersion, &item.BaseAsset, &item.QuoteAsset, &item.SettleAsset,
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
	c.instrumentOnce.Do(func() {
		if c.instrumentMu == nil {
			c.instrumentMu = &sync.Mutex{}
		}
	})
	c.instrumentMu.Lock()
	defer c.instrumentMu.Unlock()
	// Validate everything before even preparing an insert. This also ensures a
	// later malformed definition cannot partially register a startup universe.
	for index, definition := range definitions {
		if err := definition.ValidateDefinition(); err != nil {
			return nil, err
		}
		if definition.MarketType != model.MarketSpot && definition.VenueContractVersion == "" {
			return nil, fmt.Errorf("new derivative instrument %s %s requires venue_contract_version", definition.Exchange, definition.ExchangeSymbol)
		}
		for _, previous := range definitions[:index] {
			if sameInstrumentIdentity(previous, definition) && !previous.SameDefinition(definition) {
				return nil, fmt.Errorf("conflicting definitions in registration batch for %s %s version %q", definition.Exchange, definition.ExchangeSymbol, definition.VenueContractVersion)
			}
		}
	}
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
	var additions []model.Instrument
	for _, definition := range definitions {
		definition.ID = 0
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
		additions = append(additions, definition)
		existing = append(existing, definition)
		result = append(result, definition)
	}
	if len(additions) > 0 {
		if err = c.retryWrite(ctx, func(writeCtx context.Context) error { return c.insertInstruments(writeCtx, additions) }); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func sameInstrumentIdentity(a, b model.Instrument) bool {
	return a.Exchange == b.Exchange && a.MarketType == b.MarketType && a.ExchangeSymbol == b.ExchangeSymbol && a.VenueContractVersion == b.VenueContractVersion
}

func (c *Client) insertInstruments(ctx context.Context, items []model.Instrument) error {
	query := `INSERT INTO ` + c.table("instrument") + ` (instrument_id, exchange, market_type, exchange_symbol, venue_contract_version, base_asset, quote_asset, settle_asset,
contract_multiplier, price_tick_size, quantity_step_size, expiry_time)`
	batch, err := c.conn.PrepareBatch(ctx, query)
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, item := range items {
		if err = batch.Append(item.ID, item.Exchange, string(item.MarketType), item.ExchangeSymbol,
			item.VenueContractVersion, item.BaseAsset, item.QuoteAsset, item.SettleAsset, item.ContractMultiplier, item.PriceTickSize, item.QuantityStepSize, item.ExpiryTime); err != nil {
			return err
		}
	}
	return batch.Send()
}
