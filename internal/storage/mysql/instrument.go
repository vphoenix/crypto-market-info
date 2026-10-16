package mysql

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"github.com/shopspring/decimal"
	"math"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/storage/codec"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
)

func instrumentRecord(i model.Instrument) record {
	return record{"instrument_id": i.ID, "exchange": i.Exchange, "market_type": string(i.MarketType), "exchange_symbol": i.ExchangeSymbol, "venue_contract_version": i.VenueContractVersion, "base_asset": i.BaseAsset, "quote_asset": i.QuoteAsset, "settle_asset": i.SettleAsset, "contract_multiplier": i.ContractMultiplier, "price_tick_size": i.PriceTickSize, "quantity_step_size": i.QuantityStepSize, "expiry_time": i.ExpiryTime}
}
func instrumentIdentityHash(r record) ([32]byte, error) {
	t, e := Mapping("crypto_market_info", "instrument")
	if e != nil {
		return [32]byte{}, e
	}
	h := sha256.New()
	for _, col := range t.Columns {
		if col.Name == "instrument_id" {
			continue
		}
		a, e := codec.Encode(col.Type, r[col.Name])
		if e != nil {
			return [32]byte{}, e
		}
		writeFramed(h, a)
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out, nil
}
func instrumentModel(r record) (model.Instrument, error) {
	i := model.Instrument{ID: r["instrument_id"].(uint32), Exchange: r["exchange"].(string), MarketType: model.MarketType(r["market_type"].(string)), ExchangeSymbol: r["exchange_symbol"].(string), VenueContractVersion: r["venue_contract_version"].(string), BaseAsset: r["base_asset"].(string), QuoteAsset: r["quote_asset"].(string)}
	// Assign uses the exact logical decimal and timestamp types from the schema.
	i.ContractMultiplier = r["contract_multiplier"].(decimal.Decimal)
	i.PriceTickSize = r["price_tick_size"].(decimal.Decimal)
	i.QuantityStepSize = r["quantity_step_size"].(decimal.Decimal)
	if v := r["settle_asset"]; v != nil {
		s := v.(string)
		i.SettleAsset = &s
	}
	if v := r["expiry_time"]; v != nil {
		ts := v.(time.Time)
		i.ExpiryTime = &ts
	}
	return i, i.Validate()
}
func (c *Client) Instruments(ctx context.Context) ([]model.Instrument, error) {
	t, e := c.mapping("instrument")
	if e != nil {
		return nil, e
	}
	rows, e := readRecords(ctx, c.db, c.table("instrument"), t, "ORDER BY instrument_id")
	if e != nil {
		return nil, e
	}
	out := make([]model.Instrument, len(rows))
	for n, r := range rows {
		out[n], e = instrumentModel(r)
		if e != nil {
			return nil, e
		}
	}
	return out, nil
}
func (c *Client) RegisterInstruments(ctx context.Context, defs []model.Instrument) (registered []model.Instrument, err error) {
	defer func() { err = storageError(err) }()
	ctx, cancel := context.WithTimeout(ctx, 40*time.Second)
	defer cancel()
	if c.readOnly {
		return nil, fmt.Errorf("read-only MySQL client")
	}
	for n, d := range defs {
		if e := d.ValidateDefinition(); e != nil {
			return nil, e
		}
		if d.MarketType != model.MarketSpot && d.VenueContractVersion == "" {
			return nil, fmt.Errorf("derivative registration requires contract version")
		}
		for _, p := range defs[:n] {
			if p.Exchange == d.Exchange && p.MarketType == d.MarketType && p.ExchangeSymbol == d.ExchangeSymbol && p.VenueContractVersion == d.VenueContractVersion && !p.SameDefinition(d) {
				return nil, contracts.ErrConflict
			}
		}
	}
	t, e := c.mapping("instrument")
	if e != nil {
		return nil, e
	}
	tx, e := c.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if e != nil {
		return nil, e
	}
	defer tx.Rollback()
	if e = c.fence(ctx, tx); e != nil {
		return nil, e
	}
	if _, e = tx.ExecContext(ctx, "INSERT IGNORE INTO cmi_storage.identity_allocator(database_name,kind,next_id) VALUES(?,'instrument',1)", c.database); e != nil {
		return nil, e
	}
	var next uint64
	if e = tx.QueryRowContext(ctx, "SELECT next_id FROM cmi_storage.identity_allocator WHERE database_name=? AND kind='instrument' FOR UPDATE", c.database).Scan(&next); e != nil {
		return nil, e
	}
	var maximum uint64
	if e = tx.QueryRowContext(ctx, "SELECT COALESCE(MAX(instrument_id),0) FROM "+c.table("instrument")).Scan(&maximum); e != nil {
		return nil, e
	}
	if next <= maximum {
		next = maximum + 1
	}
	// Fetch previously registered identities in bounded blocks while retaining
	// the same exclusive producer and allocator locks used for ID assignment.
	byIdentity := map[string]model.Instrument{}
	identityKeys := make([][32]byte, len(defs))
	for n, def := range defs {
		h, e := instrumentIdentityHash(instrumentRecord(def))
		if e != nil {
			return nil, e
		}
		identityKeys[n] = h
	}
	for start := 0; start < len(identityKeys); start += 448 {
		part := identityKeys[start:min(start+448, len(identityKeys))]
		args := make([]any, len(part))
		for n, h := range part {
			args[n] = h[:]
		}
		rows, e := readRecords(ctx, tx, c.table("instrument"), t, "WHERE _identity_hash IN ("+placeholders(len(args))+") ORDER BY instrument_id FOR UPDATE", args...)
		if e != nil {
			return nil, e
		}
		for _, r := range rows {
			old, e := instrumentModel(r)
			if e != nil {
				return nil, e
			}
			h, e := instrumentIdentityHash(r)
			if e != nil {
				return nil, e
			}
			if prior, ok := byIdentity[string(h[:])]; ok && prior.ID != old.ID {
				return nil, contracts.ErrConflict
			}
			byIdentity[string(h[:])] = old
		}
	}
	out := make([]model.Instrument, 0, len(defs))
	fresh := []record{}
	for n, def := range defs {
		h := identityKeys[n]
		if old, ok := byIdentity[string(h[:])]; ok {
			if !old.SameDefinition(def) {
				return nil, contracts.ErrConflict
			}
			out = append(out, old)
			continue
		}
		if next > math.MaxUint32 {
			return nil, fmt.Errorf("instrument ID exhausted")
		}
		def.ID = uint32(next)
		next++
		if e = def.Validate(); e != nil {
			return nil, e
		}
		fresh = append(fresh, instrumentRecord(def))
		byIdentity[string(h[:])] = def
		out = append(out, def)
	}
	if e = putImmutableRows(ctx, tx, c.table("instrument"), t, fresh); e != nil {
		return nil, e
	}

	if _, e = tx.ExecContext(ctx, "UPDATE cmi_storage.identity_allocator SET next_id=? WHERE database_name=? AND kind='instrument'", next, c.database); e != nil {
		return nil, e
	}
	if e = tx.Commit(); e != nil {
		return nil, e
	}
	return out, nil
}

var _ contracts.InstrumentRegistry = (*Client)(nil)
