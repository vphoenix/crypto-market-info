package clickhouse

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/options"
)

// RegisterDerivativeSpecs validates all definitions first and keeps the FIRST
// definition evidence on subsequent identical catalog observations. This API
// assumes one writer process, like the existing instrument registry.
func (c *Client) RegisterDerivativeSpecs(ctx context.Context, specs []options.ContractSpec) ([]options.ContractSpec, error) {
	c.derivativeMu.Lock()
	defer c.derivativeMu.Unlock()
	defs := make([]model.Instrument, len(specs))
	for i, s := range specs {
		if err := s.Validate(); err != nil {
			return nil, err
		}
		defs[i] = s.Instrument
	}
	registered, err := c.RegisterInstruments(ctx, defs)
	if err != nil {
		return nil, err
	}
	out := append([]options.ContractSpec(nil), specs...)
	for i := range out {
		s := &out[i]
		s.Instrument = registered[i]
		s.Legs = append([]options.ComboLeg(nil), s.Legs...)
		if err := s.Validate(); err != nil {
			return nil, err
		}
		if len(s.Legs) > 0 {
			for _, leg := range s.Legs {
				var kind, base, quote, settle string
				err := c.conn.QueryRow(ctx, `SELECT market_type,base_asset,quote_asset,assumeNotNull(settle_asset) FROM `+c.table("instrument")+` FINAL WHERE instrument_id=?`, leg.InstrumentID).Scan(&kind, &base, &quote, &settle)
				if err != nil {
					return nil, err
				}
				if kind != string(model.MarketOption) || base != s.Instrument.BaseAsset || quote != s.Instrument.QuoteAsset || settle != *s.Instrument.SettleAsset {
					return nil, fmt.Errorf("unsupported combo leg currency/type")
				}
				var count uint64
				if err := c.conn.QueryRow(ctx, `SELECT count() FROM `+c.table("derivative_contract_spec")+` FINAL WHERE instrument_id=?`, leg.InstrumentID).Scan(&count); err != nil {
					return nil, err
				}
				if count != 1 {
					return nil, fmt.Errorf("combo leg spec missing")
				}
			}
		}
		var existingHash, evidence string
		err := c.conn.QueryRow(ctx, `SELECT definition_hash,definition_evidence_hash FROM `+c.table("derivative_contract_spec")+` FINAL WHERE instrument_id=?`, s.Instrument.ID).Scan(&existingHash, &evidence)
		if err == nil {
			if existingHash != s.DefinitionHash() {
				return nil, fmt.Errorf("immutable derivative spec conflict")
			}
			s.EvidenceHash = evidence
			continue
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return nil, err
		}
		var optionType, strikeCurrency *string
		var strike *decimal.Decimal
		if s.Instrument.MarketType == model.MarketOption {
			optionType = &s.OptionType
			strikeCurrency = &s.StrikeCurrency
			strike = &s.Strike
		}
		ids := make([]uint32, 0, len(s.Legs))
		ratios := make([]int32, 0, len(s.Legs))
		amounts := make([]decimal.Decimal, 0, len(s.Legs))
		for _, leg := range s.Legs {
			ids = append(ids, leg.InstrumentID)
			ratios = append(ratios, leg.SignedRatio)
			amounts = append(amounts, leg.AmountPerCombo)
		}
		err = c.retryWrite(ctx, func(writeCtx context.Context) error {
			return c.insertDerivativeRow(writeCtx, "derivative_contract_spec", `instrument_id,native_instrument_id,creation_time,definition_hash,normalization_version,source_instrument_type,native_amount_kind,native_amount_currency,source_contract_size,index_id,settlement_semantics_id,option_type,strike,strike_currency,leg_instrument_ids,leg_signed_ratios,leg_amount_per_combo,definition_evidence_hash`, s.Instrument.ID, s.NativeID, s.CreatedAt.UTC(), s.DefinitionHash(), options.NormalizationVersion, s.SourceInstrumentType, s.NativeAmountKind, s.NativeAmountCurrency, s.ContractSize, s.IndexID, s.SettlementSemanticsID, optionType, strike, strikeCurrency, ids, ratios, amounts, s.EvidenceHash)
		})
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (c *Client) WriteDerivativeTradingRule(ctx context.Context, r options.TradingRule) error {
	if err := r.Validate(); err != nil {
		return err
	}
	c.derivativeMu.Lock()
	defer c.derivativeMu.Unlock()
	var count uint64
	if err := c.conn.QueryRow(ctx, `SELECT count() FROM `+c.table("derivative_contract_spec")+` FINAL WHERE instrument_id=?`, r.InstrumentID).Scan(&count); err != nil {
		return err
	}
	if count != 1 {
		return fmt.Errorf("rule spec missing")
	}
	above, ticks := make([]decimal.Decimal, 0, len(r.Bands)), make([]decimal.Decimal, 0, len(r.Bands))
	for _, b := range r.Bands {
		above = append(above, b.AbovePrice)
		ticks = append(ticks, b.TickSize)
	}
	return c.retryWrite(ctx, func(writeCtx context.Context) error {
		return c.insertDerivativeRow(writeCtx, "derivative_trading_rule", `trading_rule_id,instrument_id,source_tick_size,min_trade_amount,amount_step,band_above_prices,band_tick_sizes,observed_at,known_from,effective_from,effective_time_basis,source_url,payload_hash`, r.ID(), r.InstrumentID, r.SourceTick, r.MinAmount, r.AmountStep, above, ticks, r.ObservedAt.UTC(), r.KnownFrom.UTC(), r.EffectiveFrom.UTC(), r.EffectiveTimeBasis, r.SourceURL, r.PayloadHash)
	})
}

func (c *Client) insertDerivativeRow(ctx context.Context, table, columns string, values ...any) error {
	b, err := c.conn.PrepareBatch(ctx, `INSERT INTO `+c.table(table)+` (`+columns+`)`)
	if err != nil {
		return err
	}
	defer b.Abort()
	if err := b.Append(values...); err != nil {
		return err
	}
	return b.Send()
}
