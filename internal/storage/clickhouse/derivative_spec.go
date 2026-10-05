package clickhouse

import (
	"context"
	"fmt"

	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/options"
)

// RegisterDerivativeSpecs validates all definitions first and keeps the FIRST
// definition evidence on subsequent identical catalog observations. This API
// assumes one writer process, like the existing instrument registry.
func (c *Client) RegisterDerivativeSpecs(ctx context.Context, specs []options.ContractSpec) ([]options.ContractSpec, error) {
	if c.readOnly {
		return nil, fmt.Errorf("read only")
	}
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
	ids := make([]uint32, 0, len(out))
	legIDs := []uint32{}
	for i := range out {
		ids = append(ids, registered[i].ID)
		for _, leg := range out[i].Legs {
			legIDs = append(legIDs, leg.InstrumentID)
		}
	}
	refs, err := c.derivativeSpecReferences(ctx, append(ids, legIDs...))
	if err != nil {
		return nil, err
	}
	legInstruments := map[uint32]model.Instrument{}
	if len(legIDs) > 0 {
		legs, err := c.instrumentsWhere(ctx, "WHERE instrument_id IN (?)", []any{legIDs})
		if err != nil {
			return nil, err
		}
		for _, leg := range legs {
			legInstruments[leg.ID] = leg
		}
	}
	var additions [][]any
	for i := range out {
		s := &out[i]
		s.Instrument = registered[i]
		s.Legs = append([]options.ComboLeg(nil), s.Legs...)
		if err := s.Validate(); err != nil {
			return nil, err
		}
		for _, leg := range s.Legs {
			i, ok := legInstruments[leg.InstrumentID]
			if !ok || i.MarketType != model.MarketOption || i.BaseAsset != s.Instrument.BaseAsset || i.QuoteAsset != s.Instrument.QuoteAsset || i.SettleAsset == nil || *i.SettleAsset != *s.Instrument.SettleAsset {
				return nil, fmt.Errorf("unsupported combo leg currency/type")
			}
			if _, ok := refs[leg.InstrumentID]; !ok {
				return nil, fmt.Errorf("combo leg spec missing")
			}
		}
		if old, ok := refs[s.Instrument.ID]; ok {
			if old.hash != s.DefinitionHash() {
				return nil, fmt.Errorf("immutable derivative spec conflict")
			}
			s.EvidenceHash = old.evidence
			continue
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
		additions = append(additions, []any{s.Instrument.ID, s.NativeID, s.CreatedAt.UTC(), s.DefinitionHash(), options.NormalizationVersion, s.SourceInstrumentType, s.NativeAmountKind, s.NativeAmountCurrency, s.ContractSize, s.IndexID, s.SettlementSemanticsID, optionType, strike, strikeCurrency, ids, ratios, amounts, s.EvidenceHash})
		// This also preserves FIRST evidence for duplicates in this batch.
		refs[s.Instrument.ID] = derivativeSpecReference{s.DefinitionHash(), s.EvidenceHash}
	}
	if len(additions) > 0 {
		err = c.retryWrite(ctx, func(writeCtx context.Context) error {
			return c.insertDerivativeRows(writeCtx, "derivative_contract_spec", `instrument_id,native_instrument_id,creation_time,definition_hash,normalization_version,source_instrument_type,native_amount_kind,native_amount_currency,source_contract_size,index_id,settlement_semantics_id,option_type,strike,strike_currency,leg_instrument_ids,leg_signed_ratios,leg_amount_per_combo,definition_evidence_hash`, additions)
		})
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

type derivativeSpecReference struct{ hash, evidence string }

func (c *Client) derivativeSpecReferences(ctx context.Context, ids []uint32) (map[uint32]derivativeSpecReference, error) {
	out := map[uint32]derivativeSpecReference{}
	if len(ids) == 0 {
		return out, nil
	}
	rows, err := c.conn.Query(ctx, `SELECT instrument_id,definition_hash,definition_evidence_hash FROM `+c.table("derivative_contract_spec")+` FINAL WHERE instrument_id IN (?)`, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id uint32
		var r derivativeSpecReference
		if err := rows.Scan(&id, &r.hash, &r.evidence); err != nil {
			return nil, err
		}
		if _, exists := out[id]; exists {
			return nil, fmt.Errorf("duplicate derivative spec reference")
		}
		out[id] = r
	}
	return out, rows.Err()
}

func (c *Client) WriteDerivativeTradingRule(ctx context.Context, r options.TradingRule) error {
	return c.WriteDerivativeTradingRules(ctx, []options.TradingRule{r})
}

// A catalog observation carries hundreds of rules. Validate the entire batch
// and its identities before one insert; retries reuse the captured rule IDs.
func (c *Client) WriteDerivativeTradingRules(ctx context.Context, rules []options.TradingRule) error {
	if c.readOnly {
		return fmt.Errorf("read only")
	}
	for _, r := range rules {
		if err := r.Validate(); err != nil {
			return err
		}
	}
	if len(rules) == 0 {
		return nil
	}
	c.derivativeMu.Lock()
	defer c.derivativeMu.Unlock()
	ids := make([]uint32, len(rules))
	for n, r := range rules {
		ids[n] = r.InstrumentID
	}
	refs, err := c.derivativeSpecReferences(ctx, ids)
	if err != nil {
		return err
	}
	seen := map[string]bool{}
	var values [][]any
	for _, r := range rules {
		if _, ok := refs[r.InstrumentID]; !ok {
			return fmt.Errorf("rule spec missing")
		}
		if seen[r.ID()] {
			continue
		}
		seen[r.ID()] = true
		above, ticks := make([]decimal.Decimal, 0, len(r.Bands)), make([]decimal.Decimal, 0, len(r.Bands))
		for _, b := range r.Bands {
			above = append(above, b.AbovePrice)
			ticks = append(ticks, b.TickSize)
		}
		values = append(values, []any{r.ID(), r.InstrumentID, r.SourceTick, r.MinAmount, r.AmountStep, above, ticks, r.ObservedAt.UTC(), r.KnownFrom.UTC(), r.EffectiveFrom.UTC(), r.EffectiveTimeBasis, r.SourceURL, r.PayloadHash})
	}
	return c.retryWrite(ctx, func(writeCtx context.Context) error {
		return c.insertDerivativeRows(writeCtx, "derivative_trading_rule", `trading_rule_id,instrument_id,source_tick_size,min_trade_amount,amount_step,band_above_prices,band_tick_sizes,observed_at,known_from,effective_from,effective_time_basis,source_url,payload_hash`, values)
	})
}

func (c *Client) insertDerivativeRow(ctx context.Context, table, columns string, values ...any) error {
	return c.insertDerivativeRows(ctx, table, columns, [][]any{values})
}
func (c *Client) insertDerivativeRows(ctx context.Context, table, columns string, values [][]any) error {
	b, err := c.conn.PrepareBatch(ctx, `INSERT INTO `+c.table(table)+` (`+columns+`)`)
	if err != nil {
		return err
	}
	defer b.Abort()
	for _, row := range values {
		if err := b.Append(row...); err != nil {
			return err
		}
	}
	return b.Send()
}
