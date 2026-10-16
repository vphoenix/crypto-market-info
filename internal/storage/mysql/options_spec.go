package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"slices"
	"time"

	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/options"
	"github.com/vphoenix/crypto-market-info/internal/storage/codec"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
)

const specColumns = "instrument_id,native_instrument_id,creation_time,definition_hash,normalization_version,source_instrument_type,native_amount_kind,native_amount_currency,source_contract_size,index_id,settlement_semantics_id,option_type,strike,strike_currency,leg_instrument_ids,leg_signed_ratios,leg_amount_per_combo,definition_evidence_hash"
const ruleColumns = "trading_rule_id,instrument_id,source_tick_size,min_trade_amount,amount_step,band_above_prices,band_tick_sizes,observed_at,known_from,effective_from,effective_time_basis,source_url,payload_hash"

func (c *Client) RegisterDerivativeSpecs(ctx context.Context, specs []options.ContractSpec) ([]options.ContractSpec, error) {
	defs := make([]model.Instrument, len(specs))
	for i, s := range specs {
		if e := s.Validate(); e != nil {
			return nil, e
		}
		defs[i] = s.Instrument
	}
	registered, e := c.RegisterInstruments(ctx, defs)
	if e != nil {
		return nil, e
	}
	out := append([]options.ContractSpec(nil), specs...)
	for i := range out {
		out[i].Instrument = registered[i]
	}
	e = c.transaction(ctx, func(tx *sql.Tx) error {
		t, e := c.mapping("derivative_contract_spec")
		if e != nil {
			return e
		}
		existing := map[uint32]record{}
		for start := 0; start < len(out); start += 448 {
			part := out[start:min(start+448, len(out))]
			args := make([]any, len(part))
			for n, s := range part {
				args[n] = s.Instrument.ID
			}
			rows, e := readRecords(ctx, tx, c.table(t.Name), t, "WHERE instrument_id IN ("+placeholders(len(args))+") ORDER BY instrument_id FOR UPDATE", args...)
			if e != nil {
				return e
			}
			for _, r := range rows {
				id := r["instrument_id"].(uint32)
				existing[id] = r
			}
		}
		for i := range out {
			s := &out[i]
			if old, ok := existing[s.Instrument.ID]; ok {
				if old["definition_hash"].(string) != s.DefinitionHash() {
					return contracts.ErrConflict
				}
				s.EvidenceHash = old["definition_evidence_hash"].(string)
				continue
			}
			for _, leg := range s.Legs {
				lt, e := c.mapping("instrument")
				if e != nil {
					return e
				}
				ir, e := readRecords(ctx, tx, c.table(lt.Name), lt, "WHERE instrument_id=?", leg.InstrumentID)
				if e != nil {
					return e
				}
				if len(ir) != 1 {
					return fmt.Errorf("combo leg instrument missing")
				}
				instrument, e := instrumentModel(ir[0])
				if e != nil {
					return e
				}
				if instrument.MarketType != model.MarketOption || instrument.BaseAsset != s.Instrument.BaseAsset || instrument.QuoteAsset != s.Instrument.QuoteAsset || instrument.SettleAsset == nil || *instrument.SettleAsset != *s.Instrument.SettleAsset {
					return fmt.Errorf("combo leg currency or type mismatch")
				}
				var count uint64
				if e = tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+c.table(t.Name)+" WHERE instrument_id=?", leg.InstrumentID).Scan(&count); e != nil {
					return e
				}
				if count != 1 {
					return fmt.Errorf("combo leg spec missing")
				}
			}
			var optionType, strikeCurrency *string
			var strike *decimal.Decimal
			if s.Instrument.MarketType == model.MarketOption {
				optionType = &s.OptionType
				strikeCurrency = &s.StrikeCurrency
				strike = &s.Strike
			}
			ids := []uint32{}
			ratios := []int32{}
			amounts := []decimal.Decimal{}
			for _, l := range s.Legs {
				ids = append(ids, l.InstrumentID)
				ratios = append(ratios, l.SignedRatio)
				amounts = append(amounts, l.AmountPerCombo)
			}
			if e = c.putValues(ctx, tx, t.Name, specColumns, []any{s.Instrument.ID, s.NativeID, s.CreatedAt, s.DefinitionHash(), options.NormalizationVersion, s.SourceInstrumentType, s.NativeAmountKind, s.NativeAmountCurrency, s.ContractSize, s.IndexID, s.SettlementSemanticsID, optionType, strike, strikeCurrency, ids, ratios, amounts, s.EvidenceHash}); e != nil {
				return e
			}
			existing[s.Instrument.ID] = record{"definition_hash": s.DefinitionHash(), "definition_evidence_hash": s.EvidenceHash}
		}
		return nil
	})
	return out, e
}
func (c *Client) LoadDerivativeSpecs(ctx context.Context, ids []uint32) ([]options.ContractSpec, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	if len(ids) > 448 {
		return nil, fmt.Errorf("spec read quota exceeded")
	}
	ordered := slices.Clone(ids)
	slices.Sort(ordered)
	missing := []uint32{}
	found := map[uint32]options.ContractSpec{}
	for n, id := range ordered {
		if n > 0 && id == ordered[n-1] {
			return nil, fmt.Errorf("missing derivative spec")
		}
		if s, ok := cachedValue[options.ContractSpec](c, fmt.Sprint("spec/", id)); ok {
			found[id] = s
		} else {
			missing = append(missing, id)
		}
	}
	if len(missing) > 0 {
		loaded, err := c.loadDerivativeSpecs(ctx, missing)
		if err != nil {
			return nil, err
		}
		for _, s := range loaded {
			found[s.Instrument.ID] = s
			rememberValue(c, fmt.Sprint("spec/", s.Instrument.ID), s)
		}
	}
	out := make([]options.ContractSpec, len(ordered))
	for n, id := range ordered {
		out[n] = found[id]
	}
	return out, nil
}
func (c *Client) loadDerivativeSpecs(ctx context.Context, ids []uint32) ([]options.ContractSpec, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	if len(ids) > 448 {
		return nil, fmt.Errorf("spec read quota exceeded")
	}
	t, e := c.mapping("derivative_contract_spec")
	if e != nil {
		return nil, e
	}
	it, e := c.mapping("instrument")
	if e != nil {
		return nil, e
	}
	args := make([]any, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	where := "WHERE instrument_id IN (" + placeholders(len(args)) + ") ORDER BY instrument_id"
	instruments, e := readRecords(ctx, c.db, c.table(it.Name), it, where, args...)
	if e != nil {
		return nil, e
	}
	byID := map[uint32]model.Instrument{}
	for _, r := range instruments {
		i, e := instrumentModel(r)
		if e != nil {
			return nil, e
		}
		byID[i.ID] = i
	}
	rows, e := readRecords(ctx, c.db, c.table(t.Name), t, where, args...)
	if e != nil {
		return nil, e
	}
	out := []options.ContractSpec{}
	for _, r := range rows {
		s := options.ContractSpec{Instrument: byID[r["instrument_id"].(uint32)], NativeID: r["native_instrument_id"].(uint64), CreatedAt: r["creation_time"].(time.Time), SourceInstrumentType: r["source_instrument_type"].(string), NativeAmountKind: r["native_amount_kind"].(string), NativeAmountCurrency: r["native_amount_currency"].(string), ContractSize: r["source_contract_size"].(decimal.Decimal), IndexID: r["index_id"].(string), SettlementSemanticsID: r["settlement_semantics_id"].(string), EvidenceHash: r["definition_evidence_hash"].(string)}
		if v := r["option_type"]; v != nil {
			s.OptionType = v.(string)
		}
		if v := r["strike"]; v != nil {
			s.Strike = v.(decimal.Decimal)
		}
		if v := r["strike_currency"]; v != nil {
			s.StrikeCurrency = v.(string)
		}
		legs, ratios, amounts := r["leg_instrument_ids"].([]any), r["leg_signed_ratios"].([]any), r["leg_amount_per_combo"].([]any)
		if len(legs) != len(ratios) || len(legs) != len(amounts) {
			return nil, fmt.Errorf("combo leg arrays mismatch")
		}
		for n, id := range legs {
			s.Legs = append(s.Legs, options.ComboLeg{InstrumentID: id.(uint32), SignedRatio: ratios[n].(int32), AmountPerCombo: amounts[n].(decimal.Decimal)})
		}
		if e = s.Validate(); e != nil {
			return nil, e
		}
		if s.DefinitionHash() != r["definition_hash"].(string) || r["normalization_version"].(string) != options.NormalizationVersion {
			return nil, fmt.Errorf("stored spec hash or normalization mismatch")
		}
		out = append(out, s)
	}
	if len(out) != len(ids) {
		return nil, fmt.Errorf("missing derivative spec")
	}
	return out, nil
}
func (c *Client) WriteDerivativeTradingRule(ctx context.Context, r options.TradingRule) error {
	return c.WriteDerivativeTradingRules(ctx, []options.TradingRule{r})
}
func (c *Client) WriteDerivativeTradingRules(ctx context.Context, rules []options.TradingRule) error {
	return c.writePreparedTradingRules(ctx, rules)
}

func (c *Client) LoadDerivativeTradingRule(ctx context.Context, id string) (options.TradingRule, error) {
	rows, e := c.LoadDerivativeTradingRules(ctx, []string{id})
	if e != nil {
		return options.TradingRule{}, e
	}
	return rows[id], nil
}
func (c *Client) LoadDerivativeTradingRules(ctx context.Context, ids []string) (map[string]options.TradingRule, error) {
	if len(ids) == 0 {
		return map[string]options.TradingRule{}, nil
	}
	if len(ids) > 2048 {
		return nil, fmt.Errorf("rule read quota exceeded")
	}
	out := map[string]options.TradingRule{}
	missing := []string{}
	seen := map[string]bool{}
	for _, id := range ids {
		if !model.ValidDigest(id) {
			return nil, fmt.Errorf("invalid trading rule ID")
		}
		if seen[id] {
			return nil, contracts.ErrNotFound
		}
		seen[id] = true
		if r, ok := cachedValue[options.TradingRule](c, "rule/"+id); ok {
			if r.ID() != id {
				return nil, contracts.ErrConflict
			}
			out[id] = r
		} else {
			missing = append(missing, id)
		}
	}
	if len(missing) > 0 {
		loaded, err := c.loadDerivativeTradingRules(ctx, missing)
		if err != nil {
			return nil, err
		}
		for id, r := range loaded {
			out[id] = r
			rememberValue(c, "rule/"+id, r)
		}
	}
	return out, nil
}
func (c *Client) loadDerivativeTradingRules(ctx context.Context, ids []string) (map[string]options.TradingRule, error) {
	t, e := c.mapping("derivative_trading_rule")
	if e != nil {
		return nil, e
	}
	args := make([]any, len(ids))
	for n, id := range ids {
		args[n] = []byte(id)
	}
	rows, e := readRecords(ctx, c.db, c.table(t.Name), t, "WHERE trading_rule_id IN ("+placeholders(len(ids))+")", args...)
	if e != nil {
		return nil, e
	}
	out := map[string]options.TradingRule{}
	for _, v := range rows {
		id := v["trading_rule_id"].(string)
		r, e := decodeTradingRule(v, id)
		if e != nil {
			return nil, e
		}
		if _, ok := out[id]; ok {
			return nil, contracts.ErrConflict
		}
		out[id] = r
	}
	if len(out) != len(ids) {
		return nil, contracts.ErrNotFound
	}
	return out, nil
}
func decodeTradingRule(v record, id string) (options.TradingRule, error) {
	var e error
	r := options.TradingRule{InstrumentID: v["instrument_id"].(uint32), SourceTick: v["source_tick_size"].(decimal.Decimal), MinAmount: v["min_trade_amount"].(decimal.Decimal), AmountStep: v["amount_step"].(decimal.Decimal), ObservedAt: v["observed_at"].(time.Time), KnownFrom: v["known_from"].(time.Time), EffectiveFrom: v["effective_from"].(time.Time), EffectiveTimeBasis: v["effective_time_basis"].(string), SourceURL: v["source_url"].(string), PayloadHash: v["payload_hash"].(string)}
	var above, ticks []decimal.Decimal
	if e = codec.Assign(reflect.ValueOf(&above).Elem(), v["band_above_prices"]); e != nil {
		return r, e
	}
	if e = codec.Assign(reflect.ValueOf(&ticks).Elem(), v["band_tick_sizes"]); e != nil {
		return r, e
	}
	if len(above) != len(ticks) {
		return r, fmt.Errorf("trading rule band mismatch")
	}
	for i := range above {
		r.Bands = append(r.Bands, options.TickBand{AbovePrice: above[i], TickSize: ticks[i]})
	}
	if e = r.Validate(); e != nil {
		return r, e
	}
	if r.ID() != id {
		return r, contracts.ErrConflict
	}
	return r, nil
}
