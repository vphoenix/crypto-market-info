package clickhouse

import (
	"context"
	"fmt"

	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/options"
)

func (c *Client) LoadDerivativeSpecs(ctx context.Context, ids []uint32) ([]options.ContractSpec, error) {
	instruments, err := c.Instruments(ctx)
	if err != nil {
		return nil, err
	}
	byID := map[uint32]model.Instrument{}
	for _, i := range instruments {
		byID[i.ID] = i
	}
	rows, err := c.conn.Query(ctx, `SELECT instrument_id,native_instrument_id,creation_time,definition_hash,normalization_version,source_instrument_type,native_amount_kind,native_amount_currency,source_contract_size,index_id,settlement_semantics_id,option_type,strike,strike_currency,leg_instrument_ids,leg_signed_ratios,leg_amount_per_combo,definition_evidence_hash FROM `+c.table("derivative_contract_spec")+` FINAL WHERE instrument_id IN (?) ORDER BY instrument_id`, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []options.ContractSpec
	for rows.Next() {
		var s options.ContractSpec
		var id uint32
		var hash, normalization string
		var optionType, strikeCurrency *string
		var strike *decimal.Decimal
		var legs []uint32
		var ratios []int32
		var amounts []decimal.Decimal
		if err = rows.Scan(&id, &s.NativeID, &s.CreatedAt, &hash, &normalization, &s.SourceInstrumentType, &s.NativeAmountKind, &s.NativeAmountCurrency, &s.ContractSize, &s.IndexID, &s.SettlementSemanticsID, &optionType, &strike, &strikeCurrency, &legs, &ratios, &amounts, &s.EvidenceHash); err != nil {
			return nil, err
		}
		s.Instrument = byID[id]
		s.CreatedAt = s.CreatedAt.UTC()
		if optionType != nil {
			s.OptionType = *optionType
		}
		if strike != nil {
			s.Strike = *strike
		}
		if strikeCurrency != nil {
			s.StrikeCurrency = *strikeCurrency
		}
		if len(legs) != len(ratios) || len(legs) != len(amounts) {
			return nil, fmt.Errorf("malformed combo legs")
		}
		for n, id := range legs {
			s.Legs = append(s.Legs, options.ComboLeg{InstrumentID: id, SignedRatio: ratios[n], AmountPerCombo: amounts[n]})
		}
		if err = s.Validate(); err != nil {
			return nil, err
		}
		if s.DefinitionHash() != hash || normalization != options.NormalizationVersion {
			return nil, fmt.Errorf("invalid stored spec digest")
		}
		result = append(result, s)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	if len(result) != len(ids) {
		return nil, fmt.Errorf("missing derivative spec")
	}
	return result, nil
}
func (c *Client) validateOptionsRunSpecs(ctx context.Context, r options.LiveRun) error {
	ids := make([]uint32, len(r.Members))
	for n, m := range r.Members {
		ids[n] = m.InstrumentID
	}
	specs, err := c.LoadDerivativeSpecs(ctx, ids)
	if err != nil {
		return err
	}
	if err = options.ValidateSelection(specs); err != nil {
		return err
	}
	for n, s := range specs {
		m := r.Members[n]
		if s.Instrument.ID != m.InstrumentID || s.Instrument.ExchangeSymbol != m.Symbol || s.DefinitionHash() != m.DefinitionHash || s.IndexID != m.IndexID {
			return fmt.Errorf("live run member/spec mismatch")
		}
	}
	return nil
}
