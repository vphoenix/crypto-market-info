package mysql

import (
	"context"
	"encoding/hex"
	"fmt"

	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
)

func capitalVersionRecords(p model.CapitalParameters) (map[string][]record, error) {
	v, err := model.SplitCapitalVersions(p)
	if err != nil {
		return nil, err
	}
	if len(p.ID) > 160 || len(p.Base) > 160 {
		return nil, fmt.Errorf("capital version identifier exceeds storage bound")
	}
	groups := map[string][]record{}
	for _, d := range []model.DiscountRuleVersion{v.Quote, v.Base} {
		b := d.Body()
		id, _ := compactBinaryDigest(d.ID)
		groups["okx_discount_rule_version"] = append(groups["okx_discount_rule_version"], record{"version_hash": id, "exchange": b["exchange"], "semantics": b["semantics"], "currency": d.Currency, "amount_unit": d.Currency, "tiers": d.Tiers})
	}
	for _, r := range []model.RiskRuleVersion{v.Perp, v.Borrow} {
		b := r.Body()
		id, _ := compactBinaryDigest(r.ID)
		groups["okx_risk_rule_version"] = append(groups["okx_risk_rule_version"], record{"version_hash": id, "exchange": b["exchange"], "semantics": b["semantics"], "instrument_id": r.InstrumentID, "base": r.Base, "amount_unit": r.Base, "role": r.Role, "mode": "cross", "tiers": r.Tiers})
	}
	row := record{"observation_id": p.ID, "instrument_id": p.InstrumentID, "observed_ms": p.ObservedMS, "source_ms": p.SourceMS, "effective_ms": p.EffectiveMS, "valid_until_ms": p.ValidUntilMS, "base": p.Base, "borrow_leverage": p.BorrowLeverage}
	for k, h := range map[string]string{"model_hash": p.ModelHash, "quote_version": v.Quote.ID, "base_version": v.Base.ID, "perp_version": v.Perp.ID, "borrow_version": v.Borrow.ID, "row_hash": v.Observation.Hash} {
		row[k], err = compactBinaryDigest(h)
		if err != nil {
			return nil, err
		}
	}
	groups["okx_capital_observation_v2"] = []record{row}
	return groups, nil
}

// A previously durable intent pins the representation across configuration
// changes. The existing begin-intent transaction still validates every field;
// this read is only preparation and never a publication/absence proof.
func (c *Client) capitalPublicationCodec(ctx context.Context, p publicationRecord) (string, error) {
	rows, err := c.records(ctx, c.db, "source_publication_intents", "WHERE source_kind=? AND source_id=?", []byte(p.Kind), []byte(p.ID))
	if err != nil {
		return "", err
	}
	if len(rows) > 1 {
		return "", contracts.ErrConflict
	}
	if len(rows) == 1 {
		old, err := scanPublication(rows[0])
		if err != nil {
			return "", err
		}
		if old.Codec != model.DiscoveryCodec && old.Codec != model.CapitalVersionsCodec {
			return "", contracts.ErrConflict
		}
		if old.Hash != p.Hash {
			return "", contracts.ErrConflict
		}
		return old.Codec, nil
	}
	if c.capitalRuleVersions {
		return model.CapitalVersionsCodec, nil
	}
	return model.DiscoveryCodec, nil
}

func decodeCapitalObservation(r record) (model.CapitalVersionObservation, error) {
	var o model.CapitalVersionObservation
	p := &o.Parameters
	var hashes [6]string
	err := scanRecord(r, "observation_id,instrument_id,observed_ms,source_ms,effective_ms,valid_until_ms,base,borrow_leverage,model_hash,quote_version,base_version,perp_version,borrow_version,row_hash", &p.ID, &p.InstrumentID, &p.ObservedMS, &p.SourceMS, &p.EffectiveMS, &p.ValidUntilMS, &p.Base, &p.BorrowLeverage, &hashes[0], &hashes[1], &hashes[2], &hashes[3], &hashes[4], &hashes[5])
	if err != nil {
		return o, err
	}
	for i := range hashes {
		if len(hashes[i]) != 32 {
			return o, fmt.Errorf("invalid capital digest length")
		}
		hashes[i] = hex.EncodeToString([]byte(hashes[i]))
	}
	p.ModelHash = hashes[0]
	o.QuoteVersion = hashes[1]
	o.BaseVersion = hashes[2]
	o.PerpVersion = hashes[3]
	o.BorrowVersion = hashes[4]
	o.Hash = hashes[5]
	return o, nil
}
func decodeDiscountVersion(r record) (model.DiscountRuleVersion, error) {
	var v model.DiscountRuleVersion
	var raw, exchange, semantics, unit string
	err := scanRecord(r, "version_hash,exchange,semantics,currency,amount_unit,tiers", &raw, &exchange, &semantics, &v.Currency, &unit, &v.Tiers)
	if err != nil {
		return v, err
	}
	v.ID = hex.EncodeToString([]byte(raw))
	body := v.Body()
	if exchange != body["exchange"] || semantics != body["semantics"] || unit != v.Currency {
		return v, fmt.Errorf("discount interpretation mismatch")
	}
	return v, v.Validate()
}
func decodeRiskVersion(r record) (model.RiskRuleVersion, error) {
	var v model.RiskRuleVersion
	var raw, exchange, semantics, unit, mode string
	err := scanRecord(r, "version_hash,exchange,semantics,instrument_id,base,amount_unit,role,mode,tiers", &raw, &exchange, &semantics, &v.InstrumentID, &v.Base, &unit, &v.Role, &mode, &v.Tiers)
	if err != nil {
		return v, err
	}
	v.ID = hex.EncodeToString([]byte(raw))
	body := v.Body()
	if exchange != body["exchange"] || semantics != body["semantics"] || unit != v.Base || mode != "cross" {
		return v, fmt.Errorf("risk interpretation mismatch")
	}
	return v, v.Validate()
}

func (c *Client) loadCapitalVersions(ctx context.Context, p DiscoveryProof) (model.CapitalParameters, string, error) {
	fail := func(err error) (model.CapitalParameters, string, error) { return model.CapitalParameters{}, "", err }
	if err := c.checkSchema(ctx, CapitalVersionTableNames()); err != nil {
		return fail(err)
	}
	rows, err := c.records(ctx, c.db, "okx_capital_observation_v2", "WHERE observation_id=?", []byte(p.ID))
	if err != nil {
		return fail(err)
	}
	if len(rows) != 1 {
		return fail(fmt.Errorf("published capital observation missing/ambiguous"))
	}
	var v model.CapitalVersions
	v.Observation, err = decodeCapitalObservation(rows[0])
	if err != nil {
		return fail(err)
	}
	o := v.Observation
	// Shared curves can be the same row (e.g. USDT as base); require each
	// requested reference to resolve exactly once, rather than counting rows.
	for _, lookup := range []struct {
		name, id string
		discount *model.DiscountRuleVersion
		risk     *model.RiskRuleVersion
	}{
		{"okx_discount_rule_version", o.QuoteVersion, &v.Quote, nil},
		{"okx_discount_rule_version", o.BaseVersion, &v.Base, nil},
		{"okx_risk_rule_version", o.PerpVersion, nil, &v.Perp},
		{"okx_risk_rule_version", o.BorrowVersion, nil, &v.Borrow},
	} {
		key, e := compactBinaryDigest(lookup.id)
		if e != nil {
			return fail(e)
		}
		rows, e := c.records(ctx, c.db, lookup.name, "WHERE version_hash=?", []byte(key))
		if e != nil {
			return fail(e)
		}
		if len(rows) != 1 {
			return fail(fmt.Errorf("capital content version missing/ambiguous"))
		}
		if lookup.discount != nil {
			*lookup.discount, e = decodeDiscountVersion(rows[0])
		} else {
			*lookup.risk, e = decodeRiskVersion(rows[0])
		}
		if e != nil {
			return fail(e)
		}
	}
	sources, err := c.discoverySources(ctx, p)
	if err != nil {
		return fail(err)
	}
	f, err := v.Rebuild(sources)
	if err != nil {
		return fail(err)
	}
	return f, o.Hash, nil
}
