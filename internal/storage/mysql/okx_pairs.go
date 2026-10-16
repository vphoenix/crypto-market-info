package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
	"time"
)

func (c *Client) scanOKX(ctx context.Context, name, cols, where string, args []any, dest ...any) error {
	rows, e := c.records(ctx, c.db, name, where, args...)
	if e != nil {
		return e
	}
	if len(rows) == 0 {
		return contracts.ErrNotFound
	}
	if len(rows) != 1 {
		return contracts.ErrConflict
	}
	return scanRecord(rows[0], cols, dest...)
}

const loanColumns = "observation_id,requested_at,observed_at,source_url,payload_hash,currencies,daily_rates,public_quotas,override_currencies,override_daily_rates,level_families,levels,level_currencies,strategy_types,quotas,interest_discounts,quota_coefficients,row_hash"
const pairColumns = "observation_id,observed_at,effective_minute,loan_policy_id,source_urls,source_hashes,requested_at,received_at,raw_counts,bases,spot_ids,perpetual_ids,spot_min_sizes,perpetual_min_sizes,row_hash"

func (c *Client) WriteOKXLoanPolicy(ctx context.Context, p model.OKXLoanPolicy) error {
	if err := p.Validate(); err != nil {
		return err
	}
	ccys, override, lf, ll, lc, st := []string{}, []string{}, []string{}, []string{}, []string{}, []string{}
	rates, limits, orates := []decimal.Decimal{}, []decimal.Decimal{}, []decimal.Decimal{}
	quotas, discounts, coeffs := []*decimal.Decimal{}, []*decimal.Decimal{}, []*decimal.Decimal{}
	for _, x := range p.Basic {
		ccys = append(ccys, x.Currency)
		rates = append(rates, x.DailyRate)
		limits = append(limits, x.PublicQuota)
	}
	for _, x := range p.Overrides {
		override = append(override, x.Currency)
		orates = append(orates, x.DailyRate)
	}
	for _, x := range p.Levels {
		lf = append(lf, x.Family)
		ll = append(ll, x.Level)
		lc = append(lc, x.Currency)
		st = append(st, x.StrategyType)
		quotas = append(quotas, x.Quota)
		discounts = append(discounts, x.InterestDiscount)
		coeffs = append(coeffs, x.QuotaCoefficient)
	}
	hash := model.ReferenceHash(p)
	if !model.ValidDigest(hash) {
		return fmt.Errorf("cannot hash loan observation")
	}
	publication := publicationRecord{Kind: "okx_public_loan_policy", ID: p.ID.String(), Hash: hash, Codec: "go-reference-json-v1", SourceMS: p.RequestedAt.UnixMilli(), ObservedMS: p.ObservedAt.UnixMilli(), KnownMS: model.CeilMilliseconds(time.Now().UTC())}
	known, errKnown := c.beginDiscoveryIntent(ctx, publication)
	if errKnown != nil {
		return errKnown
	}
	publication.KnownMS = known
	values := []any{p.ID, p.RequestedAt, p.ObservedAt, p.SourceURL, p.PayloadHash, ccys, rates, limits, override, orates, lf, ll, lc, st, quotas, discounts, coeffs, hash}

	return c.transaction(ctx, func(tx *sql.Tx) error {
		if err := c.putValues(ctx, tx, "okx_public_loan_policy", loanColumns, values); err != nil {
			return err
		}
		return c.publishDiscovery(ctx, tx, publication)
	})
}
func (c *Client) WriteOKXPairCatalog(ctx context.Context, o model.OKXPairObservation) error {
	if err := o.Validate(); err != nil {
		return err
	}
	if err := c.validatePairPolicy(ctx, o); err != nil {
		return err
	}
	registered, err := c.Instruments(ctx)
	if err != nil {
		return err
	}
	byID := map[uint32]model.Instrument{}
	for _, i := range registered {
		byID[i.ID] = i
	}
	bases := []string{}
	spots, perps := []uint32{}, []uint32{}
	sm, pm := []decimal.Decimal{}, []decimal.Decimal{}
	for _, p := range o.Pairs {
		if !p.Spot.SameDefinition(byID[p.Spot.ID]) || !p.Perpetual.SameDefinition(byID[p.Perpetual.ID]) {
			return fmt.Errorf("paired catalog has unregistered definitions")
		}
		bases = append(bases, p.Base)
		spots = append(spots, p.Spot.ID)
		perps = append(perps, p.Perpetual.ID)
		sm = append(sm, p.SpotMinSize)
		pm = append(pm, p.PerpetualMinSize)
	}
	hash := model.ReferenceHash(o)
	if !model.ValidDigest(hash) {
		return fmt.Errorf("cannot hash pair observation")
	}
	publication := publicationRecord{Kind: "okx_paired_catalog", ID: o.ID.String(), Hash: hash, Codec: "go-reference-json-v1", SourceMS: o.RequestedAt[0].UnixMilli(), ObservedMS: o.ObservedAt.UnixMilli(), KnownMS: model.CeilMilliseconds(time.Now().UTC()), EffectiveMS: o.EffectiveMinute.UnixMilli()}
	known, errKnown := c.beginDiscoveryIntent(ctx, publication)
	if errKnown != nil {
		return errKnown
	}
	publication.KnownMS = known
	values := []any{o.ID, o.ObservedAt, o.EffectiveMinute, o.LoanPolicyID, o.SourceURLs, o.SourceHashes, o.RequestedAt, o.ReceivedAt, o.RawCounts, bases, spots, perps, sm, pm, hash}

	return c.transaction(ctx, func(tx *sql.Tx) error {
		if err := c.putValues(ctx, tx, "okx_paired_catalog", pairColumns, values); err != nil {
			return err
		}
		return c.publishDiscovery(ctx, tx, publication)
	})
}
func (c *Client) LatestOKXLoanPolicy(ctx context.Context, at time.Time) (model.OKXLoanPolicy, error) {
	return c.readOKXLoanPolicy(ctx, "observed_at<=? ORDER BY observed_at DESC,observation_id DESC LIMIT 1", at.UTC())
}
func (c *Client) readOKXLoanPolicy(ctx context.Context, predicate string, argument any) (model.OKXLoanPolicy, error) {
	var p model.OKXLoanPolicy
	var hash string
	ccys, override, lf, ll, lc, st := []string{}, []string{}, []string{}, []string{}, []string{}, []string{}
	rates, limits, orates := []decimal.Decimal{}, []decimal.Decimal{}, []decimal.Decimal{}
	quotas, discounts, coeffs := []*decimal.Decimal{}, []*decimal.Decimal{}, []*decimal.Decimal{}
	err := c.scanOKX(ctx, "okx_public_loan_policy", loanColumns, "WHERE "+predicate, []any{argument}, &p.ID, &p.RequestedAt, &p.ObservedAt, &p.SourceURL, &p.PayloadHash, &ccys, &rates, &limits, &override, &orates, &lf, &ll, &lc, &st, &quotas, &discounts, &coeffs, &hash)
	if err != nil {
		return p, err
	}
	if len(ccys) != len(rates) || len(ccys) != len(limits) || len(override) != len(orates) || len(lf) != len(ll) || len(lf) != len(lc) || len(lf) != len(st) || len(lf) != len(quotas) || len(lf) != len(discounts) || len(lf) != len(coeffs) {
		return p, fmt.Errorf("incomplete loan observation")
	}
	for n, ccy := range ccys {
		p.Basic = append(p.Basic, model.OKXBasicLoan{Currency: ccy, DailyRate: rates[n], PublicQuota: limits[n]})
	}
	for n, ccy := range override {
		p.Overrides = append(p.Overrides, model.OKXCurrencyRate{Currency: ccy, DailyRate: orates[n]})
	}
	for n, f := range lf {
		p.Levels = append(p.Levels, model.OKXLoanLevel{Family: f, Level: ll[n], Currency: lc[n], StrategyType: st[n], Quota: quotas[n], InterestDiscount: discounts[n], QuotaCoefficient: coeffs[n]})
	}
	if err = p.Validate(); err != nil {
		return p, err
	}
	if hash != model.ReferenceHash(p) {
		return p, fmt.Errorf("loan observation hash mismatch")
	}
	return p, nil
}

// CheckOKXPairCatalog validates expected identities and the source-bearing
// catalog independently of whether a particular market minute is missing.
func (c *Client) LatestOKXPairCatalog(ctx context.Context, at time.Time) (model.OKXPairObservation, error) {
	var o model.OKXPairObservation
	var hash string
	var bases []string
	var spots, perps []uint32
	var sm, pm []decimal.Decimal
	err := c.scanOKX(ctx, "okx_paired_catalog", pairColumns, "WHERE observed_at<=? AND effective_minute<=? ORDER BY effective_minute DESC,observed_at DESC,observation_id DESC LIMIT 1", []any{at.UTC(), at.UTC()}, &o.ID, &o.ObservedAt, &o.EffectiveMinute, &o.LoanPolicyID, &o.SourceURLs, &o.SourceHashes, &o.RequestedAt, &o.ReceivedAt, &o.RawCounts, &bases, &spots, &perps, &sm, &pm, &hash)
	if err != nil {
		return o, err
	}
	if len(bases) != len(spots) || len(bases) != len(perps) || len(bases) != len(sm) || len(bases) != len(pm) {
		return o, fmt.Errorf("incomplete paired catalog")
	}
	instruments, err := c.Instruments(ctx)
	if err != nil {
		return o, err
	}
	byID := map[uint32]model.Instrument{}
	for _, i := range instruments {
		byID[i.ID] = i
	}
	for n, base := range bases {
		o.Pairs = append(o.Pairs, model.OKXPairMember{Base: base, Spot: byID[spots[n]], Perpetual: byID[perps[n]], SpotMinSize: sm[n], PerpetualMinSize: pm[n]})
	}
	if err = o.Validate(); err != nil {
		return o, err
	}
	if hash != model.ReferenceHash(o) {
		return o, fmt.Errorf("paired catalog hash mismatch")
	}
	if err = c.validatePairPolicy(ctx, o); err != nil {
		return o, err
	}
	return o, nil
}

// A parent UUID alone is insufficient: bind the exact loan response and verify
// that every paired currency was eligible in that complete committed response.
func (c *Client) validatePairPolicy(ctx context.Context, o model.OKXPairObservation) error {
	p, err := c.readOKXLoanPolicy(ctx, "observation_id=? LIMIT 1", o.LoanPolicyID[:])
	if err != nil {
		return fmt.Errorf("paired catalog loan parent: %w", err)
	}
	if o.SourceURLs[3] != p.SourceURL || o.SourceHashes[3] != p.PayloadHash || !o.RequestedAt[3].Equal(p.RequestedAt) || !o.ReceivedAt[3].Equal(p.ObservedAt) || o.RawCounts[3] != 1 {
		return fmt.Errorf("paired catalog loan source differs from committed parent")
	}
	quotas := map[string]decimal.Decimal{}
	for _, b := range p.Basic {
		quotas[b.Currency] = b.PublicQuota
	}
	for _, pair := range o.Pairs {
		if !quotas[pair.Base].IsPositive() {
			return fmt.Errorf("paired currency %s lacks positive public loan quota", pair.Base)
		}
	}
	return nil
}
