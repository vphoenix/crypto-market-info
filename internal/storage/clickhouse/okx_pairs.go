package clickhouse

import (
	"context"
	"fmt"
	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"time"
)

const loanColumns = "observation_id,requested_at,observed_at,source_url,payload_hash,currencies,daily_rates,public_quotas,override_currencies,override_daily_rates,level_families,levels,level_currencies,strategy_types,quotas,interest_discounts,quota_coefficients,row_hash"
const pairColumns = "observation_id,observed_at,effective_minute,loan_policy_id,source_urls,source_hashes,requested_at,received_at,raw_counts,bases,spot_ids,perpetual_ids,spot_min_sizes,perpetual_min_sizes,row_hash"

func (c *Client) InitOKXPairSchema(ctx context.Context) error {
	statements := []string{
		`CREATE TABLE IF NOT EXISTS ` + c.table("okx_public_loan_policy") + ` (
 observation_id UUID,requested_at DateTime64(6,'UTC'),observed_at DateTime64(6,'UTC'),source_url String,payload_hash FixedString(64),
 currencies Array(String),daily_rates Array(Decimal(38,18)),public_quotas Array(Decimal(38,18)),
 override_currencies Array(String),override_daily_rates Array(Decimal(38,18)),
 level_families Array(String),levels Array(String),level_currencies Array(String),strategy_types Array(String),
 quotas Array(Nullable(Decimal(38,18))),interest_discounts Array(Nullable(Decimal(38,18))),quota_coefficients Array(Nullable(Decimal(38,18))),row_hash FixedString(64),
 CONSTRAINT basic_lengths CHECK length(currencies)>0 AND length(currencies)=length(daily_rates) AND length(currencies)=length(public_quotas),
 CONSTRAINT override_lengths CHECK length(override_currencies)=length(override_daily_rates),
 CONSTRAINT level_lengths CHECK length(level_families)=length(levels) AND length(levels)=length(level_currencies) AND length(levels)=length(strategy_types) AND length(levels)=length(quotas) AND length(levels)=length(interest_discounts) AND length(levels)=length(quota_coefficients)
 ) ENGINE=ReplacingMergeTree PARTITION BY toYYYYMM(observed_at) ORDER BY observation_id SETTINGS fsync_after_insert=1,fsync_part_directory=1,min_rows_to_fsync_after_merge=1`,
		`CREATE TABLE IF NOT EXISTS ` + c.table("okx_paired_catalog") + ` (
 observation_id UUID,observed_at DateTime64(6,'UTC'),effective_minute DateTime('UTC'),loan_policy_id UUID,source_urls Array(String),source_hashes Array(FixedString(64)),
 requested_at Array(DateTime64(6,'UTC')),received_at Array(DateTime64(6,'UTC')),raw_counts Array(UInt32),
 bases Array(String),spot_ids Array(UInt32),perpetual_ids Array(UInt32),spot_min_sizes Array(Decimal(38,18)),perpetual_min_sizes Array(Decimal(38,18)),row_hash FixedString(64),
 CONSTRAINT source_lengths CHECK length(source_urls)=4 AND length(source_hashes)=4 AND length(requested_at)=4 AND length(received_at)=4 AND length(raw_counts)=4,
 CONSTRAINT pair_lengths CHECK length(bases)>0 AND length(bases)=length(spot_ids) AND length(bases)=length(perpetual_ids) AND length(bases)=length(spot_min_sizes) AND length(bases)=length(perpetual_min_sizes)
 ) ENGINE=ReplacingMergeTree PARTITION BY toYYYYMM(observed_at) ORDER BY observation_id SETTINGS fsync_after_insert=1,fsync_part_directory=1,min_rows_to_fsync_after_merge=1`,
	}
	for _, sql := range statements {
		if err := c.conn.Exec(ctx, sql); err != nil {
			return err
		}
	}
	return nil
}
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
	values := []any{p.ID, p.RequestedAt, p.ObservedAt, p.SourceURL, p.PayloadHash, ccys, rates, limits, override, orates, lf, ll, lc, st, quotas, discounts, coeffs, hash}
	return c.retryWrite(ctx, func(ctx context.Context) error {
		return c.insertDerivativeRows(ctx, "okx_public_loan_policy", loanColumns, [][]any{values})
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
	values := []any{o.ID, o.ObservedAt, o.EffectiveMinute, o.LoanPolicyID, o.SourceURLs, o.SourceHashes, o.RequestedAt, o.ReceivedAt, o.RawCounts, bases, spots, perps, sm, pm, hash}
	return c.retryWrite(ctx, func(ctx context.Context) error {
		return c.insertDerivativeRows(ctx, "okx_paired_catalog", pairColumns, [][]any{values})
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
	err := c.conn.QueryRow(ctx, "SELECT "+loanColumns+" FROM "+c.table("okx_public_loan_policy")+" FINAL WHERE "+predicate, argument).Scan(&p.ID, &p.RequestedAt, &p.ObservedAt, &p.SourceURL, &p.PayloadHash, &ccys, &rates, &limits, &override, &orates, &lf, &ll, &lc, &st, &quotas, &discounts, &coeffs, &hash)
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
	err := c.conn.QueryRow(ctx, "SELECT "+pairColumns+" FROM "+c.table("okx_paired_catalog")+" FINAL WHERE observed_at<=? AND effective_minute<=? ORDER BY effective_minute DESC,observed_at DESC,observation_id DESC LIMIT 1", at.UTC(), at.UTC()).Scan(&o.ID, &o.ObservedAt, &o.EffectiveMinute, &o.LoanPolicyID, &o.SourceURLs, &o.SourceHashes, &o.RequestedAt, &o.ReceivedAt, &o.RawCounts, &bases, &spots, &perps, &sm, &pm, &hash)
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
	p, err := c.readOKXLoanPolicy(ctx, "observation_id=? LIMIT 1", o.LoanPolicyID)
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
