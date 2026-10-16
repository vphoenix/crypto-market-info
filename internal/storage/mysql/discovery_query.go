package mysql

import (
	"context"
	"fmt"
	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
	"time"
)

const proofColumns = "source_kind,source_id,content_hash,codec,source_ms,observed_ms,known_from_ms,published_ms,effective_ms,instrument_id,source_count,provenance_hash"

func scanProof(v record) (DiscoveryProof, error) {
	var p DiscoveryProof
	e := scanRecord(v, proofColumns, &p.Kind, &p.ID, &p.Hash, &p.Codec, &p.SourceMS, &p.ObservedMS, &p.KnownMS, &p.PublishedMS, &p.EffectiveMS, &p.InstrumentID, &p.SourceCount, &p.ProvenanceHash)
	return p, e
}
func (c *Client) latestDiscoveryProof(ctx context.Context, kind string, instrument uint32, asOf time.Time) (DiscoveryProof, error) {
	var p DiscoveryProof
	rows, e := c.records(ctx, c.db, "source_batch_status", "WHERE source_kind=? AND instrument_id=? AND complete=1 AND source_ms<=observed_ms AND observed_ms<=known_from_ms AND known_from_ms<=published_ms AND published_ms<=? ORDER BY observed_ms DESC,source_id DESC LIMIT 1", []byte(kind), instrument, asOf.UnixMilli())
	if e != nil {
		return p, e
	}
	if len(rows) == 0 {
		return p, contracts.ErrNotFound
	}
	p, e = scanProof(rows[0])
	if e != nil {
		return p, e
	}

	same, e := c.records(ctx, c.db, "source_batch_status", "WHERE source_kind=? AND source_id=?", []byte(p.Kind), []byte(p.ID))
	if e != nil {
		return p, e
	}
	if len(same) != 1 || !supportedDiscoveryCodec(p.Kind, p.Codec) || !model.ValidDigest(p.Hash) {
		return p, fmt.Errorf("ambiguous publication")
	}
	return p, nil
}
func (c *Client) discoverySources(ctx context.Context, p DiscoveryProof) ([]model.PublicSource, error) {
	rows, e := c.records(ctx, c.db, "public_observation_provenance", "WHERE source_kind=? AND source_id=? ORDER BY source_order", []byte(p.Kind), []byte(p.ID))
	if e != nil {
		return nil, e
	}
	out := []model.PublicSource{}
	for _, v := range rows {
		var s model.PublicSource
		var order uint16
		if e = scanRecord(v, "source_order,source_url,payload_hash,time_basis,source_ms,observed_ms", &order, &s.URL, &s.PayloadHash, &s.TimeBasis, &s.SourceMS, &s.ObservedMS); e != nil {
			return nil, e
		}
		if int(order) != len(out) {
			return nil, fmt.Errorf("provenance order mismatch")
		}
		out = append(out, s)
	}
	if len(out) != int(p.SourceCount) || model.DiscoverySourcesHash(out) != p.ProvenanceHash {
		return nil, fmt.Errorf("provenance incomplete")
	}
	return out, model.ValidateDiscoverySources(p.Kind, out)
}
func (c *Client) scanDiscovery(ctx context.Context, p DiscoveryProof, cols string, dest ...any) error {
	rows, e := c.records(ctx, c.db, p.Kind, "WHERE observation_id=?", []byte(p.ID))
	if e != nil {
		return e
	}
	if len(rows) != 1 {
		return fmt.Errorf("published fact missing/ambiguous")
	}
	return scanRecord(rows[0], cols, dest...)
}
func (c *Client) checkDiscoveryFact(ctx context.Context, f discoveryFact, p DiscoveryProof, hash string) error {
	b := f.Body()
	if hash != p.Hash || model.DiscoveryHash(b) != hash || b["observed_ms"] != p.ObservedMS || b["source_ms"] != p.SourceMS || b["instrument_id"] != p.InstrumentID {
		return fmt.Errorf("published fact content mismatch")
	}
	if e := f.Validate(); e != nil {
		return e
	}
	if e := c.validateDiscoveryScope(ctx, f); e != nil {
		return e
	}
	rows, e := c.records(ctx, c.db, "source_batch_status", "WHERE source_kind=? AND source_id=?", []byte(p.Kind), []byte(p.ID))
	if e != nil {
		return e
	}
	if len(rows) != 1 {
		return fmt.Errorf("publication changed")
	}
	final, e := scanProof(rows[0])
	if e != nil {
		return e
	}
	if final != p {
		return fmt.Errorf("publication changed after members")
	}
	return nil
}

func (c *Client) LoadDiscoveryForecast(ctx context.Context, instrument uint32, asOf time.Time) (model.FundingPrediction, DiscoveryProof, error) {
	p, err := c.latestDiscoveryProof(ctx, "funding_forecast_observation", instrument, asOf)
	var f model.FundingPrediction
	if err != nil {
		return f, p, err
	}
	var hash string
	err = c.scanDiscovery(ctx, p, "observation_id,instrument_id,observed_ms,source_ms,funding_ms,rate,mark,mark_ms,base_index,base_index_ms,base_usd_index,quote_usd_index,quote_index_ms,row_hash", &f.ID, &f.InstrumentID, &f.ObservedMS, &f.SourceMS, &f.FundingMS, &f.Rate, &f.Mark, &f.MarkMS, &f.BaseIndex, &f.BaseIndexMS, &f.BaseUSDIndex, &f.QuoteUSDIndex, &f.QuoteIndexMS, &hash)
	if err != nil {
		return f, p, err
	}
	f.Sources, err = c.discoverySources(ctx, p)
	if err != nil {
		return f, p, err
	}
	if asOf.UnixMilli()-f.SourceMS > 90000 || asOf.UnixMilli()-f.MarkMS > 90000 || asOf.UnixMilli()-f.BaseIndexMS > 90000 || asOf.UnixMilli()-f.QuoteIndexMS > 90000 || f.FundingMS <= asOf.UnixMilli() {
		return f, p, fmt.Errorf("forecast/mark stale or settlement past")
	}
	return f, p, c.checkDiscoveryFact(ctx, f, p, hash)
}
func (c *Client) LoadDiscoveryBar(ctx context.Context, instrument uint32, asOf time.Time) (model.TradeBar, DiscoveryProof, error) {
	p, err := c.latestDiscoveryProof(ctx, "cex_trade_bar_1m", instrument, asOf)
	var f model.TradeBar
	if err != nil {
		return f, p, err
	}
	var hash string
	var final uint8
	err = c.scanDiscovery(ctx, p, "observation_id,instrument_id,observed_ms,source_ms,minute_ms,base_volume,is_final,conversion_hash,row_hash", &f.ID, &f.InstrumentID, &f.ObservedMS, &f.SourceMS, &f.MinuteMS, &f.BaseVolume, &final, &f.ConversionHash, &hash)
	if err != nil {
		return f, p, err
	}
	f.Final = final == 1
	f.Sources, err = c.discoverySources(ctx, p)
	if err != nil {
		return f, p, err
	}
	if f.MinuteMS != (asOf.UnixMilli()/60000-1)*60000 {
		return f, p, fmt.Errorf("not previous complete minute")
	}
	return f, p, c.checkDiscoveryFact(ctx, f, p, hash)
}
func (c *Client) LoadDiscoveryFees(ctx context.Context, instrument uint32, asOf time.Time) (model.ReferenceFee, DiscoveryProof, error) {
	p, err := c.latestDiscoveryProof(ctx, "cex_fee_schedule_rule", instrument, asOf)
	var f model.ReferenceFee
	if err != nil {
		return f, p, err
	}
	var hash string
	err = c.scanDiscovery(ctx, p, "observation_id,instrument_id,observed_ms,source_ms,effective_ms,valid_until_ms,perp_maker,perp_taker,spot_taker,buy_fee_currency,sell_fee_currency,spot_maximum,perp_maximum,position_limit,reference_profile,interest_rule_version,row_hash", &f.ID, &f.InstrumentID, &f.ObservedMS, &f.SourceMS, &f.EffectiveMS, &f.ValidUntilMS, &f.PerpMaker, &f.PerpTaker, &f.SpotTaker, &f.BuyFeeCurrency, &f.SellFeeCurrency, &f.SpotMaximum, &f.PerpMaximum, &f.PositionLimit, &f.Profile, &f.InterestVersion, &hash)
	if err != nil {
		return f, p, err
	}
	f.Sources, err = c.discoverySources(ctx, p)
	if err != nil {
		return f, p, err
	}
	if asOf.UnixMilli() < f.EffectiveMS || asOf.UnixMilli() >= f.ValidUntilMS {
		return f, p, fmt.Errorf("fee rule expired or not yet effective")
	}
	return f, p, c.checkDiscoveryFact(ctx, f, p, hash)
}
func (c *Client) LoadDiscoveryCapital(ctx context.Context, instrument uint32, asOf time.Time) (model.CapitalParameters, DiscoveryProof, error) {
	p, err := c.latestDiscoveryProof(ctx, "okx_public_capital_parameters", instrument, asOf)
	var f model.CapitalParameters
	if err != nil {
		return f, p, err
	}
	if p.Codec == model.CapitalVersionsCodec {
		f, hash, err := c.loadCapitalVersions(ctx, p)
		if err != nil {
			return f, p, err
		}
		if asOf.UnixMilli() < f.EffectiveMS || asOf.UnixMilli() >= f.ValidUntilMS {
			return f, p, fmt.Errorf("capital rule expired or not yet effective")
		}
		return f, p, c.checkDiscoveryFact(ctx, f, p, hash)
	}
	var hash string
	arrays := make([][]decimal.Decimal, 18)
	dest := []any{&f.ID, &f.InstrumentID, &f.ObservedMS, &f.SourceMS, &f.EffectiveMS, &f.ValidUntilMS, &f.Base, &f.BorrowLeverage, &f.ModelHash}
	for n := range arrays {
		dest = append(dest, &arrays[n])
	}
	dest = append(dest, &hash)
	columns := "observation_id,instrument_id,observed_ms,source_ms,effective_ms,valid_until_ms,base,borrow_leverage,model_hash,quote_discount_minimum,quote_discount_maximum,quote_discount_rate,quote_discount_liquidation_penalty,base_discount_minimum,base_discount_maximum,base_discount_rate,base_discount_liquidation_penalty,perp_minimum,perp_maximum,perp_imr,perp_mmr,perp_maximum_leverage,borrow_minimum,borrow_maximum,borrow_imr,borrow_mmr,borrow_maximum_leverage,row_hash"
	if err = c.scanDiscovery(ctx, p, columns, dest...); err != nil {
		return f, p, err
	}
	for _, group := range []struct{ offset, width int }{{0, 4}, {4, 4}, {8, 5}, {13, 5}} {
		for n := group.offset; n < group.offset+group.width; n++ {
			if len(arrays[n]) != len(arrays[group.offset]) {
				return f, p, fmt.Errorf("capital tier array mismatch")
			}
		}
	}
	for n := range arrays[0] {
		f.QuoteDiscount = append(f.QuoteDiscount, model.DiscountTier{Minimum: arrays[0][n], Maximum: arrays[1][n], Rate: arrays[2][n], Penalty: arrays[3][n]})
	}
	for n := range arrays[4] {
		f.BaseDiscount = append(f.BaseDiscount, model.DiscountTier{Minimum: arrays[4][n], Maximum: arrays[5][n], Rate: arrays[6][n], Penalty: arrays[7][n]})
	}
	for n := range arrays[8] {
		f.PerpTiers = append(f.PerpTiers, model.RiskTier{Minimum: arrays[8][n], Maximum: arrays[9][n], Initial: arrays[10][n], Maintenance: arrays[11][n], MaximumLeverage: arrays[12][n]})
	}
	for n := range arrays[13] {
		f.BorrowTiers = append(f.BorrowTiers, model.RiskTier{Minimum: arrays[13][n], Maximum: arrays[14][n], Initial: arrays[15][n], Maintenance: arrays[16][n], MaximumLeverage: arrays[17][n]})
	}
	f.Sources, err = c.discoverySources(ctx, p)
	if err != nil {
		return f, p, err
	}
	if asOf.UnixMilli() < f.EffectiveMS || asOf.UnixMilli() >= f.ValidUntilMS {
		return f, p, fmt.Errorf("capital rule expired or not yet effective")
	}
	return f, p, c.checkDiscoveryFact(ctx, f, p, hash)
}

func supportedDiscoveryCodec(kind, codec string) bool {
	return codec == model.DiscoveryCodec || kind == "okx_public_capital_parameters" && codec == model.CapitalVersionsCodec
}
