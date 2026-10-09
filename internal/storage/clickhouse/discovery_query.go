package clickhouse

import (
	"context"
	"fmt"
	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"time"
)

type DiscoveryProof struct {
	Kind, ID, Hash, Codec                                   string
	SourceMS, ObservedMS, KnownMS, PublishedMS, EffectiveMS int64
	InstrumentID                                            uint32
	SourceCount                                             uint16
	ProvenanceHash                                          string
}

func (c *Client) latestDiscoveryProof(ctx context.Context, kind string, instrument uint32, asOf time.Time) (DiscoveryProof, error) {
	var p DiscoveryProof
	query := "SELECT source_kind,source_id,content_hash,codec,source_ms,observed_ms,known_from_ms,published_ms,effective_ms,instrument_id,source_count,provenance_hash FROM " + c.table("source_batch_status") + " WHERE source_kind=? AND instrument_id=? AND complete=1 AND source_ms<=observed_ms AND observed_ms<=known_from_ms AND known_from_ms<=published_ms AND published_ms<=? ORDER BY observed_ms DESC,source_id DESC LIMIT 1"
	err := c.conn.QueryRow(ctx, query, kind, instrument, asOf.UnixMilli()).Scan(&p.Kind, &p.ID, &p.Hash, &p.Codec, &p.SourceMS, &p.ObservedMS, &p.KnownMS, &p.PublishedMS, &p.EffectiveMS, &p.InstrumentID, &p.SourceCount, &p.ProvenanceHash)
	if err != nil {
		return p, err
	}
	var count uint64
	if err = c.conn.QueryRow(ctx, "SELECT count() FROM "+c.table("source_batch_status")+" WHERE source_kind=? AND source_id=?", p.Kind, p.ID).Scan(&count); err != nil {
		return p, err
	}
	if count != 1 || p.Codec != model.DiscoveryCodec || !model.ValidDigest(p.Hash) {
		return p, fmt.Errorf("ambiguous or unsupported publication")
	}
	return p, nil
}
func (c *Client) discoverySources(ctx context.Context, p DiscoveryProof) ([]model.PublicSource, error) {
	rows, err := c.conn.Query(ctx, "SELECT source_order,source_url,payload_hash,time_basis,source_ms,observed_ms FROM "+c.table("public_observation_provenance")+" FINAL WHERE source_kind=? AND source_id=? ORDER BY source_order", p.Kind, p.ID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []model.PublicSource{}
	for rows.Next() {
		var s model.PublicSource
		var order uint16
		if err = rows.Scan(&order, &s.URL, &s.PayloadHash, &s.TimeBasis, &s.SourceMS, &s.ObservedMS); err != nil {
			return nil, err
		}
		if int(order) != len(result) {
			return nil, fmt.Errorf("source member order changed")
		}
		result = append(result, s)
	}
	if rows.Err() != nil {
		return nil, rows.Err()
	}
	if len(result) != int(p.SourceCount) || model.DiscoverySourcesHash(result) != p.ProvenanceHash {
		return nil, fmt.Errorf("published provenance missing or changed")
	}
	if err := model.ValidateDiscoverySources(p.Kind, result); err != nil {
		return nil, err
	}
	return result, nil
}
func (c *Client) checkDiscoveryFact(ctx context.Context, f discoveryFact, p DiscoveryProof, hash string) error {
	var count uint64
	if err := c.conn.QueryRow(ctx, "SELECT count() FROM "+c.table(f.Kind())+" WHERE observation_id=?", p.ID).Scan(&count); err != nil {
		return err
	}
	b := f.Body()
	if count != 1 || hash != p.Hash || model.DiscoveryHash(b) != hash || b["observed_ms"] != p.ObservedMS || b["source_ms"] != p.SourceMS || b["instrument_id"] != p.InstrumentID {
		return fmt.Errorf("published fact missing or changed")
	}
	if err := f.Validate(); err != nil {
		return err
	}
	if err := c.validateDiscoveryScope(ctx, f); err != nil {
		return err
	}
	// Re-read the whole immutable marker after all members, including provenance.
	var final DiscoveryProof
	err := c.conn.QueryRow(ctx, "SELECT source_kind,source_id,content_hash,codec,source_ms,observed_ms,known_from_ms,published_ms,effective_ms,instrument_id,source_count,provenance_hash FROM "+c.table("source_batch_status")+" WHERE source_kind=? AND source_id=? LIMIT 1", p.Kind, p.ID).Scan(&final.Kind, &final.ID, &final.Hash, &final.Codec, &final.SourceMS, &final.ObservedMS, &final.KnownMS, &final.PublishedMS, &final.EffectiveMS, &final.InstrumentID, &final.SourceCount, &final.ProvenanceHash)
	if err != nil {
		return err
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
	err = c.conn.QueryRow(ctx, "SELECT observation_id,instrument_id,observed_ms,source_ms,funding_ms,rate,mark,mark_ms,base_index,base_index_ms,base_usd_index,quote_usd_index,quote_index_ms,row_hash FROM "+c.table(p.Kind)+" WHERE observation_id=? LIMIT 1", p.ID).Scan(&f.ID, &f.InstrumentID, &f.ObservedMS, &f.SourceMS, &f.FundingMS, &f.Rate, &f.Mark, &f.MarkMS, &f.BaseIndex, &f.BaseIndexMS, &f.BaseUSDIndex, &f.QuoteUSDIndex, &f.QuoteIndexMS, &hash)
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
	err = c.conn.QueryRow(ctx, "SELECT observation_id,instrument_id,observed_ms,source_ms,minute_ms,base_volume,is_final,conversion_hash,row_hash FROM "+c.table(p.Kind)+" WHERE observation_id=? LIMIT 1", p.ID).Scan(&f.ID, &f.InstrumentID, &f.ObservedMS, &f.SourceMS, &f.MinuteMS, &f.BaseVolume, &final, &f.ConversionHash, &hash)
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
	err = c.conn.QueryRow(ctx, "SELECT observation_id,instrument_id,observed_ms,source_ms,effective_ms,valid_until_ms,perp_maker,perp_taker,spot_taker,buy_fee_currency,sell_fee_currency,spot_maximum,perp_maximum,position_limit,reference_profile,interest_rule_version,row_hash FROM "+c.table(p.Kind)+" WHERE observation_id=? LIMIT 1", p.ID).Scan(&f.ID, &f.InstrumentID, &f.ObservedMS, &f.SourceMS, &f.EffectiveMS, &f.ValidUntilMS, &f.PerpMaker, &f.PerpTaker, &f.SpotTaker, &f.BuyFeeCurrency, &f.SellFeeCurrency, &f.SpotMaximum, &f.PerpMaximum, &f.PositionLimit, &f.Profile, &f.InterestVersion, &hash)
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
	var hash string
	arrays := make([][]decimal.Decimal, 18)
	dest := []any{&f.ID, &f.InstrumentID, &f.ObservedMS, &f.SourceMS, &f.EffectiveMS, &f.ValidUntilMS, &f.Base, &f.BorrowLeverage, &f.ModelHash}
	for n := range arrays {
		dest = append(dest, &arrays[n])
	}
	dest = append(dest, &hash)
	columns := "observation_id,instrument_id,observed_ms,source_ms,effective_ms,valid_until_ms,base,borrow_leverage,model_hash,quote_discount_minimum,quote_discount_maximum,quote_discount_rate,quote_discount_liquidation_penalty,base_discount_minimum,base_discount_maximum,base_discount_rate,base_discount_liquidation_penalty,perp_minimum,perp_maximum,perp_imr,perp_mmr,perp_maximum_leverage,borrow_minimum,borrow_maximum,borrow_imr,borrow_mmr,borrow_maximum_leverage,row_hash"
	if err = c.conn.QueryRow(ctx, "SELECT "+columns+" FROM "+c.table(p.Kind)+" WHERE observation_id=? LIMIT 1", p.ID).Scan(dest...); err != nil {
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
