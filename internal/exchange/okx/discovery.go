package okx

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/exchange"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"html"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// PublicResponse is transient transport data. Only typed facts and provenance
// hashes are written; no raw HTTP response is persisted.
type PublicResponse struct {
	Data   json.RawMessage
	Source model.PublicSource
}

func (c *Client) DiscoveryPublic(ctx context.Context, path string, parameters url.Values) (PublicResponse, error) {
	gate := c.discoveryRequestGate(path)
	endpoint := strings.TrimRight(c.BaseURL, "/") + path
	if len(parameters) > 0 {
		endpoint += "?" + parameters.Encode()
	}
	var requested int64
	retry := c.Retry
	before := retry.BeforeRequest
	retry.BeforeRequest = func(ctx context.Context) error {
		if before != nil {
			if err := before(ctx); err != nil {
				return err
			}
		}
		if err := gate.Wait(ctx); err != nil {
			return err
		}
		// Timestamp the physical attempt that produced the response, after any
		// cooldown and pacing. Retries must acquire the same endpoint budget.
		requested = time.Now().UTC().UnixMilli()
		return nil
	}
	payload, err := exchange.Get(ctx, c.HTTP, endpoint, retry)
	if err != nil {
		return PublicResponse{}, err
	}
	observed := time.Now().UTC().UnixMilli()
	var envelope struct {
		Code string          `json:"code"`
		Data json.RawMessage `json:"data"`
	}
	if err = exchange.DecodeStrictJSON(payload, &envelope); err != nil {
		return PublicResponse{}, err
	}
	if envelope.Code != "0" || len(envelope.Data) == 0 {
		return PublicResponse{}, fmt.Errorf("public observation unsuccessful or missing data")
	}
	return PublicResponse{Data: envelope.Data, Source: model.PublicSource{URL: endpoint, PayloadHash: payloadDigest(payload), TimeBasis: "request_without_source_clock", SourceMS: requested, ObservedMS: observed}}, nil
}

func (c *Client) discoveryRequestGate(path string) exchange.WaitGate {
	c.wsGateMu.Lock()
	defer c.wsGateMu.Unlock()
	// These public endpoints have distinct IP budgets. Do not let eight
	// discovery workers spend the discount endpoint's quota concurrently.
	switch path {
	case "/api/v5/public/discount-rate-interest-free-quota":
		if c.discountGate == nil {
			c.discountGate = exchange.NewRequestGate(1100 * time.Millisecond)
		}
		return c.discountGate
	case "/api/v5/public/position-tiers":
		if c.riskTierGate == nil {
			c.riskTierGate = exchange.NewRequestGate(210 * time.Millisecond)
		}
		return c.riskTierGate
	default:
		if c.publicGate == nil {
			c.publicGate = exchange.NewRequestGate(100 * time.Millisecond)
		}
		return c.publicGate
	}
}
func ParseDiscoveryForecast(i model.Instrument, funding, mark, index, quoteIndex PublicResponse) (model.FundingPrediction, error) {
	if err := i.Validate(); err != nil {
		return model.FundingPrediction{}, err
	}
	if i.Exchange != "OKX" || i.MarketType != model.MarketPerpetual || i.QuoteAsset != "USDT" || i.SettleAsset == nil || *i.SettleAsset != "USDT" {
		return model.FundingPrediction{}, fmt.Errorf("unsupported forecast instrument")
	}
	var f []struct {
		ID      string `json:"instId"`
		Rate    string `json:"fundingRate"`
		Funding string `json:"fundingTime"`
		TS      string `json:"ts"`
	}
	var m []struct {
		ID    string `json:"instId"`
		Price string `json:"markPx"`
		TS    string `json:"ts"`
	}
	if err := exchange.DecodeStrictJSON(funding.Data, &f); err != nil {
		return model.FundingPrediction{}, err
	}
	if err := exchange.DecodeStrictJSON(mark.Data, &m); err != nil {
		return model.FundingPrediction{}, err
	}
	if len(f) != 1 || len(m) != 1 || f[0].ID != i.ExchangeSymbol || m[0].ID != i.ExchangeSymbol {
		return model.FundingPrediction{}, fmt.Errorf("forecast/mark identity mismatch")
	}
	source, err := parseEpoch(f[0].TS)
	if err != nil {
		return model.FundingPrediction{}, err
	}
	scheduled, err := parseEpoch(f[0].Funding)
	if err != nil {
		return model.FundingPrediction{}, err
	}
	markTS, err := parseEpoch(m[0].TS)
	if err != nil {
		return model.FundingPrediction{}, err
	}
	rate, err := model.ParseStrictDecimal(f[0].Rate, "independent fundingRate")
	if err != nil {
		return model.FundingPrediction{}, err
	}
	price, err := model.ParsePositiveDecimal(m[0].Price, "markPx")
	if err != nil {
		return model.FundingPrediction{}, err
	}
	var indices []struct {
		ID    string `json:"instId"`
		Price string `json:"idxPx"`
		TS    string `json:"ts"`
	}
	if err := exchange.DecodeStrictJSON(index.Data, &indices); err != nil {
		return model.FundingPrediction{}, err
	}
	if len(indices) != 1 || indices[0].ID != i.BaseAsset+"-USD" {
		return model.FundingPrediction{}, fmt.Errorf("base index identity mismatch")
	}
	indexPrice, err := model.ParsePositiveDecimal(indices[0].Price, "base index idxPx")
	if err != nil {
		return model.FundingPrediction{}, err
	}
	indexMS, err := parseEpoch(indices[0].TS)
	if err != nil {
		return model.FundingPrediction{}, err
	}
	var quoteIndices []struct {
		ID    string `json:"instId"`
		Price string `json:"idxPx"`
		TS    string `json:"ts"`
	}
	if err := exchange.DecodeStrictJSON(quoteIndex.Data, &quoteIndices); err != nil {
		return model.FundingPrediction{}, err
	}
	if len(quoteIndices) != 1 || quoteIndices[0].ID != "USDT-USD" {
		return model.FundingPrediction{}, fmt.Errorf("quote index identity mismatch")
	}
	quotePrice, err := model.ParsePositiveDecimal(quoteIndices[0].Price, "USDT-USD idxPx")
	if err != nil {
		return model.FundingPrediction{}, err
	}
	quoteMS, err := parseEpoch(quoteIndices[0].TS)
	if err != nil {
		return model.FundingPrediction{}, err
	}
	observed := max(funding.Source.ObservedMS, mark.Source.ObservedMS, index.Source.ObservedMS, quoteIndex.Source.ObservedMS)
	if source > observed || observed-source > 90000 || markTS > observed || observed-markTS > 90000 || scheduled <= observed || indexMS > observed || observed-indexMS > 90000 || quoteMS > observed || observed-quoteMS > 90000 {
		return model.FundingPrediction{}, fmt.Errorf("stale or settled forecast/mark")
	}
	funding.Source.SourceMS = source
	funding.Source.TimeBasis = "exchange_ts"
	mark.Source.SourceMS = markTS
	mark.Source.TimeBasis = "exchange_ts"
	index.Source.SourceMS = indexMS
	index.Source.TimeBasis = "exchange_ts"
	quoteIndex.Source.SourceMS = quoteMS
	quoteIndex.Source.TimeBasis = "exchange_ts"
	result := model.FundingPrediction{ID: uuid.NewString(), InstrumentID: i.ID, ObservedMS: observed, SourceMS: source, FundingMS: scheduled, Rate: rate, Mark: price, MarkMS: markTS, BaseIndex: indexPrice.DivRound(quotePrice, 36).RoundCeil(18), BaseIndexMS: indexMS, BaseUSDIndex: indexPrice, QuoteUSDIndex: quotePrice, QuoteIndexMS: quoteMS, Sources: []model.PublicSource{funding.Source, mark.Source, index.Source, quoteIndex.Source}}
	return result, result.Validate()
}
func ParseDiscoveryBar(i model.Instrument, response PublicResponse) (model.TradeBar, error) {
	if err := i.Validate(); err != nil {
		return model.TradeBar{}, err
	}
	if i.MarketType != model.MarketPerpetual || i.QuoteAsset != "USDT" {
		return model.TradeBar{}, fmt.Errorf("only linear USDT base-volume bars supported")
	}
	var rows [][]string
	if err := exchange.DecodeStrictJSON(response.Data, &rows); err != nil {
		return model.TradeBar{}, err
	}
	expected := (response.Source.SourceMS/60000 - 1) * 60000
	var matched []string
	for _, row := range rows {
		if len(row) != 9 {
			return model.TradeBar{}, fmt.Errorf("candle must include finality and native volumes")
		}
		at, err := parseEpoch(row[0])
		if err != nil {
			return model.TradeBar{}, err
		}
		if at == expected {
			if matched != nil {
				return model.TradeBar{}, fmt.Errorf("duplicate previous-minute bar")
			}
			matched = row
		}
	}
	if matched == nil || matched[8] != "1" {
		return model.TradeBar{}, fmt.Errorf("previous complete minute unavailable")
	}
	contracts, err := model.ParseStrictDecimal(matched[5], "contract volume")
	if err != nil {
		return model.TradeBar{}, err
	}
	base, err := model.ParseStrictDecimal(matched[6], "base volume")
	if err != nil {
		return model.TradeBar{}, err
	}
	if contracts.IsNegative() || base.IsNegative() || !contracts.Mul(i.ContractMultiplier).Equal(base) {
		return model.TradeBar{}, fmt.Errorf("bar volume conversion contradicts instrument")
	}
	result := model.TradeBar{ID: uuid.NewString(), InstrumentID: i.ID, ObservedMS: response.Source.ObservedMS, SourceMS: response.Source.SourceMS, MinuteMS: expected, BaseVolume: base, Final: true, ConversionHash: model.ReferenceHash(i), Sources: []model.PublicSource{response.Source}}
	return result, result.Validate()
}
func (c *Client) DiscoveryMarketFacts(ctx context.Context, i model.Instrument) (model.FundingPrediction, model.TradeBar, error) {
	f, err := c.DiscoveryPublic(ctx, "/api/v5/public/funding-rate", url.Values{"instId": {i.ExchangeSymbol}})
	if err != nil {
		return model.FundingPrediction{}, model.TradeBar{}, err
	}
	m, err := c.DiscoveryPublic(ctx, "/api/v5/public/mark-price", url.Values{"instType": {"SWAP"}, "instId": {i.ExchangeSymbol}})
	if err != nil {
		return model.FundingPrediction{}, model.TradeBar{}, err
	}
	index, err := c.DiscoveryPublic(ctx, "/api/v5/market/index-tickers", url.Values{"instId": {i.BaseAsset + "-USD"}})
	if err != nil {
		return model.FundingPrediction{}, model.TradeBar{}, err
	}
	quoteIndex, err := c.DiscoveryPublic(ctx, "/api/v5/market/index-tickers", url.Values{"instId": {"USDT-USD"}})
	if err != nil {
		return model.FundingPrediction{}, model.TradeBar{}, err
	}
	forecast, err := ParseDiscoveryForecast(i, f, m, index, quoteIndex)
	if err != nil {
		return model.FundingPrediction{}, model.TradeBar{}, err
	}
	candles, err := c.DiscoveryPublic(ctx, "/api/v5/market/candles", url.Values{"instId": {i.ExchangeSymbol}, "bar": {"1m"}, "limit": {"5"}})
	if err != nil {
		return model.FundingPrediction{}, model.TradeBar{}, err
	}
	bar, err := ParseDiscoveryBar(i, candles)
	return forecast, bar, err
}

type discountWire struct {
	Ccy          string `json:"ccy"`
	Restricted   *bool  `json:"collateralRestrict"`
	DiscountInfo []struct {
		Minimum string `json:"minAmt"`
		Maximum string `json:"maxAmt"`
		Rate    string `json:"discountRate"`
		Penalty string `json:"liqPenaltyRate"`
	} `json:"details"`
}

func ParseDiscoveryDiscount(response PublicResponse, ccy string) ([]model.DiscountTier, error) {
	var rows []discountWire
	if err := exchange.DecodeStrictJSON(response.Data, &rows); err != nil {
		return nil, err
	}
	var matched *discountWire
	for n := range rows {
		if rows[n].Ccy == ccy {
			if matched != nil {
				return nil, fmt.Errorf("duplicate discount currency")
			}
			matched = &rows[n]
		}
	}
	if matched == nil || matched.Restricted == nil || *matched.Restricted || len(matched.DiscountInfo) == 0 {
		return nil, fmt.Errorf("discount currency missing")
	}
	result := []model.DiscountTier{}
	for _, row := range matched.DiscountInfo {
		lower, err := model.ParseStrictDecimal(row.Minimum, "discount minAmt")
		if err != nil {
			return nil, err
		}
		upper := decimal.New(1, 19)
		if row.Maximum != "" {
			var err error
			upper, err = model.ParseStrictDecimal(row.Maximum, "discount maxAmt")
			if err != nil {
				return nil, err
			}
		}
		rate, err := model.ParseStrictDecimal(row.Rate, "discountRate")
		if err != nil {
			return nil, err
		}
		penalty, err := model.ParseStrictDecimal(row.Penalty, "liqPenaltyRate")
		if err != nil {
			return nil, err
		}
		result = append(result, model.DiscountTier{Minimum: lower, Maximum: upper, Rate: rate, Penalty: penalty})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Minimum.LessThan(result[j].Minimum) })
	return result, nil
}

type riskWire struct {
	Family      string `json:"instFamily"`
	ID          string `json:"instId"`
	Minimum     string `json:"minSz"`
	Maximum     string `json:"maxSz"`
	Initial     string `json:"imr"`
	Maintenance string `json:"mmr"`
	Leverage    string `json:"maxLever"`
	Ccy         string `json:"ccy"`
}

func ParseDiscoveryRisk(response PublicResponse, i model.Instrument, borrow bool) ([]model.RiskTier, error) {
	var rows []riskWire
	if err := exchange.DecodeStrictJSON(response.Data, &rows); err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("risk tiers missing")
	}
	if borrow {
		u, err := url.Parse(response.Source.URL)
		if err != nil || u.Path != "/api/v5/public/position-tiers" || u.Query().Get("ccy") != i.BaseAsset || u.Query().Get("instType") != "MARGIN" || u.Query().Get("tdMode") != "cross" || u.Query().Get("instId") != "" {
			return nil, fmt.Errorf("currency risk request scope mismatch")
		}
	}
	result := []model.RiskTier{}
	for _, row := range rows {
		if (borrow && ((row.Ccy != "" && row.Ccy != i.BaseAsset) || row.ID != "" || row.Family != "")) || (!borrow && row.Family != i.BaseAsset+"-USDT") {
			return nil, fmt.Errorf("risk tier instrument mismatch")
		}
		lower, err := model.ParseStrictDecimal(row.Minimum, "risk minSz")
		if err != nil {
			return nil, err
		}
		limit := row.Maximum
		upper, err := model.ParsePositiveDecimal(limit, "risk maxSz")
		if err != nil {
			return nil, err
		}
		imr, err := model.ParsePositiveDecimal(row.Initial, "risk imr")
		if err != nil {
			return nil, err
		}
		mmr, err := model.ParseStrictDecimal(row.Maintenance, "risk mmr")
		if err != nil {
			return nil, err
		}
		leverage, err := model.ParsePositiveDecimal(row.Leverage, "risk maxLever")
		if err != nil {
			return nil, err
		}
		if !borrow {
			lower = lower.Mul(i.ContractMultiplier)
			upper = upper.Mul(i.ContractMultiplier)
		}
		result = append(result, model.RiskTier{Minimum: lower, Maximum: upper, Initial: imr, Maintenance: mmr, MaximumLeverage: leverage})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Minimum.LessThan(result[j].Minimum) })
	// API tier lower bounds can differ by one legal contract lot. Preserve the
	// originals; the evaluator selects by inclusive maxima and legal lot sizes.
	previous := decimal.Zero
	for _, t := range result {
		if t.Minimum.LessThan(previous) || (!borrow && t.Minimum.Sub(previous).GreaterThan(i.QuantityStepSize.Mul(i.ContractMultiplier))) || (borrow && !t.Minimum.Equal(previous)) {
			return nil, fmt.Errorf("risk tier gap/overlap")
		}
		previous = t.Maximum
	}
	return result, nil
}
func (c *Client) DiscoveryCapital(ctx context.Context, i model.Instrument, modelHash string, borrowLeverage decimal.Decimal) (model.CapitalParameters, error) {
	d, err := c.DiscoveryPublic(ctx, "/api/v5/public/discount-rate-interest-free-quota", nil)
	if err != nil {
		return model.CapitalParameters{}, err
	}
	quote, err := ParseDiscoveryDiscount(d, "USDT")
	if err != nil {
		return model.CapitalParameters{}, err
	}
	base, err := ParseDiscoveryDiscount(d, i.BaseAsset)
	if err != nil {
		return model.CapitalParameters{}, err
	}
	p, err := c.DiscoveryPublic(ctx, "/api/v5/public/position-tiers", url.Values{"instType": {"SWAP"}, "tdMode": {"cross"}, "instFamily": {i.BaseAsset + "-USDT"}})
	if err != nil {
		return model.CapitalParameters{}, err
	}
	perps, err := ParseDiscoveryRisk(p, i, false)
	if err != nil {
		return model.CapitalParameters{}, err
	}
	b, err := c.DiscoveryPublic(ctx, "/api/v5/public/position-tiers", url.Values{"instType": {"MARGIN"}, "tdMode": {"cross"}, "ccy": {i.BaseAsset}})
	if err != nil {
		return model.CapitalParameters{}, err
	}
	borrows, err := ParseDiscoveryRisk(b, i, true)
	if err != nil {
		return model.CapitalParameters{}, err
	}
	observed := max(d.Source.ObservedMS, p.Source.ObservedMS, b.Source.ObservedMS)
	source := min(d.Source.SourceMS, p.Source.SourceMS, b.Source.SourceMS)
	result := model.CapitalParameters{ID: uuid.NewString(), InstrumentID: i.ID, ObservedMS: observed, SourceMS: source, EffectiveMS: source, ValidUntilMS: source + 3600000, Base: i.BaseAsset, QuoteDiscount: quote, BaseDiscount: base, PerpTiers: perps, BorrowTiers: borrows, BorrowLeverage: borrowLeverage, ModelHash: modelHash, Sources: []model.PublicSource{d.Source, p.Source, b.Source}}
	return result, result.Validate()
}

const ReferenceFeeURL = "https://www.okx.com/en-gb/help/advance-notice-spot-and-futures-trading-fee-adjustment"

// Group IDs use the official 2025-11-25 API mapping. The 2026-07-30
// consolidation notice aligns Spot groups 1/2/3 and Futures groups 1/2.
// Fiat, zero, stablecoin, special and RWA groups are outside this profile.
// The reference schedule is specifically the published Standard/Regular
// schedule. It is not an account fee observation or a universal region claim.
func (c *Client) DiscoveryReferenceFees(ctx context.Context, pair model.OKXPairMember, positionLimit decimal.Decimal) (model.ReferenceFee, error) {
	payload, err := exchange.Get(ctx, c.HTTP, ReferenceFeeURL, c.Retry)
	if err != nil {
		return model.ReferenceFee{}, err
	}
	maker, taker, spot, err := ParseReferenceFeePage(payload)
	if err != nil {
		return model.ReferenceFee{}, err
	}
	spotResponse, err := c.DiscoveryPublic(ctx, "/api/v5/public/instruments", url.Values{"instType": {"SPOT"}, "instId": {pair.Spot.ExchangeSymbol}})
	if err != nil {
		return model.ReferenceFee{}, err
	}
	perpResponse, err := c.DiscoveryPublic(ctx, "/api/v5/public/instruments", url.Values{"instType": {"SWAP"}, "instId": {pair.Perpetual.ExchangeSymbol}})
	if err != nil {
		return model.ReferenceFee{}, err
	}
	limit := func(response PublicResponse, i model.Instrument) (decimal.Decimal, error) {
		var rows []struct {
			ID     string `json:"instId"`
			State  string `json:"state"`
			Group  string `json:"groupId"`
			Market string `json:"maxMktSz"`
			Limit  string `json:"maxLmtSz"`
			Listed string `json:"listTime"`
			Tick   string `json:"tickSz"`
			Lot    string `json:"lotSz"`
		}
		if e := exchange.DecodeStrictJSON(response.Data, &rows); e != nil {
			return decimal.Zero, e
		}
		if len(rows) != 1 || rows[0].ID != i.ExchangeSymbol || rows[0].State != "live" || (i.MarketType == model.MarketPerpetual && rows[0].Listed != i.VenueContractVersion) {
			return decimal.Zero, fmt.Errorf("product definition changed")
		}
		if (i.MarketType == model.MarketSpot && (rows[0].Group != "12" && rows[0].Group != "13" && rows[0].Group != "14")) || (i.MarketType == model.MarketPerpetual && (rows[0].Group != "4" && rows[0].Group != "5")) {
			return decimal.Zero, fmt.Errorf("public Standard fee group unsupported")
		}
		tick, e := model.ParsePositiveDecimal(rows[0].Tick, "tickSz")
		if e != nil {
			return decimal.Zero, e
		}
		lot, e := model.ParsePositiveDecimal(rows[0].Lot, "lotSz")
		if e != nil {
			return decimal.Zero, e
		}
		if !tick.Equal(i.PriceTickSize) || !lot.Equal(i.QuantityStepSize) {
			return decimal.Zero, fmt.Errorf("product units changed")
		}
		market, e := model.ParsePositiveDecimal(rows[0].Market, "maxMktSz")
		if e != nil {
			return decimal.Zero, e
		}
		lmt, e := model.ParsePositiveDecimal(rows[0].Limit, "maxLmtSz")
		if e != nil {
			return decimal.Zero, e
		}
		return decimal.Min(market, lmt), nil
	}
	sm, err := limit(spotResponse, pair.Spot)
	if err != nil {
		return model.ReferenceFee{}, err
	}
	pm, err := limit(perpResponse, pair.Perpetual)
	if err != nil {
		return model.ReferenceFee{}, err
	}
	source := time.Date(2026, 8, 20, 0, 0, 0, 0, time.UTC).UnixMilli()
	effective := time.Date(2026, 9, 9, 9, 0, 0, 0, time.UTC).UnixMilli()
	// Local review expiry: never refresh an old document's source timestamp.
	until := time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC).UnixMilli()
	observed := max(spotResponse.Source.ObservedMS, perpResponse.Source.ObservedMS)
	doc := model.PublicSource{URL: ReferenceFeeURL, PayloadHash: payloadDigest(payload), TimeBasis: "reviewed_static_rule", SourceMS: source, ObservedMS: observed}
	result := model.ReferenceFee{ID: uuid.NewString(), InstrumentID: pair.Perpetual.ID, ObservedMS: observed, SourceMS: source, EffectiveMS: effective, ValidUntilMS: until, PerpMaker: maker, PerpTaker: taker, SpotTaker: spot, BuyFeeCurrency: pair.Spot.BaseAsset, SellFeeCurrency: "USDT", SpotMaximum: sm, PerpMaximum: pm, PositionLimit: positionLimit, Profile: model.OKXReferenceProfile, InterestVersion: model.OKXInterestVersion, Sources: []model.PublicSource{doc, spotResponse.Source, perpResponse.Source}}
	return result, result.Validate()
}
func ParseReferenceFeePage(payload []byte) (decimal.Decimal, decimal.Decimal, decimal.Decimal, error) {
	rows := feeRowPattern.FindAllSubmatch(payload, -1)
	var regular [][]string
	for _, row := range rows {
		cells := feeCellPattern.FindAllSubmatch(row[1], -1)
		values := []string{}
		for _, cell := range cells {
			value := html.UnescapeString(feeTagPattern.ReplaceAllString(string(cell[1]), " "))
			values = append(values, strings.Join(strings.Fields(value), " "))
		}
		if len(values) >= 3 && values[0] == "Regular" {
			regular = append(regular, values)
		}
	}
	if len(regular) != 2 {
		return decimal.Zero, decimal.Zero, decimal.Zero, fmt.Errorf("public fee page lacks exact Spot/Futures Regular rows")
	}
	cost := func(s string) (decimal.Decimal, error) {
		if !strings.HasSuffix(s, "%") {
			return decimal.Zero, fmt.Errorf("public fee percent missing")
		}
		v, e := model.ParseStrictDecimal(strings.TrimSuffix(s, "%"), "public fee percent")
		return v.Div(decimal.NewFromInt(100)), e
	}
	spot, e := cost(regular[0][len(regular[0])-1])
	if e != nil {
		return decimal.Zero, decimal.Zero, decimal.Zero, e
	}
	maker, e := cost(regular[1][len(regular[1])-2])
	if e != nil {
		return decimal.Zero, decimal.Zero, decimal.Zero, e
	}
	taker, e := cost(regular[1][len(regular[1])-1])
	if e != nil {
		return decimal.Zero, decimal.Zero, decimal.Zero, e
	}
	return maker, taker, spot, nil
}

var feeRowPattern = regexp.MustCompile(`(?is)<tr\b[^>]*>(.*?)</tr>`)
var feeCellPattern = regexp.MustCompile(`(?is)<t[dh]\b[^>]*>(.*?)</t[dh]>`)
var feeTagPattern = regexp.MustCompile(`<[^>]*>`)

func parseEpoch(s string) (int64, error) {
	if s == "" || strings.TrimSpace(s) != s {
		return 0, fmt.Errorf("source timestamp missing")
	}
	value, e := strconv.ParseInt(s, 10, 64)
	if e != nil || value <= 0 {
		return 0, fmt.Errorf("source timestamp invalid")
	}
	return value, nil
}
