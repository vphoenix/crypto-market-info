package lst

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/model"
)

type CEX struct {
	Transport *Transport
	Endpoint  string
}
type CEXMetadata struct {
	Instrument                                                             model.Instrument
	LotMin, LotMax, MarketLotMin, MarketLotMax, MarketLotStep, MinNotional decimal.Decimal
	OnboardTime                                                            time.Time
	FundingIntervalHours                                                   int
	FundingIntervalExplicit                                                bool
	RequestWeightLimit                                                     int
	ExchangeResponse, FundingInfoResponse                                  Response
}
type CEXDepth struct {
	Bids, Asks                 []model.Level
	LastUpdateId               uint64
	EventTime, TransactionTime time.Time
	Response                   Response
}
type CEXMark struct {
	MarkPriceTickE8, IndexPriceTickE8 int64
	IndicatedFundingRate              decimal.Decimal
	SourceTime, NextFundingTime       time.Time
	Response                          Response
}
type CEXMarket struct {
	Depth             CEXDepth
	Mark              CEXMark
	DepthErr, MarkErr error
}
type FundingPoint struct {
	FundingTime     time.Time
	Rate            decimal.Decimal
	MarkPriceTickE8 int64
	Response        Response
}
type FundingWindow struct {
	From, To  time.Time
	Points    []FundingPoint
	Responses []Response
	Complete  bool
}

func NewCEX(t *Transport, endpoint string) (*CEX, error) {
	u, e := transportURL(endpoint)
	if e != nil || u.User != nil || u.RawQuery != "" || (u.Path != "" && u.Path != "/") || t == nil {
		return nil, errors.New("cex_invalid_endpoint")
	}
	return &CEX{Transport: t, Endpoint: strings.TrimRight(endpoint, "/")}, nil
}
func (c *CEX) get(ctx context.Context, path string, q url.Values, class string) (Response, error) {
	u := c.Endpoint + path
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	return c.Transport.Do(ctx, "binance", http.MethodGet, u, nil, class)
}
func cexTime(ms int64) (time.Time, error) {
	if ms <= 0 {
		return time.Time{}, errors.New("cex_source_time_missing")
	}
	return time.UnixMilli(ms).UTC(), nil
}
func cexDecimal(raw, field string) (decimal.Decimal, error) {
	d, e := model.ParseStrictDecimal(raw, field)
	if e != nil {
		return d, e
	}
	if d.Exponent() < -18 || d.Abs().GreaterThanOrEqual(decimal.New(1, 20)) {
		return decimal.Zero, errors.New("cex_decimal_outside_decimal38_18")
	}
	return d, nil
}
func positiveCEX(raw, field string) (decimal.Decimal, error) {
	d, e := cexDecimal(raw, field)
	if e != nil {
		return d, e
	}
	if !d.IsPositive() {
		return d, fmt.Errorf("cex_%s_not_positive", field)
	}
	return d, nil
}

func (c *CEX) Metadata(ctx context.Context) (CEXMetadata, error) {
	var m CEXMetadata
	r, e := c.get(ctx, "/fapi/v1/exchangeInfo", nil, "normal")
	m.ExchangeResponse = r
	if e != nil {
		return m, e
	}
	var info struct {
		RateLimits []struct {
			RateLimitType, Interval string
			IntervalNum, Limit      int
		}
		Symbols []struct {
			Symbol, ContractType, Status, BaseAsset, QuoteAsset, MarginAsset string
			OnboardDate                                                      int64
			Filters                                                          []struct{ FilterType, TickSize, StepSize, MinQty, MaxQty, Notional string }
		}
	}
	if json.Unmarshal(r.Raw, &info) != nil || len(info.Symbols) == 0 {
		return m, errors.New("cex_exchange_info_invalid")
	}
	for _, limit := range info.RateLimits {
		if limit.RateLimitType != "REQUEST_WEIGHT" || limit.Interval != "MINUTE" || limit.IntervalNum != 1 {
			continue
		}
		if m.RequestWeightLimit != 0 || limit.Limit <= 0 {
			return m, errors.New("cex_weight_limit_invalid")
		}
		m.RequestWeightLimit = limit.Limit
	}
	if m.RequestWeightLimit == 0 {
		return m, errors.New("cex_weight_limit_missing")
	}
	if e = c.Transport.SetBinanceWeightLimit(c.Endpoint, m.RequestWeightLimit); e != nil {
		return m, e
	}
	found := false
	for _, s := range info.Symbols {
		if s.Symbol != "ETHUSDT" {
			continue
		}
		if found {
			return m, errors.New("cex_duplicate_symbol")
		}
		found = true
		if s.ContractType != "PERPETUAL" || s.Status != "TRADING" || s.BaseAsset != "ETH" || s.QuoteAsset != "USDT" || s.MarginAsset != "USDT" {
			return m, errors.New("cex_contract_identity_mismatch")
		}
		m.OnboardTime, e = cexTime(s.OnboardDate)
		if e != nil {
			return m, e
		}
		settle := "USDT"
		m.Instrument = model.Instrument{Exchange: "Binance", MarketType: model.MarketPerpetual, ExchangeSymbol: "ETHUSDT", VenueContractVersion: strconv.FormatInt(s.OnboardDate, 10), BaseAsset: "ETH", QuoteAsset: "USDT", SettleAsset: &settle, ContractMultiplier: decimal.NewFromInt(1)}
		seen := map[string]bool{}
		for _, f := range s.Filters {
			if seen[f.FilterType] {
				return m, errors.New("cex_duplicate_filter")
			}
			seen[f.FilterType] = true
			switch f.FilterType {
			case "PRICE_FILTER":
				m.Instrument.PriceTickSize, e = positiveCEX(f.TickSize, "tick_size")
			case "LOT_SIZE":
				m.Instrument.QuantityStepSize, e = positiveCEX(f.StepSize, "lot_step")
				if e == nil {
					m.LotMin, e = positiveCEX(f.MinQty, "lot_min")
				}
				if e == nil {
					m.LotMax, e = positiveCEX(f.MaxQty, "lot_max")
				}
			case "MARKET_LOT_SIZE":
				m.MarketLotStep, e = positiveCEX(f.StepSize, "market_lot_step")
				if e == nil {
					m.MarketLotMin, e = positiveCEX(f.MinQty, "market_lot_min")
				}
				if e == nil {
					m.MarketLotMax, e = positiveCEX(f.MaxQty, "market_lot_max")
				}
			case "MIN_NOTIONAL":
				m.MinNotional, e = positiveCEX(f.Notional, "min_notional")
			}
			if e != nil {
				return m, e
			}
		}
		for _, f := range []string{"PRICE_FILTER", "LOT_SIZE", "MARKET_LOT_SIZE", "MIN_NOTIONAL"} {
			if !seen[f] {
				return m, errors.New("cex_required_filter_missing")
			}
		}
		if m.LotMin.GreaterThan(m.LotMax) || m.MarketLotMin.GreaterThan(m.MarketLotMax) {
			return m, errors.New("cex_lot_bounds_invalid")
		}
		if e = m.Instrument.ValidateDefinition(); e != nil {
			return m, e
		}
	}
	if !found {
		return m, errors.New("cex_ethusdt_missing")
	}
	r, e = c.get(ctx, "/fapi/v1/fundingInfo", nil, "funding")
	m.FundingInfoResponse = r
	if e != nil {
		return m, e
	}
	var fi []struct {
		Symbol               string `json:"symbol"`
		FundingIntervalHours int    `json:"fundingIntervalHours"`
	}
	if json.Unmarshal(r.Raw, &fi) != nil || fi == nil {
		return m, errors.New("cex_funding_info_invalid")
	}
	// An absent symbol means no published adjustment, not evidence that every
	// historical interval was eight hours. This is only the current default.
	m.FundingIntervalHours = 8
	for _, v := range fi {
		if v.Symbol != "ETHUSDT" {
			continue
		}
		if m.FundingIntervalExplicit || v.FundingIntervalHours <= 0 || v.FundingIntervalHours > 24 {
			return m, errors.New("cex_funding_interval_invalid")
		}
		m.FundingIntervalHours = v.FundingIntervalHours
		m.FundingIntervalExplicit = true
	}
	return m, nil
}

func parseDepth(r Response, m CEXMetadata) (CEXDepth, error) {
	d := CEXDepth{Response: r}
	var v struct {
		LastUpdateId uint64 `json:"lastUpdateId"`
		E, T         int64
		Bids, Asks   [][]string
	}
	if json.Unmarshal(r.Raw, &v) != nil || v.LastUpdateId == 0 {
		return d, errors.New("cex_depth_invalid")
	}
	var e error
	d.LastUpdateId = v.LastUpdateId
	d.EventTime, e = cexTime(v.E)
	if e != nil {
		return d, e
	}
	d.TransactionTime, e = cexTime(v.T)
	if e != nil {
		return d, e
	}
	if d.TransactionTime.After(d.EventTime) {
		return d, errors.New("cex_depth_source_time_order")
	}
	parse := func(raw [][]string, bids bool) ([]model.Level, error) {
		if len(raw) == 0 || len(raw) > 10 {
			return nil, errors.New("cex_depth_not_one_to_ten_levels")
		}
		levels := make([]model.Level, 0, len(raw))
		for _, a := range raw {
			if len(a) != 2 {
				return nil, errors.New("cex_depth_invalid_level")
			}
			p, e := model.PriceTick(a[0], m.Instrument.PriceTickSize)
			if e != nil {
				return nil, e
			}
			q, e := model.QuantityLot(a[1], m.Instrument.QuantityStepSize)
			if e != nil || q == 0 {
				return nil, errors.New("cex_depth_invalid_quantity")
			}
			if len(levels) > 0 {
				prev := levels[len(levels)-1].PriceTick
				if (bids && p >= prev) || (!bids && p <= prev) {
					return nil, errors.New("cex_depth_unsorted_or_duplicate")
				}
			}
			levels = append(levels, model.Level{PriceTick: p, QtyLot: q})
		}
		return levels, nil
	}
	d.Bids, e = parse(v.Bids, true)
	if e != nil {
		return d, e
	}
	d.Asks, e = parse(v.Asks, false)
	if e != nil {
		return d, e
	}
	if d.Bids[0].PriceTick >= d.Asks[0].PriceTick {
		return d, errors.New("cex_depth_crossed")
	}
	return d, nil
}
func parseMark(r Response) (CEXMark, error) {
	m := CEXMark{Response: r}
	var v struct {
		Symbol, MarkPrice, IndexPrice, LastFundingRate string
		Time, NextFundingTime                          int64
	}
	if json.Unmarshal(r.Raw, &v) != nil || v.Symbol != "ETHUSDT" {
		return m, errors.New("cex_mark_invalid")
	}
	var e error
	m.SourceTime, e = cexTime(v.Time)
	if e != nil {
		return m, e
	}
	m.NextFundingTime, e = cexTime(v.NextFundingTime)
	if e != nil {
		return m, e
	}
	if !m.NextFundingTime.After(m.SourceTime) {
		return m, errors.New("cex_next_funding_not_future")
	}
	m.MarkPriceTickE8, e = model.PriceTick(v.MarkPrice, decimal.New(1, -8))
	if e != nil {
		return m, e
	}
	m.IndexPriceTickE8, e = model.PriceTick(v.IndexPrice, decimal.New(1, -8))
	if e != nil {
		return m, e
	}
	m.IndicatedFundingRate, e = cexDecimal(v.LastFundingRate, "indicated_funding_rate")
	return m, e
}
func (c *CEX) Market(ctx context.Context, m CEXMetadata) CEXMarket {
	var out CEXMarket
	r, e := c.get(ctx, "/fapi/v1/depth", url.Values{"symbol": {"ETHUSDT"}, "limit": {"10"}}, "normal")
	out.Depth = CEXDepth{Response: r}
	out.DepthErr = e
	if e == nil {
		out.Depth, out.DepthErr = parseDepth(r, m)
	}
	r, e = c.get(ctx, "/fapi/v1/premiumIndex", url.Values{"symbol": {"ETHUSDT"}}, "normal")
	out.Mark = CEXMark{Response: r}
	out.MarkErr = e
	if e == nil {
		out.Mark, out.MarkErr = parseMark(r)
	}
	return out
}

func (c *CEX) Funding(ctx context.Context, start, end time.Time) (FundingWindow, error) {
	w := FundingWindow{From: start.UTC(), To: end.UTC()}
	if start.UnixMilli() <= 0 || end.Before(start) || end.Sub(start) > 61*24*time.Hour || !start.Equal(time.UnixMilli(start.UnixMilli())) || !end.Equal(time.UnixMilli(end.UnixMilli())) {
		return w, errors.New("cex_funding_window_invalid")
	}
	cursor := start.UnixMilli()
	last := int64(0)
	seen := map[int64]FundingPoint{}
	for {
		r, e := c.get(ctx, "/fapi/v1/fundingRate", url.Values{"symbol": {"ETHUSDT"}, "startTime": {strconv.FormatInt(cursor, 10)}, "endTime": {strconv.FormatInt(end.UnixMilli(), 10)}, "limit": {"1000"}}, "funding")
		w.Responses = append(w.Responses, r)
		if e != nil {
			return w, e
		}
		var points []struct {
			Symbol                 string
			FundingTime            int64
			FundingRate, MarkPrice string
		}
		if json.Unmarshal(r.Raw, &points) != nil || points == nil || len(points) > 1000 {
			return w, errors.New("cex_funding_page_invalid")
		}
		pageLast := int64(0)
		for _, v := range points {
			if v.Symbol != "ETHUSDT" || v.FundingTime < cursor || v.FundingTime > end.UnixMilli() || v.FundingTime < pageLast {
				return w, errors.New("cex_funding_page_outside_window_or_unsorted")
			}
			p := FundingPoint{FundingTime: time.UnixMilli(v.FundingTime).UTC(), Response: r}
			p.Rate, e = cexDecimal(v.FundingRate, "funding_rate")
			if e != nil {
				return w, e
			}
			p.MarkPriceTickE8, e = model.PriceTick(v.MarkPrice, decimal.New(1, -8))
			if e != nil {
				return w, errors.New("cex_funding_settlement_mark_invalid")
			}
			if old, ok := seen[v.FundingTime]; ok {
				if !old.Rate.Equal(p.Rate) || old.MarkPriceTickE8 != p.MarkPriceTickE8 {
					return w, errors.New("cex_funding_duplicate_conflict")
				}
				continue
			}
			if v.FundingTime <= last {
				return w, errors.New("cex_funding_not_increasing")
			}
			seen[v.FundingTime] = p
			w.Points = append(w.Points, p)
			last = v.FundingTime
			pageLast = v.FundingTime
		}
		if len(points) < 1000 || last == end.UnixMilli() {
			w.Complete = true
			return w, nil
		}
		if last < cursor {
			return w, errors.New("cex_funding_page_no_progress")
		}
		cursor = last + 1
	}
}

func cexPtrTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	v := t.UTC().Truncate(time.Microsecond)
	return &v
}
func cexPtrHash(h string) *string {
	b, e := hex.DecodeString(strings.TrimPrefix(h, "0x"))
	if e != nil || len(b) != 32 {
		return nil
	}
	v := string(b)
	return &v
}
func decimalAtoms(d decimal.Decimal, scale int32) (*big.Int, error) {
	v := d.Shift(scale)
	if !v.Equal(v.Truncate(0)) {
		return nil, errors.New("cex_amount_not_exact_at_scale")
	}
	n := v.BigInt()
	if n.Sign() < 0 || n.BitLen() > 256 {
		return nil, errors.New("cex_amount_outside_uint256")
	}
	return n, nil
}

// ApplyHedge retains each independent source's evidence even if depth or mark
// failed. Amounts are exact; it never extrapolates beyond the ten source levels.
func ApplyHedge(q *Quote, m CEXMetadata, market CEXMarket, q0 *big.Int) error {
	q.HedgeInstrumentId = m.Instrument.ID
	q.HedgeStatus = "unknown"
	d := market.Depth
	mark := market.Mark
	q.HedgeDepthRequestedAt = cexPtrTime(d.Response.RequestedAt)
	q.HedgeDepthReceivedAt = cexPtrTime(d.Response.ReceivedAt)
	q.HedgeDepthAvailableAt = cexPtrTime(d.Response.AvailableAt)
	q.HedgeDepthPayloadHash = cexPtrHash(d.Response.PayloadHash)
	q.MarkRequestedAt = cexPtrTime(mark.Response.RequestedAt)
	q.MarkReceivedAt = cexPtrTime(mark.Response.ReceivedAt)
	q.MarkAvailableAt = cexPtrTime(mark.Response.AvailableAt)
	q.MarkPayloadHash = cexPtrHash(mark.Response.PayloadHash)
	if market.MarkErr == nil && mark.MarkPriceTickE8 > 0 {
		q.MarkPriceTickE8 = &mark.MarkPriceTickE8
		q.IndexPriceTickE8 = &mark.IndexPriceTickE8
		q.IndicatedFundingRate = &mark.IndicatedFundingRate
		q.MarkSourceTime = cexPtrTime(mark.SourceTime)
		q.NextFundingTime = cexPtrTime(mark.NextFundingTime)
	}
	fail := func(reason string) error { q.HedgeReason = reason; return errors.New(reason) }
	if market.DepthErr != nil {
		return fail("cex_depth_unavailable")
	}
	q.HedgeDepthLastUpdateId = &d.LastUpdateId
	q.HedgeDepthEventTime = cexPtrTime(d.EventTime)
	q.HedgeDepthTransactionTime = cexPtrTime(d.TransactionTime)
	if m.Instrument.ID == 0 || q0 == nil || q0.Sign() <= 0 || q0.BitLen() > 256 {
		return fail("cex_hedge_identity_or_input_invalid")
	}
	lotWei, e := decimalAtoms(m.Instrument.QuantityStepSize.Mul(m.Instrument.ContractMultiplier), 18)
	if e != nil || lotWei.Sign() <= 0 {
		return fail("cex_lot_wei_invalid")
	}
	lots := new(big.Int).Quo(q0, lotWei)
	if !lots.IsInt64() || lots.Sign() <= 0 {
		return fail("cex_hedge_lots_outside_positive_int64")
	}
	qtyLots := lots.Int64()
	hedgeWei := new(big.Int).Mul(lots, lotWei)
	q.HedgeQuantityLot = &qtyLots
	q.HedgeEthWei = hedgeWei
	q.UnhedgedEthResidualWei = new(big.Int).Sub(q0, hedgeWei)
	qty := decimal.NewFromInt(qtyLots).Mul(m.Instrument.QuantityStepSize)
	if qty.LessThan(m.LotMin) || qty.GreaterThan(m.LotMax) || qty.LessThan(m.MarketLotMin) || qty.GreaterThan(m.MarketLotMax) {
		return fail("cex_lot_limits")
	}
	if _, e = model.QuantityLot(qty.String(), m.MarketLotStep); e != nil {
		return fail("cex_market_lot_step")
	}
	notional := func(levels []model.Level) (*big.Int, error) {
		left := uint64(qtyLots)
		sum := decimal.Zero
		for _, l := range levels {
			take := min(left, l.QtyLot)
			sum = sum.Add(decimal.NewFromInt(l.PriceTick).Mul(m.Instrument.PriceTickSize).Mul(decimal.NewFromBigInt(new(big.Int).SetUint64(take), 0)).Mul(m.Instrument.QuantityStepSize).Mul(m.Instrument.ContractMultiplier))
			left -= take
			if left == 0 {
				break
			}
		}
		if left != 0 {
			return nil, errors.New("cex_ten_level_capacity_insufficient")
		}
		if sum.LessThan(m.MinNotional) {
			return nil, errors.New("cex_min_notional")
		}
		return decimalAtoms(sum, 8)
	}
	sell, e := notional(d.Bids)
	if e != nil {
		return fail(e.Error())
	}
	buy, e := notional(d.Asks)
	if e != nil {
		return fail(e.Error())
	}
	q.HedgeSellNotionalUsdtE8 = sell
	q.HedgeBuyNotionalUsdtE8 = buy
	q.HedgeStatus = "ok"
	q.HedgeReason = ""
	return nil
}
