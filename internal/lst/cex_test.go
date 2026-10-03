package lst

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/model"
)

func cexTestMetadata() CEXMetadata {
	settle := "USDT"
	return CEXMetadata{Instrument: model.Instrument{ID: 42, Exchange: "Binance", MarketType: model.MarketPerpetual, ExchangeSymbol: "ETHUSDT", VenueContractVersion: "1574840700000", BaseAsset: "ETH", QuoteAsset: "USDT", SettleAsset: &settle, ContractMultiplier: decimal.NewFromInt(1), PriceTickSize: decimal.RequireFromString("0.01"), QuantityStepSize: decimal.RequireFromString("0.001")}, LotMin: decimal.RequireFromString("0.001"), LotMax: decimal.NewFromInt(10000), MarketLotMin: decimal.RequireFromString("0.001"), MarketLotMax: decimal.NewFromInt(2000), MarketLotStep: decimal.RequireFromString("0.001"), MinNotional: decimal.NewFromInt(20)}
}
func cexTestResponse(raw string) Response {
	return Response{Raw: []byte(raw), PayloadHash: "0x" + strings.Repeat("ab", 32), RequestedAt: time.Unix(1790812800, 123456789).UTC(), ReceivedAt: time.Unix(1790812800, 234567890).UTC(), AvailableAt: time.Unix(1790812800, 345678901).UTC()}
}

const cexTestDepth = `{"lastUpdateId":1234,"E":1790812800000,"T":1790812799999,"bids":[["2000.00","1.000"],["1999.00","1.000"]],"asks":[["2001.00","2.000"]]}`
const cexTestMark = `{"symbol":"ETHUSDT","time":1790812800000,"nextFundingTime":1790841600000,"markPrice":"2000.12345678","indexPrice":"2000.98765432","lastFundingRate":"-0.00005694"}`

func TestCEXMetadataArchivedIdentityAndExactFilters(t *testing.T) {
	raw, e := os.ReadFile("../../research/2026-10-02-lst-design/binance-exchange-info.raw.json")
	if e != nil {
		t.Fatal(e)
	}
	tr, _ := transportFixture(t, func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/fapi/v1/exchangeInfo" {
			return transportReply(200, string(raw)), nil
		}
		return transportReply(200, `[{"symbol":"ETHUSDT","fundingIntervalHours":4}]`), nil
	})
	c, e := NewCEX(tr, "https://fapi.example")
	if e != nil {
		t.Fatal(e)
	}
	m, e := c.Metadata(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	if m.Instrument.VenueContractVersion != "1574840700000" || !m.Instrument.PriceTickSize.Equal(decimal.RequireFromString("0.01")) || !m.Instrument.QuantityStepSize.Equal(decimal.RequireFromString("0.001")) || m.FundingIntervalHours != 4 || !m.FundingIntervalExplicit {
		t.Fatalf("metadata wrong: %+v", m)
	}
	if m.ExchangeResponse.PayloadHash == "" || m.FundingInfoResponse.PayloadHash == "" {
		t.Fatal("metadata evidence missing")
	}
}

func TestCEXHedgeExactVWAPResidualAndIndependentMarkPrecision(t *testing.T) {
	m := cexTestMetadata()
	d, e := parseDepth(cexTestResponse(cexTestDepth), m)
	if e != nil {
		t.Fatal(e)
	}
	mark, e := parseMark(cexTestResponse(cexTestMark))
	if e != nil {
		t.Fatal(e)
	}
	q0, _ := new(big.Int).SetString("1234567000000000000", 10)
	var q Quote
	if e = ApplyHedge(&q, m, CEXMarket{Depth: d, Mark: mark}, q0); e != nil {
		t.Fatal(e)
	}
	if q.HedgeQuantityLot == nil || *q.HedgeQuantityLot != 1234 || q.HedgeSellNotionalUsdtE8.String() != "246776600000" || q.HedgeBuyNotionalUsdtE8.String() != "246923400000" || q.UnhedgedEthResidualWei.String() != "567000000000000" {
		t.Fatalf("wrong hedge %+v", q)
	}
	if *q.MarkPriceTickE8 != 200012345678 || *q.IndexPriceTickE8 != 200098765432 || q.IndicatedFundingRate.String() != "-0.00005694" {
		t.Fatal("mark rounded to trade tick or funding sign lost")
	}
	if len(*q.HedgeDepthPayloadHash) != 32 || q.HedgeDepthRequestedAt.Nanosecond()%1000 != 0 {
		t.Fatal("model evidence/time encoding wrong")
	}
}

func TestCEXInsufficientDepthUnknownAndNoFloatParsing(t *testing.T) {
	m := cexTestMetadata()
	d, _ := parseDepth(cexTestResponse(cexTestDepth), m)
	q0, _ := new(big.Int).SetString("3000000000000000000", 10)
	var q Quote
	if e := ApplyHedge(&q, m, CEXMarket{Depth: d}, q0); e == nil || q.HedgeSellNotionalUsdtE8 != nil || q.HedgeStatus != "unknown" {
		t.Fatal("insufficient depth extrapolated")
	}
	for _, raw := range []string{strings.Replace(cexTestDepth, `"2000.00"`, `2000.00`, 1), strings.Replace(cexTestDepth, `"1.000"`, `"1e0"`, 1), strings.Replace(cexTestDepth, `"1999.00"`, `"2000.00"`, 1), strings.Replace(cexTestDepth, `"T":1790812799999`, `"T":0`, 1), strings.Replace(cexTestDepth, `"2000.00"`, `"2000.001"`, 1)} {
		if _, e := parseDepth(cexTestResponse(raw), m); e == nil {
			t.Fatalf("invalid depth accepted: %s", raw)
		}
	}
	if _, e := parseMark(cexTestResponse(strings.Replace(cexTestMark, "2000.12345678", "2000.123456789", 1))); e == nil {
		t.Fatal("mark precision silently rounded")
	}
}

func TestCEXMarketSourceFailuresRemainIndependent(t *testing.T) {
	tr, _ := transportFixture(t, func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/fapi/v1/depth" {
			if r.URL.Query().Get("limit") != "10" {
				t.Error("depth over-fetch")
			}
			return transportReply(200, `{"lastUpdateId":0}`), nil
		}
		return transportReply(200, cexTestMark), nil
	})
	c, _ := NewCEX(tr, "https://fapi.example")
	market := c.Market(context.Background(), cexTestMetadata())
	if market.DepthErr == nil || market.MarkErr != nil {
		t.Fatal("source failure contaminated independent mark", market)
	}
	var q Quote
	ApplyHedge(&q, cexTestMetadata(), market, big.NewInt(100))
	if q.MarkPriceTickE8 == nil || q.HedgeStatus != "unknown" || q.HedgeDepthPayloadHash == nil {
		t.Fatal("partial market lost independent facts")
	}
}

func TestCEXFundingFixedWindowPagesAndSigns(t *testing.T) {
	start := time.UnixMilli(1790000000000).UTC()
	end := start.Add(2 * time.Hour)
	calls := 0
	tr, _ := transportFixture(t, func(r *http.Request) (*http.Response, error) {
		calls++
		q := r.URL.Query()
		if q.Get("endTime") != strconv.FormatInt(end.UnixMilli(), 10) {
			t.Error("moving endTime")
		}
		if calls == 1 {
			items := make([]map[string]any, 1000)
			for i := range items {
				items[i] = map[string]any{"symbol": "ETHUSDT", "fundingTime": start.UnixMilli() + int64(i)*1000, "fundingRate": "-0.00000001", "markPrice": "2000.12345678"}
			}
			raw, _ := json.Marshal(items)
			return transportReply(200, string(raw)), nil
		}
		if q.Get("startTime") != strconv.FormatInt(start.UnixMilli()+999001, 10) {
			t.Error("cursor not last+1")
		}
		return transportReply(200, fmt.Sprintf(`[{"symbol":"ETHUSDT","fundingTime":%d,"fundingRate":"0.00000002","markPrice":"2100.87654321"}]`, start.UnixMilli()+1000000)), nil
	})
	c, _ := NewCEX(tr, "https://fapi.example")
	w, e := c.Funding(context.Background(), start, end)
	if e != nil {
		t.Fatal(e)
	}
	if !w.Complete || len(w.Points) != 1001 || !w.Points[0].Rate.IsNegative() || w.Points[1000].MarkPriceTickE8 != 210087654321 {
		t.Fatal("funding precision/pagination", len(w.Points), e)
	}
}

func TestCEXFundingRejectsMissingMarkConflictsAndUnsorted(t *testing.T) {
	start := time.UnixMilli(1790000000000).UTC()
	end := start.Add(time.Hour)
	for _, raw := range []string{
		`[{"symbol":"ETHUSDT","fundingTime":1790000000000,"fundingRate":"0.0001"}]`,
		`[{"symbol":"ETHUSDT","fundingTime":1790000000000,"fundingRate":"0.0001","markPrice":"2000"},{"symbol":"ETHUSDT","fundingTime":1790000000000,"fundingRate":"0.0002","markPrice":"2000"}]`,
		`[{"symbol":"ETHUSDT","fundingTime":1790000000001,"fundingRate":"0.0001","markPrice":"2000"},{"symbol":"ETHUSDT","fundingTime":1790000000000,"fundingRate":"0.0001","markPrice":"2000"}]`,
		`null`,
	} {
		tr, _ := transportFixture(t, func(*http.Request) (*http.Response, error) { return transportReply(200, raw), nil })
		c, _ := NewCEX(tr, "https://fapi.example")
		w, e := c.Funding(context.Background(), start, end)
		if e == nil || w.Complete {
			t.Fatalf("invalid funding accepted %s", raw)
		}
	}
}
