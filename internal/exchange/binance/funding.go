package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/exchange"
	"github.com/vphoenix/crypto-market-info/internal/model"
)

type fundingHistoryWire struct {
	Symbol      string `json:"symbol"`
	FundingRate string `json:"fundingRate"`
	FundingTime *int64 `json:"fundingTime"`
}

func ParseFundingHistory(payload []byte, instrument model.Instrument, fundingTime time.Time) (model.FundingRate, bool, error) {
	var rows []fundingHistoryWire
	if err := json.Unmarshal(payload, &rows); err != nil {
		return model.FundingRate{}, false, fmt.Errorf("Binance funding history JSON: %w", err)
	}
	target := fundingTime.UTC().UnixMilli()
	for _, row := range rows {
		if row.Symbol != instrument.ExchangeSymbol || row.FundingTime == nil {
			continue
		}
		if *row.FundingTime != target {
			continue
		}
		rate, err := model.ParseStrictDecimal(row.FundingRate, "fundingRate")
		if err != nil {
			return model.FundingRate{}, false, err
		}
		result := model.FundingRate{InstrumentID: instrument.ID, HourTime: fundingTime.UTC().Truncate(time.Hour), FundingTime: time.UnixMilli(*row.FundingTime).UTC(), Rate: rate, IsActual: true}
		return result, true, result.Validate()
	}
	return model.FundingRate{}, false, nil
}

func (c *Client) ActualFundingRate(ctx context.Context, instrument model.Instrument, fundingTime time.Time) (model.FundingRate, bool, error) {
	target := fundingTime.UTC().UnixMilli()
	values := url.Values{"symbol": []string{instrument.ExchangeSymbol}, "startTime": []string{strconv.FormatInt(target, 10)}, "endTime": []string{strconv.FormatInt(target+1, 10)}, "limit": []string{"10"}}
	retry := c.Retry
	retry.MaxAttempts = 1
	payload, err := exchange.Get(ctx, c.HTTP, strings.TrimRight(c.FuturesBaseURL, "/")+"/fapi/v1/fundingRate?"+values.Encode(), retry)
	if err != nil {
		return model.FundingRate{}, false, err
	}
	return ParseFundingHistory(payload, instrument, fundingTime)
}
