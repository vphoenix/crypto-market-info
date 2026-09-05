package bybit

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"strconv"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/model"
)

type fundingHistoryResult struct {
	Category string          `json:"category"`
	List     json.RawMessage `json:"list"`
}

type fundingHistoryWire struct {
	Symbol               string  `json:"symbol"`
	FundingRate          *string `json:"fundingRate"`
	FundingRateTimestamp *string `json:"fundingRateTimestamp"`
}

func ParseFundingHistory(payload []byte, instrument model.Instrument, fundingTime time.Time) (model.FundingRate, bool, error) {
	if err := validateFundingTarget(fundingTime); err != nil {
		return model.FundingRate{}, false, err
	}
	raw, err := decodeResult(payload, "funding/history")
	if err != nil {
		return model.FundingRate{}, false, err
	}
	var result fundingHistoryResult
	if err = json.Unmarshal(raw, &result); err != nil {
		return model.FundingRate{}, false, fmt.Errorf("Bybit funding history result: %w", err)
	}
	if result.Category != "linear" {
		return model.FundingRate{}, false, fmt.Errorf("Bybit funding history category=%q, want linear", result.Category)
	}
	trimmed := bytes.TrimSpace(result.List)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return model.FundingRate{}, false, fmt.Errorf("Bybit funding history list must be a non-null array")
	}
	var rows []fundingHistoryWire
	if err = json.Unmarshal(trimmed, &rows); err != nil {
		return model.FundingRate{}, false, fmt.Errorf("Bybit funding history list: %w", err)
	}
	target := fundingTime.UTC().UnixMilli()
	var matched *model.FundingRate
	for index, row := range rows {
		if row.Symbol != instrument.ExchangeSymbol {
			return model.FundingRate{}, false, fmt.Errorf("Bybit funding history row %d symbol=%q, want %q", index, row.Symbol, instrument.ExchangeSymbol)
		}
		if row.FundingRate == nil || row.FundingRateTimestamp == nil {
			return model.FundingRate{}, false, fmt.Errorf("Bybit funding history row %d requires fundingRate and fundingRateTimestamp", index)
		}
		milliseconds, parseErr := strconv.ParseInt(*row.FundingRateTimestamp, 10, 64)
		if parseErr != nil || milliseconds <= 0 {
			return model.FundingRate{}, false, fmt.Errorf("Bybit funding history row %d fundingRateTimestamp must be a positive integer string", index)
		}
		rate, parseErr := model.ParseStrictDecimal(*row.FundingRate, "fundingRate")
		if parseErr != nil {
			return model.FundingRate{}, false, fmt.Errorf("Bybit funding history row %d: %w", index, parseErr)
		}
		if milliseconds != target {
			continue
		}
		if matched != nil {
			return model.FundingRate{}, false, fmt.Errorf("Bybit funding history contains duplicate target timestamp %d", target)
		}
		value := model.FundingRate{InstrumentID: instrument.ID, HourTime: fundingTime.UTC().Truncate(time.Hour), FundingTime: time.UnixMilli(milliseconds).UTC(), Rate: rate, IsActual: true}
		if parseErr = value.Validate(); parseErr != nil {
			return model.FundingRate{}, false, parseErr
		}
		matched = &value
	}
	if matched == nil {
		return model.FundingRate{}, false, nil
	}
	return *matched, true, nil
}

func (c *Client) ActualFundingRate(ctx context.Context, instrument model.Instrument, fundingTime time.Time) (model.FundingRate, bool, error) {
	if err := instrument.Validate(); err != nil || instrument.Exchange != "Bybit" || instrument.MarketType != model.MarketPerpetual {
		return model.FundingRate{}, false, fmt.Errorf("Bybit funding history requires a registered Bybit perpetual instrument")
	}
	if err := validateFundingTarget(fundingTime); err != nil {
		return model.FundingRate{}, false, err
	}
	target := fundingTime.UTC().UnixMilli()
	values := url.Values{
		"category": []string{"linear"},
		"symbol":   []string{instrument.ExchangeSymbol},
		"endTime":  []string{strconv.FormatInt(target+1, 10)},
		"limit":    []string{"1"},
	}
	retry := c.Retry
	retry.MaxAttempts = 1
	payload, err := c.apiGet(ctx, "/v5/market/funding/history", values, retry)
	if err != nil {
		return model.FundingRate{}, false, err
	}
	return ParseFundingHistory(payload, instrument, fundingTime)
}

func validateFundingTarget(fundingTime time.Time) error {
	target := fundingTime.UTC().UnixMilli()
	if fundingTime.IsZero() || target <= 0 || target == math.MaxInt64 || !fundingTime.Equal(time.UnixMilli(target).UTC()) {
		return fmt.Errorf("Bybit funding history target must be a positive UTC millisecond before the maximum timestamp")
	}
	return nil
}
