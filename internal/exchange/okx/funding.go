package okx

import (
	"bytes"
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

type fundingEnvelope struct {
	Code string          `json:"code"`
	Msg  string          `json:"msg"`
	Data json.RawMessage `json:"data"`
}
type fundingWire struct {
	InstID       string `json:"instId"`
	FundingRate  string `json:"fundingRate"`
	RealizedRate string `json:"realizedRate"`
	FundingTime  string `json:"fundingTime"`
	Timestamp    string `json:"ts"`
}

const fundingHistoryLimit = "100"

func parseFundingEnvelope(payload []byte) ([]fundingWire, error) {
	var envelope fundingEnvelope
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return nil, fmt.Errorf("OKX funding JSON: %w", err)
	}
	if envelope.Code != "0" {
		return nil, fmt.Errorf("OKX funding code=%q msg=%q", envelope.Code, envelope.Msg)
	}
	trimmed := bytes.TrimSpace(envelope.Data)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil, fmt.Errorf("OKX funding data must be a non-null array")
	}
	var rows []fundingWire
	if err := json.Unmarshal(trimmed, &rows); err != nil {
		return nil, err
	}
	return rows, nil
}

func ParseFundingHistory(payload []byte, instrument model.Instrument, fundingTime time.Time) (model.FundingRate, bool, error) {
	rows, err := parseFundingEnvelope(payload)
	if err != nil {
		return model.FundingRate{}, false, err
	}
	target := fundingTime.UTC().UnixMilli()
	for _, row := range rows {
		if row.InstID != instrument.ExchangeSymbol {
			continue
		}
		milliseconds, parseErr := strconv.ParseInt(row.FundingTime, 10, 64)
		if parseErr != nil {
			return model.FundingRate{}, false, fmt.Errorf("OKX fundingTime must be an integer string")
		}
		if milliseconds != target {
			continue
		}
		if row.RealizedRate == "" {
			return model.FundingRate{}, false, nil
		}
		rate, parseErr := model.ParseStrictDecimal(row.RealizedRate, "realizedRate")
		if parseErr != nil {
			return model.FundingRate{}, false, parseErr
		}
		result := model.FundingRate{InstrumentID: instrument.ID, HourTime: fundingTime.UTC().Truncate(time.Hour), FundingTime: time.UnixMilli(milliseconds).UTC(), Rate: rate, IsActual: true}
		return result, true, result.Validate()
	}
	return model.FundingRate{}, false, nil
}

func (c *Client) ActualFundingRate(ctx context.Context, instrument model.Instrument, fundingTime time.Time) (model.FundingRate, bool, error) {
	values := url.Values{"instId": []string{instrument.ExchangeSymbol}, "limit": []string{fundingHistoryLimit}}
	retry := c.Retry
	retry.MaxAttempts = 1
	payload, err := exchange.Get(ctx, c.HTTP, strings.TrimRight(c.BaseURL, "/")+"/api/v5/public/funding-rate-history?"+values.Encode(), retry)
	if err != nil {
		return model.FundingRate{}, false, err
	}
	return ParseFundingHistory(payload, instrument, fundingTime)
}
