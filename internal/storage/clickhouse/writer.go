package clickhouse

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/model"
)

func (c *Client) WriteMinute(ctx context.Context, batch model.MinuteBatch) error {
	if err := validateBatch(batch); err != nil {
		return err
	}
	if len(batch.Deltas) > 0 {
		if err := c.retryWrite(ctx, func(writeCtx context.Context) error { return c.insertDeltas(writeCtx, batch.Deltas) }); err != nil {
			return fmt.Errorf("write second deltas: %w", err)
		}
	}
	if err := c.retryWrite(ctx, func(writeCtx context.Context) error { return c.insertMinute(writeCtx, batch.Minute) }); err != nil {
		return fmt.Errorf("write minute visibility row: %w", err)
	}
	return nil
}

func validateBatch(batch model.MinuteBatch) error {
	wantID, err := model.MinuteID(batch.Minute.InstrumentID, batch.Minute.MinuteTime)
	if err != nil {
		return err
	}
	if batch.Minute.ID != wantID {
		return fmt.Errorf("minute id does not match instrument/time")
	}
	if batch.Minute.ValidBitmap&1 == 0 {
		return fmt.Errorf("minute batch has no valid second-zero anchor")
	}
	bids, err := unpadded(batch.Minute.Bids)
	if err != nil {
		return fmt.Errorf("minute bids: %w", err)
	}
	asks, err := unpadded(batch.Minute.Asks)
	if err != nil {
		return fmt.Errorf("minute asks: %w", err)
	}
	if err = (model.BookSnapshot{InstrumentID: batch.Minute.InstrumentID, SourceTime: batch.Minute.MinuteTime, Bids: bids, Asks: asks}).Validate(model.BookDepth); err != nil {
		return fmt.Errorf("minute snapshot: %w", err)
	}
	lastSecond := uint8(0)
	for _, delta := range batch.Deltas {
		if delta.MinuteID != batch.Minute.ID || delta.SecondOffset == 0 || delta.SecondOffset > 59 {
			return fmt.Errorf("invalid delta identity/second")
		}
		if len(delta.BidChangePrice) != len(delta.BidChangeQty) || len(delta.AskChangePrice) != len(delta.AskChangeQty) {
			return fmt.Errorf("delta array length mismatch")
		}
		if batch.Minute.ValidBitmap&(uint64(1)<<delta.SecondOffset) == 0 {
			return fmt.Errorf("delta exists for invalid second %d", delta.SecondOffset)
		}
		if delta.SecondOffset <= lastSecond {
			return fmt.Errorf("deltas must have unique increasing second offsets")
		}
		lastSecond = delta.SecondOffset
		if err = validateDeltaSide(delta.BidChangePrice, "bid"); err != nil {
			return err
		}
		if err = validateDeltaSide(delta.AskChangePrice, "ask"); err != nil {
			return err
		}
	}
	return nil
}

func validateDeltaSide(prices []int64, side string) error {
	seen := make(map[int64]struct{}, len(prices))
	for _, price := range prices {
		if price <= 0 {
			return fmt.Errorf("%s delta has non-positive price", side)
		}
		if _, exists := seen[price]; exists {
			return fmt.Errorf("%s delta repeats price %d", side, price)
		}
		seen[price] = struct{}{}
	}
	return nil
}

func unpadded(levels [model.BookDepth]model.Level) ([]model.Level, error) {
	out := make([]model.Level, 0, model.BookDepth)
	padding := false
	for index, level := range levels {
		if level.PriceTick == 0 && level.QtyLot == 0 {
			padding = true
			continue
		}
		if padding {
			return nil, fmt.Errorf("non-zero level %d follows zero padding", index+1)
		}
		if level.PriceTick <= 0 || level.QtyLot == 0 {
			return nil, fmt.Errorf("level %d has invalid price/quantity", index+1)
		}
		out = append(out, level)
	}
	return out, nil
}

func (c *Client) insertDeltas(ctx context.Context, deltas []model.BookDelta) error {
	batch, err := c.conn.PrepareBatch(ctx, `INSERT INTO `+c.table("order_book_second_delta")+` (minute_id, second_offset, bid_change_prices, bid_change_qtys, ask_change_prices, ask_change_qtys)`)
	if err != nil {
		return err
	}
	for _, delta := range deltas {
		if err = batch.Append(delta.MinuteID, delta.SecondOffset, delta.BidChangePrice, delta.BidChangeQty, delta.AskChangePrice, delta.AskChangeQty); err != nil {
			return err
		}
	}
	return batch.Send()
}

func (c *Client) insertMinute(ctx context.Context, minute model.MinuteBook) error {
	columns := MinuteColumns()
	batch, err := c.conn.PrepareBatch(ctx, `INSERT INTO `+c.table("order_book_minute")+` (`+strings.Join(columns, ",")+`)`)
	if err != nil {
		return err
	}
	values := make([]any, 0, len(columns))
	values = append(values, minute.ID, minute.InstrumentID, minute.MinuteTime.UTC(), minute.ValidBitmap)
	for _, side := range [][model.BookDepth]model.Level{minute.Bids, minute.Asks} {
		for _, level := range side {
			values = append(values, level.PriceTick, level.QtyLot)
		}
	}
	if err = batch.Append(values...); err != nil {
		return err
	}
	return batch.Send()
}

func (c *Client) UpsertFundingRate(ctx context.Context, rate model.FundingRate) error {
	if err := rate.Validate(); err != nil {
		return err
	}
	var existingRate decimal.Decimal
	var existingActual bool
	var existingFundingTime time.Time
	query := `SELECT rate, is_actual, funding_time FROM ` + c.table("funding_rate_hourly") + ` FINAL WHERE instrument_id=? AND hour_time=? LIMIT 1`
	err := c.conn.QueryRow(ctx, query, rate.InstrumentID, rate.HourTime.UTC()).Scan(&existingRate, &existingActual, &existingFundingTime)
	if err == nil {
		if existingActual && !rate.IsActual {
			return nil
		}
		if existingActual == rate.IsActual && existingRate.Equal(rate.Rate) && existingFundingTime.UTC().Equal(rate.FundingTime.UTC()) {
			return nil
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("read existing funding rate: %w", err)
	}
	insert := `INSERT INTO ` + c.table("funding_rate_hourly") + ` (instrument_id,hour_time,funding_time,rate,is_actual)`
	return c.retryWrite(ctx, func(writeCtx context.Context) error {
		batch, prepareErr := c.conn.PrepareBatch(writeCtx, insert)
		if prepareErr != nil {
			return prepareErr
		}
		if appendErr := batch.Append(rate.InstrumentID, rate.HourTime.UTC(), rate.FundingTime.UTC(), rate.Rate, rate.IsActual); appendErr != nil {
			return appendErr
		}
		return batch.Send()
	})
}
