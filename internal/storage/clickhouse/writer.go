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
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
)

func (c *Client) WriteMinute(ctx context.Context, batch model.MinuteBatch) error {
	return c.WriteCompletedMinute(ctx, model.CompletedMinute{MinuteTime: batch.Minute.MinuteTime, Batches: []model.MinuteBatch{batch}})
}

const MinuteWriteBatchInstruments = 100

// WriteCompletedMinute validates the entire envelope before its first insert.
// Each chunk's deltas become durable before its minute visibility rows appear.
func (c *Client) WriteCompletedMinute(ctx context.Context, completed model.CompletedMinute) error {
	known := model.CeilMilliseconds(time.Now().UTC())
	if completed.MinuteTime.IsZero() || !completed.MinuteTime.Equal(completed.MinuteTime.UTC().Truncate(time.Minute)) {
		return fmt.Errorf("completed minute requires an exact UTC minute")
	}
	for index, batch := range completed.Batches {
		if !batch.Minute.MinuteTime.Equal(completed.MinuteTime) || (index > 0 && completed.Batches[index-1].Minute.InstrumentID >= batch.Minute.InstrumentID) {
			return fmt.Errorf("completed minute batches must share a minute and have sorted unique instrument IDs")
		}
		if err := validateBatch(batch); err != nil {
			return fmt.Errorf("instrument %d: %w", batch.Minute.InstrumentID, err)
		}
	}
	var publications []publicationRecord
	if c.discoveryEnabled.Load() {
		publications = make([]publicationRecord, len(completed.Batches))
		for n, batch := range completed.Batches {
			p, err := minutePublication(batch, known)
			if err != nil {
				return err
			}
			publications[n] = p
		}
		for offset := 0; offset < len(publications); offset += MinuteWriteBatchInstruments {
			if err := c.beginMinutePublications(ctx, publications[offset:min(offset+MinuteWriteBatchInstruments, len(publications))], completed.Batches[offset:min(offset+MinuteWriteBatchInstruments, len(publications))]); err != nil {
				return err
			}
		}
	}
	for offset := 0; offset < len(completed.Batches); offset += MinuteWriteBatchInstruments {
		chunk := completed.Batches[offset:min(offset+MinuteWriteBatchInstruments, len(completed.Batches))]
		var deltas []model.BookDelta
		minutes := make([]model.MinuteBook, 0, len(chunk))
		for _, item := range chunk {
			deltas = append(deltas, item.Deltas...)
			minutes = append(minutes, item.Minute)
		}
		if err := c.retryWrite(ctx, func(writeCtx context.Context) error {
			if len(deltas) > 0 {
				if err := c.insertDeltas(writeCtx, deltas); err != nil {
					return fmt.Errorf("write second deltas: %w", err)
				}
			}
			if err := c.insertMinutes(writeCtx, minutes); err != nil {
				return fmt.Errorf("write minute visibility rows: %w", err)
			}
			return nil
		}); err != nil {
			return fmt.Errorf("completed minute chunk %d: %w", offset/MinuteWriteBatchInstruments, err)
		}
		if len(publications) > 0 {
			if err := c.publishMinuteChunk(ctx, chunk, publications[offset:offset+len(chunk)]); err != nil {
				return fmt.Errorf("publish completed minute chunk: %w", err)
			}
		}
	}
	return nil
}

func validateBatch(batch model.MinuteBatch) error { return contracts.ValidateMinuteBatch(batch) }

func (c *Client) insertDeltas(ctx context.Context, deltas []model.BookDelta) error {
	batch, err := c.conn.PrepareBatch(ctx, `INSERT INTO `+c.table("order_book_second_delta")+` (minute_id, second_offset, bid_change_prices, bid_change_qtys, ask_change_prices, ask_change_qtys)`)
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, delta := range deltas {
		if err = batch.Append(delta.MinuteID, delta.SecondOffset, delta.BidChangePrice, delta.BidChangeQty, delta.AskChangePrice, delta.AskChangeQty); err != nil {
			return err
		}
	}
	return batch.Send()
}

func (c *Client) insertMinute(ctx context.Context, minute model.MinuteBook) error {
	return c.insertMinutes(ctx, []model.MinuteBook{minute})
}

func (c *Client) insertMinutes(ctx context.Context, minutes []model.MinuteBook) error {
	columns := MinuteColumns()
	batch, err := c.conn.PrepareBatch(ctx, `INSERT INTO `+c.table("order_book_minute")+` (`+strings.Join(columns, ",")+`)`)
	if err != nil {
		return err
	}
	defer batch.Abort()
	for _, minute := range minutes {
		values := make([]any, 0, len(columns))
		values = append(values, minute.ID, minute.InstrumentID, minute.MinuteTime.UTC(), minute.ValidBitmap, minute.StoredDepth)
		for _, side := range [][model.LegacyBookDepth]model.Level{minute.Bids, minute.Asks} {
			for _, level := range side {
				values = append(values, level.PriceTick, level.QtyLot)
			}
		}
		if err = batch.Append(values...); err != nil {
			return err
		}
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
