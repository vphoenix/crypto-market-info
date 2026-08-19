package clickhouse

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/replay"
)

var ErrNotFound = errors.New("market data not found")

func (c *Client) LoadMinute(ctx context.Context, instrumentID uint32, minuteTime time.Time) (model.MinuteBook, error) {
	var minute model.MinuteBook
	columns := MinuteColumns()
	query := `SELECT ` + strings.Join(columns, ",") + ` FROM ` + c.table("order_book_minute") + ` FINAL WHERE instrument_id=? AND minute_time=? LIMIT 1`
	dest := make([]any, 0, len(columns))
	dest = append(dest, &minute.ID, &minute.InstrumentID, &minute.MinuteTime, &minute.ValidBitmap)
	for index := 0; index < model.BookDepth; index++ {
		dest = append(dest, &minute.Bids[index].PriceTick, &minute.Bids[index].QtyLot)
	}
	for index := 0; index < model.BookDepth; index++ {
		dest = append(dest, &minute.Asks[index].PriceTick, &minute.Asks[index].QtyLot)
	}
	if err := c.conn.QueryRow(ctx, query, instrumentID, minuteTime.UTC().Truncate(time.Minute)).Scan(dest...); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return model.MinuteBook{}, ErrNotFound
		}
		return model.MinuteBook{}, err
	}
	return minute, nil
}

func (c *Client) LoadDeltas(ctx context.Context, minuteID uint64, through uint8) ([]model.BookDelta, error) {
	rows, err := c.conn.Query(ctx, `SELECT minute_id,second_offset,bid_change_prices,bid_change_qtys,ask_change_prices,ask_change_qtys FROM `+c.table("order_book_second_delta")+` FINAL WHERE minute_id=? AND second_offset<=? ORDER BY second_offset`, minuteID, through)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.BookDelta
	for rows.Next() {
		var delta model.BookDelta
		if err = rows.Scan(&delta.MinuteID, &delta.SecondOffset, &delta.BidChangePrice, &delta.BidChangeQty, &delta.AskChangePrice, &delta.AskChangeQty); err != nil {
			return nil, err
		}
		out = append(out, delta)
	}
	return out, rows.Err()
}

func (c *Client) ReplayBook(ctx context.Context, instrumentID uint32, at time.Time) (model.BookSnapshot, bool, error) {
	at = at.UTC().Truncate(time.Second)
	minute, err := c.LoadMinute(ctx, instrumentID, at.Truncate(time.Minute))
	if err != nil {
		return model.BookSnapshot{}, false, err
	}
	second := uint8(at.Second())
	if minute.ValidBitmap&(uint64(1)<<second) == 0 {
		return model.BookSnapshot{}, false, nil
	}
	deltas, err := c.LoadDeltas(ctx, minute.ID, second)
	if err != nil {
		return model.BookSnapshot{}, false, err
	}
	snapshot, valid, err := replay.AtSecond(minute, deltas, second)
	if err == nil && valid {
		snapshot.SourceTime = at
	}
	return snapshot, valid, err
}

func (c *Client) LoadPendingFundingConfirmations(ctx context.Context, since, through time.Time) ([]model.PendingFundingConfirmation, error) {
	if since.IsZero() || through.IsZero() || since.After(through) {
		return nil, fmt.Errorf("pending funding confirmation window is invalid")
	}
	since = time.UnixMilli(since.UTC().UnixMilli()).UTC()
	through = time.UnixMilli(through.UTC().UnixMilli()).UTC()
	query := `SELECT instrument_id, funding_time
FROM ` + c.table("funding_rate_hourly") + ` FINAL
WHERE funding_time >= fromUnixTimestamp64Milli(?)
  AND funding_time <= fromUnixTimestamp64Milli(?)
GROUP BY instrument_id, funding_time
HAVING max(toUInt8(is_actual)) = 0
ORDER BY funding_time, instrument_id`
	rows, err := c.conn.Query(ctx, query, since.UnixMilli(), through.UnixMilli())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.PendingFundingConfirmation
	for rows.Next() {
		var pending model.PendingFundingConfirmation
		if err = rows.Scan(&pending.InstrumentID, &pending.FundingTime); err != nil {
			return nil, err
		}
		pending.FundingTime = pending.FundingTime.UTC()
		if err = pending.Validate(); err != nil {
			return nil, err
		}
		out = append(out, pending)
	}
	return out, rows.Err()
}
