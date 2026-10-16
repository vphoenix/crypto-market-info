package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
	"math/rand"
	"time"
)

type PerpetualDataCheck = contracts.PerpetualDataCheck
type PerpetualSourceCheck = contracts.PerpetualSourceCheck
type PerpetualPartCheck = contracts.PerpetualPartCheck

func (c *Client) CheckPerpetualData(ctx context.Context, id uuid.UUID, now time.Time, recent time.Duration) (PerpetualDataCheck, error) {
	var out PerpetualDataCheck
	if now.IsZero() || recent <= 0 {
		return out, fmt.Errorf("positive recent window required")
	}
	r, members, e := c.PerpetualUniverse(ctx, id)
	if e != nil {
		return out, e
	}
	out.Run = r
	if now.Before(r.StartedAt) {
		return out, fmt.Errorf("check precedes run")
	}
	var superseded time.Time
	e = c.db.QueryRowContext(ctx, "SELECT started_at FROM "+c.table("perpetual_universe_run")+" WHERE run_id!=? AND started_at>=? AND started_at<=? ORDER BY started_at,run_id LIMIT 1", id[:], r.StartedAt, now.UTC()).Scan(&superseded)
	if e != nil && !errors.Is(e, sql.ErrNoRows) {
		return out, e
	}
	if e == nil {
		out.RunSupersededAt = &superseded
	}
	out.BookFrom = r.StartedAt.UTC().Truncate(time.Minute)
	if out.BookFrom.Before(r.StartedAt) {
		out.BookFrom = out.BookFrom.Add(time.Minute)
	}
	out.BookThroughExclusive = now.UTC().Truncate(time.Minute)
	out.FundingFrom = r.StartedAt.UTC().Truncate(time.Hour)
	out.FundingThroughExclusive = now.UTC().Truncate(time.Hour).Add(time.Hour)
	if out.RunSupersededAt != nil {
		out.BookThroughExclusive = superseded.Truncate(time.Minute)
		out.FundingThroughExclusive = superseded.Truncate(time.Hour)
	}
	out.RecentFrom = now.UTC().Add(-recent).Truncate(time.Minute)
	if out.RecentFrom.Before(out.BookFrom) {
		out.RecentFrom = out.BookFrom
	}
	items, e := c.Instruments(ctx)
	if e != nil {
		return out, e
	}
	byID := map[uint32]model.Instrument{}
	for _, i := range items {
		byID[i.ID] = i
	}
	indexes := map[uint32]int{}
	for _, m := range members {
		i, ok := byID[m.InstrumentID]
		if !ok {
			return out, fmt.Errorf("universe instrument missing")
		}
		indexes[i.ID] = len(out.Sources)
		out.Sources = append(out.Sources, PerpetualSourceCheck{InstrumentID: i.ID, Exchange: i.Exchange, Symbol: i.ExchangeSymbol, CanonicalMarketKey: m.CanonicalMarketKey})
	}
	sub := "SELECT instrument_id FROM " + c.table("perpetual_universe_member") + " WHERE run_id=?"
	rows, e := c.db.QueryContext(ctx, "SELECT instrument_id,COUNT(*),SUM(BIT_COUNT(valid_bitmap)),SUM(IF(minute_time>=?,BIT_COUNT(valid_bitmap),0)),SUM(IF((valid_bitmap & 1)=0 OR (valid_bitmap >> 60)!=0,1,0)),MIN(minute_time),MAX(minute_time) FROM "+c.table("order_book_minute")+" WHERE instrument_id IN ("+sub+") AND minute_time>=? AND minute_time<? GROUP BY instrument_id", out.RecentFrom, id[:], out.BookFrom, out.BookThroughExclusive)
	if e != nil {
		return out, e
	}
	for rows.Next() {
		var x uint32
		var count, valid, recentValid, invalid uint64
		var first, last time.Time
		if e = rows.Scan(&x, &count, &valid, &recentValid, &invalid, &first, &last); e != nil {
			rows.Close()
			return out, e
		}
		s := &out.Sources[indexes[x]]
		s.MinuteRows = count
		s.ValidSeconds = valid
		s.RecentValidSeconds = recentValid
		s.InvalidMinuteRows = invalid
		s.FirstMinute = &first
		s.LatestMinute = &last
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return out, e
	}
	rows, e = c.db.QueryContext(ctx, "SELECT (minute_id & 4294967295),SUM((OCTET_LENGTH(bid_change_prices)-21) DIV 8+(OCTET_LENGTH(ask_change_prices)-21) DIV 8) FROM "+c.table("order_book_second_delta")+" WHERE minute_id IN (SELECT id FROM "+c.table("order_book_minute")+" WHERE instrument_id IN ("+sub+") AND minute_time>=? AND minute_time<?) GROUP BY (minute_id & 4294967295)", id[:], out.BookFrom, out.BookThroughExclusive)
	if e != nil {
		return out, e
	}
	for rows.Next() {
		var x uint32
		var changes uint64
		if e = rows.Scan(&x, &changes); e != nil {
			rows.Close()
			return out, e
		}
		out.Sources[indexes[x]].ChangedPriceLevels = changes
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return out, e
	}
	rows, e = c.db.QueryContext(ctx, "SELECT instrument_id,COUNT(*),SUM(is_actual),MAX(hour_time) FROM "+c.table("funding_rate_hourly")+" WHERE instrument_id IN ("+sub+") AND hour_time>=? AND hour_time<? GROUP BY instrument_id", id[:], out.FundingFrom, out.FundingThroughExclusive)
	if e != nil {
		return out, e
	}
	for rows.Next() {
		var x uint32
		var count, actual uint64
		var last time.Time
		if e = rows.Scan(&x, &count, &actual, &last); e != nil {
			rows.Close()
			return out, e
		}
		s := &out.Sources[indexes[x]]
		s.FundingRows = count
		s.ActualFundingRows = actual
		s.EstimatedFundingRows = count - actual
		s.LatestFundingHour = &last
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return out, e
	}
	// InnoDB allocation/row statistics are estimates, never compression measurements.
	rows, e = c.db.QueryContext(ctx, "SELECT table_name,COALESCE(table_rows,0),COALESCE(data_length,0)+COALESCE(index_length,0) FROM information_schema.tables WHERE table_schema=? ORDER BY table_name", c.database)
	if e != nil {
		return out, e
	}
	for rows.Next() {
		var p PerpetualPartCheck
		p.Approximate = true
		if e = rows.Scan(&p.Table, &p.PhysicalRows, &p.BytesOnDisk); e != nil {
			rows.Close()
			return out, e
		}
		out.DatabaseParts = append(out.DatabaseParts, p)
	}
	e = rows.Err()
	rows.Close()
	return out, e
}
func (c *Client) PerpetualReplaySeconds(ctx context.Context, instrumentID uint32, from, through time.Time, count int, seed int64) ([]time.Time, error) {
	if instrumentID == 0 || count <= 0 || count > 1000 || from.IsZero() || through.Before(from) || !from.Equal(from.Truncate(time.Minute)) || !through.Equal(through.Truncate(time.Minute)) {
		return nil, fmt.Errorf("invalid replay window/count")
	}
	rows, e := c.db.QueryContext(ctx, "SELECT minute_time,valid_bitmap FROM "+c.table("order_book_minute")+" WHERE instrument_id=? AND minute_time>=? AND minute_time<? ORDER BY minute_time", instrumentID, from.UTC(), through.UTC())
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	rng := rand.New(rand.NewSource(seed))
	out := make([]time.Time, 0, count)
	seen := int64(0)
	for rows.Next() {
		var minute time.Time
		var bitmap uint64
		if e = rows.Scan(&minute, &bitmap); e != nil {
			return nil, e
		}
		if bitmap>>60 != 0 || bitmap&1 == 0 {
			return nil, fmt.Errorf("invalid replay bitmap")
		}
		for sec := 0; sec < 60; sec++ {
			if bitmap&(uint64(1)<<sec) == 0 {
				continue
			}
			seen++
			at := minute.UTC().Add(time.Duration(sec) * time.Second)
			if len(out) < count {
				out = append(out, at)
			} else if n := rng.Int63n(seen); n < int64(count) {
				out[n] = at
			}
		}
	}
	return out, rows.Err()
}
