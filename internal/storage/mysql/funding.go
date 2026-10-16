package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
)

func (c *Client) UpsertFundingRate(ctx context.Context, rate model.FundingRate) error {
	if e := rate.Validate(); e != nil {
		return e
	}
	t, e := c.mapping("funding_rate_hourly")
	if e != nil {
		return e
	}
	r := record{"instrument_id": rate.InstrumentID, "hour_time": rate.HourTime.UTC(), "funding_time": rate.FundingTime.UTC(), "rate": rate.Rate, "is_actual": rate.IsActual}
	if _, e = rowHash(t, r); e != nil {
		return e
	}
	return c.transaction(ctx, func(tx *sql.Tx) error {
		rows, e := readRecords(ctx, tx, c.table(t.Name), t, "WHERE instrument_id=? AND hour_time=? FOR UPDATE", rate.InstrumentID, rate.HourTime.UTC())
		if e != nil {
			return e
		}
		if len(rows) > 1 {
			return contracts.ErrConflict
		}
		if len(rows) == 1 {
			if rows[0]["is_actual"].(bool) && !rate.IsActual {
				return nil
			}
			a, e := rowHash(t, rows[0])
			if e != nil {
				return e
			}
			b, e := rowHash(t, r)
			if e != nil {
				return e
			}
			if a == b {
				return nil
			}
			_, e = tx.ExecContext(ctx, "DELETE FROM "+c.table(t.Name)+" WHERE instrument_id=? AND hour_time=?", rate.InstrumentID, rate.HourTime.UTC())
			if e != nil {
				return e
			}
		}
		return insertRecord(ctx, tx, c.table(t.Name), t, r)
	})
}
func (c *Client) LoadPendingFundingConfirmations(ctx context.Context, from, through time.Time) ([]model.PendingFundingConfirmation, error) {
	if from.IsZero() || through.IsZero() || from.After(through) {
		return nil, fmt.Errorf("invalid funding confirmation window")
	}
	rows, e := c.db.QueryContext(ctx, "SELECT instrument_id,funding_time FROM "+c.table("funding_rate_hourly")+" WHERE funding_time>=? AND funding_time<=? GROUP BY instrument_id,funding_time HAVING MAX(is_actual)=0 ORDER BY funding_time,instrument_id", from.UTC().Truncate(time.Millisecond), through.UTC().Truncate(time.Millisecond))
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []model.PendingFundingConfirmation{}
	for rows.Next() {
		var r model.PendingFundingConfirmation
		if e = rows.Scan(&r.InstrumentID, &r.FundingTime); e != nil {
			return nil, e
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

var _ contracts.FundingStore = (*Client)(nil)
