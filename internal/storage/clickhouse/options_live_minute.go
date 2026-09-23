package clickhouse

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/options"
	"github.com/vphoenix/crypto-market-info/internal/replay"
)

const liveCommitColumns = `run_id,minute_time,batch_id,run_hash,prepared_at,instrument_ids,member_hashes,anchor_count,delta_count,index_ids,index_hashes`
const liveIndexColumns = `index_id,minute_time,batch_id,row_hash,prices,source_times,received_times,epochs,states`

func (c *Client) WriteOptionsMinute(ctx context.Context, input options.LiveEnvelope) error {
	if err := input.Validate(); err != nil {
		return err
	}
	e := input.Clone()
	id := e.ID()
	c.derivativeMu.Lock()
	defer c.derivativeMu.Unlock()
	r, err := c.LoadOptionsRun(ctx, e.RunID)
	if err != nil {
		return err
	}
	if err = e.ValidateRun(r); err != nil {
		return err
	}
	var old string
	err = c.conn.QueryRow(ctx, `SELECT batch_id FROM `+c.table("options_live_minute_commit")+` FINAL WHERE run_id=? AND minute_time=?`, e.RunID, e.MinuteTime).Scan(&old)
	if err == nil {
		if old != id {
			return fmt.Errorf("immutable live minute conflict")
		}
		_, err = c.LoadOptionsMinute(ctx, e.RunID, e.MinuteTime)
		return err
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if err = c.validateDerivativeReferences(ctx, e.Books); err != nil {
		return err
	}
	if err = c.validateLiveMetadata(ctx, r, e); err != nil {
		return err
	}
	return c.retryWrite(ctx, func(ctx context.Context) error {
		if err := c.insertDerivativeDeltas(ctx, id, e.Books); err != nil {
			return err
		}
		if err := c.derivativeInserted("live_delta"); err != nil {
			return err
		}
		if err := c.insertDerivativeMinutes(ctx, id, e.Books); err != nil {
			return err
		}
		if err := c.insertDerivativeQuality(ctx, id, e.Books); err != nil {
			return err
		}
		if err := c.derivativeInserted("live_book"); err != nil {
			return err
		}
		b, err := c.conn.PrepareBatch(ctx, `INSERT INTO `+c.table("options_index_minute")+` (`+liveIndexColumns+`)`)
		if err != nil {
			return err
		}
		defer b.Abort()
		var indexIDs, indexHashes []string
		for _, m := range e.Indexes {
			prices := make([]*decimal.Decimal, 60)
			sources, received := make([]*time.Time, 60), make([]*time.Time, 60)
			epochs := make([]uuid.UUID, 60)
			states := make([]uint8, 60)
			for n, s := range m.Samples {
				prices[n] = s.Price
				if !s.SourceTime.IsZero() {
					v := s.SourceTime
					sources[n] = &v
				}
				if !s.ReceivedAt.IsZero() {
					v := s.ReceivedAt
					received[n] = &v
				}
				epochs[n] = s.Epoch
				states[n] = uint8(s.State)
			}
			if err = b.Append(m.IndexID, e.MinuteTime, id, m.Hash(), prices, sources, received, epochs, states); err != nil {
				return err
			}
			indexIDs = append(indexIDs, m.IndexID)
			indexHashes = append(indexHashes, m.Hash())
		}
		if err = b.Send(); err != nil {
			return err
		}
		if err = c.derivativeInserted("live_index"); err != nil {
			return err
		}
		ids, hashes, anchors, deltas := derivativeManifest(options.BookEnvelope{Batches: e.Books})
		if err = c.insertDerivativeRow(ctx, "options_live_minute_commit", liveCommitColumns, e.RunID, e.MinuteTime, id, e.RunHash, e.PreparedAt, ids, hashes, anchors, deltas, indexIDs, indexHashes); err != nil {
			return err
		}
		return c.derivativeInserted("live_commit")
	})
}

func (c *Client) LoadOptionsMinute(ctx context.Context, run uuid.UUID, minute time.Time) (options.LiveEnvelope, error) {
	var e options.LiveEnvelope
	if run == uuid.Nil || minute.IsZero() || !minute.Equal(minute.Truncate(time.Minute)) {
		return e, fmt.Errorf("exact run/minute required")
	}
	var id string
	var ids []uint32
	var hashes, indexIDs, indexHashes []string
	var anchors, deltas uint32
	err := c.conn.QueryRow(ctx, `SELECT `+liveCommitColumns+` FROM `+c.table("options_live_minute_commit")+` FINAL WHERE run_id=? AND minute_time=?`, run, minute.UTC()).Scan(&e.RunID, &e.MinuteTime, &id, &e.RunHash, &e.PreparedAt, &ids, &hashes, &anchors, &deltas, &indexIDs, &indexHashes)
	if errors.Is(err, sql.ErrNoRows) {
		return e, ErrNotFound
	}
	if err != nil {
		return e, err
	}
	e.MinuteTime = e.MinuteTime.UTC()
	e.PreparedAt = e.PreparedAt.UTC()
	r, err := c.LoadOptionsRun(ctx, run)
	if err != nil {
		return e, fmt.Errorf("%w: %v", replay.ErrIncompleteDerivativeBatch, err)
	}
	books, err := c.loadDerivativeBookRows(ctx, options.BookEnvelope{MinuteTime: e.MinuteTime}, id, ids, hashes, anchors, deltas)
	if err != nil {
		return e, err
	}
	e.Books = books.Batches
	if len(indexIDs) != len(r.Indexes) || len(indexHashes) != len(indexIDs) {
		return e, fmt.Errorf("%w: index manifest", replay.ErrIncompleteDerivativeBatch)
	}
	rows, err := c.conn.Query(ctx, `SELECT `+liveIndexColumns+` FROM `+c.table("options_index_minute")+` FINAL WHERE minute_time=? AND batch_id=? ORDER BY index_id`, minute.UTC(), id)
	if err != nil {
		return e, err
	}
	defer rows.Close()
	for rows.Next() {
		var m options.IndexMinute
		var actualMinute time.Time
		var batch, hash string
		var prices []*decimal.Decimal
		var sources, received []*time.Time
		var epochs []uuid.UUID
		var states []uint8
		if err = rows.Scan(&m.IndexID, &actualMinute, &batch, &hash, &prices, &sources, &received, &epochs, &states); err != nil {
			return e, err
		}
		n := len(e.Indexes)
		if n >= len(indexIDs) || indexIDs[n] != m.IndexID || indexHashes[n] != hash || batch != id || !actualMinute.Equal(e.MinuteTime) || len(prices) != 60 || len(sources) != 60 || len(received) != 60 || len(epochs) != 60 || len(states) != 60 {
			return e, fmt.Errorf("%w: index row", replay.ErrIncompleteDerivativeBatch)
		}
		for sec := 0; sec < 60; sec++ {
			s := &m.Samples[sec]
			s.Price = prices[sec]
			s.Epoch = epochs[sec]
			s.State = options.IndexState(states[sec])
			if sources[sec] != nil {
				s.SourceTime = sources[sec].UTC()
			}
			if received[sec] != nil {
				s.ReceivedAt = received[sec].UTC()
			}
		}
		if m.Hash() != hash {
			return e, fmt.Errorf("%w: index hash", replay.ErrIncompleteDerivativeBatch)
		}
		e.Indexes = append(e.Indexes, m)
	}
	if err = rows.Err(); err != nil {
		return e, err
	}
	if err = e.ValidateRun(r); err != nil {
		return e, fmt.Errorf("%w: %v", replay.ErrIncompleteDerivativeBatch, err)
	}
	if e.ID() != id {
		return e, fmt.Errorf("%w: live digest", replay.ErrIncompleteDerivativeBatch)
	}
	if err = c.validateLiveMetadata(ctx, r, e); err != nil {
		return e, fmt.Errorf("%w: %v", replay.ErrIncompleteDerivativeBatch, err)
	}
	return e, nil
}

func (c *Client) LatestOptionsMinute(ctx context.Context, run uuid.UUID) (time.Time, error) {
	var at time.Time
	err := c.conn.QueryRow(ctx, `SELECT minute_time FROM `+c.table("options_live_minute_commit")+` FINAL WHERE run_id=? ORDER BY minute_time DESC LIMIT 1`, run).Scan(&at)
	if errors.Is(err, sql.ErrNoRows) {
		return at, ErrNotFound
	}
	return at.UTC(), err
}
