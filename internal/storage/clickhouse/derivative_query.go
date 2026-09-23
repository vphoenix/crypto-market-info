package clickhouse

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/options"
	"github.com/vphoenix/crypto-market-info/internal/replay"
)

// LoadDerivativeBookEnvelope verifies the WHOLE committed membership and
// content before returning any book. Orphan inserts are never query-visible.
func (c *Client) LoadDerivativeBookEnvelope(ctx context.Context, run uuid.UUID, minute time.Time) (options.BookEnvelope, error) {
	var e options.BookEnvelope
	if run == uuid.Nil || minute.IsZero() || !minute.Equal(minute.Truncate(time.Minute)) {
		return e, fmt.Errorf("exact run/minute required")
	}
	var batchID string
	var ids []uint32
	var hashes []string
	var anchorCount, deltaCount uint32
	err := c.conn.QueryRow(ctx, `SELECT `+derivativeCommitColumns+` FROM `+c.table("derivative_book_foundation_commit")+` FINAL WHERE run_id=? AND minute_time=?`, run, minute.UTC()).Scan(&e.RunID, &e.MinuteTime, &batchID, &e.Origin, &e.EvidenceHash, &e.PreparedAt, &ids, &hashes, &anchorCount, &deltaCount)
	if errors.Is(err, sql.ErrNoRows) {
		return e, ErrNotFound
	}
	if err != nil {
		return e, err
	}
	e, err = c.loadDerivativeBookRows(ctx, e, batchID, ids, hashes, anchorCount, deltaCount)
	if err != nil {
		return e, err
	}
	if err := e.Validate(); err != nil {
		return e, fmt.Errorf("%w: %v", replay.ErrIncompleteDerivativeBatch, err)
	}
	if e.ID() != batchID {
		return e, fmt.Errorf("%w: envelope digest mismatch", replay.ErrIncompleteDerivativeBatch)
	}
	return e, nil
}

// This helper reads common book rows, without publishing or interpreting an
// offline origin. Live callers supply the manifest from their own commit table.
func (c *Client) loadDerivativeBookRows(ctx context.Context, e options.BookEnvelope, batchID string, ids []uint32, hashes []string, anchorCount, deltaCount uint32) (options.BookEnvelope, error) {
	minute := e.MinuteTime
	var err error
	if len(ids) == 0 || len(ids) > 448 || len(ids) != len(hashes) || !model.ValidDigest(batchID) {
		return e, fmt.Errorf("%w: malformed manifest", replay.ErrIncompleteDerivativeBatch)
	}
	e.MinuteTime, e.PreparedAt = e.MinuteTime.UTC(), e.PreparedAt.UTC()
	e.Batches = make([]model.DerivativeMinuteBatch, len(ids))
	index := make(map[uint32]int, len(ids))
	minuteIDs := make([]uint64, len(ids))
	for i, id := range ids {
		if id == 0 || (i > 0 && id <= ids[i-1]) || !model.ValidDigest(hashes[i]) {
			return e, fmt.Errorf("invalid manifest membership")
		}
		index[id] = i
		minuteIDs[i], err = model.MinuteID(id, e.MinuteTime)
		if err != nil {
			return e, err
		}
		e.Batches[i] = model.DerivativeMinuteBatch{InstrumentID: id, MinuteTime: e.MinuteTime}
	}
	rows, err := c.conn.Query(ctx, `SELECT `+derivativeQualityColumns+` FROM `+c.table("derivative_book_quality_minute")+` FINAL WHERE instrument_id IN (?) AND minute_time=? AND batch_id=? ORDER BY instrument_id`, ids, minute.UTC(), batchID)
	if err != nil {
		return e, err
	}
	seen := map[uint32]bool{}
	for rows.Next() {
		var r derivativeQualityRow
		if err := rows.Scan(r.scanArgs()...); err != nil {
			rows.Close()
			return e, err
		}
		i, ok := index[r.id]
		if !ok || seen[r.id] || r.batch != batchID || r.hash != hashes[i] || !r.minute.Equal(e.MinuteTime) {
			rows.Close()
			return e, fmt.Errorf("%w: quality identity/hash mismatch", replay.ErrIncompleteDerivativeBatch)
		}
		seen[r.id] = true
		e.Batches[i].Signed = r.signed
		e.Batches[i].Quality, err = r.quality()
		if err != nil {
			rows.Close()
			return e, err
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return e, err
	}
	if len(seen) != len(ids) {
		return e, fmt.Errorf("%w: missing quality member", replay.ErrIncompleteDerivativeBatch)
	}
	rows, err = c.conn.Query(ctx, `SELECT `+derivativeMinuteColumns+` FROM `+c.table("derivative_book_minute")+` FINAL WHERE instrument_id IN (?) AND minute_time=? AND batch_id=? ORDER BY instrument_id`, ids, minute.UTC(), batchID)
	if err != nil {
		return e, err
	}
	var anchors uint32
	for rows.Next() {
		var m model.DerivativeMinute
		var actualBatch string
		var bp, ap []int64
		var bq, aq []uint64
		if err := rows.Scan(&m.ID, &m.InstrumentID, &m.MinuteTime, &actualBatch, &m.EncodingVersion, &m.StoredDepth, &m.Signed, &m.ValidBitmap, &m.DeltaBitmap, &bp, &bq, &ap, &aq); err != nil {
			rows.Close()
			return e, err
		}
		i, ok := index[m.InstrumentID]
		if !ok || e.Batches[i].Minute != nil || actualBatch != batchID || len(bp) != len(bq) || len(ap) != len(aq) {
			rows.Close()
			return e, fmt.Errorf("%w: malformed anchor", replay.ErrIncompleteDerivativeBatch)
		}
		m.MinuteTime = m.MinuteTime.UTC()
		m.Bids = derivativeZip(bp, bq)
		m.Asks = derivativeZip(ap, aq)
		e.Batches[i].Minute = &m
		anchors++
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return e, err
	}
	if anchors != anchorCount {
		return e, fmt.Errorf("%w: missing anchor", replay.ErrIncompleteDerivativeBatch)
	}
	rows, err = c.conn.Query(ctx, `SELECT `+derivativeDeltaColumns+` FROM `+c.table("derivative_book_second_delta")+` FINAL WHERE minute_id IN (?) AND batch_id=? ORDER BY minute_id,second_offset`, minuteIDs, batchID)
	if err != nil {
		return e, err
	}
	var deltas uint32
	for rows.Next() {
		var d model.BookDelta
		var actualBatch string
		if err := rows.Scan(&d.MinuteID, &d.SecondOffset, &actualBatch, &d.BidChangePrice, &d.BidChangeQty, &d.AskChangePrice, &d.AskChangeQty); err != nil {
			rows.Close()
			return e, err
		}
		i, ok := index[uint32(d.MinuteID)]
		if !ok || d.MinuteID != minuteIDs[i] || actualBatch != batchID {
			rows.Close()
			return e, fmt.Errorf("unexpected delta identity")
		}
		e.Batches[i].Deltas = append(e.Batches[i].Deltas, d)
		deltas++
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return e, err
	}
	if deltas != deltaCount {
		return e, fmt.Errorf("%w: missing delta", replay.ErrIncompleteDerivativeBatch)
	}
	if err := options.ValidateBookBatches(e.MinuteTime, e.Batches); err != nil {
		return e, fmt.Errorf("%w: %v", replay.ErrIncompleteDerivativeBatch, err)
	}
	for i, b := range e.Batches {
		if model.DerivativeBatchHash(b) != hashes[i] {
			return e, fmt.Errorf("%w: member content digest mismatch", replay.ErrIncompleteDerivativeBatch)
		}
	}
	if err := c.validateDerivativeReferences(ctx, e.Batches); err != nil {
		return e, fmt.Errorf("%w: %v", replay.ErrIncompleteDerivativeBatch, err)
	}
	return e, nil
}

func derivativeZip(p []int64, q []uint64) []model.Level {
	ls := make([]model.Level, len(p))
	for i := range p {
		ls[i] = model.Level{PriceTick: p[i], QtyLot: q[i]}
	}
	return ls
}
