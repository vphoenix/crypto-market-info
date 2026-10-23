package mysql

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	keeper "github.com/vphoenix/crypto-market-info/internal/justlendkeeper"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
)

func utcArgument(v any) (time.Time, error) {
	if t, ok := v.(time.Time); ok {
		return t.UTC(), nil
	}
	if s, ok := v.(string); ok {
		for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05.999999999", "2006-01-02 15:04:05"} {
			if t, e := time.ParseInLocation(layout, s, time.UTC); e == nil {
				return t.UTC(), nil
			}
		}
		return time.Time{}, fmt.Errorf("invalid UTC timestamp")
	}
	return time.Time{}, fmt.Errorf("UTC timestamp required")
}
func (c *Client) KeeperCaptures(ctx context.Context, from, to any) ([]keeper.Capture, error) {
	lo, e := utcArgument(from)
	if e != nil {
		return nil, e
	}
	hi, e := utcArgument(to)
	if e != nil {
		return nil, e
	}
	return readModels[keeper.Capture](ctx, c, c.db, "jl_keeper_capture", "WHERE capture_started_at>=? AND capture_started_at<? ORDER BY capture_started_at,capture_id", lo, hi)
}

// Each priority is already ordered by the existing parent index. Sorting the
// expression capture_mode IN (...) made every poll sort the complete backlog.
// Keep the indexed, materialized child group: a dependent scan of all children
// for every historical parent would amplify work on an exhausted backlog.
func (c *Client) keeperNextEnrichmentQuery(priority bool) string {
	mode := "p.capture_mode IN ('live','catchup')"
	if !priority {
		mode = "p.capture_mode NOT IN ('bootstrap','live','catchup')"
	}
	return "SELECT p.capture_id FROM " + c.table("jl_keeper_capture") + " p FORCE INDEX (cmi_keeper_enrichment_parent) LEFT JOIN (SELECT parent_capture_id FROM " + c.table("jl_keeper_capture") + " WHERE committed=1 AND capture_kind='enrichment' AND config_hash=? AND parent_capture_id IS NOT NULL GROUP BY parent_capture_id HAVING SUM(status='complete')>0 OR COUNT(*)>=3 OR MAX(available_at)>?) done ON done.parent_capture_id=p.capture_id WHERE p.committed=1 AND p.capture_kind='event_index' AND p.status='complete' AND p.config_hash=? AND " + mode + " AND done.parent_capture_id IS NULL ORDER BY p.requested_from,p.capture_started_at,p.capture_id LIMIT 1"
}

func (c *Client) KeeperNextEnrichment(ctx context.Context, config string, now time.Time) (result *keeper.Capture, err error) {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	defer func() { err = storageError(err) }()
	// Both priority checks and the full immutable capture read share one read
	// snapshot. A concurrent parent/child publication cannot change priority
	// between these queries. No generation or business write lock is acquired.
	tx, err := c.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	for _, priority := range []bool{true, false} {
		var raw []byte
		err = tx.QueryRowContext(ctx, c.keeperNextEnrichmentQuery(priority), []byte(config), now.UTC().Add(-30*time.Minute), []byte(config)).Scan(&raw)
		if err == sql.ErrNoRows {
			continue
		}
		if err != nil {
			return nil, err
		}
		id, err := uuid.FromBytes(raw)
		if err != nil {
			return nil, err
		}
		rows, err := readModels[keeper.Capture](ctx, c, tx, "jl_keeper_capture", "WHERE capture_id=?", id[:])
		if err != nil {
			return nil, err
		}
		if len(rows) != 1 {
			return nil, contracts.ErrConflict
		}
		return &rows[0], nil
	}
	return nil, nil
}
func (c *Client) KeeperEvidenceFrontier(ctx context.Context, config, kind string, from, to time.Time, chainComplete func([]keeper.Capture) (bool, error)) (result time.Time, err error) {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	defer func() { err = storageError(err) }()
	if from.IsZero() || !from.Before(to) {
		return from, nil
	}
	if chainComplete == nil {
		return from, fmt.Errorf("keeper frontier requires chain validator")
	}
	// Select one complete source window using only its boundaries. The former
	// query sorted every wide capture overlapping the cursor on every pass.
	var lo, hi time.Time
	query := "SELECT requested_from,requested_to FROM " + c.table("jl_keeper_capture") + " WHERE committed=1 AND capture_kind='event_index' AND status='complete' AND config_hash=? AND event_kind=? AND capture_mode IN ('live','catchup') AND requested_from<=? AND requested_to>? AND requested_to<=? ORDER BY requested_from,requested_to LIMIT 1"
	e := c.db.QueryRowContext(ctx, query, []byte(config), kind, from.UTC(), from.UTC(), to.UTC()).Scan(&lo, &hi)
	if e == sql.ErrNoRows {
		return from, nil
	}
	if e != nil {
		return from, e
	}
	// A page boundary is never a source-window boundary. Finish the keyset scan
	// before invoking the pagination-chain validator or advancing the cursor.
	var caps []keeper.Capture
	var lastTime time.Time
	var lastID uuid.UUID
	for {
		args := []any{[]byte(config), kind, lo, hi}
		where := "WHERE committed=1 AND capture_kind='event_index' AND status='complete' AND config_hash=? AND event_kind=? AND capture_mode IN ('live','catchup') AND requested_from=? AND requested_to=?"
		if len(caps) > 0 {
			where += " AND (capture_started_at,capture_id)>(?,?)"
			args = append(args, lastTime, lastID[:])
		}
		rs, e := c.db.QueryContext(ctx, "SELECT capture_id,capture_started_at FROM "+c.table("jl_keeper_capture")+" "+where+" ORDER BY capture_started_at,capture_id LIMIT 256", args...)
		if e != nil {
			return from, e
		}
		ids := []uuid.UUID{}
		for rs.Next() {
			var raw []byte
			if e = rs.Scan(&raw, &lastTime); e != nil {
				rs.Close()
				return from, e
			}
			lastID, e = uuid.FromBytes(raw)
			if e != nil {
				rs.Close()
				return from, e
			}
			ids = append(ids, lastID)
		}
		e = rs.Err()
		rs.Close()
		if e != nil {
			return from, e
		}
		if len(ids) == 0 {
			break
		}
		rows, e := c.KeeperParentCaptures(ctx, ids)
		if e != nil {
			return from, e
		}
		byID := map[uuid.UUID]keeper.Capture{}
		for _, cap := range rows {
			byID[cap.CaptureId] = cap
		}
		childArgs := []any{[]byte(config)}
		for _, id := range ids {
			childArgs = append(childArgs, id[:])
		}
		children, e := c.db.QueryContext(ctx, "SELECT DISTINCT parent_capture_id FROM "+c.table("jl_keeper_capture")+" WHERE committed=1 AND capture_kind='enrichment' AND status='complete' AND config_hash=? AND parent_capture_id IN ("+placeholders(len(ids))+")", childArgs...)
		if e != nil {
			return from, e
		}
		done := map[uuid.UUID]bool{}
		for children.Next() {
			var raw []byte
			if e = children.Scan(&raw); e != nil {
				children.Close()
				return from, e
			}
			id, e := uuid.FromBytes(raw)
			if e != nil {
				children.Close()
				return from, e
			}
			done[id] = true
		}
		e = children.Err()
		children.Close()
		if e != nil {
			return from, e
		}
		for _, id := range ids {
			cap, ok := byID[id]
			if !ok || cap.RequestedFrom == nil || cap.RequestedTo == nil || !cap.RequestedFrom.Equal(lo) || !cap.RequestedTo.Equal(hi) {
				return from, contracts.ErrConflict
			}
			if !done[id] {
				return from, nil
			}
			caps = append(caps, cap)
		}
		if len(ids) < 256 {
			break
		}
	}
	if len(caps) == 0 {
		return from, nil
	}
	complete, e := chainComplete(caps)
	if e != nil || !complete {
		return from, e
	}
	return hi, nil
}

func placeholders(n int) string { return strings.TrimSuffix(strings.Repeat("?,", n), ",") }
func (c *Client) keeperPages(ctx context.Context, q executor, caps []keeper.Capture) (map[uuid.UUID]keeper.IndexPage, error) {
	out := map[uuid.UUID]keeper.IndexPage{}
	for start := 0; start < len(caps); start += 256 {
		chunk := caps[start:min(start+256, len(caps))]
		args := []any{}
		for _, cap := range chunk {
			args = append(args, cap.CaptureId[:])
		}
		rows, e := readModels[keeper.IndexPage](ctx, c, q, "jl_keeper_index_page", "WHERE capture_id IN ("+placeholders(len(args))+")", args...)
		if e != nil {
			return nil, e
		}
		for _, row := range rows {
			if _, ok := out[row.CaptureId]; ok {
				return nil, contracts.ErrConflict
			}
			out[row.CaptureId] = row
		}
	}
	return out, nil
}
func (c *Client) KeeperIndexPages(ctx context.Context, caps []keeper.Capture) (map[uuid.UUID]keeper.IndexPage, error) {
	return c.keeperPages(ctx, c.db, caps)
}
func (c *Client) WriteKeeperIndexPages(ctx context.Context, pages []keeper.IndexPage) error {
	if len(pages) > 256 {
		return fmt.Errorf("keeper page batch exceeds quota")
	}
	if len(pages) == 0 {
		return nil
	}
	return c.transaction(ctx, func(tx *sql.Tx) error { return putModels(ctx, c, tx, "jl_keeper_index_page", pages) })
}
func (c *Client) KeeperParentCaptures(ctx context.Context, ids []uuid.UUID) ([]keeper.Capture, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	if len(ids) > 256 {
		return nil, fmt.Errorf("keeper parent batch exceeds quota")
	}
	args := []any{}
	for _, id := range ids {
		args = append(args, id[:])
	}
	return readModels[keeper.Capture](ctx, c, c.db, "jl_keeper_capture", "WHERE capture_id IN ("+placeholders(len(args))+")", args...)
}
func (c *Client) keeperBatch(ctx context.Context, q executor, cap keeper.Capture) (keeper.Batch, error) {
	b := keeper.Batch{Capture: cap}
	var e error
	where := "WHERE capture_id=?"
	id := cap.CaptureId[:]
	if b.Events, e = readModels[keeper.RentalEvent](ctx, c, q, "jl_keeper_rental_event", where, id); e != nil {
		return b, e
	}
	if b.Receipts, e = readModels[keeper.TxReceipt](ctx, c, q, "jl_keeper_tx_receipt", where, id); e != nil {
		return b, e
	}
	if b.Probes, e = readModels[keeper.Probe](ctx, c, q, "jl_keeper_probe", where, id); e != nil {
		return b, e
	}
	if b.Costs, e = readModels[keeper.CostObservation](ctx, c, q, "jl_keeper_cost_observation", where, id); e != nil {
		return b, e
	}
	if b.IndexedEvents, e = readModels[keeper.IndexedEvent](ctx, c, q, "jl_keeper_indexed_event", where+" ORDER BY row_ordinal", id); e != nil {
		return b, e
	}
	if cap.CaptureKind == "event_index" && cap.Status == "complete" {
		pages, e := c.keeperPages(ctx, q, []keeper.Capture{cap})
		if e != nil {
			return b, e
		}
		if p, ok := pages[cap.CaptureId]; ok {
			b.IndexPage = &p
		}
	}
	if err := c.readKeeperRentalState(ctx, q, &b); err != nil {
		return b, err
	}
	if err := c.readKeeperDynamic(ctx, q, &b); err != nil {
		return b, err
	}
	return b, keeper.Validate(b)
}
func (c *Client) KeeperBatch(ctx context.Context, cap keeper.Capture) (result keeper.Batch, err error) {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	defer func() { err = storageError(err) }()
	rows, e := readModels[keeper.Capture](ctx, c, c.db, "jl_keeper_capture", "WHERE capture_id=? AND committed=1", cap.CaptureId[:])
	if e != nil {
		return keeper.Batch{}, e
	}
	if len(rows) != 1 {
		return keeper.Batch{}, fmt.Errorf("keeper capture not committed")
	}
	a, e := keeper.Freeze(cap)
	if e != nil {
		return keeper.Batch{}, e
	}
	z, e := keeper.Freeze(rows[0])
	if e != nil {
		return keeper.Batch{}, e
	}
	if !bytes.Equal(a, z) {
		return keeper.Batch{}, contracts.ErrConflict
	}
	return c.keeperBatch(ctx, c.db, rows[0])
}
func (c *Client) keeperAnchors(ctx context.Context, q executor, b keeper.Batch) error {
	known := map[uint64]string{}
	for _, r := range b.Events {
		if h, ok := known[r.BlockNumber]; ok && h != r.BlockHash {
			return contracts.ErrConflict
		}
		known[r.BlockNumber] = r.BlockHash
	}
	for _, r := range b.Receipts {
		if h, ok := known[r.BlockNumber]; ok && h != r.BlockHash {
			return contracts.ErrConflict
		}
		known[r.BlockNumber] = r.BlockHash
	}
	for number, hash := range known {
		for _, table := range []string{"jl_keeper_rental_event", "jl_keeper_tx_receipt"} {
			rows, e := q.QueryContext(ctx, "SELECT block_hash FROM "+c.table(table)+" WHERE block_number=?", number)
			if e != nil {
				return e
			}
			for rows.Next() {
				var old []byte
				if e = rows.Scan(&old); e != nil {
					rows.Close()
					return e
				}
				if string(old) != hash {
					rows.Close()
					return contracts.ErrConflict
				}
			}
			e = rows.Err()
			rows.Close()
			if e != nil {
				return e
			}
		}
	}
	return nil
}
func (c *Client) WriteKeeperBatch(ctx context.Context, b keeper.Batch) error {
	if e := keeper.Validate(b); e != nil {
		return e
	}
	return c.transaction(ctx, func(tx *sql.Tx) error {
		if b.Capture.ParentCaptureId != nil {
			id := *b.Capture.ParentCaptureId
			parents, e := readModels[keeper.Capture](ctx, c, tx, "jl_keeper_capture", "WHERE capture_id=?", id[:])
			if e != nil {
				return e
			}
			if len(parents) != 1 {
				return fmt.Errorf("keeper enrichment parent missing")
			}
			if e = keeper.ValidateEnrichmentParent(b.Capture, parents[0]); e != nil {
				return e
			}
		}
		old, e := readModels[keeper.Capture](ctx, c, tx, "jl_keeper_capture", "WHERE capture_id=? FOR UPDATE", b.Capture.CaptureId[:])
		if e != nil {
			return e
		}
		if len(old) > 1 {
			return contracts.ErrConflict
		}
		if len(old) == 1 {
			a, e := keeper.Freeze(old[0])
			if e != nil {
				return e
			}
			z, e := keeper.Freeze(b.Capture)
			if e != nil {
				return e
			}
			if !bytes.Equal(a, z) {
				return contracts.ErrConflict
			}
			if b.IndexPage != nil {
				if e = putModels(ctx, c, tx, "jl_keeper_index_page", []keeper.IndexPage{*b.IndexPage}); e != nil {
					return e
				}
			}
			existing, err := c.keeperBatch(ctx, tx, old[0])
			if err != nil {
				return err
			}
			if !keeper.RentalStateProofEqual(existing.RentalStateProof, b.RentalStateProof) || !keeper.DynamicProofEqual(existing, b) {
				return contracts.ErrConflict
			}
			return nil
		}
		if e = c.keeperAnchors(ctx, tx, b); e != nil {
			return e
		}
		if b.IndexPage != nil {
			if e = putModels(ctx, c, tx, "jl_keeper_index_page", []keeper.IndexPage{*b.IndexPage}); e != nil {
				return e
			}
		}
		if e = putModels(ctx, c, tx, "jl_keeper_indexed_event", b.IndexedEvents); e != nil {
			return e
		}
		if e = putModels(ctx, c, tx, "jl_keeper_rental_event", b.Events); e != nil {
			return e
		}
		if e = putModels(ctx, c, tx, "jl_keeper_tx_receipt", b.Receipts); e != nil {
			return e
		}
		if e = putModels(ctx, c, tx, "jl_keeper_probe", b.Probes); e != nil {
			return e
		}
		if e = putModels(ctx, c, tx, "jl_keeper_cost_observation", b.Costs); e != nil {
			return e
		}
		if e = c.writeKeeperRentalState(ctx, tx, b); e != nil {
			return e
		}
		if e = c.writeKeeperDynamic(ctx, tx, b); e != nil {
			return e
		}
		if _, e = c.keeperBatch(ctx, tx, b.Capture); e != nil {
			return e
		}
		return putModels(ctx, c, tx, "jl_keeper_capture", []keeper.Capture{b.Capture})
	})
}

var _ keeper.Store = (*Client)(nil)
var _ keeper.ReportReader = (*Client)(nil)
var _ keeper.CollectionReader = (*Client)(nil)
var _ keeper.IndexPageReader = (*Client)(nil)
var _ keeper.IndexPageWriter = (*Client)(nil)

// KeeperCapturePage is only for explicit stopped-service conversion/rebuild.
// Keep the existing enrichment/frontier timeout fixes intact.
func (c *Client) KeeperCapturePage(ctx context.Context, config string, after []byte, limit int) ([]keeper.Capture, []byte, error) {
	if len(config) != 32 || len(after) != 0 && len(after) != 16 || limit < 1 || limit > 500 {
		return nil, nil, fmt.Errorf("invalid Keeper conversion page")
	}
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	where := "FORCE INDEX (PRIMARY) WHERE config_hash=? AND committed=1"
	args := []any{[]byte(config)}
	if len(after) > 0 {
		where += " AND capture_id>?"
		args = append(args, after)
	}
	where += fmt.Sprintf(" ORDER BY capture_id LIMIT %d", limit)
	rows, err := readModels[keeper.Capture](ctx, c, c.db, "jl_keeper_capture", where, args...)
	if err != nil || len(rows) == 0 {
		return rows, nil, err
	}
	id := rows[len(rows)-1].CaptureId
	return rows, append([]byte(nil), id[:]...), nil
}
