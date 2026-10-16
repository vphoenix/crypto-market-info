package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/across"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
)

// AcrossCapture uses the physical primary key derived from the frozen scope,
// chain, kind and capture ID. It is used for unknown/stale revision recovery.
func (c *Client) AcrossCapture(ctx context.Context, want across.Capture) (across.Capture, error) {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	t, err := c.mapping("across_capture")
	if err != nil {
		return across.Capture{}, err
	}
	if !t.HashedKey {
		return across.Capture{}, fmt.Errorf("Across recovery expects reviewed hashed key")
	}
	r, err := modelRecord(want)
	if err != nil {
		return across.Capture{}, err
	}
	key, err := keyHash(t, r)
	if err != nil {
		return across.Capture{}, err
	}
	rows, err := readModels[across.Capture](ctx, c, c.db, "across_capture", "WHERE _key_hash=?", key[:])
	if err != nil {
		return across.Capture{}, err
	}
	if len(rows) == 0 {
		return across.Capture{}, sql.ErrNoRows
	}
	if len(rows) != 1 || rows[0].CaptureId != want.CaptureId || rows[0].ManifestHash != want.ManifestHash || rows[0].ChainId != want.ChainId || rows[0].CaptureKind != want.CaptureKind {
		return across.Capture{}, contracts.ErrConflict
	}
	return rows[0], nil
}

// AcrossCapturePage is only for explicit frozen-input conversion. Its opaque
// key cursor can be saved after each page; normal runtime never calls it.
func (c *Client) AcrossCapturePage(ctx context.Context, manifest string, after []byte, limit int) ([]across.Capture, []byte, error) {
	if len(manifest) != 32 || (len(after) != 0 && len(after) != 32) || limit < 1 || limit > 500 {
		return nil, nil, fmt.Errorf("invalid Across conversion page")
	}
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	where := "FORCE INDEX (PRIMARY) WHERE manifest_hash=?"
	args := []any{[]byte(manifest)}
	if len(after) > 0 {
		where += " AND _key_hash>?"
		args = append(args, after)
	}
	where += fmt.Sprintf(" ORDER BY _key_hash LIMIT %d", limit)
	rows, err := readModels[across.Capture](ctx, c, c.db, "across_capture", where, args...)
	if err != nil || len(rows) == 0 {
		return rows, nil, err
	}
	t, err := c.mapping("across_capture")
	if err != nil {
		return nil, nil, err
	}
	if !t.HashedKey {
		return nil, nil, fmt.Errorf("Across conversion expects reviewed hashed key")
	}
	r, err := modelRecord(rows[len(rows)-1])
	if err != nil {
		return nil, nil, err
	}
	key, err := keyHash(t, r)
	if err != nil {
		return nil, nil, err
	}
	return rows, key[:], nil
}

func (c *Client) AcrossCaptures(ctx context.Context, manifest string) ([]across.Capture, error) {
	rows, e := readCaptureModels[across.Capture](ctx, c, "across_capture", "manifest_hash=?", []byte(manifest))
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].StartedAt.Equal(rows[j].StartedAt) {
			return rows[i].CaptureId < rows[j].CaptureId
		}
		return rows[i].StartedAt.Before(rows[j].StartedAt)
	})
	return rows, e
}
func (c *Client) acrossBatch(ctx context.Context, q executor, cap across.Capture) (across.Batch, error) {
	b := across.Batch{Capture: cap}
	var e error
	where := "WHERE capture_id=?"
	id := []byte(cap.CaptureId)
	if b.Deposits, e = readModels[across.Deposit](ctx, c, q, "across_deposit", where, id); e != nil {
		return b, e
	}
	if b.Updates, e = readModels[across.DepositUpdate](ctx, c, q, "across_deposit_update", where, id); e != nil {
		return b, e
	}
	if b.Fills, e = readModels[across.Fill](ctx, c, q, "across_fill", where, id); e != nil {
		return b, e
	}
	if b.Refunds, e = readModels[across.Refund](ctx, c, q, "across_refund", where, id); e != nil {
		return b, e
	}
	if b.Receipts, e = readModels[across.TxReceipt](ctx, c, q, "across_tx_receipt", where, id); e != nil {
		return b, e
	}
	if b.Probes, e = readModels[across.OrderProbe](ctx, c, q, "across_order_probe", where, id); e != nil {
		return b, e
	}
	if len(cap.TableIds) == 7 {
		if b.Transfers, e = readModels[across.ReceiptTransfers](ctx, c, q, "across_receipt_transfers", where, id); e != nil {
			return b, e
		}
	}
	return b, across.Validate(b)
}
func (c *Client) AcrossBatch(ctx context.Context, cap across.Capture) (across.Batch, error) {
	rows, e := readModels[across.Capture](ctx, c, c.db, "across_capture", "WHERE capture_id=?", []byte(cap.CaptureId))
	if e != nil {
		return across.Batch{}, e
	}
	if len(rows) != 1 || !rows[0].Committed {
		return across.Batch{}, fmt.Errorf("Across capture not committed")
	}
	if acrossMembersID(rows[0]) != acrossMembersID(cap) {
		return across.Batch{}, contracts.ErrConflict
	}
	return c.acrossBatch(ctx, c.db, rows[0])
}
func acrossMembersID(cap across.Capture) string {
	cap.Canonical = false
	cap.Finality = ""
	cap.Reason = ""
	cap.Revision = 0
	return across.ID(cap)
}
func (c *Client) WriteAcrossBatch(ctx context.Context, b across.Batch) error {
	if e := across.Validate(b); e != nil {
		return e
	}
	if !b.Capture.Committed {
		return fmt.Errorf("Across batch must commit complete envelope")
	}
	return c.transaction(ctx, func(tx *sql.Tx) error {
		rows, e := readModels[across.Capture](ctx, c, tx, "across_capture", "WHERE capture_id=? FOR UPDATE", []byte(b.Capture.CaptureId))
		if e != nil {
			return e
		}
		if len(rows) > 1 {
			return contracts.ErrConflict
		}
		if len(rows) == 1 {
			if acrossMembersID(rows[0]) != acrossMembersID(b.Capture) || (rows[0].Revision == b.Capture.Revision && across.ID(rows[0]) != across.ID(b.Capture)) {
				return contracts.ErrConflict
			}
			_, e = c.acrossBatch(ctx, tx, rows[0])
			return e
		}
		if e = putModels(ctx, c, tx, "across_deposit", b.Deposits); e != nil {
			return e
		}
		if e = putModels(ctx, c, tx, "across_deposit_update", b.Updates); e != nil {
			return e
		}
		if e = putModels(ctx, c, tx, "across_fill", b.Fills); e != nil {
			return e
		}
		if e = putModels(ctx, c, tx, "across_refund", b.Refunds); e != nil {
			return e
		}
		if e = putModels(ctx, c, tx, "across_tx_receipt", b.Receipts); e != nil {
			return e
		}
		if e = putModels(ctx, c, tx, "across_order_probe", b.Probes); e != nil {
			return e
		}
		if e = putModels(ctx, c, tx, "across_receipt_transfers", b.Transfers); e != nil {
			return e
		}
		if _, e = c.acrossBatch(ctx, tx, b.Capture); e != nil {
			return e
		}
		return putModels(ctx, c, tx, "across_capture", []across.Capture{b.Capture})
	})
}
func (c *Client) WriteAcrossRevision(ctx context.Context, next across.Capture) error {
	return c.transaction(ctx, func(tx *sql.Tx) error {
		rows, e := readModels[across.Capture](ctx, c, tx, "across_capture", "WHERE capture_id=? FOR UPDATE", []byte(next.CaptureId))
		if e != nil {
			return e
		}
		if len(rows) != 1 {
			return fmt.Errorf("Across revision missing capture")
		}
		old := rows[0]
		if acrossMembersID(old) != acrossMembersID(next) {
			return contracts.ErrConflict
		}
		if next.Revision < old.Revision {
			return nil
		}
		if next.Revision == old.Revision {
			if across.ID(old) == across.ID(next) {
				return nil
			}
			return contracts.ErrConflict
		}
		if old.Finality == "finalized" && (next.Finality != "finalized" || next.Canonical != old.Canonical) {
			return contracts.ErrConflict
		}
		if _, e = c.acrossBatch(ctx, tx, next); e != nil {
			return e
		}
		return replaceModel(ctx, c, tx, "across_capture", next)
	})
}

var _ across.Store = (*Client)(nil)
