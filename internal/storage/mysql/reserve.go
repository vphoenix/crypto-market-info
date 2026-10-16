package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/dex"
	"github.com/vphoenix/crypto-market-info/internal/reserve"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
)

// ReserveCapture verifies the complete four-column physical identity. Recovery
// does not broaden a lookup to other manifests or attempts with the same ID.
func (c *Client) ReserveCapture(ctx context.Context, want reserve.Capture) (reserve.Capture, error) {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	rows, err := readModels[reserve.Capture](ctx, c, c.db, "reserve_capture", "WHERE chain_id=? AND manifest_hash=? AND capture_id=? AND batch_id=?", want.ChainId, []byte(want.ManifestHash), []byte(want.CaptureId), []byte(want.BatchId))
	if err != nil {
		return reserve.Capture{}, err
	}
	if len(rows) == 0 {
		return reserve.Capture{}, sql.ErrNoRows
	}
	if len(rows) != 1 || rows[0].ChainId != want.ChainId || rows[0].ManifestHash != want.ManifestHash || rows[0].CaptureId != want.CaptureId || rows[0].BatchId != want.BatchId {
		return reserve.Capture{}, contracts.ErrConflict
	}
	return rows[0], nil
}

// ReserveCapturePage is used only by explicit frozen-input conversion. The
// opaque cursor preserves both binary key components, including equal IDs.
func (c *Client) ReserveCapturePage(ctx context.Context, manifest string, after []byte, limit int) ([]reserve.Capture, []byte, error) {
	if len(manifest) != 32 || (len(after) != 0 && len(after) != 64) || limit < 1 || limit > 500 {
		return nil, nil, fmt.Errorf("invalid Reserve conversion page")
	}
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	where := "FORCE INDEX (PRIMARY) WHERE chain_id=1 AND manifest_hash=?"
	args := []any{[]byte(manifest)}
	if len(after) > 0 {
		where += " AND (capture_id>? OR (capture_id=? AND batch_id>?))"
		args = append(args, after[:32], after[:32], after[32:])
	}
	where += fmt.Sprintf(" ORDER BY capture_id,batch_id LIMIT %d", limit)
	rows, err := readModels[reserve.Capture](ctx, c, c.db, "reserve_capture", where, args...)
	if err != nil || len(rows) == 0 {
		return rows, nil, err
	}
	last := rows[len(rows)-1]
	next := append(append([]byte(nil), []byte(last.CaptureId)...), []byte(last.BatchId)...)
	return rows, next, nil
}

func (c *Client) ReserveCaptures(ctx context.Context, manifest string) ([]reserve.Capture, error) {
	rows, e := readCaptureModels[reserve.Capture](ctx, c, "reserve_capture", "chain_id=1 AND manifest_hash=?", []byte(manifest))
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].ToBlock == rows[j].ToBlock {
			return rows[i].BatchId < rows[j].BatchId
		}
		return rows[i].ToBlock < rows[j].ToBlock
	})
	return rows, e
}
func (c *Client) ReserveReceipt(ctx context.Context, a dex.Anchor, tx dex.Hash) (dex.Receipt, bool, error) {
	rows, e := c.dexReceipts(ctx, c.db, "WHERE chain_id=1 AND block_hash=? AND tx_hash=?", a.Hash[:], tx[:])
	if e != nil {
		return dex.Receipt{}, false, e
	}
	if len(rows) > 1 {
		return dex.Receipt{}, false, contracts.ErrConflict
	}
	if len(rows) == 0 {
		return dex.Receipt{}, false, nil
	}
	return rows[0], true, nil
}
func (c *Client) reserveBatch(ctx context.Context, q executor, cap reserve.Capture) (reserve.Batch, error) {
	b := reserve.Batch{Capture: cap}
	var e error
	where := "WHERE chain_id=1 AND manifest_hash=? AND capture_id=? AND batch_id=?"
	args := []any{[]byte(cap.ManifestHash), []byte(cap.CaptureId), []byte(cap.BatchId)}
	if b.States, e = readModels[reserve.State](ctx, c, q, "reserve_folio_state", where, args...); e != nil {
		return b, e
	}
	if b.Quotes, e = readModels[reserve.Quote](ctx, c, q, "reserve_route_quote", where, args...); e != nil {
		return b, e
	}
	if b.Logs, e = c.dexLogs(ctx, q, "WHERE chain_id=1 AND manifest_hash=? AND batch_id=? ORDER BY block_number,log_index", []byte(cap.ManifestHash), []byte(cap.BatchId)); e != nil {
		return b, e
	}
	for _, ref := range cap.ReceiptRefs {
		rs, e := c.dexReceipts(ctx, q, "WHERE chain_id=1 AND block_hash=? AND tx_hash=?", []byte(ref.BlockHash), []byte(ref.TxHash))
		if e != nil {
			return b, e
		}
		if len(rs) != 1 || rawHash(rs[0].ReceiptHash) != ref.ReceiptHash || rawHash(rs[0].CalldataHash) != ref.CalldataHash {
			return b, fmt.Errorf("Reserve receipt reference mismatch")
		}
		data, e := readModels[reserve.ReceiptData](ctx, c, q, "reserve_receipt_data", "WHERE chain_id=1 AND block_hash=? AND tx_hash=?", []byte(ref.BlockHash), []byte(ref.TxHash))
		if e != nil {
			return b, e
		}
		if len(data) != 1 {
			return b, fmt.Errorf("Reserve receipt data missing")
		}
		if e = reserve.ValidateReceiptData(data[0], rs[0]); e != nil {
			return b, e
		}
		b.Receipts = append(b.Receipts, rs[0])
		b.ReceiptData = append(b.ReceiptData, data[0])
	}
	return b, reserve.Validate(b)
}
func (c *Client) ReserveBatch(ctx context.Context, cap reserve.Capture) (reserve.Batch, error) {
	rows, e := readModels[reserve.Capture](ctx, c, c.db, "reserve_capture", "WHERE chain_id=1 AND capture_id=? AND batch_id=?", []byte(cap.CaptureId), []byte(cap.BatchId))
	if e != nil {
		return reserve.Batch{}, e
	}
	if len(rows) != 1 || !rows[0].Committed {
		return reserve.Batch{}, fmt.Errorf("Reserve capture not committed")
	}
	if reserveFrozen(rows[0]) != reserveFrozen(cap) {
		return reserve.Batch{}, contracts.ErrConflict
	}
	return c.reserveBatch(ctx, c.db, rows[0])
}
func reserveFrozen(cap reserve.Capture) string {
	cap.Canonical = false
	cap.Finality = ""
	cap.Revision = 0
	cap.Reason = ""
	// Reconcile replaces the capture-level finality evidence hash while all
	// original member anchors/digests and observation times remain immutable.
	cap.PayloadHash = ""
	return reserveCaptureHash(cap)
}
func receiptLogs(logs []dex.Log, r dex.Receipt) []dex.Log {
	out := []dex.Log{}
	for _, l := range logs {
		if l.Hash == r.Hash && l.TxHash == r.TxHash {
			out = append(out, l)
		}
	}
	return out
}
func (c *Client) WriteReserveBatch(ctx context.Context, b reserve.Batch) error {
	if e := reserve.Validate(b); e != nil {
		return e
	}
	if len(b.ReceiptData) != len(b.Receipts) {
		return fmt.Errorf("Reserve receipt data members missing")
	}
	for i, d := range b.ReceiptData {
		if e := reserve.ReceiptDataContains(d, b.Receipts[i], receiptLogs(b.Logs, b.Receipts[i])); e != nil {
			return e
		}
	}
	return c.transaction(ctx, func(tx *sql.Tx) error {
		old, e := readModels[reserve.Capture](ctx, c, tx, "reserve_capture", "WHERE chain_id=1 AND capture_id=? AND batch_id=? FOR UPDATE", []byte(b.Capture.CaptureId), []byte(b.Capture.BatchId))
		if e != nil {
			return e
		}
		if len(old) > 1 {
			return contracts.ErrConflict
		}
		if len(old) == 1 {
			if reserveFrozen(old[0]) != reserveFrozen(b.Capture) || (old[0].Revision == b.Capture.Revision && reserveCaptureHash(old[0]) != reserveCaptureHash(b.Capture)) {
				return contracts.ErrConflict
			}
			_, e = c.reserveBatch(ctx, tx, old[0])
			return e
		}
		if e = putModels(ctx, c, tx, "reserve_folio_state", b.States); e != nil {
			return e
		}
		if e = putModels(ctx, c, tx, "reserve_route_quote", b.Quotes); e != nil {
			return e
		}
		for _, l := range b.Logs {
			existing, e := c.dexLogs(ctx, tx, "WHERE chain_id=1 AND block_hash=? AND tx_hash=? AND log_index=?", l.Hash[:], l.TxHash[:], l.Index)
			if e != nil {
				return e
			}
			for _, r := range existing {
				if reserve.LogFact(r) != reserve.LogFact(l) {
					return contracts.ErrConflict
				}
			}
			if e = c.putValues(ctx, tx, "dex_log", dexLogColumns, logValues(l)); e != nil {
				return e
			}
		}
		for i, r := range b.Receipts {
			if e = c.putSharedReceipt(ctx, tx, r); e != nil {
				return e
			}
			if e = c.putReceiptData(ctx, tx, b.ReceiptData[i], r); e != nil {
				return e
			}
		}
		if _, e = c.reserveBatch(ctx, tx, b.Capture); e != nil {
			return e
		}
		return putModels(ctx, c, tx, "reserve_capture", []reserve.Capture{b.Capture})
	})
}
func (c *Client) WriteReserveRevision(ctx context.Context, next reserve.Capture) error {
	return c.transaction(ctx, func(tx *sql.Tx) error {
		rows, e := readModels[reserve.Capture](ctx, c, tx, "reserve_capture", "WHERE chain_id=1 AND capture_id=? AND batch_id=? FOR UPDATE", []byte(next.CaptureId), []byte(next.BatchId))
		if e != nil {
			return e
		}
		if len(rows) != 1 {
			return fmt.Errorf("Reserve revision capture missing")
		}
		old := rows[0]
		if reserveFrozen(old) != reserveFrozen(next) {
			return contracts.ErrConflict
		}
		if next.Revision < old.Revision {
			return nil
		}
		if next.Revision == old.Revision {
			if reserveCaptureHash(old) == reserveCaptureHash(next) {
				return nil
			}
			return contracts.ErrConflict
		}
		if old.Finality == "finalized" && (next.Finality != "finalized" || next.Canonical != old.Canonical) {
			return contracts.ErrConflict
		}
		if _, e = c.reserveBatch(ctx, tx, next); e != nil {
			return e
		}
		return replaceModel(ctx, c, tx, "reserve_capture", next)
	})
}
func (c *Client) ReserveBatches(ctx context.Context, caps []reserve.Capture) (map[string]reserve.Batch, error) {
	out := map[string]reserve.Batch{}
	for _, cap := range caps {
		b, e := c.ReserveBatch(ctx, cap)
		if e != nil {
			return nil, e
		}
		if _, ok := out[cap.BatchId]; ok {
			return nil, contracts.ErrConflict
		}
		out[cap.BatchId] = b
	}
	return out, nil
}
func (c *Client) ReserveReceiptData(ctx context.Context, a dex.Anchor, tx dex.Hash) (reserve.ReceiptData, bool, error) {
	rows, e := readModels[reserve.ReceiptData](ctx, c, c.db, "reserve_receipt_data", "WHERE chain_id=1 AND block_hash=? AND tx_hash=?", a.Hash[:], tx[:])
	if e != nil {
		return reserve.ReceiptData{}, false, e
	}
	if len(rows) == 0 {
		return reserve.ReceiptData{}, false, nil
	}
	if len(rows) != 1 {
		return reserve.ReceiptData{}, false, contracts.ErrConflict
	}
	r, ok, e := c.ReserveReceipt(ctx, a, tx)
	if e != nil {
		return rows[0], false, e
	}
	if !ok {
		return rows[0], false, fmt.Errorf("Reserve receipt summary missing")
	}
	if e = reserve.ValidateReceiptData(rows[0], r); e != nil {
		return rows[0], false, e
	}
	return rows[0], true, nil
}
func (c *Client) putReceiptData(ctx context.Context, q executor, d reserve.ReceiptData, r dex.Receipt) error {
	if e := reserve.ValidateReceiptData(d, r); e != nil {
		return e
	}
	rows, e := readModels[reserve.ReceiptData](ctx, c, q, "reserve_receipt_data", "WHERE chain_id=1 AND block_hash=? AND tx_hash=? FOR UPDATE", r.Hash[:], r.TxHash[:])
	if e != nil {
		return e
	}
	if len(rows) > 1 {
		return contracts.ErrConflict
	}
	if len(rows) == 1 {
		if rows[0].DataHash != d.DataHash {
			return contracts.ErrConflict
		}
		return nil
	}
	return putModels(ctx, c, q, "reserve_receipt_data", []reserve.ReceiptData{d})
}
func (c *Client) WriteReserveReceiptData(ctx context.Context, d reserve.ReceiptData, r dex.Receipt) error {
	return c.transaction(ctx, func(tx *sql.Tx) error {
		old, e := c.dexReceipts(ctx, tx, "WHERE chain_id=1 AND block_hash=? AND tx_hash=?", r.Hash[:], r.TxHash[:])
		if e != nil {
			return e
		}
		if len(old) != 1 || old[0].ReceiptHash != r.ReceiptHash {
			return fmt.Errorf("Reserve receipt summary missing or changed")
		}
		return c.putReceiptData(ctx, tx, d, r)
	})
}
func (c *Client) ReserveReceipts(ctx context.Context) ([]dex.Receipt, error) {
	return c.dexReceipts(ctx, c.db, "WHERE chain_id=1 ORDER BY block_number,tx_hash")
}
func (c *Client) WriteReserveSimulation(ctx context.Context, s reserve.Simulation) error {
	if e := reserve.ValidateSimulation(s); e != nil {
		return e
	}
	return c.transaction(ctx, func(tx *sql.Tx) error {
		return putModels(ctx, c, tx, "reserve_route_simulation", []reserve.Simulation{s})
	})
}
func (c *Client) ReserveSimulations(ctx context.Context, manifest string) ([]reserve.Simulation, error) {
	rows, e := readModels[reserve.Simulation](ctx, c, c.db, "reserve_route_simulation", "WHERE chain_id=1 AND manifest_hash=? ORDER BY block_number,quote_id,available_at", []byte(manifest))
	if e != nil {
		return nil, e
	}
	for _, r := range rows {
		if e = reserve.ValidateSimulation(r); e != nil {
			return nil, e
		}
	}
	return rows, nil
}

var _ reserve.Store = (*Client)(nil)
var _ reserve.SimulationStore = (*Client)(nil)
var _ reserve.ReceiptDataStore = (*Client)(nil)

func reserveCaptureHash(cap reserve.Capture) string { return reserve.ID(cap) }
