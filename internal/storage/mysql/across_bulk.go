package mysql

import (
	"context"
	"fmt"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/across"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
)

// Keep historical recovery bounded while fetching each member table once per
// chunk. Per-capture member hashes, declarations and revisions remain mandatory.
const acrossSQLCaptureLimit = 64

func readAcrossMembers[T any](ctx context.Context, c *Client, name, where string, args []any, batches map[string]*across.Batch, id func(T) string, add func(*across.Batch, T)) error {
	rows, e := readModels[T](ctx, c, c.db, name, where, args...)
	if e != nil {
		return e
	}
	for _, row := range rows {
		if b := batches[id(row)]; b != nil {
			add(b, row)
		}
	}
	return nil
}

func (c *Client) AcrossBatches(ctx context.Context, caps []across.Capture) (out across.BatchLoad, err error) {
	started := time.Now()
	var queries uint64
	out = across.BatchLoad{Batches: map[string]across.Batch{}, Errors: map[string]error{}, Stats: across.BatchLoadStats{Mode: "mysql_typed_bulk", Captures: uint64(len(caps)), SQLQueries: &queries}}
	defer func() { out.Stats.TotalMicros = time.Since(started).Microseconds() }()
	requested := map[string]across.Capture{}
	for _, cap := range caps {
		if _, exists := requested[cap.CaptureId]; exists {
			return out, contracts.ErrConflict
		}
		requested[cap.CaptureId] = cap
	}
	for start := 0; start < len(caps); start += acrossSQLCaptureLimit {
		if e := ctx.Err(); e != nil {
			return out, e
		}
		chunk := caps[start:min(start+acrossSQLCaptureLimit, len(caps))]
		args := make([]any, len(chunk))
		for i, cap := range chunk {
			args[i] = []byte(cap.CaptureId)
		}
		where := "WHERE capture_id IN (" + placeholders(len(chunk)) + ")"
		queries++
		stored, e := readModels[across.Capture](ctx, c, c.db, "across_capture", where, args...)
		if e != nil {
			return out, e
		}
		batches := map[string]*across.Batch{}
		seen := map[string]bool{}
		transfers := false
		for _, cap := range stored {
			id := cap.CaptureId
			if seen[id] {
				out.Errors[id] = contracts.ErrConflict
				delete(batches, id)
				continue
			}
			seen[id] = true
			if !cap.Committed {
				out.Errors[id] = fmt.Errorf("Across capture not committed")
				continue
			}
			if acrossMembersID(cap) != acrossMembersID(requested[id]) {
				out.Errors[id] = contracts.ErrConflict
				continue
			}
			b := &across.Batch{Capture: cap, Deposits: []across.Deposit{}, Updates: []across.DepositUpdate{}, Fills: []across.Fill{}, Refunds: []across.Refund{}, Receipts: []across.TxReceipt{}, Probes: []across.OrderProbe{}}
			if len(cap.TableIds) == 7 {
				transfers = true
				b.Transfers = []across.ReceiptTransfers{}
			}
			batches[id] = b
		}
		for _, cap := range chunk {
			if !seen[cap.CaptureId] {
				out.Errors[cap.CaptureId] = fmt.Errorf("Across capture not committed")
			}
		}
		read := func(load func() error) error { queries++; return load() }
		if e = read(func() error {
			return readAcrossMembers(ctx, c, "across_deposit", where, args, batches, func(r across.Deposit) string { return r.CaptureId }, func(b *across.Batch, r across.Deposit) { b.Deposits = append(b.Deposits, r) })
		}); e != nil {
			return out, e
		}
		if e = read(func() error {
			return readAcrossMembers(ctx, c, "across_deposit_update", where, args, batches, func(r across.DepositUpdate) string { return r.CaptureId }, func(b *across.Batch, r across.DepositUpdate) { b.Updates = append(b.Updates, r) })
		}); e != nil {
			return out, e
		}
		if e = read(func() error {
			return readAcrossMembers(ctx, c, "across_fill", where, args, batches, func(r across.Fill) string { return r.CaptureId }, func(b *across.Batch, r across.Fill) { b.Fills = append(b.Fills, r) })
		}); e != nil {
			return out, e
		}
		if e = read(func() error {
			return readAcrossMembers(ctx, c, "across_refund", where, args, batches, func(r across.Refund) string { return r.CaptureId }, func(b *across.Batch, r across.Refund) { b.Refunds = append(b.Refunds, r) })
		}); e != nil {
			return out, e
		}
		if e = read(func() error {
			return readAcrossMembers(ctx, c, "across_tx_receipt", where, args, batches, func(r across.TxReceipt) string { return r.CaptureId }, func(b *across.Batch, r across.TxReceipt) { b.Receipts = append(b.Receipts, r) })
		}); e != nil {
			return out, e
		}
		if e = read(func() error {
			return readAcrossMembers(ctx, c, "across_order_probe", where, args, batches, func(r across.OrderProbe) string { return r.CaptureId }, func(b *across.Batch, r across.OrderProbe) { b.Probes = append(b.Probes, r) })
		}); e != nil {
			return out, e
		}
		if transfers {
			if e = read(func() error {
				return readAcrossMembers(ctx, c, "across_receipt_transfers", where, args, batches, func(r across.ReceiptTransfers) string { return r.CaptureId }, func(b *across.Batch, r across.ReceiptTransfers) {
					if len(b.Capture.TableIds) == 7 {
						b.Transfers = append(b.Transfers, r)
					}
				})
			}); e != nil {
				return out, e
			}
		}
		for id, b := range batches {
			if e = across.Validate(*b); e != nil {
				out.Errors[id] = e
			} else {
				out.Batches[id] = *b
			}
		}
		out.Stats.Chunks++
	}
	return out, nil
}

var _ across.BatchLoader = (*Client)(nil)
