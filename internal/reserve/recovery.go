package reserve

import (
	"context"
	"errors"
	"fmt"
	"github.com/vphoenix/crypto-market-info/internal/dex"
	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
	"sort"
)

var ErrArchiveAuthorization = errors.New("archive_source_authorization_required")

// Skip complete coverage regardless of the page boundaries used by an earlier
// run. Partial receipts require another attempt even when the log cursor moved.
func CoveredThrough(caps []Capture, start uint64) uint64 {
	cursor := start - 1
	ranges := append([]Capture(nil), caps...)
	sort.Slice(ranges, func(i, j int) bool { return ranges[i].FromBlock < ranges[j].FromBlock })
	for _, v := range ranges {
		if !v.Canonical || !v.Committed || v.CaptureKind != "logs" || v.Finality != "finalized" || v.LogCoverage != "complete" || v.ReceiptCoverage != "complete" {
			continue
		}
		if v.FromBlock > cursor+1 {
			break
		}
		cursor = max(cursor, v.ToBlock)
	}
	return cursor
}

// A bounded retry of one incomplete receipt range. Never revises the earlier
// capture's availability or receipt references; a new committed attempt is used.
func (c *Collector) RepairReceipts(ctx context.Context, final, safe dex.Block, progress func(string)) error {
	caps, e := c.Store.ReserveCaptures(ctx, Hash(c.Manifest.Hash))
	if e != nil {
		return e
	}
	latest := map[string]Capture{}
	for _, v := range caps {
		if v.Canonical && v.Committed && v.CaptureKind == "logs" {
			old, ok := latest[v.CaptureId]
			if !ok || v.AvailableAt.After(old.AvailableAt) {
				latest[v.CaptureId] = v
			}
		}
	}
	pending := []Capture{}
	for _, v := range latest {
		if v.LogCoverage == "complete" && v.ReceiptCoverage != "complete" {
			pending = append(pending, v)
		}
	}
	if len(pending) == 0 {
		return nil
	}
	sort.Slice(pending, func(i, j int) bool { return pending[i].AvailableAt.Before(pending[j].AvailableAt) })
	v := pending[0]
	a, e := c.Header(ctx, ethereum.Height(v.FromBlock))
	if e != nil {
		return e
	}
	b, e := c.Header(ctx, ethereum.Height(v.ToBlock))
	if e != nil {
		return e
	}
	if Hash(a.Hash) != v.FromHash || Hash(b.Hash) != v.ToHash {
		return errors.New("receipt_retry_branch_changed")
	}
	finality := "head"
	if b.Number <= safe.Number {
		finality = "safe"
	}
	if b.Number <= final.Number {
		finality = "finalized"
	}
	batch, e := c.Logs(ctx, a, b, v.CaptureMode, finality)
	if e != nil {
		return e
	}
	if e = c.Store.WriteReserveBatch(ctx, batch); e != nil {
		return e
	}
	progress(fmt.Sprintf("receipt_retry blocks=%d..%d receipts=%d/%d coverage=%s reason=%s", v.FromBlock, v.ToBlock, len(batch.Receipts), batch.Capture.ExpectedReceipts, batch.Capture.ReceiptCoverage, batch.Capture.Reason))
	return nil
}
