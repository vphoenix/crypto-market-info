package clickhouse

import (
	"context"
	"errors"
	"github.com/vphoenix/crypto-market-info/internal/dex"
	"github.com/vphoenix/crypto-market-info/internal/reserve"
)

// Read every member table even when counts are zero, so extra or missing rows
// still fail the same digest validation as a single-capture read.
func (c *Client) ReserveBatches(ctx context.Context, caps []reserve.Capture) (map[string]reserve.Batch, error) {
	out := map[string]reserve.Batch{}
	receipts := map[string]dex.Receipt{}
	details := map[string]reserve.ReceiptData{}
	for _, cap := range caps {
		if _, ok := out[cap.BatchId]; ok {
			return nil, errors.New("duplicate_batch_in_read")
		}
		out[cap.BatchId] = reserve.Batch{Capture: cap}
	}
	for start := 0; start < len(caps); start += 256 {
		part := caps[start:min(start+256, len(caps))]
		ids := make([]string, len(part))
		for i, v := range part {
			ids[i] = v.BatchId
		}
		states, e := reserveRead[reserve.State](ctx, c, "reserve_folio_state", "WHERE chain_id=1 AND batch_id IN (?)", ids)
		if e != nil {
			return nil, e
		}
		for _, v := range states {
			b, ok := out[v.BatchId]
			if !ok {
				return nil, errors.New("unexpected_state_batch")
			}
			b.States = append(b.States, v)
			out[v.BatchId] = b
		}
		quotes, e := reserveRead[reserve.Quote](ctx, c, "reserve_route_quote", "WHERE chain_id=1 AND batch_id IN (?)", ids)
		if e != nil {
			return nil, e
		}
		for _, v := range quotes {
			b, ok := out[v.BatchId]
			if !ok {
				return nil, errors.New("unexpected_quote_batch")
			}
			b.Quotes = append(b.Quotes, v)
			out[v.BatchId] = b
		}
		logs, e := c.reserveLogs(ctx, "WHERE chain_id=1 AND batch_id IN (?)", ids)
		if e != nil {
			return nil, e
		}
		for _, v := range logs {
			id := reserve.Hash(v.Batch)
			b, ok := out[id]
			if !ok {
				return nil, errors.New("unexpected_log_batch")
			}
			b.Logs = append(b.Logs, v)
			out[id] = b
		}
		for _, cap := range part {
			b := out[cap.BatchId]
			for _, ref := range cap.ReceiptRefs {
				key := ref.BlockHash + ref.TxHash
				r, ok := receipts[key]
				if !ok {
					var a dex.Anchor
					var tx dex.Hash
					copy(a.Hash[:], ref.BlockHash)
					copy(tx[:], ref.TxHash)
					r, ok, e = c.ReserveReceipt(ctx, a, tx)
					if e != nil {
						return nil, e
					}
					if !ok {
						return nil, errors.New("capture_receipt_reference_missing")
					}
					receipts[key] = r
				}
				if reserve.Hash(r.ReceiptHash) != ref.ReceiptHash || reserve.Hash(r.CalldataHash) != ref.CalldataHash {
					return nil, errors.New("capture_receipt_reference_mismatch")
				}
				b.Receipts = append(b.Receipts, r)
				d, present := details[key]
				if !present {
					var found bool
					d, found, e = c.ReserveReceiptData(ctx, r.Anchor, r.TxHash)
					if e != nil {
						return nil, e
					}
					if !found {
						return nil, errors.New("capture_receipt_data_missing")
					}
					details[key] = d
				}
				if e = reserve.ReceiptDataContains(d, r, receiptLogs(b.Logs, r)); e != nil {
					return nil, e
				}
				b.ReceiptData = append(b.ReceiptData, d)
			}
			if e = reserve.Validate(b); e != nil {
				return nil, e
			}
			out[cap.BatchId] = b
		}
	}
	return out, nil
}
