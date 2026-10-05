package clickhouse

import (
	"context"
	"errors"
	"reflect"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/across"
)

const acrossReadCaptureLimit = 512

// AcrossBatches uses six SELECTs per bounded chunk, including tables whose
// expected counts are zero: unexpected extra members must still be detected.
func (c *Client) AcrossBatches(ctx context.Context, captures []across.Capture) (out across.BatchLoad, err error) {
	started := time.Now()
	queries := uint64(0)
	sqlMicros := int64(0)
	out = across.BatchLoad{Batches: map[string]across.Batch{}, Errors: map[string]error{}, Stats: across.BatchLoadStats{Mode: "clickhouse_batched", Captures: uint64(len(captures)), SQLQueries: &queries, SQLMicros: &sqlMicros}}
	defer func() { out.Stats.TotalMicros = time.Since(started).Microseconds() }()
	for _, cap := range captures {
		if _, ok := out.Batches[cap.CaptureId]; ok {
			return out, errors.New("duplicate_capture_in_batch_load")
		}
		out.Batches[cap.CaptureId] = across.Batch{Capture: cap}
	}
	for start := 0; start < len(captures); start += acrossReadCaptureLimit {
		chunk := captures[start:min(start+acrossReadCaptureLimit, len(captures))]
		ids := make([]string, len(chunk))
		for i, cap := range chunk {
			ids[i] = cap.CaptureId
		}
		out.Stats.Chunks++
		chunkStarted := time.Now()
		across.RecordLoadProgress(ctx, across.LoadProgress{Phase: "fact_chunk_started", Completed: uint64(start), Total: uint64(len(captures)), Queries: queries, SQLMicros: sqlMicros, ElapsedMicros: time.Since(started).Microseconds()})
		readers := []func() error{
			func() error {
				return acrossReadGrouped(ctx, c, "across_deposit", ids, out.Batches, func(b *across.Batch, v across.Deposit) { b.Deposits = append(b.Deposits, v) })
			},
			func() error {
				return acrossReadGrouped(ctx, c, "across_deposit_update", ids, out.Batches, func(b *across.Batch, v across.DepositUpdate) { b.Updates = append(b.Updates, v) })
			},
			func() error {
				return acrossReadGrouped(ctx, c, "across_fill", ids, out.Batches, func(b *across.Batch, v across.Fill) { b.Fills = append(b.Fills, v) })
			},
			func() error {
				return acrossReadGrouped(ctx, c, "across_refund", ids, out.Batches, func(b *across.Batch, v across.Refund) { b.Refunds = append(b.Refunds, v) })
			},
			func() error {
				return acrossReadGrouped(ctx, c, "across_tx_receipt", ids, out.Batches, func(b *across.Batch, v across.TxReceipt) { b.Receipts = append(b.Receipts, v) })
			},
			func() error {
				return acrossReadGrouped(ctx, c, "across_order_probe", ids, out.Batches, func(b *across.Batch, v across.OrderProbe) { b.Probes = append(b.Probes, v) })
			},
		}
		tables := []string{"across_deposit", "across_deposit_update", "across_fill", "across_refund", "across_tx_receipt", "across_order_probe"}
		for _, cap := range chunk {
			if len(cap.TableIds) == 7 {
				readers = append(readers, func() error {
					return acrossReadGrouped(ctx, c, "across_receipt_transfers", ids, out.Batches, func(b *across.Batch, v across.ReceiptTransfers) { b.Transfers = append(b.Transfers, v) })
				})
				tables = append(tables, "across_receipt_transfers")
				break
			}
		}
		for i, read := range readers {
			if err = ctx.Err(); err != nil {
				return out, err
			}
			at := time.Now()
			queries++
			across.RecordLoadProgress(ctx, across.LoadProgress{Phase: "fact_query_started", Table: tables[i], Queries: queries, ElapsedMicros: time.Since(started).Microseconds()})
			err = read()
			sqlMicros += time.Since(at).Microseconds()
			across.RecordLoadProgress(ctx, across.LoadProgress{Phase: "fact_query_finished", Table: tables[i], Queries: queries, SQLMicros: time.Since(at).Microseconds(), ElapsedMicros: time.Since(started).Microseconds()})
			if err != nil {
				return out, err
			}
		}
		for _, cap := range chunk {
			if e := across.Validate(out.Batches[cap.CaptureId]); e != nil {
				out.Errors[cap.CaptureId] = e
				delete(out.Batches, cap.CaptureId)
			}
		}
		across.RecordLoadProgress(ctx, across.LoadProgress{Phase: "fact_chunk_finished", Completed: uint64(start + len(chunk)), Total: uint64(len(captures)), Queries: queries, SQLMicros: sqlMicros, ElapsedMicros: time.Since(chunkStarted).Microseconds()})
	}
	return out, nil
}

func acrossReadGrouped[T any](ctx context.Context, c *Client, table string, ids []string, batches map[string]across.Batch, add func(*across.Batch, T)) error {
	rows, err := acrossRead[T](ctx, c, table, "WHERE capture_id IN (?)", ids)
	if err != nil {
		return err
	}
	for _, row := range rows {
		id := reflect.ValueOf(row).FieldByName("CaptureId").String()
		batch, ok := batches[id]
		if !ok {
			return errors.New("unexpected_capture_in_sql_results")
		}
		add(&batch, row)
		batches[id] = batch
	}
	return nil
}
