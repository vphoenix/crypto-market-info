package across

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"

	"github.com/vphoenix/crypto-market-info/internal/dex"
	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
)

type TransferMigrationStats struct {
	ReceiptsChecked     uint64 `json:"receipts_checked"`
	RowsWritten         uint64 `json:"rows_written"`
	TransferLogs        uint64 `json:"transfer_logs"`
	BodiesAlreadyAbsent uint64 `json:"bodies_already_absent"`
}

// This is a local, idempotent migration. It never requests chain history.
// Only old refund receipts need their missing Transfer fields migrated.
// Ordinary old receipts are already stored and do not need a second copy.
func MigrateReceiptTransfers(ctx context.Context, store Store, m Manifest, a ethereum.Archive) (stats TransferMigrationStats, err error) {
	caps, err := store.AcrossCaptures(ctx, m.Hash)
	if err != nil {
		return stats, err
	}
	caps, err = latestReadCaptures(caps)
	if err != nil {
		return stats, err
	}
	selected := []Capture{}
	for _, c := range caps {
		if c.Committed && (c.CaptureKind == "logs" || c.CaptureKind == "receipts" || c.CaptureKind == "receipt_transfers") {
			selected = append(selected, c)
		}
	}
	loaded, err := LoadBatches(ctx, store, selected)
	if err != nil {
		return stats, err
	}
	done := map[string]bool{}
	needed := map[string]bool{}
	for _, c := range selected {
		if err := loaded.Errors[c.CaptureId]; err != nil {
			return stats, err
		}
		for _, v := range loaded.Batches[c.CaptureId].Refunds {
			chain, ok := m.Chain(v.ChainId)
			if ok && v.Token == chain.USDC {
				needed[reportReceiptKey(v.ChainId, v.BlockHash, v.TxHash)] = true
			}
		}
		for _, v := range loaded.Batches[c.CaptureId].Transfers {
			done[v.PayloadHash] = true
		}
	}
	collector := Collector{Manifest: m, Store: store, Archive: a}
	for _, cap := range selected {
		for _, r := range loaded.Batches[cap.CaptureId].Receipts {
			if err = ctx.Err(); err != nil {
				return stats, err
			}
			if !needed[reportReceiptKey(r.ChainId, r.BlockHash, r.TxHash)] || done[r.ReceiptPayloadHash] {
				continue
			}
			stats.ReceiptsChecked++
			var hash dex.Hash
			copy(hash[:], r.ReceiptPayloadHash)
			raw, err := a.Get(hash)
			if errors.Is(err, os.ErrNotExist) {
				stats.BodiesAlreadyAbsent++
				continue
			}
			if err != nil {
				return stats, err
			}
			var env struct{ Response []byte }
			if json.Unmarshal(raw, &env) != nil || len(env.Response) == 0 {
				return stats, errors.New("invalid_receipt_envelope")
			}
			var results []struct {
				Result json.RawMessage
				Error  json.RawMessage
			}
			if json.Unmarshal(env.Response, &results) != nil {
				return stats, errors.New("invalid_receipt_responses")
			}
			var receiptResult json.RawMessage
			for _, result := range results {
				var anchor struct{ TransactionHash string }
				if json.Unmarshal(result.Result, &anchor) == nil && strings.EqualFold(anchor.TransactionHash, Hex(r.TxHash)) {
					if receiptResult != nil || len(result.Error) != 0 && string(result.Error) != "null" {
						return stats, errors.New("ambiguous_receipt_result")
					}
					receiptResult = result.Result
				}
			}
			chain, ok := m.Chain(r.ChainId)
			if !ok {
				return stats, errors.New("unknown_receipt_chain")
			}
			transfers, err := parseReceiptTransferResult(receiptResult, r, chain.USDC)
			if err != nil {
				return stats, err
			}
			row := receiptTransferRow(r, chain.USDC, transfers)
			// Original response availability remains original; the migration capture
			// separately records when these fields were written into their table.
			row.AvailableAt = r.AvailableAt
			id := ID(struct{ Manifest, Payload, Token string }{m.Hash, r.ReceiptPayloadHash, chain.USDC})
			row.CaptureId = id
			b := Batch{Capture: Capture{ManifestHash: m.Hash, CaptureId: id, ChainId: r.ChainId, CaptureKind: "receipt_transfers", CaptureMode: "local_migration", StartedAt: Now(), SourceId: cap.SourceId, Status: "complete", ExpectedTasks: 1, CompletedTasks: 1, Canonical: cap.Canonical, Finality: cap.Finality, Revision: 1}, Transfers: []ReceiptTransfers{row}}
			header := Block{ChainID: r.ChainId, Number: r.BlockNumber, Hash: r.BlockHash, Time: r.BlockTime}
			anchorCapture(&b.Capture, header, header)
			if err := collector.persist(ctx, &b, []EvidenceHeader{evidenceHeader(header)}, []string{r.ReceiptPayloadHash}); err != nil {
				return stats, err
			}
			done[r.ReceiptPayloadHash] = true
			stats.RowsWritten++
			if stats.RowsWritten%50 == 0 {
				RecordLoadProgress(ctx, LoadProgress{Phase: "transfer_rows_written", Completed: stats.RowsWritten})
			}
			stats.TransferLogs += uint64(len(transfers))
		}
	}
	return stats, nil
}
