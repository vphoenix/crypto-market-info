package across

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/dex"
	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
)

type ResponsePruneStats struct {
	CapturesChecked  uint64 `json:"captures_checked"`
	FilesChecked     uint64 `json:"files_checked"`
	FilesDeleted     uint64 `json:"files_deleted"`
	FileBytesDeleted int64  `json:"file_bytes_deleted"`
	ProtectedRefs    uint64 `json:"protected_response_refs"`
}

// One-off cleanup of old response files, after the writer stopped archiving
// bodies. Preserve unresolved logs and refunds not yet migrated. All DB/member
// checks finish before deletion. Hash-only live requests create no new files.
func PruneStoredResponses(ctx context.Context, store Store, m Manifest, a ethereum.Archive) (stats ResponsePruneStats, err error) {
	cutoff := Now().Add(-2 * time.Minute) // leave any old writer's in-flight responses alone
	caps, err := store.AcrossCaptures(ctx, m.Hash)
	if err != nil {
		return stats, err
	}
	caps, err = latestReadCaptures(caps)
	if err != nil {
		return stats, err
	}
	selected := []Capture{}
	for _, cap := range caps {
		if cap.Committed {
			selected = append(selected, cap)
		}
	}
	loaded, err := LoadBatches(ctx, store, selected)
	if err != nil {
		return stats, err
	}
	RecordLoadProgress(ctx, LoadProgress{Phase: "prune_members_checked", Completed: uint64(len(selected)), Total: uint64(len(selected))})
	refunds, transfers := map[string]bool{}, map[string]bool{}
	for _, cap := range selected {
		if err := loaded.Errors[cap.CaptureId]; err != nil {
			return stats, fmt.Errorf("prune_incomplete_members: %w", err)
		}
		b := loaded.Batches[cap.CaptureId]
		for _, v := range b.Refunds {
			refunds[reportReceiptKey(v.ChainId, v.BlockHash, v.TxHash)] = true
		}
		for _, v := range b.Transfers {
			transfers[reportReceiptKey(v.ChainId, v.BlockHash, v.TxHash)] = true
		}
	}
	keep := map[string]bool{}
	decoded := map[uint64][]blockInterval{}
	for _, ch := range m.Chains {
		decoded[ch.ChainID] = historyIntervals(caps, ch.ChainID, true)
	}
	for _, cap := range selected {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		ev, err := ReadCaptureEvidence(a, cap)
		if err != nil {
			return stats, err
		}
		protect := false
		if cap.Canonical && cap.CaptureKind == "logs" && (cap.Status != "complete" || cap.UnknownEventCount != 0) {
			protect = !anchored(cap) || firstUncovered(*cap.FromBlock, *cap.ToBlock, decoded[cap.ChainId]) <= *cap.ToBlock
		}
		for _, v := range loaded.Batches[cap.CaptureId].Receipts {
			if refunds[reportReceiptKey(v.ChainId, v.BlockHash, v.TxHash)] && !transfers[reportReceiptKey(v.ChainId, v.BlockHash, v.TxHash)] {
				protect = true
			}
		}
		if protect {
			for _, ref := range ev.RPCPayloads {
				value, err := ParseHex(ref, 32)
				if err != nil {
					return stats, err
				}
				keep[value] = true
			}
		}
		stats.CapturesChecked++
		if stats.CapturesChecked%1000 == 0 {
			RecordLoadProgress(ctx, LoadProgress{Phase: "prune_metadata_checked", Completed: stats.CapturesChecked, Total: uint64(len(selected))})
		}
	}
	stats.ProtectedRefs = uint64(len(keep))
	// Enumerate actual files, also collecting orphaned poll heads, failed attempts
	// and other old responses that no committed capture references.
	dirs, err := os.ReadDir(a.Dir)
	if err != nil {
		return stats, err
	}
	workCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	tasks := make(chan string, len(dirs))
	type result struct {
		stats ResponsePruneStats
		err   error
	}
	results := make(chan result, 8)
	for _, dir := range dirs {
		if dir.IsDir() && len(dir.Name()) == 2 {
			tasks <- dir.Name()
		}
	}
	close(tasks)
	var workers sync.WaitGroup
	for range 8 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for prefix := range tasks {
				partial, err := pruneResponseDirectory(workCtx, a, prefix, cutoff, keep)
				results <- result{partial, err}
			}
		}()
	}
	go func() { workers.Wait(); close(results) }()
	for r := range results {
		stats.FilesChecked += r.stats.FilesChecked
		stats.FilesDeleted += r.stats.FilesDeleted
		stats.FileBytesDeleted += r.stats.FileBytesDeleted
		if r.err != nil && err == nil {
			err = r.err
			cancel()
		}
		RecordLoadProgress(ctx, LoadProgress{Phase: "prune_files_checked", Completed: stats.FilesChecked})
	}
	return stats, err
}

func pruneResponseDirectory(ctx context.Context, a ethereum.Archive, prefix string, cutoff time.Time, keep map[string]bool) (stats ResponsePruneStats, err error) {
	entries, err := os.ReadDir(filepath.Join(a.Dir, prefix))
	if err != nil {
		return stats, err
	}
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return stats, err
		}
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".json.gz") || len(name) != 72 || name[:2] != prefix {
			continue
		}
		ref, err := ParseHex("0x"+strings.TrimSuffix(name, ".json.gz"), 32)
		if err != nil {
			return stats, err
		}
		stats.FilesChecked++
		if keep[ref] {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return stats, err
		}
		if !info.Mode().IsRegular() || !info.ModTime().Before(cutoff) {
			continue
		}
		var hash dex.Hash
		copy(hash[:], ref)
		raw, err := a.Get(hash)
		if err != nil {
			return stats, err
		}
		var object map[string]json.RawMessage
		if json.Unmarshal(raw, &object) != nil {
			return stats, errors.New("invalid_archive_object")
		}
		// These keys distinguish RPC/API envelopes from compact capture, finality,
		// ABI and manifest metadata. Unrecognized objects stay untouched.
		if object["Response"] == nil || object["Source"] == nil || (object["Requests"] == nil && object["Symbol"] == nil) {
			continue
		}
		if err := os.Remove(filepath.Join(a.Dir, prefix, name)); err != nil {
			return stats, err
		}
		stats.FilesDeleted++
		stats.FileBytesDeleted += info.Size()
	}
	return stats, nil
}
