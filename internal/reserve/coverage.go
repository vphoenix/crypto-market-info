package reserve

import (
	"context"
	"sort"
)

type BlockRange struct {
	From uint64 `json:"from"`
	To   uint64 `json:"to"`
}
type Coverage struct {
	Mode                 string       `json:"mode"`
	From                 uint64       `json:"from"`
	To                   uint64       `json:"to"`
	CoveredBlocks        uint64       `json:"covered_blocks"`
	MissingBlocks        uint64       `json:"missing_blocks"`
	ReceiptCoveredBlocks uint64       `json:"receipt_covered_blocks"`
	ReceiptMissingBlocks uint64       `json:"receipt_missing_blocks"`
	Gaps                 []BlockRange `json:"gaps"`
}

func intervalCoverage(ranges []BlockRange, from, to uint64) (uint64, []BlockRange) {
	sort.Slice(ranges, func(i, j int) bool { return ranges[i].From < ranges[j].From })
	cursor := from
	count := uint64(0)
	gaps := []BlockRange{}
	for _, v := range ranges {
		a, b := max(v.From, from), min(v.To, to)
		if a > b || b < cursor {
			continue
		}
		if a > cursor {
			gaps = append(gaps, BlockRange{cursor, a - 1})
		}
		a = max(a, cursor)
		count += b - a + 1
		cursor = b + 1
		if cursor > to {
			break
		}
	}
	if cursor <= to {
		gaps = append(gaps, BlockRange{cursor, to})
	}
	return count, gaps
}

// Summaries include failed attempts when defining requested bounds, and use a
// union of successful intervals. An attempt counter is never a missing height.
func CoverageScopes(caps []Capture, finalized bool) []Coverage {
	groups := map[string][]Capture{}
	for _, v := range caps {
		if v.Canonical && v.Committed && v.CaptureKind == "logs" && (!finalized || v.Finality == "finalized") {
			groups[v.CaptureMode] = append(groups[v.CaptureMode], v)
		}
	}
	out := []Coverage{}
	for mode, rows := range groups {
		v := Coverage{Mode: mode, From: rows[0].FromBlock, To: rows[0].ToBlock}
		logs := []BlockRange{}
		receipts := []BlockRange{}
		for _, c := range rows {
			v.From = min(v.From, c.FromBlock)
			v.To = max(v.To, c.ToBlock)
			if c.LogCoverage == "complete" {
				logs = append(logs, BlockRange{c.FromBlock, c.ToBlock})
				if c.ReceiptCoverage == "complete" {
					receipts = append(receipts, BlockRange{c.FromBlock, c.ToBlock})
				}
			}
		}
		v.CoveredBlocks, v.Gaps = intervalCoverage(logs, v.From, v.To)
		v.MissingBlocks = v.To - v.From + 1 - v.CoveredBlocks
		v.ReceiptCoveredBlocks, _ = intervalCoverage(receipts, v.From, v.To)
		v.ReceiptMissingBlocks = v.To - v.From + 1 - v.ReceiptCoveredBlocks
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Mode < out[j].Mode })
	return out
}
func selectedCaptures(caps []Capture, finalized bool) []Capture {
	latest := map[string]Capture{}
	score := func(c Capture) int {
		if c.CaptureKind == "logs" {
			if c.LogCoverage == "complete" {
				if c.ReceiptCoverage == "complete" {
					return 2
				}
				return 1
			}
			return 0
		}
		if c.StateCoverage == "complete" {
			if c.QuoteCoverage == "complete" {
				return 2
			}
			return 1
		}
		return 0
	}
	for _, c := range caps {
		if !c.Canonical || !c.Committed || finalized && c.Finality != "finalized" {
			continue
		}
		old, ok := latest[c.CaptureId]
		if !ok || score(c) > score(old) || score(c) == score(old) && c.AvailableAt.After(old.AvailableAt) {
			latest[c.CaptureId] = c
		}
	}
	out := []Capture{}
	for _, v := range latest {
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].ToBlock != out[j].ToBlock {
			return out[i].ToBlock < out[j].ToBlock
		}
		return out[i].BatchId < out[j].BatchId
	})
	return out
}
func loadBatches(ctx context.Context, store Store, caps []Capture) (map[string]Batch, error) {
	if bulk, ok := store.(interface {
		ReserveBatches(context.Context, []Capture) (map[string]Batch, error)
	}); ok {
		return bulk.ReserveBatches(ctx, caps)
	}
	out := map[string]Batch{}
	for _, c := range caps {
		b, e := store.ReserveBatch(ctx, c)
		if e != nil {
			return nil, e
		}
		out[c.BatchId] = b
	}
	return out, nil
}
