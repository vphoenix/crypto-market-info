package across

import (
	"context"
	"errors"
	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
	"strings"
)

// A long outage must not hold current-state observations behind old blocks.
// The committed intervals retain the skipped hole for RepairRawGapStep.
func watchLogWindow(cursor, head, maxBlocks uint64, once bool) (uint64, uint64, bool) {
	if cursor != 0 && cursor >= head {
		return 0, 0, false
	}
	from := cursor + 1
	if cursor == 0 || (!once && head-cursor > maxBlocks) {
		from = head
	}
	return from, from + min(maxBlocks, head-from+1) - 1, true
}

// newestRawGap expects intervals sorted by start, including overlapping captures.
// Advancing the union end avoids treating a nested interval as a coverage hole.
func newestRawGap(covered []blockInterval) (HistoryRange, bool) {
	if len(covered) < 2 {
		return HistoryRange{}, false
	}
	end := covered[0].to
	var window HistoryRange
	found := false
	for _, span := range covered[1:] {
		if span.from > end && span.from-end > 1 {
			window = HistoryRange{From: end + 1, To: span.from - 1}
			found = true
		}
		end = max(end, span.to)
	}
	return window, found
}

// RepairRawGapStep scans the newest bounded hole between retained captures.
// It never asks for blocks preceding the original collection start or beyond
// the most recently captured block. Old archive restrictions cannot delay a
// newer recoverable hole. Partial ABI/state coverage remains partial.
func (c *Collector) RepairRawGapStep(ctx context.Context, networkContext ...context.Context) error {
	if err := c.load(ctx); err != nil {
		return err
	}
	caps, err := latestReadCaptures(c.captures)
	if err != nil {
		return err
	}
	count := len(c.Manifest.Chains)
	for offset := 0; offset < count; offset++ {
		index := (c.rawRepairNext + offset) % count
		chain := c.Manifest.Chains[index].ChainID
		if c.Readers[chain] == nil {
			continue
		}
		covered := historyIntervals(caps, chain, false)
		if len(covered) == 0 {
			continue
		}
		window, ok := newestRawGap(covered)
		if !ok {
			continue
		}
		limit := min(historyRoundBlocks, c.Manifest.MaxLogBlocks)
		if saved := c.rawRepairBlocks[chain]; saved > 0 {
			limit = min(limit, saved)
		}
		window.From = window.To - min(limit, window.To-window.From+1) + 1
		c.rawRepairNext = (index + 1) % count
		_, err = c.CollectRange(ctx, chain, window.From, window.To, "catchup", "head", networkContext...)
		// A busy range can exceed the existing twelve-second maintenance
		// budget. Retry fewer blocks next turn, preserving failed coverage.
		// Provider rate limits and archive permissions are not size errors.
		if err != nil && ctx.Err() == nil && !ethereum.IsRateLimited(err) && (errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "deadline_exceeded") || strings.Contains(err.Error(), "rpc_operation_budget_exhausted")) {
			cooldown := false
			if q := c.Readers[chain].SourceQuota; q != nil {
				delay, checkErr := q.Cooldown(ctx)
				cooldown = checkErr != nil || delay > 0
			}
			if !cooldown && window.To > window.From {
				if c.rawRepairBlocks == nil {
					c.rawRepairBlocks = map[uint64]uint64{}
				}
				c.rawRepairBlocks[chain] = max(uint64(1), (window.To-window.From+1)/2)
			}
		}
		return err
	}
	return nil
}
