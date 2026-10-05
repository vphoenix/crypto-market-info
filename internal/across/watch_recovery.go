package across

import "context"

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

// RepairRawGapStep scans one bounded hole between already retained captures.
// It never asks for blocks preceding the original collection start or beyond
// the most recently captured block. Partial ABI/state coverage remains partial.
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
		last := covered[0].to
		for _, span := range covered[1:] {
			last = max(last, span.to)
		}
		window, ok := historyUncovered(covered[0].from, last, covered)
		if !ok {
			continue
		}
		window.To = window.From + min(uint64(c.Manifest.MaxLogBlocks), window.To-window.From+1) - 1
		c.rawRepairNext = (index + 1) % count
		_, err = c.CollectRange(ctx, chain, window.From, window.To, "catchup", "head", networkContext...)
		return err
	}
	return nil
}
