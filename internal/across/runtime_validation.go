package across

import (
	"fmt"
	"math"
	"reflect"

	"github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint"
)

// Check semantic structure as well as the file checksum before admitting work.
// This is a local working-set check; it never reconstructs database history.
func (s *runtimeState) validate(m Manifest, binding runtimecheckpoint.Binding) error {
	fail := func(why string) error { return fmt.Errorf("across runtime %s: %w", why, runtimecheckpoint.ErrRecovery) }
	if err := s.validateConversion(m, binding); err != nil {
		return err
	}
	if len(s.Chains) != len(m.Chains) || s.Active == nil || s.Retired == nil || s.Partial == nil || s.Receipts == nil || s.Watch.Orders == nil || s.Watch.Cursors == nil || s.Skipped == nil || s.ProbeAdmission == nil || s.ReceiptAttempts == nil || s.Finalized == nil || s.PartialRetry == nil || s.RawRepairBlocks == nil || s.Invalidations == nil {
		return fail("missing working set")
	}
	for _, chain := range m.Chains {
		ch := s.Chains[chain.ChainID]
		if ch == nil || ch.Started && (ch.Start > ch.End || ch.End == math.MaxUint64) {
			return fail("chain bounds")
		}
		for _, ranges := range [][]RuntimeRange{ch.Raw, ch.Decoded} {
			for i, r := range ranges {
				if r.From > r.To || r.To == math.MaxUint64 || !ch.Started || r.From < ch.Start || r.To > ch.End || i > 0 && r.From <= ranges[i-1].To+1 {
					return fail("coverage intervals")
				}
			}
		}
		for _, r := range ch.Decoded {
			if firstUncovered(r.From, r.To, intervalSlice(ch.Raw)) <= r.To {
				return fail("decoded without raw")
			}
		}
		if ch.Anchor != nil {
			c := *ch.Anchor
			if c.ManifestHash != m.Hash || c.ChainId != chain.ChainID || !anchored(c) || !c.Canonical || !c.Committed || c.Finality != "finalized" || c.Revision == 0 {
				return fail("finalized anchor")
			}
		}
		if ch.Started {
			next := firstUncovered(ch.Start, ch.End, s.coverage(chain.ChainID, false))
			cursor := uint64(0)
			if next > 0 {
				cursor = next - 1
			}
			if s.Watch.Cursors[chain.ChainID] != cursor {
				return fail("continuous cursor")
			}
		}
	}
	var captureError error
	for id, job := range s.Invalidations {
		ch := s.Chains[job.Chain]
		if id != ID(job) || ch == nil || len(job.Bad) == 0 || len(job.Bad) > 2 || job.Reason == "" {
			return fail("reorg recovery job")
		}
		for _, n := range job.Bad {
			if ch.Anchor != nil && n <= *ch.Anchor.ToBlock {
				return fail("finalized reorg")
			}
		}
	}
	s.eachRetry(func(key string, r runtimeRetry) {
		if key == "" || s.Chains[r.Window.ChainID] == nil || r.Window.From > r.Window.To {
			captureError = fail("partial retry identity")
		}
	})
	s.eachCapture(false, func(c Capture) {
		if c.ManifestHash != m.Hash || s.Chains[c.ChainId] == nil || !anchored(c) || !c.Committed || !c.Canonical || c.Finality == "finalized" || c.Revision == 0 {
			captureError = fail("active capture")
		}
	})
	s.eachCapture(true, func(c Capture) {
		if c.ManifestHash != m.Hash || !runtimeRaw(c) || c.Status != "partial" || firstUncovered(*c.FromBlock, *c.ToBlock, s.coverage(c.ChainId, true)) > *c.ToBlock {
			captureError = fail("partial repair")
		}
	})
	if captureError != nil {
		return captureError
	}
	if s.captureErr != nil {
		return s.captureErr
	}
	var receiptError error
	s.eachReceipt(func(id string, r runtimeReceipt) {
		if id != ID(r.Task) || s.Chains[r.Task.ChainID] == nil || r.Parents == nil || len(r.Task.BlockHash) != 32 || len(r.Task.TxHash) != 32 {
			receiptError = fail("receipt identity")
			return
		}
		recomputed := r
		recomputed.recompute()
		if !reflect.DeepEqual(r, recomputed) {
			receiptError = fail("receipt dependency flags")
		}
	})
	if receiptError != nil {
		return receiptError
	}
	if s.receiptErr != nil {
		return s.receiptErr
	}
	for key, order := range s.Watch.Orders {
		if order == nil || key != orderKey(order.Deposit) || !route(order.Deposit, m) {
			return fail("order identity")
		}
	}
	seen := map[string]bool{}
	for _, task := range s.Watch.Tasks {
		id := runtimeProbeTaskID(task)
		if s.Watch.Orders[task.Key] == nil || task.Planned.IsZero() || seen[id] {
			return fail("probe parent/task identity")
		}
		seen[id] = true
	}
	return nil
}

func (s *runtimeState) validateConversion(m Manifest, binding runtimecheckpoint.Binding) error {
	if s.Version != 1 || s.Manifest != m.Hash || binding.ScopeHash != Hex(m.Hash) || binding.StateVersion != 1 || !s.Conversion.Verified || s.Conversion.Version != 1 || s.Conversion.Binding != binding || len(s.Conversion.InputDigest) != 64 {
		return fmt.Errorf("across runtime scope or verified conversion: %w", runtimecheckpoint.ErrRecovery)
	}
	return nil
}
