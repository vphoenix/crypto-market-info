package across

import (
	"errors"
	"github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint"
	"math"
	"sort"
	"strings"
	"time"
)

// RuntimeRange contains proven inclusive coverage, not a synthetic capture.
// Only finalized coverage may be folded into these unions; the active branch
// retains exact capture identities so revisions can remove one contribution.
type RuntimeRange struct{ From, To uint64 }
type runtimeChain struct {
	Raw, Decoded               []RuntimeRange
	CurrentRaw, CurrentDecoded []RuntimeRange
	Anchor                     *Capture
	Started                    bool
	Start, End                 uint64
}
type runtimeReceipt struct {
	Task                                           receiptTask
	Block                                          uint64
	Refund, Known, FeeComplete, Transfers, Pending bool
	RetryUnknown                                   bool
	Parents                                        map[string]uint8
	FirstObserved, LastAttempt                     time.Time
}
type runtimeRetry struct {
	Window    HistoryRange
	At        time.Time
	Failures  uint
	Attempted bool
}

// runtimeState holds outstanding work and compact finalized coverage. It is
// owned by runtimeOwner; workers receive copies and never mutate these maps.
type runtimeState struct {
	Version                          uint32
	Conversion                       RuntimeConversionReport
	Manifest                         string
	Chains                           map[uint64]*runtimeChain
	Active                           map[string]Capture
	Retired                          map[string]Capture // only the bounded recovery tail
	Partial                          map[string]Capture
	ActiveRoot, PartialRoot          runtimeCaptureRoot
	Invalidations                    map[string]runtimeInvalidation
	activePages, partialPages        *runtimeCapturePages
	captureErr                       error
	Receipts                         map[string]runtimeReceipt
	ReceiptRoot, ReceiptScheduleRoot runtimecheckpoint.PageRoot
	ReceiptCleanupAfter              string
	receiptPages                     *runtimecheckpoint.PagedMap[runtimeReceipt]
	receiptSchedule                  *runtimecheckpoint.PagedMap[string]
	receiptParents                   map[string]map[string]bool
	receiptErr                       error
	Watch                            watcher
	Skipped                          map[string]pendingOrder
	ProbeAdmission                   map[string]time.Time
	ReceiptAttempts                  map[string]time.Time
	Finalized                        map[uint64]Block
	PartialRetry                     map[string]runtimeRetry
	RetryRoot, SkippedRoot           runtimecheckpoint.PageRoot
	RetryCleanupAfter                string
	retryPages                       *runtimecheckpoint.PagedMap[runtimeRetry]
	skippedPages                     *runtimecheckpoint.PagedMap[pendingOrder]
	pendingErr                       error
	RawRepairBlocks                  map[uint64]uint64
	RawRepairNext, PartialNext       int
}

func newRuntimeState(m Manifest) runtimeState {
	s := runtimeState{Version: 1, Manifest: m.Hash, Chains: map[uint64]*runtimeChain{}, Active: map[string]Capture{}, Partial: map[string]Capture{}, Receipts: map[string]runtimeReceipt{}, Watch: watcher{Orders: map[string]*pendingOrder{}, Cursors: map[uint64]uint64{}}, Finalized: map[uint64]Block{}, PartialRetry: map[string]runtimeRetry{}, RawRepairBlocks: map[uint64]uint64{}}
	for _, ch := range m.Chains {
		s.Chains[ch.ChainID] = &runtimeChain{}
	}
	s.Skipped = map[string]pendingOrder{}
	s.Retired = map[string]Capture{}
	s.ProbeAdmission = map[string]time.Time{}
	s.ReceiptAttempts = map[string]time.Time{}
	s.Invalidations = map[string]runtimeInvalidation{}
	return s
}

func mergeRuntimeRanges(in []RuntimeRange) []RuntimeRange {
	if len(in) == 0 {
		return nil
	}
	sort.Slice(in, func(i, j int) bool {
		if in[i].From == in[j].From {
			return in[i].To < in[j].To
		}
		return in[i].From < in[j].From
	})
	out := in[:0]
	for _, r := range in {
		if len(out) == 0 || out[len(out)-1].To != math.MaxUint64 && r.From > out[len(out)-1].To+1 {
			out = append(out, r)
		} else if r.To > out[len(out)-1].To {
			out[len(out)-1].To = r.To
		}
	}
	return out
}
func intervalSlice(in []RuntimeRange) []blockInterval {
	out := make([]blockInterval, len(in))
	for i, r := range in {
		out[i] = blockInterval{r.From, r.To}
	}
	return out
}
func runtimeRaw(c Capture) bool {
	return c.Canonical && c.Committed && c.CaptureKind == "logs" && c.CompletedTasks == 1 && anchored(c)
}
func runtimeDecoded(c Capture) bool {
	return runtimeRaw(c) && c.Status == "complete" && c.UnknownEventCount == 0
}

func (s *runtimeState) coverage(chain uint64, decoded bool) []blockInterval {
	ch := s.Chains[chain]
	if ch == nil {
		return nil
	}
	if s.activePages != nil {
		if decoded {
			return intervalSlice(ch.CurrentDecoded)
		}
		return intervalSlice(ch.CurrentRaw)
	}
	base := ch.Raw
	if decoded {
		base = ch.Decoded
	}
	work := append([]RuntimeRange(nil), base...)
	for _, c := range s.Active {
		if c.ChainId == chain && runtimeRaw(c) && (!decoded || runtimeDecoded(c)) && !s.invalidated(c) {
			work = append(work, RuntimeRange{*c.FromBlock, *c.ToBlock})
		}
	}
	return intervalSlice(mergeRuntimeRanges(work))
}
func (s *runtimeState) decodedBlock(chain, block uint64) bool {
	return firstUncovered(block, block, s.coverage(chain, true)) > block
}

// An unfinalized height alone never identifies an event's branch. Repeated
// events there are deduplicated by their block-hash/task identity, not by height.
func (s *runtimeState) finalizedDecodedBlock(chain, block uint64) bool {
	ch := s.Chains[chain]
	return ch != nil && firstUncovered(block, block, intervalSlice(ch.Decoded)) > block
}

func (s *runtimeState) capture(c Capture) error {
	if c.ManifestHash != s.Manifest || s.Chains[c.ChainId] == nil || !c.Committed || c.Revision == 0 || (anchored(c) && (*c.FromBlock > *c.ToBlock || *c.ToBlock == math.MaxUint64)) {
		return errors.New("runtime_capture_scope_or_commit")
	}
	if c.Canonical && c.Finality == "finalized" && s.invalidated(c) {
		return errors.New("finalized_hash_conflict")
	}
	old, wasActive := s.captureGet(c.CaptureId, false)
	if wasActive {
		if c.Revision < old.Revision {
			return nil
		}
		if c.Revision == old.Revision && ID(old) != ID(c) {
			return errors.New("runtime_capture_revision_conflict")
		}
	}
	ch := s.Chains[c.ChainId]
	if runtimeRaw(c) {
		if !ch.Started {
			ch.Start, ch.End, ch.Started = *c.FromBlock, *c.ToBlock, true
		} else {
			ch.Start = min(ch.Start, *c.FromBlock)
			ch.End = max(ch.End, *c.ToBlock)
		}
	}
	if c.Canonical && c.Finality == "finalized" && anchored(c) {
		if runtimeRaw(c) {
			ch.Raw = mergeRuntimeRanges(append(ch.Raw, RuntimeRange{*c.FromBlock, *c.ToBlock}))
		}
		if runtimeDecoded(c) {
			ch.Decoded = mergeRuntimeRanges(append(ch.Decoded, RuntimeRange{*c.FromBlock, *c.ToBlock}))
		}
		if ch.Anchor == nil || *c.ToBlock > *ch.Anchor.ToBlock || *c.ToBlock == *ch.Anchor.ToBlock && (c.StartedAt.Before(ch.Anchor.StartedAt) || c.StartedAt.Equal(ch.Anchor.StartedAt) && c.CaptureId < ch.Anchor.CaptureId) {
			v := c
			ch.Anchor = &v
		}
		s.captureDelete(c.CaptureId, false)
	} else if c.Canonical && anchored(c) {
		s.captureSet(c, false)
	} else {
		s.captureDelete(c.CaptureId, false)
	}
	if s.activePages != nil {
		if !c.Canonical || wasActive && (runtimeRaw(old) && !runtimeRaw(c) || runtimeDecoded(old) && !runtimeDecoded(c)) {
			s.rebuildCoverage(c.ChainId)
		} else if !s.invalidated(c) {
			if runtimeRaw(c) {
				ch.CurrentRaw = mergeRuntimeRanges(append(ch.CurrentRaw, RuntimeRange{*c.FromBlock, *c.ToBlock}))
			}
			if runtimeDecoded(c) {
				ch.CurrentDecoded = mergeRuntimeRanges(append(ch.CurrentDecoded, RuntimeRange{*c.FromBlock, *c.ToBlock}))
			}
		}
	}
	if runtimeRaw(c) && c.Status == "partial" && !strings.Contains(c.Reason, repairAbandonedMarker) {
		if firstUncovered(*c.FromBlock, *c.ToBlock, s.coverage(c.ChainId, true)) <= *c.ToBlock {
			s.captureSet(c, true)
		}
	} else {
		s.captureDelete(c.CaptureId, true)
	}
	// A fully decoded replacement covers a partial attempt's work without
	// retaining that completed attempt as a permanent historical cache.
	if runtimeDecoded(c) {
		rr := s.coverage(c.ChainId, true)
		s.eachCapture(true, func(p Capture) {
			if p.ChainId == c.ChainId && firstUncovered(*p.FromBlock, *p.ToBlock, rr) > *p.ToBlock {
				s.captureDelete(p.CaptureId, true)
			}
		})
	}
	if ch.Started {
		next := firstUncovered(ch.Start, ch.End, s.coverage(c.ChainId, false))
		if next > 0 {
			s.Watch.Cursors[c.ChainId] = next - 1
		} else {
			s.Watch.Cursors[c.ChainId] = 0
		}
	}
	return s.captureErr
}

func (s *runtimeState) newestRawGap(chain uint64) (HistoryRange, bool) {
	ch := s.Chains[chain]
	if ch == nil || !ch.Started {
		return HistoryRange{}, false
	}
	next := ch.Start
	var gap HistoryRange
	found := false
	for _, r := range s.coverage(chain, false) {
		if r.to < next {
			continue
		}
		if r.from > next {
			gap = HistoryRange{ChainID: chain, From: next, To: r.from - 1}
			found = true
		}
		next = max(next, r.to+1)
	}
	if next <= ch.End {
		gap = HistoryRange{ChainID: chain, From: next, To: ch.End}
		found = true
	}
	return gap, found
}

// revise accepts the row actually confirmed by storage, which may be newer
// than a worker's proposed revision. A stale successful SQL no-op cannot move
// local canonical/finality backwards.
func (s *runtimeState) revise(expected uint64, c Capture) error {
	old, ok := s.captureGet(c.CaptureId, false)
	if !ok {
		old, ok = s.captureGet(c.CaptureId, true)
	}
	if !ok {
		old, ok = s.Retired[c.CaptureId]
	}
	if !ok {
		if ch := s.Chains[c.ChainId]; ch != nil && ch.Anchor != nil && ch.Anchor.CaptureId == c.CaptureId {
			old = *ch.Anchor
			ok = true
		}
	}
	if !ok {
		return errors.New("runtime_revision_identity_requires_verification")
	}
	if c.Revision < old.Revision {
		return nil
	}
	if c.Revision == old.Revision {
		if ID(c) != ID(old) {
			return errors.New("runtime_revision_content_conflict")
		}
		return nil
	}
	if expected > old.Revision {
		return errors.New("runtime_revision_predecessor_missing")
	}
	if old.Finality == "finalized" && (!c.Canonical || c.Finality != "finalized") {
		return errors.New("finalized_hash_conflict")
	}
	if old.FromHash != nil && (c.FromHash == nil || *old.FromHash != *c.FromHash || *old.ToHash != *c.ToHash) {
		return errors.New("runtime_revision_changed_immutable_anchor")
	}
	if err := s.capture(c); err != nil {
		return err
	}
	s.sealReceiptParents(c)
	if !c.Canonical {
		// Receipt identities include the branch hash. Only dependencies anchored
		// in the invalidated range are dropped; other branch tasks remain intact.
		for _, id := range s.parentReceipts(c.CaptureId) {
			r, _ := s.receiptGet(id)
			delete(r.Parents, c.CaptureId)
			r.recompute()
			if len(r.Parents) == 0 {
				s.receiptDelete(id)
			} else {
				s.receiptSet(id, r)
			}
		}
		for _, o := range s.Watch.Orders {
			if o.Deposit.CaptureId == c.CaptureId {
				o.Terminal = true
			}
		}
		if runtimeRaw(old) {
			rr := s.coverage(c.ChainId, false)
			n := firstUncovered(*old.FromBlock, *old.ToBlock, rr)
			if n <= *old.ToBlock && n > 0 && n <= s.Watch.Cursors[c.ChainId] {
				s.Watch.Cursors[c.ChainId] = n - 1
			}
		}
	}
	return nil
}

const (
	runtimeReceiptEvent uint8 = 1 << iota
	runtimeReceiptRefund
	runtimeReceiptKnown
	runtimeReceiptFee
	runtimeReceiptTransfers
)

func (r *runtimeReceipt) recompute() {
	var bits uint8
	for _, v := range r.Parents {
		bits |= v
	}
	r.Refund = bits&runtimeReceiptRefund != 0
	r.Known = bits&runtimeReceiptKnown != 0
	r.FeeComplete = bits&runtimeReceiptFee != 0
	r.Transfers = bits&runtimeReceiptTransfers != 0
	r.Pending = bits&runtimeReceiptEvent != 0 && !r.Known || r.Refund && !r.Transfers || r.RetryUnknown
}
func (s *runtimeState) receiptFact(task receiptTask, block uint64, parent string, bits uint8, observed ...time.Time) {
	id := ID(task)
	r, _ := s.receiptGet(id)
	if r.FirstObserved.IsZero() {
		if len(observed) > 0 {
			r.FirstObserved = observed[0]
		} else {
			r.FirstObserved = Now()
		}
	}
	r.Task = task
	r.Block = block
	if r.Parents == nil {
		r.Parents = map[string]uint8{}
	}
	if bits&runtimeReceiptKnown != 0 {
		r.RetryUnknown = false
	}
	r.Parents[parent] |= bits
	r.recompute()
	s.receiptSet(id, r)
}
func (s *runtimeState) sealReceiptParents(c Capture) {
	if !c.Canonical || c.Finality != "finalized" {
		return
	}
	for _, id := range s.parentReceipts(c.CaptureId) {
		r, _ := s.receiptGet(id)
		if bits, ok := r.Parents[c.CaptureId]; ok {
			r.Parents[""] |= bits
			delete(r.Parents, c.CaptureId)
			s.receiptSet(id, r)
		}
	}
}

// batch derives child work from the frozen, committed input before adding its
// coverage. All of these changes are checkpointed together with the operation.
func (s *runtimeState) batch(b Batch, m Manifest, restoring bool) error {
	c := b.Capture
	if c.ManifestHash != s.Manifest {
		return errors.New("runtime_batch_scope")
	}
	if c.Canonical && c.Committed {
		for _, d := range b.Deposits {
			if !s.finalizedDecodedBlock(d.ChainId, d.BlockNumber) {
				origin := "catchup"
				if restoring {
					origin = "restart"
				} else if d.LiveReceivedAt != nil {
					origin = "live"
				}
				if !s.Watch.add(d, origin, m) {
					// Preserve the original MaxPending policy's skipped_budget
					// observation as child work until that observation commits.
					s.skippedSet(orderKey(d), pendingOrder{Deposit: d, Origin: origin})
				}
			}
		}
		for _, f := range b.Fills {
			if ch, ok := m.Chain(f.ChainId); ok && f.OutputToken == WordAddress(ch.USDC) && !s.finalizedDecodedBlock(f.ChainId, f.BlockNumber) {
				s.receiptFact(receiptTask{f.ChainId, f.BlockHash, f.TxHash}, f.BlockNumber, c.CaptureId, runtimeReceiptEvent, c.AvailableAt)
			}
		}
		for _, r := range b.Refunds {
			if ch, ok := m.Chain(r.ChainId); ok && r.Token == ch.USDC && !s.finalizedDecodedBlock(r.ChainId, r.BlockNumber) {
				s.receiptFact(receiptTask{r.ChainId, r.BlockHash, r.TxHash}, r.BlockNumber, c.CaptureId, runtimeReceiptEvent|runtimeReceiptRefund, c.AvailableAt)
			}
		}
		for _, tr := range b.Transfers {
			s.receiptFact(receiptTask{tr.ChainId, tr.BlockHash, tr.TxHash}, tr.BlockNumber, c.CaptureId, runtimeReceiptTransfers, c.AvailableAt)
		}
		if c.CaptureKind == "receipts" && c.CompletedTasks == 1 {
			for _, rec := range b.Receipts {
				bits := runtimeReceiptKnown
				if rec.FeeComplete {
					bits |= runtimeReceiptFee
				}
				s.receiptFact(receiptTask{rec.ChainId, rec.BlockHash, rec.TxHash}, rec.BlockNumber, c.CaptureId, bits, c.AvailableAt)
			}
		}
	}
	if err := s.capture(c); err != nil {
		return err
	}
	s.sealReceiptParents(c)
	// Completed identities are needed only while another normal partial scan
	// can rediscover their source event. The exact receipt facts remain in SQL.
	s.pruneReceipts()
	return s.ioError()
}

func (s *runtimeState) captureView() []Capture {
	byID := map[string]Capture{}
	s.eachCapture(false, func(c Capture) { byID[c.CaptureId] = c })
	s.eachCapture(true, func(c Capture) { byID[c.CaptureId] = c })
	for _, ch := range s.Chains {
		if ch.Anchor != nil {
			byID[ch.Anchor.CaptureId] = *ch.Anchor
		}
	}
	out := make([]Capture, 0, len(byID))
	for _, c := range byID {
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].StartedAt.Equal(out[j].StartedAt) {
			return out[i].CaptureId < out[j].CaptureId
		}
		return out[i].StartedAt.Before(out[j].StartedAt)
	})
	return out
}
