package justlendkeeper

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint"
)

type dynamicOrder struct {
	Candidate         Candidate
	Amount, Deposit   *big.Int
	BaseAt, FirstSeen time.Time
	SourceCapture     uuid.UUID
	SourceOrdinal     uint32
	ClosurePending    bool
	Conflicts         []RawEvent
}
type dynamicCheck struct {
	Generation                                               string
	Verified                                                 bool
	Attempts, Failures                                       uint8
	Revision                                                 string
	NextSeed, NextCheck, FairSince, LastAttempt, LastSuccess time.Time
	LatestStatus                                             string
	Liquidatable                                             *bool
	Deposit                                                  *big.Int
	ObservedAt                                               time.Time
	StateCapture                                             *uuid.UUID
}
type dynamicRound struct {
	ID       uuid.UUID
	Started  time.Time
	Slots    uint8
	Selected []string
	Members  []runtimecheckpoint.Reference
	Active   *SelectionMember
	Continue bool
	Closing  string
	// Used only by unit fixtures without a native runtime.
	members []SelectionMember
}
type dynamicWork struct {
	Version                         uint32
	Ready                           bool
	Cursor                          []byte
	RebuildStarted, RebuildFinished time.Time
	RebuildCaptures                 uint64
	HistoryTo                       time.Time
	HistoryDone                     map[string]bool
	HistoryNext                     time.Time
	HistoryTurn                     uint8
	CoverageScope                   string
	OrderRoot, CheckRoot            runtimecheckpoint.PageRoot
	NextRate, NextRound             time.Time
	Rate                            *RentalRateObservation
	Round                           *dynamicRound
	LastRound                       uuid.UUID
	LastRoundAt                     time.Time
	orders                          *runtimecheckpoint.PagedMap[dynamicOrder]
	checks                          *runtimecheckpoint.PagedMap[dynamicCheck]
	files                           *runtimecheckpoint.Store
	memoryOrders                    map[string]dynamicOrder
	memoryChecks                    map[string]dynamicCheck
	err                             error
	DiscoveryTimeRevision           uint8
}

func dynamicPair(can Candidate) string { return Hex(can.Renter) + Hex(can.Receiver) }
func generationBytes(can Candidate) string {
	b, _ := hex.DecodeString(researchGeneration(can))
	return string(b)
}
func copyInt(n *big.Int) *big.Int {
	if n == nil {
		return nil
	}
	return new(big.Int).Set(n)
}

func (w *dynamicWork) open(files *runtimecheckpoint.Store) error {
	if w.Version != 1 {
		return errors.New("unsupported_dynamic_state_version")
	}
	w.files = files
	var err error
	w.orders, err = runtimecheckpoint.OpenPagedMap[dynamicOrder](files, w.OrderRoot, 4<<20, func(v dynamicOrder) bool { return !v.Candidate.Closed })
	if err != nil {
		return err
	}
	w.checks, err = runtimecheckpoint.OpenPagedMap[dynamicCheck](files, w.CheckRoot, 2<<20, func(dynamicCheck) bool { return true }, func(v dynamicCheck) time.Time { return v.FairSince })
	return err
}
func (w *dynamicWork) references() ([]runtimecheckpoint.Reference, error) {
	if w.err != nil {
		return nil, w.err
	}
	var refs, more []runtimecheckpoint.Reference
	var err error
	w.OrderRoot, refs, err = w.orders.Snapshot()
	if err != nil {
		return nil, err
	}
	w.CheckRoot, more, err = w.checks.Snapshot()
	if err != nil {
		return nil, err
	}
	refs = append(refs, more...)
	if w.Round != nil {
		refs = append(refs, w.Round.Members...)
	}
	return refs, nil
}
func (w *dynamicWork) getOrder(key string) (dynamicOrder, bool) {
	if w.orders == nil {
		v, ok := w.memoryOrders[key]
		return v, ok
	}
	v, ok, err := w.orders.Get(key)
	if err != nil {
		w.err = err
	}
	return v, ok
}
func (w *dynamicWork) setOrder(key string, v dynamicOrder) {
	if w.orders == nil {
		if w.memoryOrders == nil {
			w.memoryOrders = map[string]dynamicOrder{}
		}
		w.memoryOrders[key] = v
		return
	}
	if err := w.orders.Set(key, v); err != nil {
		w.err = err
	}
}
func (w *dynamicWork) getCheck(key string, o dynamicOrder) dynamicCheck {
	var v dynamicCheck
	var ok bool
	if w.checks == nil {
		v, ok = w.memoryChecks[key]
	} else {
		var err error
		v, ok, err = w.checks.Get(key)
		if err != nil {
			w.err = err
		}
	}
	gen := generationBytes(o.Candidate)
	if !ok || v.Generation != gen {
		return dynamicCheck{Generation: gen, Verified: o.Candidate.Verified, Revision: seedRevision, FairSince: v.FairSince, LastAttempt: v.LastAttempt}
	}
	if v.Revision != seedRevision {
		v.Attempts = 0
		v.Revision = seedRevision
		v.NextSeed = time.Time{}
	}
	return v
}
func (w *dynamicWork) setCheck(key string, v dynamicCheck) {
	if w.checks == nil {
		if w.memoryChecks == nil {
			w.memoryChecks = map[string]dynamicCheck{}
		}
		w.memoryChecks[key] = v
		return
	}
	if err := w.checks.Set(key, v); err != nil {
		w.err = err
	}
}
func (w *dynamicWork) each(fn func(string, dynamicOrder) bool) {
	if w.orders == nil {
		keys := make([]string, 0, len(w.memoryOrders))
		for k := range w.memoryOrders {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			if !fn(k, w.memoryOrders[k]) {
				return
			}
		}
		return
	}
	for after := ""; ; {
		keys, rows, err := w.orders.Scan(after, 128, false)
		if err != nil {
			w.err = err
			return
		}
		if len(keys) == 0 {
			return
		}
		for i, k := range keys {
			if !fn(k, rows[i]) {
				return
			}
		}
		after = keys[len(keys)-1]
	}
}
func (w *dynamicWork) population() uint64 {
	if w.orders != nil {
		return w.orders.Len()
	}
	return uint64(len(w.memoryOrders))
}

func (c *Collector) dynamicApplyRaw(raw RawEvent, capture uuid.UUID, ordinal uint32, verified *RentalEvent) {
	w := c.State.Dynamic
	if w == nil {
		return
	}
	r, err := EventRow(raw)
	if err != nil {
		w.err = err
		return
	}
	can := Candidate{Renter: r.Renter, Receiver: r.Receiver, LastActivity: r.BlockTime, Origin: raw}
	key := dynamicPair(can)
	old, exists := w.getOrder(key)
	// SQL rebuilding is ordered by capture identity, not observation time. An
	// older source record can establish an earlier discovery without replacing
	// the current chain generation or resetting an already attempted member.
	if exists && !r.AvailableAt.IsZero() && (old.FirstSeen.IsZero() || r.AvailableAt.Before(old.FirstSeen)) {
		old.FirstSeen = r.AvailableAt
		w.setOrder(key, old)
		e := w.getCheck(key, old)
		if e.LastAttempt.IsZero() && (e.FairSince.IsZero() || r.AvailableAt.Before(e.FairSince)) {
			e.FairSince = r.AvailableAt
			w.setCheck(key, e)
		}
	}

	same := exists && old.Candidate.Origin.Transaction == raw.Transaction && old.Candidate.Origin.Index == raw.Index
	if exists && !same {
		if raw.BlockNumber < old.Candidate.Origin.BlockNumber {
			return
		}
		if raw.BlockNumber == old.Candidate.Origin.BlockNumber {
			if verified == nil || !old.Candidate.Verified {
				// Source indexes do not establish cross-transaction order. Keep a
				// deterministic candidate and require hydration, rather than guess.
				if raw.Transaction+fmt.Sprint(raw.Index) < old.Candidate.Origin.Transaction+fmt.Sprint(old.Candidate.Origin.Index) {
					old.Candidate.Ambiguous = true
					old.Conflicts = appendDynamicConflict(old.Conflicts, old.Candidate.Origin)
					old.Conflicts = appendDynamicConflict(old.Conflicts, raw)
					w.setOrder(key, old)
					return
				}
				can.Ambiguous = true
			} else if verified.TransactionIndex == nil || verified.ReceiptLogIndex == nil || *verified.TransactionIndex < old.Candidate.LastTx || *verified.TransactionIndex == old.Candidate.LastTx && *verified.ReceiptLogIndex <= old.Candidate.LastLog {
				return
			}
		}
	}
	if same && old.Candidate.Verified && verified == nil {
		return
	}
	if same {
		can = old.Candidate
	}
	can.Origin = raw
	can.LastActivity = r.BlockTime
	first := old.FirstSeen
	if first.IsZero() {
		first = r.AvailableAt
		if first.IsZero() {
			first = UTC(c.Now())
		}
	}
	can.Lifecycle = old.Candidate.Lifecycle
	closing := r.EventKind == "liquidate" || r.EventKind == "return" && r.AmountSun != nil && r.AmountSun.Sign() == 0
	if verified != nil && verifiedResearchEvent(*verified) {
		can.Verified = true
		can.Ambiguous = false
		can.LastNumber = verified.BlockNumber
		can.LastTx = *verified.TransactionIndex
		can.LastLog = *verified.ReceiptLogIndex
		can.Closed = closing
		if r.EventKind == "rent" && (old.Candidate.Closed || can.Lifecycle == "") {
			can.Lifecycle = Hex(verified.TxId) + fmt.Sprint(":", *verified.ReceiptLogIndex)
		}
	} else if !same {
		can.Verified = false
		can.Closed = false
	}
	o := dynamicOrder{Candidate: can, Amount: copyInt(r.AmountSun), Deposit: copyInt(r.SecurityDepositSun), BaseAt: r.BlockTime, FirstSeen: first, SourceCapture: capture, SourceOrdinal: ordinal, ClosurePending: closing && !can.Verified}
	if exists && raw.BlockNumber == old.Candidate.Origin.BlockNumber && (!same || len(old.Conflicts) > 0) {
		o.Conflicts = appendDynamicConflict(old.Conflicts, old.Candidate.Origin)
		o.Conflicts = appendDynamicConflict(o.Conflicts, raw)
		o.Candidate.Ambiguous = true
		o.Candidate.Verified = false
	}
	w.setOrder(key, o)
	check := w.getCheck(key, o)
	check.Verified = can.Verified
	if check.FairSince.IsZero() {
		check.FairSince = first
	}
	w.setCheck(key, check)
}
func (c *Collector) dynamicApplyBatch(b Batch) {
	if c.State.Dynamic == nil {
		return
	}
	for _, r := range b.IndexedEvents {
		raw, err := rawFromIndexed(r)
		if err != nil {
			c.State.Dynamic.err = err
			return
		}
		c.dynamicApplyRaw(raw, r.CaptureId, r.Ordinal, nil)
	}
	// Resolve same-block uncertainty only when this batch authenticates every
	// competing source origin. The receipt transaction/log positions establish
	// the winner; source hash order never establishes chain order.
	c.dynamicResolveConflicts(b.Events)
	for _, r := range b.Events {
		if verifiedResearchEvent(r) {
			row := r
			c.dynamicApplyRaw(RawFromRow(r), r.CaptureId, r.ProviderEventIndex, &row)
		}
	}
}

// RebuildDynamic is an explicit stopped-service, SQL-only conversion. It reads
// every committed indexed/verified capture, never the bounded 1,500 cache.
func (c *Collector) RebuildDynamic(ctx context.Context) error {
	c.scheduler()
	c.Log(fmt.Sprintf("dynamic_rebuild_start budget_day=%s used=%d", c.State.Day, c.State.Used))
	if c.runtime == nil {
		return errors.New("dynamic_selection_requires_native_runtime")
	}
	pager, ok := c.Store.(runtimeCapturePager)
	if !ok {
		return errors.New("dynamic_rebuild_requires_capture_pager")
	}
	reader, ok := c.Store.(interface {
		KeeperBatch(context.Context, Capture) (Batch, error)
	})
	if !ok {
		return errors.New("dynamic_rebuild_requires_typed_reader")
	}
	if c.State.Dynamic == nil {
		c.State.Dynamic = &dynamicWork{Version: 1, RebuildStarted: UTC(c.Now()), CoverageScope: "all_discovered_orders_history_incomplete"}
		if err := c.State.Dynamic.open(c.runtime.files); err != nil {
			return err
		}
	}
	w := c.State.Dynamic
	if w.Ready && w.DiscoveryTimeRevision >= 1 {
		return nil
	}
	if w.Ready {
		w.Ready = false
		w.Cursor = nil
		w.RebuildCaptures = 0
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		caps, next, err := pager.KeeperCapturePage(ctx, c.Config.Hash(), w.Cursor, 500)
		if err != nil {
			return err
		}
		if len(caps) == 0 {
			break
		}
		if len(next) != 16 || bytes.Compare(next, w.Cursor) <= 0 {
			return errors.New("dynamic_rebuild_cursor_not_advancing")
		}
		wanted := []Capture{}
		for _, cap := range caps {
			if (cap.CaptureKind == "event_index" || cap.CaptureKind == "enrichment" || cap.CaptureKind == "events") && cap.Status == "complete" && cap.IndexedRows+cap.EventRows > 0 {
				wanted = append(wanted, cap)
			}
		}
		for i := 0; i < len(wanted); i += 32 {
			group := wanted[i:min(i+32, len(wanted))]
			if bulk, ok := c.Store.(interface {
				KeeperBatches(context.Context, []Capture) (map[uuid.UUID]Batch, error)
			}); ok {
				batches, err := bulk.KeeperBatches(ctx, group)
				if err != nil {
					return err
				}
				for _, cap := range group {
					b, ok := batches[cap.CaptureId]
					if !ok || !captureEqual(cap, b.Capture) {
						return errors.New("dynamic_rebuild_capture_mismatch")
					}
					if err = Validate(b); err != nil {
						return err
					}
					c.dynamicApplyBatch(b)
					w.RebuildCaptures++
				}
			} else {
				for _, cap := range group {
					b, err := reader.KeeperBatch(ctx, cap)
					if err != nil {
						return err
					}
					if err = Validate(b); err != nil {
						return err
					}
					c.dynamicApplyBatch(b)
					w.RebuildCaptures++
				}
			}
		}

		w.Cursor = append([]byte(nil), next...)
		if err := c.Save(); err != nil {
			return err
		}
		c.Log(fmt.Sprintf("dynamic_rebuild captures=%d population=%d", w.RebuildCaptures, w.population()))
	}
	// Retain legacy discoveries as unverified hints, never as population proof.
	for _, pool := range [][]Candidate{c.State.Candidates, c.State.Cohort} {
		for _, can := range pool {
			c.dynamicApplyRaw(can.Origin, uuid.Nil, can.Origin.Ordinal, nil)
		}
	}
	// Preserve historical verification retry caps before retiring the old ledger.
	w.each(func(key string, o dynamicOrder) bool {
		e := w.getCheck(key, o)
		if c.State.Scheduler != nil {
			if old, ok := c.State.Scheduler.Ledger[researchGeneration(o.Candidate)]; ok && old.Revision == seedRevision {
				e.Attempts = max(e.Attempts, old.Attempts)
				if old.NextSeed.After(e.NextSeed) {
					e.NextSeed = old.NextSeed
				}
			}
		}
		for _, can := range c.State.Cohort {
			if researchGeneration(can) == researchGeneration(o.Candidate) && can.SeedRevision == seedRevision {
				e.Attempts = max(e.Attempts, can.SeedAttempts)
				if can.SeedNext.After(e.NextSeed) {
					e.NextSeed = can.SeedNext
				}
			}
		}
		w.setCheck(key, e)
		return true
	})
	c.scheduler().Ledger = map[string]ResearchEntry{}
	w.DiscoveryTimeRevision = 1
	w.Ready = true
	w.RebuildFinished = UTC(c.Now())
	c.Log(fmt.Sprintf("dynamic_rebuild_ready population=%d budget_day=%s used=%d", w.population(), c.State.Day, c.State.Used))
	return c.Save()
}

func (c *Collector) dynamicCoverage() time.Time {
	var oldest time.Time
	for _, k := range []string{"RentResource", "ReturnResource", "Liquidate"} {
		v := c.State.EventCursors[k]
		if v.IsZero() {
			return time.Time{}
		}
		if oldest.IsZero() || v.Before(oldest) {
			oldest = v
		}
	}
	return oldest
}

func appendDynamicConflict(rows []RawEvent, r RawEvent) []RawEvent {
	for _, old := range rows {
		if old.Transaction == r.Transaction && old.Index == r.Index {
			return rows
		}
	}
	return append(rows, r)
}
func (c *Collector) dynamicResolveConflicts(events []RentalEvent) {
	w := c.State.Dynamic
	done := map[string]bool{}
	for _, ev := range events {
		key := dynamicPair(Candidate{Renter: ev.Renter, Receiver: ev.Receiver})
		if done[key] {
			continue
		}
		done[key] = true
		old, ok := w.getOrder(key)
		if !ok || len(old.Conflicts) == 0 {
			continue
		}
		var newest *RentalEvent
		complete := true
		for _, raw := range old.Conflicts {
			found := false
			for i := range events {
				r := &events[i]
				if verifiedResearchEvent(*r) && r.Renter == ev.Renter && r.Receiver == ev.Receiver && Hex(r.TxId) == raw.Transaction && r.ProviderEventIndex == raw.Index {
					found = true
					if newest == nil || r.BlockNumber > newest.BlockNumber || r.BlockNumber == newest.BlockNumber && (*r.TransactionIndex > *newest.TransactionIndex || *r.TransactionIndex == *newest.TransactionIndex && *r.ReceiptLogIndex > *newest.ReceiptLogIndex) {
						newest = r
					}
					break
				}
			}
			if !found {
				complete = false
				break
			}
		}
		if complete && newest != nil {
			old.Conflicts = nil
			old.Candidate.Ambiguous = false
			old.Candidate.Verified = false
			old.Candidate.Origin = RawFromRow(*newest)
			w.setOrder(key, old)
			c.dynamicApplyRaw(RawFromRow(*newest), newest.CaptureId, newest.ProviderEventIndex, newest)
		}
	}
}
