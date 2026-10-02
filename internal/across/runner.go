package across

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

type pendingOrder struct {
	Deposit   Deposit
	Origin    string
	Terminal  bool
	Followups bool
}
type probeTask struct {
	Key     string
	Planned time.Time
	Delay   *uint32
	Kind    string
}
type watcher struct {
	Orders  map[string]*pendingOrder
	Tasks   []probeTask
	Cursors map[uint64]uint64
}

func orderKey(d Deposit) string {
	return ID(struct {
		Chain       uint64
		Hash, Block string
	}{d.ChainId, d.RelayHash, d.BlockHash})
}
func route(d Deposit, m Manifest) bool {
	a, ok := m.Chain(d.ChainId)
	if !ok {
		return false
	}
	z, ok := m.Chain(d.DestinationChainId)
	return ok && a.ChainID != z.ChainID && d.SpokePool == a.SpokePool && d.InputToken == WordAddress(a.USDC) && d.OutputToken == WordAddress(z.USDC) && d.Message == ""
}
func exclusive(d Deposit) bool {
	return d.ExclusiveRelayer != strings.Repeat("\x00", 32) && d.ExclusivityDeadline != 0
}
func ProbeOpen(d Deposit, p OrderProbe) bool {
	return p.ProbeStatus == "ok" && p.FillStatus != nil && *p.FillStatus < 2 && p.PausedFills != nil && !*p.PausedFills && p.ContractTime != nil && *p.ContractTime <= uint64(d.FillDeadline) && (!exclusive(d) || *p.ContractTime > uint64(d.ExclusivityDeadline))
}
func (w *watcher) add(d Deposit, origin string, m Manifest) bool {
	if !route(d, m) || uint64(d.FillDeadline) < uint64(Now().Unix()) {
		return true
	}
	k := orderKey(d)
	if _, ok := w.Orders[k]; ok {
		return true
	}
	if len(w.Orders) >= int(m.MaxPending) {
		return false
	}
	p := &pendingOrder{Deposit: d, Origin: origin}
	w.Orders[k] = p
	w.Tasks = append(w.Tasks, probeTask{k, Now(), nil, "baseline"})
	if origin == "live" && !exclusive(d) {
		w.follow(k, d.AvailableAt)
		p.Followups = true
	}
	return true
}
func (w *watcher) follow(k string, base time.Time) {
	for _, d := range []uint32{2000, 5000, 10000} {
		w.Tasks = append(w.Tasks, probeTask{k, base.Add(time.Duration(d) * time.Millisecond), Ptr(d), "followup"})
	}
}
func (c *Collector) restore(ctx context.Context) (*watcher, error) {
	if e := c.load(ctx); e != nil {
		return nil, e
	}
	w := &watcher{Orders: map[string]*pendingOrder{}, Cursors: map[uint64]uint64{}}
	// Restore only canonical committed evidence; restored orders never inherit live continuity.
	for _, cap := range c.captures {
		if !cap.Canonical || !cap.Committed {
			continue
		}

		if cap.CaptureKind != "logs" && cap.CaptureKind != "receipts" {
			continue
		}
		b, e := c.Store.AcrossBatch(ctx, cap)
		if e != nil {
			return nil, e
		}
		for _, d := range b.Deposits {
			w.add(d, "restart", c.Manifest)
		}
		for _, f := range b.Fills {
			ch, ok := c.Manifest.Chain(f.ChainId)
			if ok && f.OutputToken == WordAddress(ch.USDC) {
				c.queueReceipt(f.ChainId, f.BlockHash, f.TxHash)
			}
		}
		for _, r := range b.Refunds {
			ch, ok := c.Manifest.Chain(r.ChainId)
			if ok && r.Token == ch.USDC {
				c.queueReceipt(r.ChainId, r.BlockHash, r.TxHash)
			}
		}
	}
	// Receipt retries are recoverable from the immutable event facts on restart.
	for _, cap := range c.captures {
		if cap.Canonical && cap.Committed && cap.CaptureKind == "receipts" && cap.CompletedTasks == 1 {
			b, e := c.Store.AcrossBatch(ctx, cap)
			if e != nil {
				return nil, e
			}
			for _, r := range b.Receipts {
				delete(c.receiptQueue, ID(receiptTask{r.ChainId, r.BlockHash, r.TxHash}))
			}
		}
	}
	for _, chain := range c.Manifest.Chains {
		rr := logIntervals(c.captures, chain.ChainID)
		if len(rr) > 0 {
			next := firstUncovered(rr[0].from, rr[len(rr)-1].to, rr)
			w.Cursors[chain.ChainID] = next - 1
		}
	}
	return w, nil
}
func (c *Collector) recordProbe(ctx context.Context, o *pendingOrder, t probeTask, forced string, networkContext ...context.Context) (OrderProbe, error) {
	rpcCtx := ctx
	if len(networkContext) > 0 {
		rpcCtx = networkContext[0]
	}
	d := o.Deposit
	r := c.Readers[d.DestinationChainId]
	r.Members = nil
	b := c.newBatch(r, "probes", o.Origin)
	b.Capture.ExpectedTasks = 1
	p := OrderProbe{CaptureId: b.Capture.CaptureId, ProbeId: ID(struct {
		C  string
		K  string
		At time.Time
	}{b.Capture.CaptureId, t.Key, t.Planned}), ChainId: d.DestinationChainId, OriginChainId: d.ChainId, OriginSpokePool: d.SpokePool, OriginBlockHash: d.BlockHash, DepositId: d.DepositId, RelayHash: d.RelayHash, DestinationSpokePool: r.Chain.SpokePool, PlannedForAt: t.Planned, TargetDelayMs: t.Delay, RequestedAt: Now(), AvailableAt: Now(), ObservationOrigin: o.Origin, ProbeStatus: forced}
	headers := []EvidenceHeader{}
	if forced == "" {
		// Source branch eligibility is checked as well as target branch state.
		source := c.Readers[d.ChainId]
		origin, e := source.Header(rpcCtx, height(d.BlockNumber))
		if len(origin.PayloadHash) == 32 && origin.PayloadHash != strings.Repeat("\x00", 32) {
			r.Members = append(r.Members, origin.PayloadHash)
		}
		if e == nil {
			headers = append(headers, evidenceHeader(origin))
		}
		if e != nil {
			p.ProbeStatus = "rpc_error"
			p.Reason = "source_canonical_unknown"
		} else if origin.Hash != d.BlockHash {
			p.ProbeStatus = "cancelled_terminal"
			p.Reason = "source_orphaned"
			o.Terminal = true
		} else {
			h, e := r.Header(rpcCtx, "latest")
			if e != nil {
				p.ProbeStatus = "rpc_error"
				p.Reason = e.Error()
			} else {
				headers = append(headers, evidenceHeader(h))
				anchorCapture(&b.Capture, h, h)
				if !fresh(Now(), h.Time, c.Manifest) {
					p.BlockNumber = Ptr(h.Number)
					p.BlockHash = Ptr(h.Hash)
					p.BlockTime = Ptr(h.Time)
					p.ProbeStatus = "stale_head"
					p.Reason = "target_head_outside_freshness"
				} else {
					observed, err := r.Probe(rpcCtx, d, h)
					p.BlockNumber = observed.BlockNumber
					p.BlockHash = observed.BlockHash
					p.BlockTime = observed.BlockTime
					p.ContractTime = observed.ContractTime
					p.FillStatus = observed.FillStatus
					p.PausedFills = observed.PausedFills
					p.PayloadHash = observed.PayloadHash
					p.AvailableAt = Now()
					p.ProbeStatus = "ok"
					if err != nil {
						p.ProbeStatus = "rpc_error"
						p.Reason = err.Error()
					} else if !fresh(p.AvailableAt, h.Time, c.Manifest) {
						p.ProbeStatus = "stale_head"
					} else if t.Delay != nil && p.AvailableAt.After(t.Planned.Add(time.Second)) {
						p.ProbeStatus = "late"
					}
				}
			}
		}
	} else {
		p.Reason = forced
		if forced == "cancelled_terminal" && Now().Unix() > int64(d.FillDeadline)+2 {
			p.Reason = "local_deadline_elapsed_state_unverified"
		}
	}
	p.AvailableAt = Now()
	if len(p.PayloadHash) != 32 {
		hash, e := c.Archive.PutObject(struct {
			Capture, Status, Reason string
			Requested, Available    time.Time
		}{Hex(b.Capture.CaptureId), p.ProbeStatus, p.Reason, p.RequestedAt, p.AvailableAt})
		if e != nil {
			return p, e
		}
		p.PayloadHash = string(hash[:])
	}
	if c.Prices != nil {
		c.Prices.Attach(&p)
	}
	b.Probes = []OrderProbe{p}
	if p.ProbeStatus == "ok" || p.ProbeStatus == "late" {
		b.Capture.CompletedTasks = 1
	} else {
		b.Capture.Status = "partial"
		b.Capture.Reason = p.ProbeStatus
	}
	if e := c.persist(ctx, &b, headers, r.Members); e != nil {
		return p, e
	}
	return p, nil
}
func (c *Collector) runTasks(ctx context.Context, w *watcher) error {
	sort.SliceStable(w.Tasks, func(i, j int) bool { return w.Tasks[i].Planned.Before(w.Tasks[j].Planned) })
	current := w.Tasks
	w.Tasks = nil
	used := 0
	roundCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	for _, t := range current {
		o, ok := w.Orders[t.Key]
		if !ok {
			continue
		}
		forced := ""
		if Now().Unix() > int64(o.Deposit.FillDeadline)+2 {
			o.Terminal = true
		}
		if o.Terminal {
			forced = "cancelled_terminal"
		} else if Now().Before(t.Planned) {
			w.Tasks = append(w.Tasks, t)
			continue
		} else if used >= 20 || roundCtx.Err() != nil {
			forced = "skipped_budget"
		}
		p, e := c.recordProbe(ctx, o, t, forced, roundCtx)
		if e != nil {
			return e
		}
		used++
		if terminalProbe(o.Deposit, p) {
			o.Terminal = true
		}
		if forced == "skipped_budget" && t.Delay == nil {
			w.Tasks = append(w.Tasks, probeTask{t.Key, Now().Add(time.Second), nil, t.Kind})
			continue
		}
		if o.Terminal {
			continue
		}
		if o.Origin == "live" && !o.Followups && ProbeOpen(o.Deposit, p) {
			w.follow(t.Key, p.AvailableAt)
			o.Followups = true
		}
		if t.Delay == nil {
			next := Now().Add(time.Second)
			if p.ContractTime != nil && exclusive(o.Deposit) && *p.ContractTime <= uint64(o.Deposit.ExclusivityDeadline) {
				next = Now().Add(time.Duration(uint64(o.Deposit.ExclusivityDeadline)-*p.ContractTime+1) * time.Second)
			}
			w.Tasks = append(w.Tasks, probeTask{t.Key, next, nil, "open_check"})
		}
	}
	// Cancel future planned samples explicitly before dropping a terminal order.
	kept := w.Tasks[:0]
	for _, t := range w.Tasks {
		if o := w.Orders[t.Key]; o != nil && o.Terminal {
			if _, e := c.recordProbe(ctx, o, t, "cancelled_terminal"); e != nil {
				return e
			}
		} else {
			kept = append(kept, t)
		}
	}
	w.Tasks = kept
	for k, o := range w.Orders {
		if o.Terminal {
			delete(w.Orders, k)
		}
	}
	return nil
}
func (c *Collector) Watch(ctx context.Context, once bool, progress func(string)) error {
	w, e := c.restore(ctx)
	if e != nil {
		return e
	}
	nextReorg := time.Time{}
	nextReceipts := time.Time{}
	verified := map[uint64]bool{}
	for {
		start := Now()
		anySuccess := false
		for _, chain := range c.Manifest.Chains {
			r := c.Readers[chain.ChainID]
			h, e := r.Header(ctx, "latest")
			if e != nil {
				if progress != nil {
					progress(fmt.Sprintf("chain=%d head_unavailable: %v", chain.ChainID, e))
				}
				continue
			}
			if !verified[chain.ChainID] {
				if e = r.PreflightIdentity(ctx, h); e != nil {
					if progress != nil {
						progress(fmt.Sprintf("chain=%d preflight: %v", chain.ChainID, e))
					}
					continue
				}
				verified[chain.ChainID] = true
			}
			if !Now().Before(nextReorg) {
				if e = c.Reconcile(ctx, chain.ChainID); e != nil {
					if strings.Contains(e.Error(), "finalized_hash_conflict") || strings.Contains(e.Error(), "across_write_frozen_capture_") {
						return e
					}
					if progress != nil {
						progress(fmt.Sprintf("chain=%d reconcile: %v", chain.ChainID, e))
					}
					continue
				}
				// A split successful replay collectively covers an orphaned range.
				rr := logIntervals(c.captures, chain.ChainID)
				for _, cap := range c.captures {
					if cap.ChainId != chain.ChainID || cap.Canonical || cap.FromBlock == nil || *cap.FromBlock == 0 {
						continue
					}
					missing := firstUncovered(*cap.FromBlock, *cap.ToBlock, rr)
					if missing <= *cap.ToBlock && missing <= w.Cursors[chain.ChainID] {
						w.Cursors[chain.ChainID] = missing - 1
					}
				}
			}
			n := w.Cursors[chain.ChainID] + 1
			if w.Cursors[chain.ChainID] == 0 {
				n = h.Number
			}
			if n > h.Number {
				anySuccess = true
				continue
			}
			end := min(h.Number, n+c.Manifest.MaxLogBlocks-1)
			batches, e := c.collectSplit(ctx, chain.ChainID, n, end, "live", "head")
			if e != nil {
				if strings.Contains(e.Error(), "across_write_frozen_capture_") {
					return e
				}
				if progress != nil {
					progress(fmt.Sprintf("chain=%d capture: %v", chain.ChainID, e))
				}
				continue
			}
			w.Cursors[chain.ChainID] = end
			anySuccess = true
			for _, b := range batches {
				for _, d := range b.Deposits {
					origin := "catchup"
					if d.LiveReceivedAt != nil {
						origin = "live"
					}
					if !w.add(d, origin, c.Manifest) {
						o := &pendingOrder{Deposit: d, Origin: origin}
						if _, e = c.recordProbe(ctx, o, probeTask{Key: orderKey(d), Planned: Now()}, "skipped_budget"); e != nil {
							return e
						}
					}
				}
			}
		}
		if !Now().Before(nextReorg) {
			nextReorg = Now().Add(30 * time.Second)
		}
		if e = c.runTasks(ctx, w); e != nil {
			return e
		}
		if once {
			if !anySuccess {
				return errors.New("all_chains_unavailable")
			}
			return nil
		}
		// Price refresh and receipt maintenance only after due live work, bounded to 5 s.
		if c.Prices != nil {
			budget, cancel := context.WithTimeout(ctx, 5*time.Second)
			c.Prices.Refresh(budget)
			cancel()
		}
		if !Now().Before(nextReceipts) {
			budget, cancel := context.WithTimeout(ctx, 5*time.Second)
			e = c.DrainReceipts(ctx, 20, budget)
			cancel()
			if e != nil {
				return e
			}
			nextReceipts = Now().Add(5 * time.Second)
		}
		if progress != nil {
			progress(fmt.Sprintf("pending=%d scheduled=%d receipt_backlog=%d cycle_ms=%d", len(w.Orders), len(w.Tasks), len(c.receiptQueue), Now().Sub(start).Milliseconds()))
		}
		delay := time.Duration(c.Manifest.PollMillis)*time.Millisecond - Now().Sub(start)
		if !anySuccess {
			delay = 5 * time.Second
		}
		if delay < 0 {
			delay = 0
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func terminalProbe(d Deposit, p OrderProbe) bool {
	return (p.ProbeStatus == "ok" || p.ProbeStatus == "late") && (p.FillStatus != nil && *p.FillStatus == 2 || p.ContractTime != nil && *p.ContractTime > uint64(d.FillDeadline))
}

type blockInterval struct{ from, to uint64 }

func logIntervals(caps []Capture, chain uint64) []blockInterval {
	out := []blockInterval{}
	for _, c := range caps {
		if c.ChainId == chain && c.CaptureKind == "logs" && c.Canonical && c.Committed && c.CompletedTasks == 1 && c.FromBlock != nil {
			out = append(out, blockInterval{*c.FromBlock, *c.ToBlock})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].from == out[j].from {
			return out[i].to < out[j].to
		}
		return out[i].from < out[j].from
	})
	return out
}
func firstUncovered(from, to uint64, rr []blockInterval) uint64 {
	n := from
	for _, r := range rr {
		if r.to < n {
			continue
		}
		if r.from > n {
			return n
		}
		n = r.to + 1
		if n > to {
			return n
		}
	}
	return n
}
