package across

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
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
	latest, e := latestReadCaptures(c.captures)
	if e != nil {
		return nil, e
	}
	c.captures = latest
	selected := []Capture{}
	for _, cap := range c.captures {
		if cap.Canonical && cap.Committed && (cap.CaptureKind == "logs" || cap.CaptureKind == "receipts") {
			selected = append(selected, cap)
		}
	}
	loaded, e := LoadBatches(ctx, c.Store, selected)
	if e != nil {
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
		if loadErr := loaded.Errors[cap.CaptureId]; loadErr != nil {
			return nil, loadErr
		}
		b := loaded.Batches[cap.CaptureId]
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
			b := loaded.Batches[cap.CaptureId]
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
	} else if o.Origin == "live" {
		rpcCtx = probeContext(ctx)
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
			p.Reason = "source_canonical_unknown: " + e.Error()
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
	if p.ProbeStatus == "rpc_error" && !strings.Contains(p.Reason, "network_issue_stop") {
		if errors.Is(rpcCtx.Err(), context.DeadlineExceeded) {
			p.Reason = "probe_budget_exhausted: " + p.Reason
		} else if errors.Is(rpcCtx.Err(), context.Canceled) && ctx.Err() == nil {
			p.Reason = "probe_preempted_for_live: " + p.Reason
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
func priorityProbe(o *pendingOrder, task probeTask, at time.Time, m Manifest) bool {
	if o == nil || o.Origin != "live" || task.Planned.After(at) {
		return false
	}
	if task.Delay != nil {
		return !at.After(task.Planned.Add(time.Second))
	}
	return task.Kind == "baseline" && at.Sub(task.Planned) <= time.Duration(m.FreshMillis)*time.Millisecond
}

func (c *Collector) runTasks(ctx context.Context, w *watcher, backgroundContext ...context.Context) error {
	at := Now()
	sort.SliceStable(w.Tasks, func(i, j int) bool {
		rank := func(t probeTask) int {
			if t.Planned.After(at) {
				return 4
			}
			if priorityProbe(w.Orders[t.Key], t, at, c.Manifest) {
				return 0
			}
			if o := w.Orders[t.Key]; o != nil && o.Origin == "live" && t.Kind != "open_check" {
				return 2
			}
			return 3
		}
		if a, b := rank(w.Tasks[i]), rank(w.Tasks[j]); a != b {
			return a < b
		}
		if rank(w.Tasks[i]) == 0 {
			deadline := func(task probeTask) time.Time {
				if task.Delay != nil {
					return task.Planned.Add(time.Second)
				}
				return task.Planned.Add(time.Duration(c.Manifest.FreshMillis) * time.Millisecond)
			}
			if a, b := deadline(w.Tasks[i]), deadline(w.Tasks[j]); !a.Equal(b) {
				return a.Before(b)
			}
		}
		return w.Tasks[i].Planned.Before(w.Tasks[j].Planned)
	})
	current := w.Tasks
	w.Tasks = nil
	used := 0
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
		} else if used >= 1 {
			w.Tasks = append(w.Tasks, t)
			continue
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		networkCtx := ctx
		if len(backgroundContext) > 0 {
			networkCtx = backgroundContext[0]
		}
		budget := 12 * time.Second
		if priorityProbe(o, t, Now(), c.Manifest) {
			networkCtx = probeContext(ctx)
			budget = 5 * time.Second
		} else if deadline, ok := nextLiveProbe(w, current); ok && deadline.After(Now()) && time.Until(deadline) < budget {
			// An old order must not occupy the probe worker across a known live deadline.
			w.Tasks = append(w.Tasks, t)
			continue
		}
		probeCtx, cancel := context.WithTimeout(networkCtx, budget)
		p, e := c.recordProbe(ctx, o, t, forced, probeCtx)
		cancel()
		if strings.Contains(p.Reason, "network_issue_stop") {
			return errors.New(p.Reason)
		}
		if e != nil {
			return e
		}
		if forced == "" {
			used++
		}
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
			if p.ProbeStatus == "rpc_error" || p.ProbeStatus == "stale_head" {
				next = Now().Add(10 * time.Second)
			} else if o.Origin != "live" {
				next = Now().Add(5 * time.Second)
			}
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

func nextLiveProbe(w *watcher, tasks []probeTask) (time.Time, bool) {
	var next time.Time
	for _, task := range tasks {
		if order := w.Orders[task.Key]; order != nil && !order.Terminal && order.Origin == "live" && (task.Kind == "baseline" || task.Delay != nil) && !Now().After(task.Planned.Add(time.Second)) && (next.IsZero() || task.Planned.Before(next)) {
			next = task.Planned
		}
	}
	return next, !next.IsZero()
}

func nextProbeWake(w *watcher) time.Duration {
	live, hasLive := nextLiveProbe(w, w.Tasks)
	wait := time.Hour
	for _, task := range w.Tasks {
		order := w.Orders[task.Key]
		if order == nil {
			continue
		}
		if (order.Origin != "live" || task.Kind == "open_check" || Now().After(task.Planned.Add(time.Second))) && hasLive && time.Until(live) < 12*time.Second {
			continue
		}
		wait = min(wait, max(time.Until(task.Planned), time.Duration(0)))
	}
	return wait
}

// worker uses independent Reader evidence buffers. The transport and source
// quota are shared; independent collection must never reset another capture's
// member list. ClickHouse remains owned by this one writer process.
func (c *Collector) worker() *Collector {
	out := &Collector{Manifest: c.Manifest, Store: c.Store, Archive: c.Archive, Prices: c.Prices, Readers: map[uint64]*Reader{}, ReconcileLimit: c.ReconcileLimit}
	for chain, r := range c.Readers {
		rr := NewReader(r.RPC.Clone(), r.Chain)
		rr.MaxLogs = r.MaxLogs
		rr.RPCMinInterval = r.RPCMinInterval
		rr.BatchLimit = r.BatchLimit
		out.Readers[chain] = rr
	}
	return out
}

type discoveredOrder struct {
	deposit Deposit
	origin  string
}

func (c *Collector) probeLoop(ctx context.Context, w *watcher, input <-chan discoveredOrder) error {
	for {
		// Drain discovery before selecting the next actual deadline.
		for {
			select {
			case d := <-input:
				if !w.add(d.deposit, d.origin, c.Manifest) {
					if _, err := c.recordProbe(ctx, &pendingOrder{Deposit: d.deposit, Origin: d.origin}, probeTask{Key: orderKey(d.deposit), Planned: Now()}, "skipped_budget"); err != nil {
						return err
					}
				}
			default:
				goto drained
			}
		}
	drained:
		// The task worker alone owns Orders/Tasks. The supervisor only buffers
		// discovery, canceling ordinary network work when a live order arrives;
		// failure evidence still persists with the uncanceled parent context.
		background, stopBackground := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- c.runTasks(ctx, w, background) }()
		pending := []discoveredOrder{}
	processing:
		for {
			discovery := input
			if len(pending) >= int(c.Manifest.MaxPending) {
				discovery = nil
			}
			select {
			case err := <-done:
				stopBackground()
				if err != nil {
					return err
				}
				break processing
			case d := <-discovery:
				pending = append(pending, d)
				if d.origin == "live" {
					stopBackground()
				}
			case <-ctx.Done():
				stopBackground()
				err := <-done
				if err != nil {
					return err
				}
				return ctx.Err()
			}
		}
		for _, d := range pending {
			if !w.add(d.deposit, d.origin, c.Manifest) {
				if _, err := c.recordProbe(ctx, &pendingOrder{Deposit: d.deposit, Origin: d.origin}, probeTask{Key: orderKey(d.deposit), Planned: Now()}, "skipped_budget"); err != nil {
					return err
				}
			}
		}
		wait := nextProbeWake(w)
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case d := <-input:
			timer.Stop()
			if !w.add(d.deposit, d.origin, c.Manifest) {
				if _, err := c.recordProbe(ctx, &pendingOrder{Deposit: d.deposit, Origin: d.origin}, probeTask{Key: orderKey(d.deposit), Planned: Now()}, "skipped_budget"); err != nil {
					return err
				}
			}
		case <-timer.C:
		}
	}
}
func fatalCollectorError(e error) bool {
	return e != nil && (strings.Contains(e.Error(), "finalized_hash_conflict") || strings.Contains(e.Error(), "across_write_frozen_capture_") || strings.Contains(e.Error(), "network_issue_stop"))
}
func (c *Collector) finishWatchResult(result error, errs <-chan error) error {
	if networkErr := c.networkFaultError(); networkErr != nil {
		return networkErr
	}
	if !errors.Is(result, context.Canceled) && !errors.Is(result, context.DeadlineExceeded) {
		return result
	}
	for {
		select {
		case workerErr, ok := <-errs:
			if !ok {
				return result
			}
			if fatalCollectorError(workerErr) {
				return workerErr
			}
			if workerErr != nil && !errors.Is(workerErr, context.Canceled) && !errors.Is(workerErr, context.DeadlineExceeded) {
				result = workerErr
			}
		default:
			return result
		}
	}
}
func (c *Collector) Watch(ctx context.Context, once bool, progress func(string)) (result error) {
	w, e := c.restore(ctx)
	if e != nil {
		return e
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	input := make(chan discoveredOrder, int(c.Manifest.MaxPending))
	errs := make(chan error, 2)
	defer func() {
		result = c.finishWatchResult(result, errs)
	}()
	probe := c.worker()
	var workers sync.WaitGroup
	if !once {
		workers.Add(2)
		go func() { defer workers.Done(); errs <- probe.probeLoop(ctx, w, input); cancel() }()
		maintenance := c.worker()
		go func() { defer workers.Done(); errs <- maintenance.maintenanceLoop(ctx, progress); cancel() }()
		defer func() { cancel(); workers.Wait() }()
	}
	verified := map[uint64]bool{}
	refresh := Now().Add(30 * time.Second)
	for {
		if err := c.networkFaultError(); err != nil {
			return err
		}
		start := Now()
		anySuccess := false
		select {
		case err := <-errs:
			return err
		default:
		}
		if !Now().Before(refresh) {
			caps, err := c.Store.AcrossCaptures(ctx, c.Manifest.Hash)
			if err != nil {
				return err
			}
			c.captures = caps
			for _, chain := range c.Manifest.Chains {
				rr := logIntervals(c.captures, chain.ChainID)
				for _, cap := range caps {
					if cap.ChainId == chain.ChainID && !cap.Canonical && anchored(cap) && *cap.FromBlock > 0 {
						n := firstUncovered(*cap.FromBlock, *cap.ToBlock, rr)
						if n <= *cap.ToBlock && n <= w.Cursors[chain.ChainID] {
							w.Cursors[chain.ChainID] = n - 1
						}
					}
				}
			}
			refresh = Now().Add(30 * time.Second)
		}
		for _, chain := range c.Manifest.Chains {
			r := c.Readers[chain.ChainID]
			h, err := r.Header(ctx, "latest")
			if err != nil {
				if progress != nil {
					progress(fmt.Sprintf("chain=%d head_unavailable: %v", chain.ChainID, err))
				}
				continue
			}
			if !verified[chain.ChainID] {
				if err = r.PreflightIdentity(ctx, h); err != nil {
					if progress != nil {
						progress(fmt.Sprintf("chain=%d preflight: %v", chain.ChainID, err))
					}
					continue
				}
				verified[chain.ChainID] = true
			}
			n, end, scan := watchLogWindow(w.Cursors[chain.ChainID], h.Number, uint64(c.Manifest.MaxLogBlocks), once)
			if !scan {
				anySuccess = true
				continue
			}
			batches, err := c.collectSplit(ctx, chain.ChainID, n, end, "live", "head")
			if err != nil && fatalCollectorError(err) {
				return err
			}
			next := firstUncovered(n, end, logIntervals(c.captures, chain.ChainID))
			if next > n {
				w.Cursors[chain.ChainID] = next - 1
				anySuccess = true

			}
			if err != nil && progress != nil {
				progress(fmt.Sprintf("chain=%d capture: %v", chain.ChainID, err))
			}
			for _, b := range batches {
				for _, d := range b.Deposits {
					origin := "catchup"
					if d.LiveReceivedAt != nil {
						origin = "live"
					}
					if once {
						w.add(d, origin, c.Manifest)
					} else {
						select {
						case input <- discoveredOrder{d, origin}:
						case err := <-errs:
							return err
						case <-ctx.Done():
							select {
							case err := <-errs:
								return err
							default:
							}
							return ctx.Err()
						}
					}
				}
			}
		}
		if once {
			if !anySuccess {
				return errors.New("all_chains_unavailable")
			}
			return c.runTasks(ctx, w)
		}
		if progress != nil {
			progress(fmt.Sprintf("discovery_queue=%d receipt_backlog=%d cycle_ms=%d", len(input), len(c.receiptQueue), Now().Sub(start).Milliseconds()))
		}
		// Receipts belong to maintenance, recovered from committed event facts.
		c.receiptQueue = map[string]receiptTask{}
		delay := time.Duration(c.Manifest.PollMillis)*time.Millisecond - Now().Sub(start)
		if !anySuccess {
			delay = 5 * time.Second
		}
		if delay < 0 {
			delay = 0
		}
		if err := sleepContext(ctx, delay); err != nil {
			return err
		}
	}
}
func (c *Collector) maintenanceLoop(ctx context.Context, progress func(string)) error {
	refresh := Now().Add(time.Minute)
	if err := c.PrepareRepair(ctx); err != nil {
		return err
	}
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !Now().Before(refresh) {
			if err := c.RefreshRepairReceipts(ctx); err != nil {
				return err
			}
			refresh = Now().Add(time.Minute)
		} else {
			caps, err := c.Store.AcrossCaptures(ctx, c.Manifest.Hash)
			if err != nil {
				return err
			}
			c.captures = caps
		}
		for _, chain := range c.Manifest.Chains {
			budget, cancel := context.WithTimeout(ctx, 20*time.Second)
			err := c.Reconcile(ctx, chain.ChainID, budget)
			cancel()
			if fatalCollectorError(err) {
				return err
			}
			if progress != nil {
				count := 0
				for _, cap := range c.captures {
					if cap.ChainId == chain.ChainID && cap.Canonical && cap.Finality != "finalized" && anchored(cap) {
						count++
					}
				}
				progress(fmt.Sprintf("chain=%d finality_backlog=%d reconcile_error=%v", chain.ChainID, count, err))
			}
		}
		// Recover skipped outage ranges independently of current-head discovery.
		rawBudget, rawCancel := context.WithTimeout(ctx, 12*time.Second)
		rawErr := c.RepairRawGapStep(ctx, rawBudget)
		rawCancel()
		if fatalCollectorError(rawErr) {
			return rawErr
		}
		if rawErr != nil && progress != nil {
			progress(fmt.Sprintf("raw_gap_repair: %v", rawErr))
		}
		repairBudget, repairCancel := context.WithTimeout(ctx, 12*time.Second)
		repairErr := c.RepairPartialStep(ctx, repairBudget)
		repairCancel()
		if fatalCollectorError(repairErr) {
			return repairErr
		}
		if repairErr != nil && progress != nil {
			progress(fmt.Sprintf("partial_repair: %v", repairErr))
		}
		if c.Prices != nil {
			budget, cancel := context.WithTimeout(ctx, 5*time.Second)
			c.Prices.Refresh(budget)
			cancel()
		}
		budget, cancel := context.WithTimeout(ctx, 8*time.Second)
		err := c.RepairReceiptStep(ctx, budget)
		cancel()
		if err != nil {
			return err
		}
		if err := c.networkFaultError(); err != nil {
			return err
		}
		if err = sleepContext(ctx, time.Second); err != nil {
			return err
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

func (c *Collector) networkFaultError() error {
	for _, r := range c.Readers {
		if e := r.RPC.ConfirmedNetworkFault(); e != nil {
			return e
		}
	}
	return nil
}
