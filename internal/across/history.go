package across

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
)

// HistoryRange is an inclusive, fixed block window. Neither retries nor process
// restarts move its endpoints to a newer finalized head.
type HistoryRange struct {
	ChainID uint64
	From    uint64
	To      uint64
}

// PrepareRepair rebuilds receipt work once at startup from canonical committed
// facts. A complete receipt wins over older incomplete observations of the same
// chain/block/transaction. An incomplete successful receipt is retried once in
// this run; DrainReceipts removes it even if the provider still omits its fee.
// Like the collector itself, this method requires a single sequential caller.
func (c *Collector) PrepareRepair(ctx context.Context) error {
	if err := c.load(ctx); err != nil {
		return err
	}
	return c.refreshRepairReceipts(ctx, true)
}

// RefreshRepairReceipts observes newly committed facts without repeatedly
// requesting receipts which were successfully fetched with unknown fees.
// Startup unknown-fee work which has not yet run remains queued.
func (c *Collector) RefreshRepairReceipts(ctx context.Context) error {
	return c.refreshRepairReceipts(ctx, false)
}

func (c *Collector) refreshRepairReceipts(ctx context.Context, includeUnknown bool) error {
	caps, err := c.Store.AcrossCaptures(ctx, c.Manifest.Hash)
	if err != nil {
		return err
	}
	latest, err := latestReadCaptures(caps)
	if err != nil {
		return err
	}
	selected := []Capture{}
	for _, cap := range latest {
		if !cap.Canonical || !cap.Committed || (cap.CaptureKind != "logs" && cap.CaptureKind != "receipts" && cap.CaptureKind != "receipt_transfers") {
			continue
		}
		selected = append(selected, cap)
	}
	loaded, err := LoadBatches(ctx, c.Store, selected)
	if err != nil {
		return err
	}
	queued := map[string]receiptTask{}
	refunds, transfers := map[string]bool{}, map[string]bool{}
	complete := map[string]bool{}
	known := map[string]bool{}
	for _, cap := range selected {
		if err := loaded.Errors[cap.CaptureId]; err != nil {
			return err
		}
		b := loaded.Batches[cap.CaptureId]
		for _, v := range b.Transfers {
			transfers[ID(receiptTask{v.ChainId, v.BlockHash, v.TxHash})] = true
		}
		for _, f := range b.Fills {
			ch, ok := c.Manifest.Chain(f.ChainId)
			if ok && f.OutputToken == WordAddress(ch.USDC) {
				task := receiptTask{f.ChainId, f.BlockHash, f.TxHash}
				queued[ID(task)] = task
			}
		}
		for _, refund := range b.Refunds {
			ch, ok := c.Manifest.Chain(refund.ChainId)
			if ok && refund.Token == ch.USDC {
				task := receiptTask{refund.ChainId, refund.BlockHash, refund.TxHash}
				queued[ID(task)] = task
				refunds[ID(task)] = true
			}
		}
		if cap.CaptureKind == "receipts" && cap.CompletedTasks == 1 {
			for _, receipt := range b.Receipts {
				task := receiptTask{receipt.ChainId, receipt.BlockHash, receipt.TxHash}
				key := ID(task)
				queued[key] = task
				known[key] = true
				complete[key] = complete[key] || receipt.FeeComplete
			}
		}
	}
	c.receiptDataMissing = map[string]bool{}
	for key := range queued {
		if refunds[key] && !transfers[key] {
			c.receiptDataMissing[key] = true
			continue
		}
		_, pending := c.receiptQueue[key]
		if complete[key] || (known[key] && !includeUnknown && !pending) {
			delete(queued, key)
		}
	}
	c.captures = latest
	c.loaded = true
	c.receiptQueue = queued
	return nil
}

type historyChain struct {
	window    HistoryRange
	next      uint64
	ready     bool
	retryAt   time.Time
	failures  uint
	repairs   []HistoryRange
	repaired  map[HistoryRange]bool
	preferFix bool
	unknown   int
}

// Small rounds give the other chain a turn even when header/state lookups are
// paced. This is below the protocol's 512-block maximum, not a new data model.
const historyRoundBlocks uint64 = 64

func historyIntervals(caps []Capture, chain uint64, decoded bool) []blockInterval {
	out := []blockInterval{}
	for _, cap := range caps {
		if cap.ChainId != chain || cap.CaptureKind != "logs" || !cap.Canonical || !cap.Committed || cap.CompletedTasks != 1 || cap.FromBlock == nil || cap.ToBlock == nil {
			continue
		}
		if decoded && (cap.Status != "complete" || cap.UnknownEventCount != 0) {
			continue
		}
		out = append(out, blockInterval{*cap.FromBlock, *cap.ToBlock})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].from < out[j].from })
	return out
}

func historyUncovered(from, to uint64, covered []blockInterval) (HistoryRange, bool) {
	next := firstUncovered(from, to, covered)
	if next > to {
		return HistoryRange{}, false
	}
	end := min(to, next+min(historyRoundBlocks, to-next+1)-1)
	for _, span := range covered {
		if span.from > next {
			end = min(end, span.from-1)
			break
		}
	}
	return HistoryRange{From: next, To: end}, true
}

func (s *historyChain) addRepair(r HistoryRange) {
	if s.repaired[r] {
		return
	}
	for _, existing := range s.repairs {
		if existing == r {
			return
		}
	}
	s.repairs = append(s.repairs, r)
}

func historyFatal(err error) bool {
	if err == nil {
		return false
	}
	text := err.Error()
	return strings.Contains(text, "across_write_frozen_capture_") || strings.Contains(text, "evidence_write_failed") || strings.Contains(text, "finalized_hash_conflict") || strings.Contains(text, "network_issue_stop")
}

func historyRetryDelay(err error, failures uint) (time.Duration, string) {
	if failures > 5 {
		failures = 5
	}
	delay := 2 * time.Second * time.Duration(uint64(1)<<failures)
	if ethereum.IsRateLimited(err) || strings.Contains(err.Error(), "rpc_rate_limited") {
		return max(delay, 5*time.Second), "rate_limited"
	}
	var httpErr *ethereum.HTTPError
	if errors.As(err, &httpErr) && httpErr.Status >= 500 || errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "rpc_operation_budget_exhausted") || strings.Contains(err.Error(), "timeout") || strings.Contains(err.Error(), "truncated") || strings.Contains(err.Error(), "rpc_transport_failure") {
		return delay, "temporary_source_error"
	}
	return max(delay, time.Minute), "source_blocked"
}

// History advances each selected chain at most one small range per round.
// Partial captures count as raw scans, but are also explicitly attempted for
// decoding repair; a successful still-unknown repair is not retried forever.
// Every repair writes a new capture, preserving all older immutable evidence.
func (c *Collector) History(ctx context.Context, ranges []HistoryRange, progress func(string)) error {
	if len(ranges) == 0 {
		return errors.New("history_requires_fixed_ranges")
	}
	seen := map[uint64]bool{}
	states := []*historyChain{}
	for _, window := range ranges {
		if seen[window.ChainID] || c.Readers[window.ChainID] == nil || window.From > window.To || window.To == math.MaxUint64 {
			return errors.New("invalid_history_range")
		}
		if _, ok := c.Manifest.Chain(window.ChainID); !ok {
			return errors.New("history_unknown_chain")
		}
		seen[window.ChainID] = true
		states = append(states, &historyChain{window: window, next: window.From, repaired: map[HistoryRange]bool{}})
	}
	if progress != nil {
		progress("history_prepare_repair_start")
	}
	if err := c.PrepareRepair(ctx); err != nil {
		return err
	}
	if progress != nil {
		progress(fmt.Sprintf("history_prepare_repair_done captures=%d receipts=%d", len(c.captures), len(c.receiptQueue)))
	}
	for _, state := range states {
		for _, cap := range c.captures {
			if cap.ChainId != state.window.ChainID || cap.CaptureKind != "logs" || !cap.Canonical || !cap.Committed || cap.CompletedTasks != 1 || cap.Status != "partial" || cap.FromBlock == nil || cap.ToBlock == nil {
				continue
			}
			from, to := max(state.window.From, *cap.FromBlock), min(state.window.To, *cap.ToBlock)
			for from <= to {
				end := min(to, from+min(historyRoundBlocks, to-from+1)-1)
				state.addRepair(HistoryRange{state.window.ChainID, from, end})
				from = end + 1
			}
		}
		state.preferFix = len(state.repairs) > 0
	}
	receiptAttempts := map[string]time.Time{}
	for {
		if err := c.networkFaultError(); err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		waiting := false
		for _, state := range states {
			// Validate stored finalized anchors even if this fixed raw window is
			// already covered. Never trust a restored checkpoint without querying it.
			if !state.ready {
				waiting = true
				if time.Now().Before(state.retryAt) {
					continue
				}
				preflight, cancel := context.WithTimeout(ctx, 20*time.Second)
				head, err := c.Readers[state.window.ChainID].Header(preflight, "finalized")
				if err == nil && state.window.To > head.Number {
					err = errors.New("history_end_above_finalized")
				}
				if err == nil {
					err = c.Readers[state.window.ChainID].PreflightIdentity(preflight, head)
				}
				if err == nil {
					err = c.Reconcile(ctx, state.window.ChainID, preflight)
				}
				cancel()
				if err != nil {
					if ctx.Err() != nil {
						return ctx.Err()
					}
					if historyFatal(err) {
						return err
					}
					state.deferSource(err, progress)
					continue
				}
				state.ready = true
			}
			raw := historyIntervals(c.captures, state.window.ChainID, false)
			next, scan := historyUncovered(state.next, state.window.To, raw)
			if scan {
				next.ChainID = state.window.ChainID
			} else {
				state.next = state.window.To + 1
			}
			decoded := historyIntervals(c.captures, state.window.ChainID, true)
			for len(state.repairs) > 0 {
				candidate := state.repairs[0]
				if firstUncovered(candidate.From, candidate.To, decoded) <= candidate.To && !state.repaired[candidate] {
					break
				}
				state.repairs = state.repairs[1:]
			}
			fix := len(state.repairs) > 0 && (state.preferFix || !scan)
			if !scan && !fix {
				continue
			}
			waiting = true
			if time.Now().Before(state.retryAt) {
				continue
			}
			if fix {
				next = state.repairs[0]
			}
			batch, err := c.CollectRange(ctx, next.ChainID, next.From, next.To, "backfill", "finalized")
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if historyFatal(err) {
					return err
				}
				state.deferSource(err, progress)
				continue
			}
			if partialRetryable(batch.Capture) {
				if !fix {
					state.next = next.To + 1
					state.addRepair(next)
					state.preferFix = true
				}
				state.deferSource(errors.New(batch.Capture.Reason), progress)
				continue
			}
			state.failures, state.retryAt = 0, time.Time{}
			if fix {
				state.repaired[next] = true
				state.repairs = state.repairs[1:]
				state.preferFix = false
				if batch.Capture.Status != "complete" || batch.Capture.UnknownEventCount != 0 {
					state.unknown++
				}
			} else {
				state.next = next.To + 1
				if batch.Capture.Status != "complete" || batch.Capture.UnknownEventCount != 0 {
					state.addRepair(next)
					state.preferFix = true
				}
			}
			if progress != nil {
				progress(fmt.Sprintf("history chain=%d blocks=%d..%d repair=%t status=%s", next.ChainID, next.From, next.To, fix, batch.Capture.Status))
			}
		}
		// One selected receipt per chain, each with an independent network budget.
		// Persistence uses the parent context, so expiry does not discard failure evidence.
		for _, state := range states {
			key := c.historyReceiptKey(state.window.ChainID, receiptAttempts, !waiting)
			if key == "" {
				continue
			}
			maintenance, cancel := context.WithTimeout(ctx, 8*time.Second)
			receiptAttempts[key] = time.Now()
			err := c.historyDrainReceipt(ctx, maintenance, key)
			cancel()
			if err != nil {
				return err
			}
		}
		if !waiting {
			untried := false
			for _, state := range states {
				if c.historyReceiptKey(state.window.ChainID, receiptAttempts, true) != "" {
					untried = true
				}
			}
			if !untried {
				if progress != nil {
					unknown := 0
					for _, state := range states {
						unknown += state.unknown
					}
					progress(fmt.Sprintf("history_fixed_raw_windows_processed repair_unknown=%d receipt_pending=%d", unknown, len(c.receiptQueue)))
				}
				return nil
			}
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (s *historyChain) deferSource(err error, progress func(string)) {
	delay, kind := historyRetryDelay(err, s.failures)
	s.failures++
	s.retryAt = time.Now().Add(delay)
	if progress != nil {
		progress(fmt.Sprintf("history chain=%d %s retry_after=%s reason=%s", s.window.ChainID, kind, delay, err.Error()))
	}
}

func (c *Collector) historyReceiptKey(chain uint64, attempts map[string]time.Time, untriedOnly bool) string {
	keys := []string{}
	for key, task := range c.receiptQueue {
		if task.ChainID != chain {
			continue
		}
		last := attempts[key]
		if !last.IsZero() && (untriedOnly || time.Since(last) < time.Minute) {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if len(keys) == 0 {
		return ""
	}
	return keys[0]
}

func (c *Collector) historyDrainReceipt(ctx, network context.Context, key string) error {
	// DrainReceipts already owns receipt validation and frozen evidence writes.
	// Isolate one selected task while the single writer runs it, then restore all
	// other work even on failure. This avoids its sorted first task starving peers.
	all := c.receiptQueue
	c.receiptQueue = map[string]receiptTask{key: all[key]}
	err := c.DrainReceipts(ctx, 1, network)
	if task, remains := c.receiptQueue[key]; remains {
		all[key] = task
	} else {
		delete(all, key)
	}
	c.receiptQueue = all
	return err
}

func partialRetryable(cap Capture) bool {
	if cap.Status != "partial" {
		return false
	}
	for _, kind := range []string{"rpc_operation_budget_exhausted", "rpc_rate_limited", "rpc_http_429", "rpc_http_5", "timeout", "rpc_transport_failure", "rpc_response_truncated", "deadline_exceeded"} {
		if strings.Contains(cap.Reason, kind) {
			return true
		}
	}
	return false
}

type partialRepairState struct {
	attempted        map[string]bool
	retryAt          map[string]time.Time
	failures         map[string]uint
	cursor           int
	receiptAttemptAt map[string]time.Time
}

// RepairReceiptStep processes one fair receipt task. A failed task cools down
// for a minute; work not yet attempted always precedes retries. Successful
// incomplete fees are not looped: the reader removes the task and Refresh sees
// the already stored observation. The parent context owns evidence persistence.
func (c *Collector) RepairReceiptStep(ctx context.Context, networkContext ...context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	rpcCtx := ctx
	if len(networkContext) != 0 && networkContext[0] != nil {
		rpcCtx = networkContext[0]
	}
	if rpcCtx.Err() != nil {
		return nil
	}
	if c.partialRepair == nil {
		c.partialRepair = &partialRepairState{attempted: map[string]bool{}, retryAt: map[string]time.Time{}, failures: map[string]uint{}}
	}
	if c.partialRepair.receiptAttemptAt == nil {
		c.partialRepair.receiptAttemptAt = map[string]time.Time{}
	}
	attempts := c.partialRepair.receiptAttemptAt
	key := ""
	var oldest time.Time
	for candidate, task := range c.receiptQueue {
		if c.Readers[task.ChainID] == nil {
			continue
		}
		last := attempts[candidate]
		if !last.IsZero() && time.Since(last) < time.Minute {
			continue
		}
		urgent := c.receiptDataMissing[candidate]
		selectedUrgent := c.receiptDataMissing[key]
		if key == "" || urgent && !selectedUrgent || urgent == selectedUrgent && (last.Before(oldest) || last.Equal(oldest) && candidate < key) {
			key, oldest = candidate, last
		}
	}
	if key == "" {
		return nil
	}
	attempts[key] = time.Now()
	err := c.historyDrainReceipt(ctx, rpcCtx, key)
	if _, pending := c.receiptQueue[key]; !pending {
		delete(attempts, key)
	}
	return err
}

type partialCandidate struct {
	window   HistoryRange
	key      string
	finality string
}

// An operator may abandon a known historical decode gap without changing its
// incomplete status, frozen facts, anchors or finality.
const repairAbandonedMarker = "repair_abandoned=user_requested_historical_state_unavailable"

func partialKey(cap Capture, window HistoryRange) string {
	return ID(struct {
		Window           HistoryRange
		FromHash, ToHash *string
	}{window, cap.FromHash, cap.ToHash})
}

// RepairPartialStep attempts at most one 64-block partial interval. Permanent
// unknown ABI/state remains unknown and is tried only once per process; explicit
// transport/rate-limit failures retain their work with per-interval backoff.
// Callers refresh c.captures through their existing maintenance snapshot; this
// method never replaces capture facts or calls a second source/route.
func (c *Collector) RepairPartialStep(ctx context.Context, networkContext ...context.Context) error {
	if err := c.load(ctx); err != nil {
		return err
	}
	latest, err := latestReadCaptures(c.captures)
	if err != nil {
		return err
	}
	if c.partialRepair == nil {
		c.partialRepair = &partialRepairState{attempted: map[string]bool{}, retryAt: map[string]time.Time{}, failures: map[string]uint{}}
	}
	state := c.partialRepair
	candidates := []partialCandidate{}
	for _, cap := range latest {
		if !cap.Canonical || !cap.Committed || cap.CaptureKind != "logs" || cap.Status != "partial" || cap.CompletedTasks != 1 || cap.FromBlock == nil || cap.ToBlock == nil || c.Readers[cap.ChainId] == nil {
			continue
		}
		if strings.Contains(cap.Reason, repairAbandonedMarker) {
			continue
		}
		decoded := historyIntervals(latest, cap.ChainId, true)
		for from := *cap.FromBlock; from <= *cap.ToBlock; {
			window, ok := historyUncovered(from, *cap.ToBlock, decoded)
			if !ok {
				break
			}
			window.ChainID = cap.ChainId
			key := partialKey(cap, window)
			if !state.attempted[key] && !time.Now().Before(state.retryAt[key]) {
				finality := "head"
				if cap.Finality == "finalized" {
					finality = "finalized"
				}
				candidates = append(candidates, partialCandidate{window, key, finality})
			}
			if window.To == math.MaxUint64 {
				break
			}
			from = window.To + 1
		}
	}
	if len(candidates) == 0 {
		return nil
	}
	candidate := candidates[state.cursor%len(candidates)]
	state.cursor++
	window := candidate.window
	rpcCtx := ctx
	if len(networkContext) != 0 && networkContext[0] != nil {
		rpcCtx = networkContext[0]
	}
	batch, err := c.CollectRange(ctx, window.ChainID, window.From, window.To, "backfill", candidate.finality, rpcCtx)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if historyFatal(err) {
			return err
		}
		delay, _ := historyRetryDelay(err, state.failures[candidate.key])
		state.failures[candidate.key]++
		state.retryAt[candidate.key] = time.Now().Add(delay)
		return nil
	}
	if partialRetryable(batch.Capture) {
		delay, _ := historyRetryDelay(errors.New(batch.Capture.Reason), state.failures[candidate.key])
		state.failures[candidate.key]++
		state.retryAt[candidate.key] = time.Now().Add(delay)
		// The newly appended attempt must share the same cooldown; otherwise its
		// different parent endpoint hashes could immediately bypass that backoff.
		state.retryAt[partialKey(batch.Capture, window)] = state.retryAt[candidate.key]
		return nil
	}
	state.attempted[candidate.key] = true
	state.attempted[partialKey(batch.Capture, window)] = true
	delete(state.retryAt, candidate.key)
	return nil
}
