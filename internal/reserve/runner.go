package reserve

import (
	"context"
	"errors"
	"fmt"
	"github.com/vphoenix/crypto-market-info/internal/dex"
	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
	"sort"
	"time"
)

// Reconcile revises every attempt on the unfinalized tail, including partial
// captures. A previously finalized hash conflict is fatal. It does not fetch
// replacement quotes for old blocks or advance their availability timestamps.
func (c *Collector) Reconcile(ctx context.Context, finalized, safe dex.Block) error {
	c.Rewind = 0
	caps, e := c.Store.ReserveCaptures(ctx, Hash(c.Manifest.Hash))
	if e != nil {
		return e
	}
	for _, m := range c.RelatedManifests {
		more, e := c.Store.ReserveCaptures(ctx, Hash(m))
		if e != nil {
			return e
		}
		caps = append(caps, more...)
	}
	var sentinel *Capture
	for i := range caps {
		v := &caps[i]
		if v.Canonical && v.Finality == "finalized" && (sentinel == nil || v.ToBlock > sentinel.ToBlock) {
			sentinel = v
		}
	}
	unique := map[uint64]bool{}
	numbers := []uint64{}
	add := func(n uint64) {
		if !unique[n] {
			unique[n] = true
			numbers = append(numbers, n)
		}
	}
	if sentinel != nil {
		add(sentinel.ToBlock)
	}
	for _, v := range caps {
		if v.Canonical && v.Finality != "finalized" {
			add(v.FromBlock)
			add(v.ToBlock)
		}
	}
	checked, e := c.Headers(ctx, numbers)
	if e != nil {
		return e
	}
	header := func(n uint64) (dex.Block, error) {
		b, ok := checked[n]
		if !ok {
			return b, errors.New("reconcile_header_missing")
		}
		return b, nil
	}
	if sentinel != nil {
		b, e := header(sentinel.ToBlock)
		if e != nil {
			return e
		}
		if Hash(b.Hash) != sentinel.ToHash {
			return errors.New("finalized_hash_conflict")
		}
	}
	for _, v := range caps {
		if v.Finality == "finalized" || !v.Canonical {
			continue
		}
		to, e := header(v.ToBlock)
		if e != nil {
			return e
		}
		from, e := header(v.FromBlock)
		if e != nil {
			return e
		}
		canonical := Hash(to.Hash) == v.ToHash && Hash(from.Hash) == v.FromHash
		finality := "head"
		if v.ToBlock <= safe.Number {
			finality = "safe"
		}
		if v.ToBlock <= finalized.Number {
			finality = "finalized"
		}
		if canonical && v.Finality == finality {
			continue
		}
		if !canonical {
			v.Canonical = false
			v.Reason = "orphaned_branch"
			if c.Rewind == 0 || v.FromBlock < c.Rewind {
				c.Rewind = v.FromBlock
			}
		} else {
			v.Finality = finality
		}
		proof, e := c.RPC.Archive.PutObject(struct {
			Previous              string
			From, To, Safe, Final dex.Hash
			At                    time.Time
		}{Hex(v.PayloadHash), from.Payload, to.Payload, safe.Payload, finalized.Payload, dex.Now()})
		if e != nil {
			return e
		}
		v.PayloadHash = Hash(proof)
		v.Revision = max(v.Revision+1, uint64(dex.Now().UnixMicro()))
		if e = c.Store.WriteReserveRevision(ctx, v); e != nil {
			return e
		}
	}
	return nil
}
func (c *Collector) Backfill(ctx context.Context, from, to uint64, chunk uint64, maxRanges int, progress func(string)) error {
	if from == 0 || from > to || chunk == 0 || chunk > 512 {
		return errors.New("invalid_backfill_range")
	}
	final, e := c.Header(ctx, "finalized")
	if e != nil {
		return e
	}
	if to > final.Number {
		return errors.New("backfill_requires_finalized_range")
	}
	previous, e := c.Store.ReserveCaptures(ctx, Hash(c.Manifest.Hash))
	if e != nil {
		return e
	}

	count := 0
	for start := from; start <= to; {
		end := min(start+chunk-1, to)
		if maxRanges > 0 && count >= maxRanges {
			break
		}
		if covered := CoveredThrough(previous, start); covered >= start {
			start = covered + 1
			continue
		}
		count++
		a, e := c.Header(ctx, ethereum.Height(start))
		if e != nil {
			return e
		}
		b, e := c.Header(ctx, ethereum.Height(end))
		if e != nil {
			return e
		}
		var batch Batch
		for attempt := 0; attempt < 3; attempt++ {
			batch, e = c.Logs(ctx, a, b, "backfill", "finalized")
			if e != nil {
				return e
			}
			if e = c.Store.WriteReserveBatch(ctx, batch); e != nil {
				return e
			}
			progress(fmt.Sprintf("backfill blocks=%d..%d logs=%d receipts=%d/%d coverage=%s reason=%s", start, end, len(batch.Logs), len(batch.Receipts), batch.Capture.ExpectedReceipts, batch.Capture.LogCoverage, batch.Capture.Reason))
			if batch.Capture.LogCoverage == "complete" && batch.Capture.ReceiptCoverage == "complete" {
				break
			}
			if !TransientRPC(errors.New(batch.Capture.Reason)) || attempt == 2 {
				break
			}
			if e = c.RPC.WaitReady(ctx); e != nil {
				return e
			}
			timer := time.NewTimer(time.Duration(attempt+1) * 3 * time.Second)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
		if batch.Capture.LogCoverage != "complete" || batch.Capture.ReceiptCoverage != "complete" {
			if batch.Capture.Reason == "rpc_archive_auth_required" {
				return fmt.Errorf("%w: blocks=%d..%d", ErrArchiveAuthorization, start, end)
			}
			return fmt.Errorf("backfill_incomplete_range_preserved: %s", batch.Capture.Reason)
		}
		start = end + 1
	}
	return nil
}

// BlockAtTime uses actual header timestamps, never estimates the 30-day range
// from a presumed 12-second block interval.
func (c *Collector) BlockAtTime(ctx context.Context, want time.Time, top dex.Block) (uint64, error) {
	lo, hi := uint64(1), top.Number
	for lo < hi {
		mid := lo + (hi-lo)/2
		b, e := c.Header(ctx, ethereum.Height(mid))
		if e != nil {
			return 0, e
		}
		if b.Time.Before(want) {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo, nil
}
func (c *Collector) Watch(ctx context.Context, once bool, progress func(string)) error {
	tags, e := c.RPC.HeaderTags(ctx, "latest", "safe", "finalized")
	if e != nil {
		return e
	}
	head, safe, final := tags[0], tags[1], tags[2]
	head.Manifest = c.Manifest.Hash
	preflight := NewReader(c.RPC, c.Manifest)
	preflight.Limit = 1000
	if e = preflight.Preflight(ctx, head); e != nil {
		return fmt.Errorf("preflight: %w", e)
	}
	if e = c.Reconcile(ctx, final, safe); e != nil {
		return e
	}
	lastLog := head.Number - 1
	if c.StartBlock > 0 {
		lastLog = c.StartBlock - 1
	}
	caps, e := c.Store.ReserveCaptures(ctx, Hash(c.Manifest.Hash))
	if e != nil {
		return e
	}
	lastLog = ContiguousLiveCursor(caps, lastLog, c.StartBlock == 0)
	var lastQuote time.Time
	var lastSimulation time.Time
	var nextLogAttempt time.Time
	active := false
	var burstUntil uint64
	pendingSnapshot := false
	if c.Rewind > 0 {
		lastLog = min(lastLog, c.Rewind-1)
	}
	var lastHash dex.Hash
	var lastHead uint64
	lastReconcile := time.Now()
	pendingReconcile := false
	lastReceiptRepair := time.Now()
	var priorHead dex.Block
	logChunk := uint64(512)
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		headChanged := head.Number != lastHead || head.Hash != lastHash
		if headChanged || lastLog < head.Number {
			forceSnapshot := false
			// Catch every intervening height. Live log failures leave the cursor at
			// its preceding covered height. Authorization failures are checked at
			// most once per five minutes, without shrinking or skipping the range.
			if lastLog < head.Number && !time.Now().Before(nextLogAttempt) {
				start := lastLog + 1
				end := min(head.Number, start+logChunk-1)
				from, e := c.Header(ctx, ethereum.Height(start))
				if e != nil {
					if TransientRPC(e) {
						progress("log_header_retry: " + e.Error())
						goto poll
					}
					return e
				}
				to := head
				if end == start {
					to = from
				} else if end != head.Number {
					to, e = c.Header(ctx, ethereum.Height(end))
					if e != nil {
						if TransientRPC(e) {
							progress("log_header_retry: " + e.Error())
							goto poll
						}
						return e
					}
				}
				b, e := c.Logs(ctx, from, to, "live", "head")
				if e != nil {
					if TransientRPC(e) {
						progress("logs_retry: " + e.Error())
						goto poll
					}
					return e
				}
				if e = c.Store.WriteReserveBatch(ctx, b); e != nil {
					return e
				}
				if b.Capture.LogCoverage == "complete" {
					lastLog = end
				} else if b.Capture.Reason == "rpc_archive_auth_required" {
					nextLogAttempt = time.Now().Add(5 * time.Minute)
					progress(fmt.Sprintf("logs_authorization_deferred retry_at=%s", nextLogAttempt.UTC().Format(time.RFC3339)))
				}
				logChunk = nextLogChunk(logChunk, end-start+1, b.Capture.LogCoverage == "complete", b.Capture.Reason)
				forceSnapshot = SnapshotEvent(b.Logs)
				pendingSnapshot = pendingSnapshot || forceSnapshot
				progress(fmt.Sprintf("logs blocks=%d..%d count=%d coverage=%s reason=%s", start, end, len(b.Logs), b.Capture.LogCoverage, b.Capture.Reason))
			}
			if pendingSnapshot || lastQuote.IsZero() || headChanged && (active || head.Number <= burstUntil) || time.Since(lastQuote) >= 60*time.Second {
				b, e := c.Snapshot(ctx, head, "live")
				if e != nil {
					if TransientRPC(e) {
						progress("snapshot_retry: " + e.Error())
						goto poll
					}
					return e
				}
				if e = c.Store.WriteReserveBatch(ctx, b); e != nil {
					return e
				}
				pendingSnapshot = b.Capture.StateCoverage != "complete"
				active = false
				for _, s := range b.States {
					if AuctionMonitor(s) {
						active = true
					}
				}
				for _, q := range b.Quotes {
					if (q.RouteKind == "mint" || q.RouteKind == "redeem") && q.Quality != "incomplete" && q.AmountInRaw != nil && q.AmountOutRaw != nil && q.AmountOutRaw.Cmp(q.AmountInRaw) > 0 {
						burstUntil = max(burstUntil, head.Number+10)
					}
				}
				if c.SimulateEvery > 0 && (lastSimulation.IsZero() || time.Since(lastSimulation) >= c.SimulateEvery) {
					if e = c.SimulateBatch(ctx, head, b, 2, progress); e != nil {
						return e
					}
					lastSimulation = time.Now()
				}
				lastQuote = time.Now()
				progress(fmt.Sprintf("snapshot block=%d states=%d quotes=%d coverage=%s rpc_evidence=%s", head.Number, len(b.States), len(b.Quotes), b.Capture.QuoteCoverage, Hex(b.Capture.PayloadHash)))
			}
			lastHead = head.Number
			lastHash = head.Hash
		}
		if once {
			return nil
		}
	poll:
		if e = c.RPC.WaitReady(ctx); e != nil {
			return e
		}
		timer := time.NewTimer(6 * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		// Quotes retain their original sampling cadence. Safe/finalized are
		// only needed by the minute reconciliation, not every six-second poll.
		refreshFinality := pendingReconcile || time.Since(lastReconcile) >= time.Minute
		tags, e = c.pollHeaders(ctx, refreshFinality)
		if e != nil {
			if TransientRPC(e) {
				progress("head_poll_retry: " + e.Error())
				goto poll
			}
			return e
		}
		priorHead = head
		head = tags[0]
		if refreshFinality {
			safe, final = tags[1], tags[2]
		}
		head.Manifest = c.Manifest.Hash
		pendingReconcile = pendingReconcile || ReconcileDue(priorHead, head, lastReconcile, time.Now())
		if pendingReconcile {
			if !refreshFinality {
				// A newly observed branch conflict must use fresh finality tags.
				tags, e = c.RPC.HeaderTags(ctx, "safe", "finalized")
				if e != nil {
					if TransientRPC(e) {
						progress("finality_poll_retry: " + e.Error())
						goto poll
					}
					return e
				}
				safe, final = tags[0], tags[1]
			}
			if e = c.Reconcile(ctx, final, safe); e != nil {
				if TransientRPC(e) {
					progress("reconcile_retry: " + e.Error())
					goto poll
				}
				return e
			}
			if c.Rewind > 0 {
				lastLog = min(lastLog, c.Rewind-1)
			}
			lastReconcile = time.Now()
			pendingReconcile = false
		}
		if time.Since(lastReceiptRepair) >= 60*time.Second {
			if e = c.RepairReceipts(ctx, final, safe, progress); e != nil {
				if !TransientRPC(e) {
					return e
				}
				progress("receipt_retry_deferred: " + e.Error())
			}
			lastReceiptRepair = time.Now()
		}
	}
}

func (c *Collector) pollHeaders(ctx context.Context, finality bool) ([]dex.Block, error) {
	if finality {
		return c.RPC.HeaderTags(ctx, "latest", "safe", "finalized")
	}
	return c.RPC.HeaderTags(ctx, "latest")
}

// Only an explicit provider range/result limit justifies splitting. In
// particular authorization, throttling and timeouts cannot cause permanent
// single-block polling. Successful ranges gradually restore the batch size.
func nextLogChunk(current, attempted uint64, complete bool, reason string) uint64 {
	if complete {
		return min(uint64(512), max(uint64(1), current)*2)
	}
	if reason == "rpc_log_range_limit" && attempted > 1 {
		return max(uint64(1), attempted/2)
	}
	return current
}

func ReconcileDue(previous, next dex.Block, last, now time.Time) bool {
	return now.Sub(last) >= 60*time.Second || next.Number < previous.Number || next.Number == previous.Number && next.Hash != previous.Hash || next.Number == previous.Number+1 && next.Parent != previous.Hash
}

// Resume a union of successful intervals, never the largest endpoint over gaps.
func ContiguousLiveCursor(caps []Capture, fallback uint64, discover bool) uint64 {
	ranges := []Capture{}
	for _, v := range caps {
		if v.CaptureKind == "logs" && v.CaptureMode == "live" && v.Canonical && v.Committed && v.LogCoverage == "complete" {
			ranges = append(ranges, v)
		}
	}
	if len(ranges) == 0 {
		return fallback
	}
	sort.Slice(ranges, func(i, j int) bool { return ranges[i].FromBlock < ranges[j].FromBlock })
	cursor := fallback
	if discover {
		cursor = ranges[0].FromBlock - 1
	}
	for _, v := range ranges {
		if v.FromBlock > cursor+1 {
			break
		}
		if v.ToBlock > cursor {
			cursor = v.ToBlock
		}
	}
	return cursor
}

func SnapshotEvent(logs []dex.Log) bool {
	for _, l := range logs {
		switch l.Event {
		case "AuctionOpened", "AuctionClosed", "RebalanceStarted", "RebalanceEnded", "BasketTokenAdded", "BasketTokenRemoved", "MintFeeSet", "TVLFeeSet", "BidsEnabledSet", "unknown", "unknown_version", "unknown_upgrade_block":
			return true
		}
	}
	return false
}

// Pending auctions are monitored per block, while economic quoting still
// requires startTime <= blockTime through AuctionActive.
func AuctionMonitor(s State) bool {
	return MintAllowed(s) && s.AuctionId != nil && s.AuctionEndTime != nil && uint64(s.BlockTime.Unix()) <= *s.AuctionEndTime && s.RebalanceBidsEnabled != nil && *s.RebalanceBidsEnabled && s.RebalanceNonce != nil && s.AuctionRebalanceNonce != nil && s.RebalanceNonce.Cmp(s.AuctionRebalanceNonce) == 0
}
