package reserve

import (
	"context"
	"errors"
	"fmt"
	"github.com/vphoenix/crypto-market-info/internal/dex"
	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
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
	done := map[[2]uint64]bool{}
	for _, v := range previous {
		if v.Canonical && v.Committed && v.Finality == "finalized" && v.CaptureKind == "logs" && v.LogCoverage == "complete" && v.ReceiptCoverage == "complete" {
			done[[2]uint64{v.FromBlock, v.ToBlock}] = true
		}
	}
	count := 0
	for start := from; start <= to; {
		end := min(start+chunk-1, to)
		if maxRanges > 0 && count >= maxRanges {
			break
		}
		if done[[2]uint64{start, end}] {
			start = end + 1
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
		batch, e := c.Logs(ctx, a, b, "backfill", "finalized")
		if e != nil {
			return e
		}
		if e = c.Store.WriteReserveBatch(ctx, batch); e != nil {
			return e
		}
		progress(fmt.Sprintf("backfill blocks=%d..%d logs=%d receipts=%d/%d coverage=%s reason=%s", start, end, len(batch.Logs), len(batch.Receipts), batch.Capture.ExpectedReceipts, batch.Capture.LogCoverage, batch.Capture.Reason))
		if batch.Capture.LogCoverage != "complete" || batch.Capture.ReceiptCoverage != "complete" {
			return errors.New("backfill_incomplete_range_preserved")
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
	caps, e := c.Store.ReserveCaptures(ctx, Hash(c.Manifest.Hash))
	if e != nil {
		return e
	}
	for _, v := range caps {
		if v.CaptureKind == "logs" && v.CaptureMode == "live" && v.Canonical && v.Committed && v.LogCoverage == "complete" && v.ToBlock > 0 {
			if lastLog == head.Number-1 || v.ToBlock > lastLog {
				lastLog = v.ToBlock
			}
		}
	}
	var lastQuote time.Time
	active := false
	var burstUntil uint64
	pendingSnapshot := false
	if c.Rewind > 0 {
		lastLog = min(lastLog, c.Rewind-1)
	}
	var lastHash dex.Hash
	var lastHead uint64
	logChunk := uint64(512)
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		headChanged := head.Number != lastHead || head.Hash != lastHash
		if headChanged || lastLog < head.Number {
			forceSnapshot := false
			// Catch every intervening height. Live log failures leave the cursor at
			// its preceding covered height and retry on the next head.
			if lastLog < head.Number {
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
				if end != head.Number {
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
				} else if end > start {
					logChunk = max(uint64(1), (end-start+1)/2)
				}
				forceSnapshot = SnapshotEvent(b.Logs)
				pendingSnapshot = pendingSnapshot || forceSnapshot
				progress(fmt.Sprintf("logs blocks=%d..%d count=%d coverage=%s", start, end, len(b.Logs), b.Capture.LogCoverage))
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
		timer := time.NewTimer(2 * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
		tags, e = c.RPC.HeaderTags(ctx, "latest", "safe", "finalized")
		if e != nil {
			if TransientRPC(e) {
				progress("head_poll_retry: " + e.Error())
				goto poll
			}
			return e
		}
		head, safe, final = tags[0], tags[1], tags[2]
		head.Manifest = c.Manifest.Hash
		if head.Number != lastHead || head.Hash != lastHash {
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
		}
	}
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
