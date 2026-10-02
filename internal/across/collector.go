package across

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
)

type Collector struct {
	Manifest     Manifest
	Readers      map[uint64]*Reader
	Store        Store
	Archive      ethereum.Archive
	Prices       *Prices
	captures     []Capture
	loaded       bool
	receiptQueue map[string]receiptTask
	finalized    map[uint64]Block
}
type receiptTask struct {
	ChainID           uint64
	BlockHash, TxHash string
}

func height(n uint64) string { return fmt.Sprintf("0x%x", n) }
func (c *Collector) load(ctx context.Context) error {
	if c.loaded {
		return nil
	}
	v, e := c.Store.AcrossCaptures(ctx, c.Manifest.Hash)
	if e != nil {
		return e
	}
	c.captures = v
	c.loaded = true
	c.receiptQueue = map[string]receiptTask{}
	return nil
}
func (c *Collector) newBatch(r *Reader, kind, mode string) Batch {
	at := Now()
	if mode == "restart" {
		mode = "catchup"
	}
	return Batch{Capture: Capture{ManifestHash: c.Manifest.Hash, CaptureId: ID(uuid.NewString()), ChainId: r.Chain.ChainID, CaptureKind: kind, CaptureMode: mode, StartedAt: at, AvailableAt: at, SourceId: r.RPC.SourceID, Status: "complete", Canonical: true, Finality: "head", Revision: 1}}
}
func evidenceHeader(b Block) EvidenceHeader {
	return EvidenceHeader{ChainID: b.ChainID, Number: b.Number, Hash: Hex(b.Hash), Time: b.Time}
}
func anchorCapture(c *Capture, a, z Block) {
	c.FromBlock = Ptr(a.Number)
	c.ToBlock = Ptr(z.Number)
	c.FromHash = Ptr(a.Hash)
	c.ToHash = Ptr(z.Hash)
}
func (c *Collector) persist(ctx context.Context, b *Batch, headers []EvidenceHeader, refs []string) error {
	b.Capture.AvailableAt = Now()
	if e := ArchiveBatch(c.Archive, b, headers, refs); e != nil {
		return fmt.Errorf("across_write_frozen_capture_%s: archive: %w", Hex(b.Capture.CaptureId), e)
	}
	if e := c.Store.WriteAcrossBatch(ctx, *b); e != nil {
		return fmt.Errorf("across_write_frozen_capture_%s: %w", Hex(b.Capture.CaptureId), e)
	}
	c.captures = append(c.captures, b.Capture)
	return nil
}
func (c *Collector) failed(ctx context.Context, r *Reader, b *Batch, headers []EvidenceHeader, err error) error {
	*b = Batch{Capture: b.Capture}
	b.Capture.Status = "error"
	b.Capture.Reason = err.Error()
	if e := c.persist(ctx, b, headers, r.Members); e != nil {
		return e
	}
	return err
}
func fresh(at, block time.Time, m Manifest) bool {
	return !block.After(at.Add(time.Duration(m.FutureMillis)*time.Millisecond)) && at.Sub(block) <= time.Duration(m.FreshMillis)*time.Millisecond
}
func appendFacts(b *Batch, v Batch) {
	b.Deposits = append(b.Deposits, v.Deposits...)
	b.Updates = append(b.Updates, v.Updates...)
	b.Fills = append(b.Fills, v.Fills...)
	b.Refunds = append(b.Refunds, v.Refunds...)
}

// CollectRange records raw coverage even when a deployment has no verified ABI.
// A failed RPC range never returns a successful empty observation.
func (c *Collector) CollectRange(ctx context.Context, chain, from, to uint64, mode, finality string) (Batch, error) {
	r := c.Readers[chain]
	if r == nil {
		return Batch{}, errors.New("missing_chain_reader")
	}
	r.Members = nil
	b := c.newBatch(r, "logs", mode)
	b.Capture.Finality = finality
	b.Capture.ExpectedTasks = 1
	headers := []EvidenceHeader{}
	a, e := r.Header(ctx, height(from))
	if e != nil {
		return b, c.failed(ctx, r, &b, headers, e)
	}
	headers = append(headers, evidenceHeader(a))
	z := a
	if to != from {
		z, e = r.Header(ctx, height(to))
		if e != nil {
			return b, c.failed(ctx, r, &b, headers, e)
		}
		headers = append(headers, evidenceHeader(z))
	}
	anchorCapture(&b.Capture, a, z)
	logs, e := r.Logs(ctx, from, to)
	if e != nil {
		return b, c.failed(ctx, r, &b, headers, e)
	}
	if len(logs) >= int(c.Manifest.MaxLogs) {
		return b, c.failed(ctx, r, &b, headers, errors.New("rpc_log_limit_reached"))
	}
	blocks := map[uint64]Block{from: a, to: z}
	upgrades := map[uint64]bool{}
	missing := []uint64{}
	requested := map[uint64]bool{}
	for _, l := range logs {
		if IsUpgradeLog(l) {
			upgrades[l.BlockNumber] = true
		}
		if _, ok := blocks[l.BlockNumber]; !ok && !requested[l.BlockNumber] {
			missing = append(missing, l.BlockNumber)
			requested[l.BlockNumber] = true
		}
	}
	if len(missing) > 0 {
		hh, err := r.Headers(ctx, missing)
		if err != nil {
			return b, c.failed(ctx, r, &b, headers, err)
		}
		for _, h := range hh {
			blocks[h.Number] = h
			headers = append(headers, evidenceHeader(h))
		}
	}

	abi, identityErr := r.VerifyImplementation(ctx, a)
	endABI, endErr := abi, identityErr
	if z.Number != a.Number {
		endABI, endErr = r.VerifyImplementation(ctx, z)
	}
	if identityErr == nil && (endErr != nil || endABI != abi) && len(upgrades) == 0 {
		identityErr = errors.New("implementation_range_mismatch")
	}
	if mode == "live" && !fresh(Now(), z.Time, c.Manifest) {
		b.Capture.CaptureMode = "catchup"
	}
	for _, l := range logs {
		h := blocks[l.BlockNumber]
		if h.Hash != l.BlockHash {
			return b, c.failed(ctx, r, &b, headers, errors.New("log_header_hash_mismatch"))
		}
		if upgrades[l.BlockNumber] {
			b.Capture.UnknownEventCount++
			b.Capture.Status = "partial"
			b.Capture.Reason = "upgrade_boundary_unknown"
			continue
		}
		revision := abi
		idErr := identityErr
		if len(upgrades) > 0 {
			revision, idErr = r.VerifyImplementation(ctx, h)
		}
		if idErr != nil {
			b.Capture.UnknownEventCount++
			b.Capture.Status = "partial"
			b.Capture.Reason = "implementation_unknown: " + idErr.Error()
			continue
		}
		at := Now()
		var live *time.Time
		if b.Capture.CaptureMode == "live" && fresh(at, h.Time, c.Manifest) {
			live = Ptr(at)
		}
		decoded, known, err := DecodeLog(l, h, DecodeMeta{CaptureID: b.Capture.CaptureId, ABIRevision: revision, AvailableAt: at, LiveReceivedAt: live})
		if err != nil {
			b.Capture.UnknownEventCount++
			b.Capture.Status = "partial"
			b.Capture.Reason = "event_decode_incomplete"
			continue
		}
		if !known {
			b.Capture.UnknownEventCount++
			b.Capture.Status = "partial"
			b.Capture.Reason = "unknown_event"
		}
		available := Now()
		if live != nil && !fresh(available, h.Time, c.Manifest) {
			live = nil
		}
		for i := range decoded.Deposits {
			decoded.Deposits[i].AvailableAt = available
			decoded.Deposits[i].LiveReceivedAt = live
		}
		for i := range decoded.Updates {
			decoded.Updates[i].AvailableAt = available
			decoded.Updates[i].LiveReceivedAt = live
		}
		for i := range decoded.Fills {
			decoded.Fills[i].AvailableAt = available
			decoded.Fills[i].LiveReceivedAt = live
		}
		for i := range decoded.Refunds {
			decoded.Refunds[i].AvailableAt = available
		}
		appendFacts(&b, decoded)
	}
	if identityErr != nil && len(logs) == 0 {
		b.Capture.Status = "partial"
		b.Capture.Reason = "implementation_unknown: " + identityErr.Error()
	}
	// Re-check the branch after all decoding/state work; no orphaned range commits canonical.
	after, err := r.Headers(ctx, []uint64{from, to})
	if err != nil {
		return b, c.failed(ctx, r, &b, headers, err)
	}
	if after[0].Hash != a.Hash || after[1].Hash != z.Hash {
		b = Batch{Capture: b.Capture}
		b.Capture.Canonical = false
		b.Capture.Finality = "orphaned"
		return b, c.failed(ctx, r, &b, headers, errors.New("range_reorg"))
	}
	b.Capture.CompletedTasks = 1
	if e = c.persist(ctx, &b, headers, r.Members); e != nil {
		return b, e
	}
	for _, f := range b.Fills {
		if f.OutputToken == string(make([]byte, 12))+r.Chain.USDC {
			c.queueReceipt(chain, f.BlockHash, f.TxHash)
		}
	}
	for _, v := range b.Refunds {
		if v.Token == r.Chain.USDC {
			c.queueReceipt(chain, v.BlockHash, v.TxHash)
		}
	}
	return b, nil
}
func (c *Collector) queueReceipt(chain uint64, block, tx string) {
	if c.receiptQueue == nil {
		c.receiptQueue = map[string]receiptTask{}
	}
	t := receiptTask{chain, block, tx}
	c.receiptQueue[ID(t)] = t
}
func (c *Collector) collectSplit(ctx context.Context, chain, from, to uint64, mode, finality string) ([]Batch, error) {
	b, e := c.CollectRange(ctx, chain, from, to, mode, finality)
	if e == nil {
		return []Batch{b}, nil
	}
	if ctx.Err() != nil || from == to || b.Capture.CompletedTasks == 1 || strings.Contains(e.Error(), "across_write_frozen_capture_") {
		return nil, e
	}
	// Only bounded log/result errors warrant smaller ranges. Storage failure stops.
	if b.Capture.Status != "error" || b.Capture.Committed == false {
		return nil, e
	}
	mid := from + (to-from)/2
	left, e := c.collectSplit(ctx, chain, from, mid, mode, finality)
	if e != nil {
		return nil, e
	}
	right, e := c.collectSplit(ctx, chain, mid+1, to, mode, finality)
	return append(left, right...), e
}
func (c *Collector) BlockAtTime(ctx context.Context, chain uint64, target time.Time, end Block) (uint64, error) {
	r := c.Readers[chain]
	lo, hi := uint64(0), end.Number
	for lo < hi {
		mid := lo + (hi-lo)/2
		b, e := r.Header(ctx, height(mid))
		if e != nil {
			return 0, e
		}
		if b.Time.Before(target) {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo, nil
}

// Backfill resumes only fully scanned canonical intervals under the same manifest.
func (c *Collector) Backfill(ctx context.Context, chain, from, to uint64, maxRanges int, progress func(string)) error {
	if _, e := c.restore(ctx); e != nil {
		return e
	}
	r := c.Readers[chain]
	end, e := r.Header(ctx, "finalized")
	if e != nil {
		return fmt.Errorf("finality_capability_unknown: %w", e)
	}
	if from > to || to > end.Number {
		return errors.New("invalid_finalized_backfill_range")
	}
	if e = c.Reconcile(ctx, chain); e != nil {
		return e
	}
	scanned := []Capture{}
	for _, v := range c.captures {
		if v.ChainId == chain && v.CaptureKind == "logs" && v.Canonical && v.Committed && v.CompletedTasks == 1 && v.FromBlock != nil {
			scanned = append(scanned, v)
		}
	}
	sort.Slice(scanned, func(i, j int) bool { return *scanned[i].FromBlock < *scanned[j].FromBlock })
	count := 0
	for n := from; n <= to; {
		skip := n
		for _, v := range scanned {
			if *v.FromBlock <= skip && *v.ToBlock >= skip {
				skip = *v.ToBlock + 1
			}
		}
		if skip > n {
			n = skip
			continue
		}
		z := min(to, n+uint64(c.Manifest.MaxLogBlocks)-1)
		if _, e = c.collectSplit(ctx, chain, n, z, "backfill", "finalized"); e != nil {
			return e
		}
		if progress != nil {
			progress(fmt.Sprintf("chain=%d blocks=%d..%d receipt_backlog=%d", chain, n, z, len(c.receiptQueue)))
		}
		if e = c.DrainReceipts(ctx, 20); e != nil {
			return e
		}
		n = z + 1
		count++
		if maxRanges > 0 && count >= maxRanges {
			break
		}
	}
	return c.DrainReceipts(ctx, 20)
}
func (c *Collector) DrainReceipts(ctx context.Context, limit int, networkContext ...context.Context) error {
	rpcCtx := ctx
	if len(networkContext) > 0 {
		rpcCtx = networkContext[0]
	}
	keys := []string{}
	for k := range c.receiptQueue {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys[:min(limit, len(keys))] {
		t := c.receiptQueue[k]
		r := c.Readers[t.ChainID]
		r.Members = nil
		b := c.newBatch(r, "receipts", "research")
		b.Capture.ExpectedTasks = 1
		if rpcCtx.Err() != nil {
			break
		}
		row, e := r.Receipt(rpcCtx, t.TxHash)
		if e != nil {
			b.Capture.Status = "error"
			b.Capture.Reason = e.Error()
			if e = c.persist(ctx, &b, nil, r.Members); e != nil {
				return e
			}
			continue
		}
		if row.BlockHash != t.BlockHash {
			delete(c.receiptQueue, k)
			continue
		}
		row.CaptureId = b.Capture.CaptureId
		b.Receipts = []TxReceipt{row}
		b.Capture.CompletedTasks = 1
		h := Block{ChainID: t.ChainID, Number: row.BlockNumber, Hash: row.BlockHash, Time: row.BlockTime}
		if f, ok := c.finalized[t.ChainID]; ok && h.Number <= f.Number {
			b.Capture.Finality = "finalized"
		}
		anchorCapture(&b.Capture, h, h)
		if e = c.persist(ctx, &b, []EvidenceHeader{evidenceHeader(h)}, r.Members); e != nil {
			return e
		}
		delete(c.receiptQueue, k)
	}
	return nil
}

// Reconcile invalidates every attempt whose entire range intersects an orphan.
// Failed/partial captures are included, so old successful revisions cannot revive.
func (c *Collector) Reconcile(ctx context.Context, chain uint64) error {
	if e := c.load(ctx); e != nil {
		return e
	}
	r := c.Readers[chain]
	fin, e := r.Header(ctx, "finalized")
	if e != nil {
		return fmt.Errorf("finality_capability_unknown: %w", e)
	}
	if c.finalized == nil {
		c.finalized = map[uint64]Block{}
	}
	c.finalized[chain] = fin
	cache := map[uint64]Block{}
	wanted := []uint64{}
	seen := map[uint64]bool{}
	for _, cap := range c.captures {
		if cap.ChainId != chain || !cap.Canonical || cap.ToBlock == nil || cap.Finality == "finalized" {
			continue
		}
		for _, n := range []uint64{*cap.FromBlock, *cap.ToBlock} {
			if !seen[n] {
				seen[n] = true
				wanted = append(wanted, n)
			}
		}
	}
	for start := 0; start < len(wanted); start += 512 {
		hh, err := r.Headers(ctx, wanted[start:min(start+512, len(wanted))])
		if err != nil {
			return err
		}
		for _, h := range hh {
			cache[h.Number] = h
		}
	}
	bad := map[uint64]bool{}
	for _, cap := range c.captures {
		if cap.ChainId != chain || !cap.Canonical || cap.ToBlock == nil {
			continue
		}
		if cap.Finality == "finalized" {
			continue
		}
		for _, point := range []struct {
			n uint64
			h string
		}{{*cap.FromBlock, *cap.FromHash}, {*cap.ToBlock, *cap.ToHash}} {
			now, ok := cache[point.n]
			if !ok {
				now, e = r.Header(ctx, height(point.n))
				if e != nil {
					return e
				}
				cache[point.n] = now
			}
			if now.Hash != point.h {
				bad[point.n] = true
			}
		}
	}
	// A finalized checkpoint is checked even when the whole tail is already final.
	var checkpoint *Capture
	for i := range c.captures {
		v := c.captures[i]
		if v.ChainId == chain && v.Canonical && v.Finality == "finalized" && v.ToBlock != nil && (checkpoint == nil || *v.ToBlock > *checkpoint.ToBlock) {
			checkpoint = &v
		}
	}
	if checkpoint != nil {
		h, err := r.Header(ctx, height(*checkpoint.ToBlock))
		if err != nil {
			return err
		}
		if h.Hash != *checkpoint.ToHash {
			return errors.New("finalized_hash_conflict")
		}
	}
	for i := range c.captures {
		v := c.captures[i]
		if v.ChainId != chain || !v.Canonical || v.ToBlock == nil {
			continue
		}
		orphan := false
		for n := range bad {
			if *v.FromBlock <= n && *v.ToBlock >= n {
				orphan = true
			}
		}
		if !orphan && v.Finality == "finalized" {
			continue
		}
		if orphan {
			if v.Finality == "finalized" {
				return errors.New("finalized_hash_conflict")
			}
			v.Canonical = false
			v.Finality = "orphaned"
			v.Reason = "reorg_invalidated"
		} else if *v.ToBlock <= fin.Number {
			v.Finality = "finalized"
		} else {
			continue
		}
		v.Revision++
		if e = c.Store.WriteAcrossRevision(ctx, v); e != nil {
			return fmt.Errorf("across_write_frozen_capture_%s: revision: %w", Hex(v.CaptureId), e)
		}
		c.captures[i] = v
	}
	return nil
}
