package app

import (
	"context"
	"errors"
	"fmt"
	"github.com/vphoenix/crypto-market-info/internal/config"
	"github.com/vphoenix/crypto-market-info/internal/dex"
	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
	chstore "github.com/vphoenix/crypto-market-info/internal/storage/clickhouse"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

var errDEXFinality = errors.New("DEX finalized chain contradiction; branch paused")

type dexLogRepair struct {
	Block dex.Block
	Logs  []dex.Log
}
type dexRunner struct {
	pendingLogs     *dexLogRepair
	client          *ethereum.Client
	store           *chstore.Client
	logger          *slog.Logger
	pendingLive     *dex.Batch
	pendingBackfill *dex.Batch
	last            *dex.Block
	head            atomic.Pointer[dex.Block]
	checkpoint      atomic.Pointer[dex.Block]
	checkedAt       time.Time
	receiptRetry    map[dex.Hash]time.Time
}

func runDEX(ctx context.Context, cfg config.Config, store *chstore.Client, logger *slog.Logger) error {
	// Source/setup failures remain local; runComponents cancels all CEX streams
	// on a returned error, so this branch retries internally until shutdown.
	for ctx.Err() == nil {
		client, e := ethereum.NewClient(cfg.DEXRPCURL, cfg.DEXEvidenceDir)
		if e == nil {
			_, e = client.Archive.Put(ethereum.ManifestJSON)
		}
		if e == nil {
			e = client.VerifyChain(ctx)
		}
		if e == nil {
			e = store.InitDEXSchema(ctx)
		}
		r := &dexRunner{client: client, store: store, logger: logger, receiptRetry: map[dex.Hash]time.Time{}}
		if e == nil {
			r.last, e = store.DEXLastBlock(ctx, ethereum.ManifestHash(), false)
		}
		if e == nil {
			checkpoint, loadErr := store.DEXLastBlock(ctx, ethereum.ManifestHash(), true)
			e = loadErr
			if e == nil && checkpoint != nil {
				r.checkpoint.Store(checkpoint)
			}
		}
		if e == nil {
			e = r.run(ctx)
		}
		if ctx.Err() != nil {
			return nil
		}
		logger.Error("DEX branch retrying; other sources continue", "error", e)
		if errors.Is(e, errDEXFinality) {
			<-ctx.Done()
			return nil
		}
		if !dexDelay(ctx, 10*time.Second) {
			return nil
		}
	}
	return nil
}
func dexDelay(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
func (r *dexRunner) run(ctx context.Context) error {
	return runDEXLoops(ctx, r.logger, r.liveStep, r.maintenanceStep, 2*time.Second)
}

// Maintenance may block on an old block or a slow RPC. It must never hold up
// polling the newest head; shared head/checkpoint pointers remain immutable.
func runDEXLoops(ctx context.Context, logger *slog.Logger, live, maintenance func(context.Context) error, poll time.Duration) error {
	work, cancel := context.WithCancel(ctx)
	var workers sync.WaitGroup
	fatal := make(chan error, 1)
	workers.Add(1)
	go func() {
		defer workers.Done()
		for work.Err() == nil {
			if e := maintenance(work); e != nil {
				if errors.Is(e, errDEXFinality) {
					fatal <- e
					cancel()
					return
				}
				if work.Err() == nil {
					logger.Warn("DEX maintenance incomplete", "error", e)
				}
				if !dexDelay(work, 5*time.Second) {
					return
				}
			}
			if !dexDelay(work, poll) {
				return
			}
		}
	}()
	defer func() { cancel(); workers.Wait() }()
	for work.Err() == nil {
		select {
		case e := <-fatal:
			return e
		default:
		}
		if e := live(work); e != nil {
			select {
			case fatalErr := <-fatal:
				return fatalErr
			default:
			}
			if errors.Is(e, errDEXFinality) {
				return e
			}
			if work.Err() == nil {
				logger.Warn("DEX sample incomplete", "error", e)
			}
		}
		if !dexDelay(work, poll) {
			break
		}
	}
	select {
	case e := <-fatal:
		return e
	default:
	}
	return nil
}
func (r *dexRunner) liveStep(ctx context.Context) error {
	if r.pendingLive != nil {
		if e := r.store.WriteDEXBatch(ctx, *r.pendingLive); e != nil {
			return e
		}
		if r.last == nil || r.pendingLive.Block.Number >= r.last.Number {
			b := r.pendingLive.Block
			r.last = &b
		}
		r.pendingLive = nil
	}
	head, e := r.client.Header(ctx, "latest")
	if e != nil {
		return fmt.Errorf("latest_header: %w", e)
	}
	if e = checkDEXHead(head, r.last, r.checkpoint.Load()); e != nil {
		return e
	}
	r.head.Store(&head)
	// A replacement at the same height is new work; an old hash is never reused.
	if r.last == nil || head.Hash != r.last.Hash {
		b, e := r.client.Collect(ctx, head, true)
		if e != nil {
			return fmt.Errorf("live_collect: %w", e)
		}
		r.pendingLive = &b
		if e = r.store.WriteDEXBatch(ctx, b); e != nil {
			return e
		}
		r.pendingLive = nil
		r.last = &b.Block
		r.logger.Info("DEX block stored", "number", head.Number, "quote_coverage", b.Block.QuoteCoverage, "log_coverage", b.Block.LogCoverage, "receipts", b.Block.ReceiptCoverage)
	}
	return nil
}

func (r *dexRunner) maintenanceStep(ctx context.Context) error {
	if r.pendingLogs != nil {
		if e := r.store.CompleteDEXLogs(ctx, r.pendingLogs.Block, r.pendingLogs.Logs); e != nil {
			return e
		}
		r.pendingLogs = nil
	}
	if r.pendingBackfill != nil {
		if e := r.store.WriteDEXBatch(ctx, *r.pendingBackfill); e != nil {
			return e
		}
		r.pendingBackfill = nil
	}
	head := r.head.Load()
	if head == nil {
		return nil
	}
	if time.Since(r.checkedAt) > 30*time.Second {
		r.checkedAt = time.Now()
		maintenanceCtx, cancel := context.WithTimeout(ctx, 6*time.Second)
		e := r.reconcile(maintenanceCtx, *head)
		cancel()
		if e != nil {
			if errors.Is(e, errDEXFinality) {
				return fmt.Errorf("reconcile: %w", e)
			}
			r.logger.Warn("DEX reconcile incomplete; live polling continues", "error", e)
		}
		return nil
	}
	// Handle one missed block per pass. Persisted gaps survive restart, and
	// backfill never labels delayed data as a live quote.
	gap, e := r.store.DEXGapStart(ctx, ethereum.ManifestHash())
	if e != nil {
		return fmt.Errorf("gap_lookup: %w", e)
	}
	if gap != 0 {
		old, e := r.client.Header(ctx, ethereum.Height(gap))
		if e != nil {
			return fmt.Errorf("backfill_header: %w", e)
		}
		b, e := r.client.Collect(ctx, old, false)
		if e != nil {
			return fmt.Errorf("backfill_collect: %w", e)
		}
		r.pendingBackfill = &b
		if e = r.store.WriteDEXBatch(ctx, b); e != nil {
			return e
		}
		r.pendingBackfill = nil
		return nil
	}
	if repaired, e := r.retryLogs(ctx); repaired || e != nil {
		if e != nil {
			return fmt.Errorf("retry_logs: %w", e)
		}
		return nil
	}
	if e := r.retryReceipts(ctx); e != nil {
		return fmt.Errorf("retry_receipts: %w", e)
	}
	return nil
}
func (r *dexRunner) reconcile(ctx context.Context, head dex.Block) error {
	checkpoint := r.checkpoint.Load()
	tags := []string{"finalized", "safe"}
	if checkpoint != nil {
		tags = append(tags, ethereum.Height(checkpoint.Number))
	}
	heads, e := r.client.HeaderTags(ctx, tags...)
	if e != nil {
		return e
	}
	final, safe := heads[0], heads[1]
	if final.Number > safe.Number || safe.Number > head.Number {
		return fmt.Errorf("inconsistent finality tags")
	}
	from := uint64(0)
	if checkpoint != nil {
		check := heads[2]
		if check.Hash != checkpoint.Hash {
			return errDEXFinality
		}
		if final.Number < checkpoint.Number {
			return fmt.Errorf("stale RPC finalized tag below known checkpoint")
		}
		from = checkpoint.Number
	}
	// Backfilled old blocks also need finality updates, so include a bounded set
	// of pre-checkpoint incomplete statuses, via the separate query below.
	blocks, e := r.store.DEXBlockRange(ctx, ethereum.ManifestHash(), from, head.Number)
	if e != nil {
		return e
	}
	older, e := r.store.DEXUnfinalizedBlocks(ctx, ethereum.ManifestHash(), from)
	if e != nil {
		return e
	}
	blocks = append(older, blocks...)
	nums := []uint64{}
	seen := map[uint64]bool{}
	for _, b := range blocks {
		if !seen[b.Number] {
			nums = append(nums, b.Number)
			seen[b.Number] = true
		}
	}
	sort.Slice(nums, func(i, j int) bool { return nums[i] < nums[j] })
	// Bound each maintenance RPC; later passes advance the checkpoint.
	if len(nums) > 20 {
		nums = nums[:20]
	}
	actual, e := r.client.Headers(ctx, nums)
	if e != nil {
		return e
	}
	for i := 1; i < len(nums); i++ {
		if nums[i] == nums[i-1]+1 && actual[nums[i]].Parent != actual[nums[i-1]].Hash {
			return fmt.Errorf("canonical parent mismatch")
		}
	}
	for _, b := range blocks {
		now, ok := actual[b.Number]
		if !ok {
			continue
		}
		canonical := now.Hash == b.Hash
		if b.Finality == "finalized" && !canonical {
			return errDEXFinality
		}
		state := "orphaned"
		if canonical {
			state = "head"
			if b.Number <= safe.Number {
				state = "safe"
			}
			if b.Number <= final.Number {
				state = "finalized"
			}
		}
		if canonical != b.Canonical || state != b.Finality {
			b.Canonical = canonical
			b.Finality = state
			b.Revision = max(b.Revision+1, uint64(dex.Now().UnixMicro()))
			if e = r.store.WriteDEXBlock(ctx, b); e != nil {
				return e
			}
		}
		if canonical && state == "finalized" && (checkpoint == nil || b.Number > checkpoint.Number) {
			copy := b
			checkpoint = &copy
			r.checkpoint.Store(checkpoint)
		}
	}
	return nil
}
func (r *dexRunner) retryReceipts(ctx context.Context) error {
	blocks, e := r.store.DEXPendingReceiptBlocks(ctx, ethereum.ManifestHash())
	if e != nil {
		return e
	}
	for _, b := range blocks {
		if time.Now().Before(r.receiptRetry[b.Hash]) {
			continue
		}
		r.receiptRetry[b.Hash] = time.Now().Add(time.Minute)
		logs, e := r.store.DEXLogs(ctx, b)
		if e != nil {
			return e
		}
		done, e := r.store.DEXReceiptHashes(ctx, b)
		if e != nil {
			return e
		}
		pending := []dex.Log{}
		for _, l := range logs {
			if !done[l.TxHash] {
				pending = append(pending, l)
			}
		}
		if len(pending) == 0 {
			return r.completeReceipts(ctx, b)
		}
		if e = r.client.Canonical(ctx, b); e != nil {
			return e
		}
		rr, fetchErr := r.client.Receipts(ctx, b.Anchor, pending)
		if e = r.store.WriteDEXReceipts(ctx, rr); e != nil {
			return e
		}
		if fetchErr == nil {
			return r.completeReceipts(ctx, b)
		}
		return fetchErr
	}
	return nil
}

// Repair only missing logs. Existing quote timestamps/batch remain unchanged;
// new evidence records when the logs actually became available.
func (r *dexRunner) retryLogs(ctx context.Context) (bool, error) {
	blocks, e := r.store.DEXPendingLogBlocks(ctx, ethereum.ManifestHash())
	if e != nil {
		return false, e
	}
	for _, b := range blocks {
		if time.Now().Before(r.receiptRetry[b.Hash]) {
			continue
		}
		r.receiptRetry[b.Hash] = time.Now().Add(time.Minute)
		logs, payload, e := r.client.Logs(ctx, b.Anchor)
		if e != nil {
			return true, e
		}
		if e = r.client.Canonical(ctx, b); e != nil {
			return true, e
		}
		evidence, e := r.client.Archive.PutObject(struct {
			Original, Logs dex.Hash
			RecoveredAt    time.Time
		}{b.Payload, payload, dex.Now()})
		if e != nil {
			return true, e
		}
		b.Payload = evidence
		b.LogCoverage = "complete"
		b.LogCount = uint32(len(logs))
		b.LogMembers = dex.LogDigest(logs)
		b.Revision = max(b.Revision+1, uint64(dex.Now().UnixMicro()))
		if len(logs) == 0 {
			b.ReceiptCoverage = "complete"
		}
		r.pendingLogs = &dexLogRepair{Block: b, Logs: logs}
		if e = r.store.CompleteDEXLogs(ctx, b, logs); e != nil {
			return true, e
		}
		r.pendingLogs = nil
		return true, nil
	}
	return false, nil
}
func (r *dexRunner) completeReceipts(ctx context.Context, b dex.Block) error {
	rr, e := r.store.DEXReceipts(ctx, b)
	if e != nil {
		return e
	}
	logs, e := r.store.DEXLogs(ctx, b)
	if e != nil {
		return e
	}
	wanted := map[dex.Hash]bool{}
	for _, l := range logs {
		wanted[l.TxHash] = true
	}
	seen := map[dex.Hash]bool{}
	for _, receipt := range rr {
		seen[receipt.TxHash] = true
	}
	for h := range wanted {
		if !seen[h] {
			return nil
		}
	}
	b.ReceiptCoverage = "complete"
	b.ReceiptCount = uint32(len(rr))
	b.ReceiptMembers = dex.ReceiptDigest(rr)
	b.Revision = max(b.Revision+1, uint64(dex.Now().UnixMicro()))
	evidence, e := r.client.Archive.PutObject(struct {
		Original, ReceiptMembers dex.Hash
		RecoveredAt              time.Time
	}{b.Payload, b.ReceiptMembers, dex.Now()})
	if e != nil {
		return e
	}
	b.Payload = evidence
	return r.store.WriteDEXBlock(ctx, b)
}

func checkDEXHead(head dex.Block, last, checkpoint *dex.Block) error {
	if checkpoint != nil {
		if head.Number < checkpoint.Number {
			return fmt.Errorf("stale RPC head below finalized checkpoint")
		}
		if head.Number == checkpoint.Number && head.Hash != checkpoint.Hash {
			return errDEXFinality
		}
	}
	if last != nil && head.Number < last.Number {
		return fmt.Errorf("stale RPC head below last observed height")
	}
	return nil
}
