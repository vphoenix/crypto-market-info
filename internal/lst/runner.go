package lst

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
)

func writeGob(path string, v any) error {
	if e := os.MkdirAll(filepath.Dir(path), 0700); e != nil {
		return e
	}
	f, e := os.CreateTemp(filepath.Dir(path), ".pending-")
	if e != nil {
		return e
	}
	name := f.Name()
	defer os.Remove(name)
	if e = encodePersistent(f, v); e == nil {
		e = f.Sync()
	}
	if ce := f.Close(); e == nil {
		e = ce
	}
	if e != nil {
		return e
	}
	if e = os.Rename(name, path); e != nil {
		return e
	}
	d, e := os.Open(filepath.Dir(path))
	if e != nil {
		return e
	}
	defer d.Close()
	return d.Sync()
}
func readGob(path string, v any) error {
	f, e := os.Open(path)
	if e != nil {
		return e
	}
	defer f.Close()
	return decodePersistent(f, v)
}
func (c *Collector) FlushPending(ctx context.Context) error {
	file := filepath.Join(c.StateDir, "pending-batch.gob")
	var b Batch
	if e := readGob(file, &b); e != nil {
		if os.IsNotExist(e) {
			return nil
		}
		return e
	}
	if e := Validate(b); e != nil {
		return e
	}
	if e := c.Store.WriteLST(ctx, b); e != nil {
		return e
	}
	if c.PruneCommittedResponses {
		if e := c.pruneRawEvidence(b); e != nil {
			return e
		}
		if len(b.RawEvidenceHashes) != 0 {
			log.Printf("raw_evidence_released capture_id=%s candidates=%d diagnostic=%t", b.Capture.CaptureId, len(b.RawEvidenceHashes), needsRawDiagnostic(b))
		}
	}
	return os.Remove(file)
}
func (c *Collector) Commit(ctx context.Context, b Batch) error {
	if e := Validate(b); e != nil {
		return e
	}
	file := filepath.Join(c.StateDir, "pending-batch.gob")
	if _, e := os.Stat(file); e == nil {
		return errors.New("unflushed_pending_batch")
	}
	if e := writeGob(file, b); e != nil {
		return e
	}
	return c.FlushPending(ctx)
}

type followupState struct {
	Seeds       []Quote
	Done        map[string]bool
	LastSeedDay string
}

func followupKey(ref string, delay uint32) string { return Hex(ref) + fmt.Sprintf(":%d", delay) }
func (c *Collector) restoreFollowups(ctx context.Context) (followupState, error) {
	s := followupState{Done: map[string]bool{}}
	caps, e := c.Store.LSTCaptures(ctx, c.Manifest.Hash)
	if e != nil {
		return s, e
	}
	cut := Now().Add(-31 * 24 * time.Hour)
	for _, cap := range caps {
		if cap.CaptureKind != "market" || !cap.Committed || cap.StartedAt.Before(cut) {
			continue
		}
		b, e := c.Store.LSTBatch(ctx, cap)
		if e != nil {
			return s, e
		}
		for _, q := range b.Quotes {
			if q.IsFollowupSeed && cap.Canonical {
				s.Seeds = append(s.Seeds, q)
				day := q.AvailableAt.Format("2006-01-02")
				if day > s.LastSeedDay {
					s.LastSeedDay = day
				}
			}
			if q.QuoteRole == "followup" && q.ReferenceQuoteId != nil && q.TargetDelaySeconds != nil {
				s.Done[followupKey(*q.ReferenceQuoteId, *q.TargetDelaySeconds)] = true
			}
		}
	}
	sort.Slice(s.Seeds, func(i, j int) bool { return s.Seeds[i].AvailableAt.Before(s.Seeds[j].AvailableAt) })
	if len(s.Seeds) > 30 {
		s.Seeds = s.Seeds[len(s.Seeds)-30:]
	}
	return s, nil
}
func (s *followupState) due(now time.Time) []Quote {
	out := []Quote{}
	for _, seed := range s.Seeds {
		for _, days := range []uint32{1, 3, 7, 14, 30} {
			delay := days * 86400
			at := seed.AvailableAt.Add(time.Duration(delay) * time.Second)
			key := followupKey(seed.QuoteId, delay)
			if s.Done[key] || at.After(now) {
				continue
			}
			q := seed
			q.ReferenceQuoteId = Ptr(seed.QuoteId)
			q.TargetDelaySeconds = &delay
			q.PlannedForAt = &at
			out = append(out, q)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].PlannedForAt.Before(*out[j].PlannedForAt) })
	if len(out) > 30 {
		out = out[:30]
	}
	return out
}
func (s *followupState) selectSeed(b *Batch) {
	now := b.Capture.AvailableAt
	day := now.Format("2006-01-02")
	if s.LastSeedDay == day || now.Hour() != 12 || now.Minute() >= 5 || !b.Capture.Canonical {
		return
	}
	route := "A"
	if now.Unix()/86400%2 != 0 {
		route = "B"
	}
	for i := range b.Quotes {
		q := &b.Quotes[i]
		if q.QuoteRole == "entry" && q.RouteId == route && q.PurchaseBudgetUsdtRaw.String() == "100000000000" && completeQuote(*q) {
			q.IsFollowupSeed = true
			s.LastSeedDay = day
			return
		}
	}
}
func (s *followupState) committed(b Batch) {
	for _, q := range b.Quotes {
		if q.IsFollowupSeed {
			s.Seeds = append(s.Seeds, q)
		}
		if q.QuoteRole == "followup" && q.ReferenceQuoteId != nil && q.TargetDelaySeconds != nil {
			s.Done[followupKey(*q.ReferenceQuoteId, *q.TargetDelaySeconds)] = true
		}
	}
	if len(s.Seeds) > 30 {
		s.Seeds = s.Seeds[len(s.Seeds)-30:]
	}
}

type logProgress struct {
	Manifest                     string
	Start, Next                  uint64
	RangeSize                    uint64
	CoverageStartedAt            time.Time
	BlockedSource, BlockedReason string
}

func (p *logProgress) sourceAllowed(source string) bool {
	if p.BlockedSource == "" {
		return true
	}
	if p.BlockedSource == source {
		return false
	}
	p.BlockedSource, p.BlockedReason = "", ""
	return true
}

func (p *logProgress) result(c *Collector, to uint64, err error) {
	if err == nil {
		p.Next = to + 1
		p.RangeSize = min(max(uint64(1), p.RangeSize)+1, c.liveLogBlocks())
	} else if errors.Is(err, ErrLogSourceUnsupported) {
		p.BlockedSource, p.BlockedReason = c.logSourceIdentity(), err.Error()
	} else if errors.Is(err, ErrLogRange) && p.RangeSize > 1 {
		p.RangeSize = max(uint64(1), p.RangeSize/2)
	} else if (strings.Contains(err.Error(), "budget_exhausted") || errors.Is(err, context.DeadlineExceeded)) && p.RangeSize > 1 {
		// Range verification can need many event headers. Bound the work on
		// the next scheduled pass, without retrying in the same minute.
		p.RangeSize = max(uint64(1), p.RangeSize/2)
	}
}

func (c *Collector) liveLogCursorPath() string {
	if c.LiveLogsFromBlock != 0 {
		return filepath.Join(c.StateDir, fmt.Sprintf("live-logs-from-%d.gob", c.LiveLogsFromBlock))
	}
	return filepath.Join(c.StateDir, "live-logs.gob")
}

func (c *Collector) logProgress(ctx context.Context) (logProgress, error) {
	file := c.liveLogCursorPath()
	var p logProgress
	e := readGob(file, &p)
	if e == nil && p.Manifest != c.Manifest.Hash {
		return p, errors.New("live_cursor_manifest_mismatch")
	}
	if e != nil && !os.IsNotExist(e) {
		return p, e
	}
	if os.IsNotExist(e) {
		p.Manifest = c.Manifest.Hash
		if c.LiveLogsFromBlock != 0 {
			head, err := c.RPC.Header(ctx, fmt.Sprintf("0x%x", c.LiveLogsFromBlock))
			if err != nil {
				return p, err
			}
			if head.Number != c.LiveLogsFromBlock {
				return p, errors.New("live_cursor_start_header_mismatch")
			}
			p.Start, p.Next, p.CoverageStartedAt = head.Number, head.Number, head.Time
		}
	} else if c.LiveLogsFromBlock != 0 && (p.Start != c.LiveLogsFromBlock || p.Next < p.Start) {
		return p, errors.New("live_cursor_start_mismatch")
	}
	if p.RangeSize == 0 {
		p.RangeSize = c.liveLogBlocks()
	}
	p.RangeSize = min(p.RangeSize, c.liveLogBlocks())
	caps, e := c.Store.LSTCaptures(ctx, c.Manifest.Hash)
	if e != nil {
		return p, e
	}
	// A cursor file can survive a reboot while the last acknowledged database
	// range does not. Rebuild the continuous prefix from its retained start;
	// the persisted Next alone must never prove coverage across a missing row.
	p.Next = p.Start
	sort.Slice(caps, func(i, j int) bool {
		if caps[i].FromBlock == nil {
			return false
		}
		if caps[j].FromBlock == nil {
			return true
		}
		return *caps[i].FromBlock < *caps[j].FromBlock
	})
	for _, cap := range caps {
		// A bounded manual gap repair has the same verified chain coverage as
		// watch. Consume its committed range without re-fetching it on restart.
		// A backfill must not choose the starting point for a new legacy cursor.
		modeAllowed := cap.CaptureMode == "live" || cap.CaptureMode == "backfill" && p.Start != 0
		if cap.ManifestHash == c.Manifest.Hash && cap.CaptureKind == "logs" && cap.Status == "complete" && cap.Committed && cap.Canonical && cap.Finality == "finalized" && cap.FromBlock != nil && cap.FromBlockTime != nil && cap.ToBlock != nil && *cap.FromBlock <= *cap.ToBlock && modeAllowed {
			if p.Start == 0 {
				p.Start = *cap.FromBlock
				p.CoverageStartedAt = *cap.FromBlockTime
			}
			if p.Next == 0 {
				p.Next = *cap.FromBlock
			}
			if *cap.FromBlock <= p.Next && *cap.ToBlock+1 > p.Next {
				p.Next = *cap.ToBlock + 1
			}
		}
	}
	if p.Next == 0 {
		head, e := c.RPC.Header(ctx, "finalized")
		if e != nil {
			return p, e
		}
		p = logProgress{Manifest: c.Manifest.Hash, Start: head.Number, Next: head.Number, RangeSize: c.liveLogBlocks(), CoverageStartedAt: head.Time}
	}
	if e = writeGob(file, p); e != nil {
		return p, e
	}
	return p, nil
}

// Restore only the outage gap before the explicit live segment. This shares the
// original cursor, writer, lock and provider gates; there is no history worker.
// Fetch failures are committed for diagnosis but never move that cursor.
func (c *Collector) catchUpOldLogGap(ctx, commitCtx context.Context, finalized Block, progress func(string)) error {
	if c.LiveLogsFromBlock == 0 || c.LogMode != "range" || c.PauseLiveLogs {
		return nil
	}
	old := *c
	old.LiveLogsFromBlock = 0
	if _, err := os.Stat(old.liveLogCursorPath()); os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}
	p, err := old.logProgress(ctx)
	if err != nil {
		return err
	}
	if p.Next >= c.LiveLogsFromBlock || !p.sourceAllowed(old.logSourceIdentity()) {
		return nil
	}
	p.RangeSize = min(max(uint64(1), p.RangeSize), uint64(32))
	from := p.Next
	to := min(from+p.RangeSize-1, c.LiveLogsFromBlock-1, finalized.Number)
	if from > to {
		return nil
	}
	b, fetchErr := old.logsWithFinalized(ctx, from, to, "live", &finalized)
	if err := Validate(b); err != nil {
		return err
	}
	if err := old.Commit(commitCtx, b); err != nil {
		return err
	}
	p.result(&old, to, fetchErr)
	p.RangeSize = min(p.RangeSize, uint64(32))
	if err := writeGob(old.liveLogCursorPath(), p); err != nil {
		return err
	}
	progress(fmt.Sprintf("old_log_gap from=%d to=%d status=%s next=%d target=%d request=%d finalization=%d claim=%d reason=%q", from, to, b.Capture.Status, p.Next, c.LiveLogsFromBlock-1, len(b.Requests), len(b.Finalizations), len(b.Claims), b.Capture.Reason))
	return nil
}

func (c *Collector) oldLogGapHasRoom(ctx context.Context) (bool, error) {
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) < 35*time.Second {
		return false, nil
	}
	primarySlots, err := c.RPC.Transport.AvailableRPCSlots(c.RPC.URL)
	if err != nil {
		return false, err
	}
	if c.logRPC().URL == c.RPC.URL {
		return primarySlots >= 10, nil
	}
	logSlots, err := c.RPC.Transport.AvailableRPCSlots(c.logRPC().URL)
	return primarySlots >= 4 && logSlots >= 6, err
}
func (c *Collector) FinalizePending(ctx context.Context) error {
	return c.finalizePending(ctx, nil)
}
func (c *Collector) finalizePending(ctx context.Context, finalized *Block) error {
	caps, e := c.Store.LSTCaptures(ctx, c.Manifest.Hash)
	if e != nil {
		return e
	}
	// No RPC is needed when all committed markets already have finality.
	pending := false
	for _, cap := range caps {
		if cap.CaptureKind == "market" && cap.Committed && cap.ToBlock != nil && cap.Finality != "finalized" && cap.Finality != "orphaned" {
			pending = true
			break
		}
	}
	if !pending {
		return nil
	}
	var end Block
	if finalized != nil {
		end = *finalized
	} else {
		end, e = c.RPC.Header(ctx, "finalized")
		if e != nil {
			return e
		}
	}
	count := 0
	limit := 4
	if c.RPC.ProtocolMulticall {
		limit = 1
	}
	for _, cap := range caps {
		if count >= limit {
			break
		}
		if cap.CaptureKind != "market" || !cap.Committed || cap.Finality == "finalized" || cap.Finality == "orphaned" || cap.ToBlock == nil || *cap.ToBlock > end.Number {
			continue
		}
		h, e := c.RPC.Header(ctx, fmt.Sprintf("0x%x", *cap.ToBlock))
		if e != nil {
			return e
		}
		cap.Revision++
		if cap.ToBlockHash == nil || h.Hash != *cap.ToBlockHash {
			cap.Canonical = false
			cap.Finality = "orphaned"
		} else {
			cap.Canonical = true
			cap.Finality = "finalized"
		}
		if e = c.Store.WriteLSTRevision(ctx, cap); e != nil {
			return e
		}
		count++
	}
	return nil
}
func waitContext(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(max(d, 0))
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

type fundingConfirmation struct {
	At, NextAttempt time.Time
	Failures        int
}
type fundingSchedule struct {
	NextRegular, RetryNotBefore time.Time
	Failures                    int
	Pending                     []fundingConfirmation
}

func fundingRetry(failures int) time.Duration {
	if failures <= 1 {
		return 5 * time.Minute
	}
	if failures == 2 {
		return 15 * time.Minute
	}
	return time.Hour
}
func (s *fundingSchedule) observe(b Batch, now time.Time) {
	keep := s.Pending[:0]
	for _, p := range s.Pending {
		if !p.At.Before(now.Add(-48 * time.Hour)) {
			keep = append(keep, p)
		}
	}
	s.Pending = keep
	for _, q := range b.Quotes {
		if q.NextFundingTime == nil || q.MarkPayloadHash == nil || q.MarkSourceTime == nil {
			continue
		}
		at := q.NextFundingTime.UTC()
		if at.Before(now.Add(-48*time.Hour)) || at.After(now.Add(24*time.Hour)) {
			continue
		}
		seen := false
		for _, p := range s.Pending {
			if p.At.Equal(at) {
				seen = true
				break
			}
		}
		if !seen && len(s.Pending) < 64 {
			s.Pending = append(s.Pending, fundingConfirmation{At: at, NextAttempt: at.Add(2 * time.Minute)})
		}
	}
}
func (s *fundingSchedule) due(now time.Time) bool {
	if now.Before(s.RetryNotBefore) {
		return false
	}
	if s.Failures > 0 {
		return true
	}
	if !now.Before(s.NextRegular) {
		return true
	}
	for _, p := range s.Pending {
		if !now.Before(p.NextAttempt) {
			return true
		}
	}
	return false
}
func (s *fundingSchedule) result(now time.Time, b Batch, err error) {
	s.NextRegular = now.Add(15 * time.Minute)
	if err != nil || b.Capture.Status != "complete" {
		s.Failures++
		s.RetryNotBefore = now.Add(fundingRetry(s.Failures))
	} else {
		s.Failures = 0
		s.RetryNotBefore = time.Time{}
	}
	keep := s.Pending[:0]
	for _, p := range s.Pending {
		found := false
		if err == nil && b.Capture.Status == "complete" {
			for _, r := range b.Funding {
				if !r.FundingTime.Before(p.At) && r.FundingTime.Before(p.At.Add(time.Minute)) && r.SettlementMarkPriceTickE8 != nil {
					found = true
					break
				}
			}
		}
		if found {
			continue
		}
		if !now.Before(p.NextAttempt) {
			p.Failures++
			p.NextAttempt = now.Add(fundingRetry(p.Failures))
		}
		keep = append(keep, p)
	}
	s.Pending = keep
}

func (c *Collector) refreshMetadata(ctx context.Context) error {
	meta, e := c.CEX.Metadata(ctx)
	if e != nil {
		return e
	}
	if !meta.Instrument.SameDefinition(c.Metadata.Instrument) {
		return errors.New("metadata_contract_identity_changed")
	}
	meta.Instrument.ID = c.Metadata.Instrument.ID
	oldExchange, oldFunding := c.Metadata.ExchangeResponse.PayloadHash, c.Metadata.FundingInfoResponse.PayloadHash
	proofs := c.IdentityResponses[:0]
	for _, r := range c.IdentityResponses {
		if r.PayloadHash != oldExchange && r.PayloadHash != oldFunding {
			proofs = append(proofs, r)
		}
	}
	c.IdentityResponses = append(proofs, meta.ExchangeResponse, meta.FundingInfoResponse)
	c.Metadata = meta
	return nil
}

func quietTask(parent context.Context, roundStart time.Time, budget time.Duration) (context.Context, context.CancelFunc) {
	deadline := Now().Add(budget)
	end := roundStart.Add(55 * time.Second)
	if end.Before(deadline) {
		deadline = end
	}
	return context.WithDeadline(parent, deadline)
}

func latestCaptures(caps []Capture) []Capture {
	latest := map[uuid.UUID]Capture{}
	for _, c := range caps {
		if old, ok := latest[c.CaptureId]; !ok || c.Revision > old.Revision {
			latest[c.CaptureId] = c
		}
	}
	out := make([]Capture, 0, len(latest))
	for _, c := range latest {
		out = append(out, c)
	}
	return out
}

// Fixed range planners include finalized/canonical headers and all possible
// receipt/ABI calls. Live watch instead stops before an unaffordable next read.
func receiptPieceBlocks(planned uint64, slots int) uint64 {
	if slots < 6 {
		return 0
	}
	return min(planned, uint64((slots-3)/3))
}
func receiptPieceBlocksWithFinalized(planned uint64, slots int) uint64 {
	if slots < 5 {
		return 0
	}
	return min(planned, uint64((slots-2)/3))
}

func (c *Collector) Watch(ctx context.Context, once bool, progress func(string)) error {
	if progress == nil {
		progress = func(string) {}
	}
	if err := c.ValidateReady(); err != nil {
		return err
	}
	if err := c.FlushPending(ctx); err != nil {
		return err
	}
	s, err := c.restoreFollowups(ctx)
	if err != nil {
		return err
	}
	var cursor logProgress
	var nextLogs, nextOldGap, nextGas, nextMarket time.Time
	if c.PauseLiveLogs {
		progress("log_scan paused=configuration; cursor retained; history coverage remains incomplete")
	}
	if c.LiveLogsFromBlock != 0 {
		progress(fmt.Sprintf("log_scan explicit_start_block=%d cursor=%s; original live-logs.gob retained; earlier gaps remain unknown", c.LiveLogsFromBlock, c.liveLogCursorPath()))
	}
	nextMetadata := Now().Add(6 * time.Hour)
	funding := fundingSchedule{}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		// Optional maintenance may have frozen a batch just as its deadline
		// expired. Finish that database write before collecting fresh evidence.
		if err := c.FlushPending(ctx); err != nil {
			return err
		}
		start := Now()
		marketRan := !start.Before(nextMarket)
		if marketRan {
			slots := 25
			if c.EntryRoutesPerRound == 1 {
				slots = 22
			}
			if c.RPC.ProtocolMulticall {
				slots = 13
				if c.EntryRoutesPerRound == 1 {
					slots = 10
				}
			}
			waitingAt := Now()
			if err := c.RPC.Transport.WaitRPCSlots(ctx, c.RPC.URL, slots); err != nil {
				return err
			}
			if waited := Now().Sub(waitingAt); waited >= time.Second {
				progress(fmt.Sprintf("market_rpc_capacity_wait=%s; observation starts after wait", waited.Round(time.Millisecond)))
			}
			// Base this round's maintenance and next interval on the actual
			// start too; a quota wait must not replay an already expired minute.
			start = Now()
			b, err := c.Market(ctx, s.due(Now()))
			if err != nil {
				return err
			}
			// Admission may wait for a recent head. The actual observation
			// start controls maintenance, the next interval and UTC seed rules.
			start = b.Capture.StartedAt
			s.selectSeed(&b)
			if err = b.Seal(); err != nil {
				return err
			}
			if err = c.Commit(ctx, b); err != nil {
				return err
			}
			s.committed(b)
			funding.observe(b, Now())
			scheduled, usable := 0, 0
			for _, q := range b.Quotes {
				if q.QuoteRole == "entry" && q.TimingStatus != "not_scheduled" {
					scheduled++
					if completeQuote(q) {
						usable++
					}
				}
			}
			progress(fmt.Sprintf("market capture=%s status=%s quotes=%d scheduled_entry=%d usable_entry=%d canonical=%t reason=%q protocol_reason=%q", b.Capture.CaptureId, b.Capture.Status, len(b.Quotes), scheduled, usable, b.Capture.Canonical, b.Capture.Reason, b.Protocols[0].Reason))
			for _, q := range b.Quotes {
				if q.TimingStatus != "not_scheduled" && !completeQuote(q) {
					progress(fmt.Sprintf("quote role=%s route=%s buy=%s conversion=%s exit=%s hedge=%s timing=%s buy_reason=%q conversion_reason=%q exit_reason=%q hedge_reason=%q reason=%q", q.QuoteRole, q.RouteId, q.BuyStatus, q.ConversionStatus, q.ExitStatus, q.HedgeStatus, q.TimingStatus, q.BuyReason, q.ConversionReason, q.ExitReason, q.HedgeReason, q.Reason))
				}
			}
			if once {
				return nil
			}
			nextMarket = start.Add(c.marketInterval())
		}
		// Serial maintenance stays inside the quiet part of this minute. It
		// never queues market intervals or races writers on pending-batch.gob.
		quiet, cancel := context.WithDeadline(ctx, start.Add(55*time.Second))
		now := Now()
		if !now.Before(nextMetadata) && quiet.Err() == nil {
			metadataCtx, stop := quietTask(quiet, start, 20*time.Second)
			metadataErr := c.refreshMetadata(metadataCtx)
			stop()
			if metadataErr != nil {
				if err := c.saveUncommittedDiagnostic("maintenance", metadataErr); err != nil {
					cancel()
					return err
				}
				if metadataErr.Error() == "metadata_contract_identity_changed" {
					cancel()
					return metadataErr
				}
				progress("metadata: " + metadataErr.Error())
				nextMetadata = Now().Add(5 * time.Minute)
			} else {
				nextMetadata = Now().Add(6 * time.Hour)
				progress("metadata refreshed with unchanged instrument identity")
			}
		}
		now = Now()
		if funding.due(now) && quiet.Err() == nil {
			from, to := now.Add(-48*time.Hour).Truncate(time.Millisecond), now.Truncate(time.Millisecond)
			fund, fundingErr := c.Funding(quiet, from, to)
			if validationErr := Validate(fund); validationErr == nil {
				if commitErr := c.Commit(ctx, fund); commitErr != nil {
					cancel()
					return commitErr
				}
			} else if fundingErr == nil {
				fundingErr = validationErr
			}
			funding.result(Now(), fund, fundingErr)
			if fundingErr != nil {
				progress("funding: " + fundingErr.Error())
			}
		}
		var maintenanceFinalized *Block
		now = Now()
		if !c.PauseLiveLogs && !now.Before(nextLogs) && quiet.Err() == nil {
			passes := 1
			if c.marketInterval() > time.Minute && c.LogMode == "receipts" {
				passes = 2
			}
			for pass := 0; pass < passes && quiet.Err() == nil; pass++ {
				plannedBlocks := c.liveLogBlocks()
				if c.LogMode == "receipts" && c.logRPC().URL == c.RPC.URL {
					slots, slotErr := c.RPC.Transport.AvailableRPCSlots(c.RPC.URL)
					if slotErr != nil {
						cancel()
						return slotErr
					}
					gap := c.RPC.Transport.spacing("rpc", Now())
					minWindow := 4*gap + 2*c.RPC.Transport.cfg.HTTP.Timeout
					endQuiet, _ := quiet.Deadline()
					plannedBlocks = 0
					if slots >= 4 && time.Until(endQuiet) >= minWindow {
						plannedBlocks = c.liveLogBlocks()
					}
					if plannedBlocks == 0 && time.Until(endQuiet) >= minWindow {
						// Wait for rolling reservations to expire within this
						// quiet window. The actual sends retain the original gate.
						waitErr := c.RPC.Transport.WaitRPCSlots(quiet, c.RPC.URL, 4)
						if waitErr == nil {
							slots, slotErr = c.RPC.Transport.AvailableRPCSlots(c.RPC.URL)
							if slotErr != nil {
								cancel()
								return slotErr
							}
							if slots >= 4 && time.Until(endQuiet) >= minWindow {
								plannedBlocks = c.liveLogBlocks()
							}
						}
					}
					if plannedBlocks == 0 {
						progress(fmt.Sprintf("log_scan deferred=live_piece_budget available_rpc=%d required_initial_rpc=4 next=%d", slots, cursor.Next))
						break
					}
				}
				// Use a new local error for this job. A previous finality/funding
				// failure must never silently skip an otherwise healthy log pass.
				var logErr error
				if cursor.Next == 0 {
					cursor, logErr = c.logProgress(quiet)
				}
				if logErr == nil && !cursor.sourceAllowed(c.logSourceIdentity()) {
					progress("logs paused: " + cursor.BlockedReason + "; cursor retained; configure a verified log source")
				} else if logErr == nil {
					end, headerErr := c.RPC.Header(quiet, "finalized")
					logErr = headerErr
					if headerErr == nil {
						maintenanceFinalized = &end
					}
					if headerErr == nil && cursor.Next <= end.Number {
						to := min(cursor.Next+min(min(cursor.RangeSize, c.liveLogBlocks()), plannedBlocks)-1, end.Number)
						from := cursor.Next
						logs, fetchErr := c.logsRange(quiet, from, to, "live", &end, c.LogMode == "receipts")
						if validationErr := Validate(logs); validationErr == nil {
							if commitErr := c.Commit(ctx, logs); commitErr != nil {
								cancel()
								return commitErr
							}
						} else if fetchErr == nil {
							fetchErr = validationErr
						}
						logErr = fetchErr
						coveredTo, cursorTo := uint64(0), to
						if fetchErr == nil && logs.Capture.ToBlock != nil {
							coveredTo = *logs.Capture.ToBlock
							cursorTo = coveredTo
						}
						cursor.result(c, cursorTo, fetchErr)
						progress(fmt.Sprintf("log_scan mode=%s planned_from=%d planned_to=%d status=%s covered_to=%d next=%d finalized=%d backlog_blocks=%d request=%d finalization=%d claim=%d reason=%q", c.LogMode, from, to, logs.Capture.Status, coveredTo, cursor.Next, end.Number, end.Number+1-cursor.Next, len(logs.Requests), len(logs.Finalizations), len(logs.Claims), logs.Capture.Reason))
						if writeErr := writeGob(c.liveLogCursorPath(), cursor); writeErr != nil {
							cancel()
							return writeErr
						}
					}
				}
				if logErr != nil {
					progress("logs: " + logErr.Error())
					break
				}
			}
			nextLogs = start.Add(c.logInterval())
		}
		if quiet.Err() == nil {
			if finalityErr := c.finalizePending(quiet, maintenanceFinalized); finalityErr != nil {
				if err := c.saveUncommittedDiagnostic("maintenance", finalityErr); err != nil {
					cancel()
					return err
				}
				if quiet.Err() == nil {
					progress("finality: " + finalityErr.Error())
				}
			}
		}
		if c.LiveLogsFromBlock != 0 && !c.PauseLiveLogs && !marketRan && maintenanceFinalized != nil && cursor.Next > maintenanceFinalized.Number && !Now().Before(nextOldGap) && quiet.Err() == nil {
			room, roomErr := c.oldLogGapHasRoom(quiet)
			if roomErr != nil {
				cancel()
				return roomErr
			}
			if room {
				nextOldGap = Now().Add(5 * time.Minute)
				if gapErr := c.catchUpOldLogGap(quiet, ctx, *maintenanceFinalized, progress); gapErr != nil {
					cancel()
					return gapErr
				}
			}
		}
		now = Now()
		quietEnd, _ := quiet.Deadline()
		if !now.Before(nextGas) && quiet.Err() == nil && time.Until(quietEnd) >= 10*time.Second {
			gasCtx, stop := quietTask(quiet, start, 10*time.Second)
			gasErr := c.GasPass(gasCtx, 10)
			stop()
			if gasErr != nil {
				if err := c.saveUncommittedDiagnostic("maintenance", gasErr); err != nil {
					cancel()
					return err
				}
			}
			if gasErr != nil && !errors.Is(gasErr, context.DeadlineExceeded) {
				progress("gas: " + gasErr.Error())
			}
			nextGas = Now().Add(5 * time.Minute)
		}
		cancel()
		progress(fmt.Sprintf("http_stats=%v", c.RPC.Transport.Stats()))
		if err := waitContext(ctx, time.Until(start.Add(time.Minute))); err != nil {
			return err
		}
	}
}

type historicalProgress struct {
	Manifest               string
	Days                   int
	From, To, Next         uint64
	RangeSize              uint64
	FundingFrom, FundingTo time.Time
	FundingDone            bool
}

// A successful database commit can precede the local cursor fsync. Recover
// contiguous coverage from committed facts before making any public request.
func recoverHistorical(p *historicalProgress, caps []Capture) {
	caps = latestCaptures(caps)
	sort.Slice(caps, func(i, j int) bool {
		if caps[i].FromBlock == nil {
			return false
		}
		if caps[j].FromBlock == nil {
			return true
		}
		return *caps[i].FromBlock < *caps[j].FromBlock
	})
	for _, cap := range caps {
		if cap.ManifestHash != p.Manifest || !cap.Committed || !cap.Canonical || cap.Status != "complete" {
			continue
		}
		if cap.CaptureKind == "funding" && cap.WindowFromAt != nil && cap.WindowToAt != nil && cap.WindowFromAt.Equal(p.FundingFrom) && cap.WindowToAt.Equal(p.FundingTo) {
			p.FundingDone = true
		}
		if cap.CaptureKind != "logs" || cap.Finality != "finalized" || cap.FromBlock == nil || cap.ToBlock == nil || *cap.FromBlock > p.Next || *cap.ToBlock < p.Next || p.Next > p.To {
			continue
		}
		p.Next = min(*cap.ToBlock, p.To) + 1
	}
}

func (c *Collector) Backfill(ctx context.Context, days, maxRanges int, progress func(string)) error {
	if c.LogMode == "receipts" {
		return errors.New("receipts_history_backfill_disabled_use_bounded_gap")
	}
	if progress == nil {
		progress = func(string) {}
	}
	if days < 1 || days > 30 {
		return errors.New("backfill_days_1_to_30")
	}
	if e := c.ValidateReady(); e != nil {
		return e
	}
	if e := c.FlushPending(ctx); e != nil {
		return e
	}
	file := filepath.Join(c.StateDir, "backfill.gob")
	var p historicalProgress
	e := readGob(file, &p)
	if e != nil && !os.IsNotExist(e) {
		return e
	}
	if e == nil && p.Manifest != c.Manifest.Hash {
		return errors.New("backfill_manifest_mismatch")
	}
	if e == nil && p.Days != days {
		return errors.New("backfill_days_mismatch")
	}
	if os.IsNotExist(e) {
		end, e := c.RPC.Header(ctx, "finalized")
		if e != nil {
			return e
		}
		target := end.Time.Add(-time.Duration(days+30) * 24 * time.Hour)
		from, e := c.BlockAtTime(ctx, target, end)
		if e != nil {
			return e
		}
		p = historicalProgress{Manifest: c.Manifest.Hash, Days: days, From: from, To: end.Number, Next: from, RangeSize: c.Manifest.MaxLogBlocks, FundingFrom: end.Time.Add(-time.Duration(days) * 24 * time.Hour).Truncate(time.Millisecond), FundingTo: end.Time.Truncate(time.Millisecond)}
		if e = writeGob(file, p); e != nil {
			return e
		}
	}
	caps, readErr := c.Store.LSTCaptures(ctx, c.Manifest.Hash)
	if readErr != nil {
		return readErr
	}
	recoverHistorical(&p, caps)
	if p.RangeSize == 0 {
		p.RangeSize = c.liveLogBlocks()
	}
	if e = writeGob(file, p); e != nil {
		return e
	}
	if !p.FundingDone {
		b, err := c.Funding(ctx, p.FundingFrom, p.FundingTo)
		b.Capture.CaptureMode = "backfill"
		if e = b.Seal(); e != nil {
			return e
		}
		if e = c.Commit(ctx, b); e != nil {
			return e
		}
		if err != nil {
			return err
		}
		p.FundingDone = true
		if e = writeGob(file, p); e != nil {
			return e
		}
	}
	// Already committed ranges were recovered above; only the uncovered suffix
	// is sent to the provider, still subject to the persisted global gate.
	count := 0
	for p.Next <= p.To && (maxRanges == 0 || count < maxRanges) {
		end := min(p.Next+p.RangeSize-1, p.To)
		b, err := c.Logs(ctx, p.Next, end, "backfill")
		if e := Validate(b); e == nil {
			if e = c.Commit(ctx, b); e != nil {
				return e
			}
		} else {
			return e
		}
		count++
		if errors.Is(err, ErrLogRange) && p.RangeSize > 1 {
			p.RangeSize = max(uint64(1), p.RangeSize/2)
			if e = writeGob(file, p); e != nil {
				return e
			}
			continue
		}
		if err != nil {
			return err
		}
		p.Next = end + 1
		p.RangeSize = c.Manifest.MaxLogBlocks
		if e = writeGob(file, p); e != nil {
			return e
		}
		progress(fmt.Sprintf("backfill committed through=%d target=%d", end, p.To))
	}
	gasCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	gasErr := c.GasPass(gasCtx, 10)
	cancel()
	if gasErr != nil {
		progress("gas: " + gasErr.Error())
	}
	return nil
}
