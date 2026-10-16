package across

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint"
)

type runtimeCapturePager interface {
	AcrossCapturePage(context.Context, string, []byte, int) ([]Capture, []byte, error)
}
type conversionOrder struct {
	CaptureTime time.Time
	CaptureID   string
	Member      int
	Deposit     Deposit
}
type conversionPlan struct {
	Version       uint32
	FrozenAt      time.Time
	Binding       runtimecheckpoint.Binding
	Phase         string
	Cursor        []byte
	Pages         []runtimecheckpoint.Reference
	FactPage      int
	HeaderDigests map[string]string
	State         runtimeState
	Orders        []conversionOrder
	Report        RuntimeConversionReport
}
type RuntimeConversionReport struct {
	InputInventory                    runtimecheckpoint.InputInventory
	Version                           uint32
	FrozenAt, VerifiedAt              time.Time
	Binding                           runtimecheckpoint.Binding
	Headers, FactBatches, FactPages   uint64
	Unreadable                        []string
	InputDigest                       string
	RawCursors                        map[uint64]uint64
	Active, Partial, Receipts, Orders int
	CheckpointBytes                   int
	Verified                          bool
}

func conversionDigest(captures map[string]string) string {
	keys := make([]string, 0, len(captures))
	for key := range captures {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	h := sha256.New()
	for _, key := range keys {
		h.Write([]byte(key))
		h.Write([]byte(captures[key]))
	}
	return hex.EncodeToString(h.Sum(nil))
}
func conversionOrderBefore(a, b conversionOrder) bool {
	if !a.CaptureTime.Equal(b.CaptureTime) {
		return a.CaptureTime.Before(b.CaptureTime)
	}
	if a.CaptureID != b.CaptureID {
		return a.CaptureID < b.CaptureID
	}
	return a.Member < b.Member
}
func (p *conversionPlan) addOrder(candidate conversionOrder, m Manifest) {
	if !route(candidate.Deposit, m) || uint64(candidate.Deposit.FillDeadline) < uint64(p.FrozenAt.Unix()) {
		return
	}
	key := orderKey(candidate.Deposit)
	for i, old := range p.Orders {
		if orderKey(old.Deposit) == key {
			if conversionOrderBefore(candidate, old) {
				p.Orders[i] = candidate
			}
			sort.SliceStable(p.Orders, func(i, j int) bool { return conversionOrderBefore(p.Orders[i], p.Orders[j]) })
			return
		}
	}
	p.Orders = append(p.Orders, candidate)
	sort.SliceStable(p.Orders, func(i, j int) bool { return conversionOrderBefore(p.Orders[i], p.Orders[j]) })
	if len(p.Orders) > int(m.MaxPending) {
		p.Orders = p.Orders[:m.MaxPending]
	}
}

// Conversion processes the complete frozen applicable range. Final receipt
// identities are pruned only after the last fact page, because an earlier page
// may contain a receipt whose event is on a later primary-key page.
func (p *conversionPlan) facts(b Batch, m Manifest) {
	c := b.Capture
	for i, d := range b.Deposits {
		p.addOrder(conversionOrder{c.StartedAt, c.CaptureId, i, d}, m)
	}
	for _, f := range b.Fills {
		if ch, ok := m.Chain(f.ChainId); ok && f.OutputToken == WordAddress(ch.USDC) {
			p.State.receiptFact(receiptTask{f.ChainId, f.BlockHash, f.TxHash}, f.BlockNumber, c.CaptureId, runtimeReceiptEvent, c.AvailableAt)
		}
	}
	for _, r := range b.Refunds {
		if ch, ok := m.Chain(r.ChainId); ok && r.Token == ch.USDC {
			p.State.receiptFact(receiptTask{r.ChainId, r.BlockHash, r.TxHash}, r.BlockNumber, c.CaptureId, runtimeReceiptEvent|runtimeReceiptRefund, c.AvailableAt)
		}
	}
	for _, r := range b.Transfers {
		p.State.receiptFact(receiptTask{r.ChainId, r.BlockHash, r.TxHash}, r.BlockNumber, c.CaptureId, runtimeReceiptTransfers, c.AvailableAt)
	}
	if c.CaptureKind == "receipts" && c.CompletedTasks == 1 {
		for _, r := range b.Receipts {
			bits := runtimeReceiptKnown
			if r.FeeComplete {
				bits |= runtimeReceiptFee
			}
			p.State.receiptFact(receiptTask{r.ChainId, r.BlockHash, r.TxHash}, r.BlockNumber, c.CaptureId, bits, c.AvailableAt)
		}
	}
	p.State.sealReceiptParents(c)
	p.Report.FactBatches++
}
func (p *conversionPlan) finish(m Manifest) {
	for key, r := range p.State.Receipts {
		if r.Known && !r.FeeComplete {
			r.RetryUnknown = true
			r.Pending = true
		}
		if r.FeeComplete && !r.Pending && p.State.finalizedDecodedBlock(r.Task.ChainID, r.Block) {
			delete(p.State.Receipts, key)
		} else {
			p.State.Receipts[key] = r
		}
	}
	for _, order := range p.Orders {
		p.State.Watch.addAt(order.Deposit, "restart", m, p.FrozenAt)
	}
	for id, c := range p.State.Partial {
		if firstUncovered(*c.FromBlock, *c.ToBlock, p.State.coverage(c.ChainId, true)) > *c.ToBlock {
			delete(p.State.Partial, id)
		}
	}
}

func (p *conversionPlan) compare(ctx context.Context, c *Collector) error {
	// These are the actual pre-change recovery functions on the same frozen
	// database input. Their cost is explicit conversion cost, never startup.
	legacy := &Collector{Manifest: c.Manifest, Store: c.Store}
	w, err := legacy.restore(ctx, p.FrozenAt)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(w.Orders, p.State.Watch.Orders) || !reflect.DeepEqual(w.Tasks, p.State.Watch.Tasks) {
		return errors.New("runtime_conversion_order_or_task_mismatch")
	}
	if !reflect.DeepEqual(w.Cursors, p.State.Watch.Cursors) {
		return errors.New("runtime_conversion_cursor_mismatch")
	}
	if err = legacy.PrepareRepair(ctx); err != nil {
		return err
	}
	got := map[string]receiptTask{}
	missing := map[string]bool{}
	for key, r := range p.State.Receipts {
		if r.Pending {
			got[key] = r.Task
			if r.Refund && !r.Transfers {
				missing[key] = true
			}
		}
	}
	if !reflect.DeepEqual(got, legacy.receiptQueue) || !reflect.DeepEqual(missing, legacy.receiptDataMissing) {
		return errors.New("runtime_conversion_receipt_mismatch")
	}
	actual := map[string]string{}
	for _, cap := range legacy.captures {
		actual[cap.CaptureId] = ID(cap)
	}
	if conversionDigest(actual) != conversionDigest(p.HeaderDigests) || len(actual) != len(p.HeaderDigests) {
		return errors.New("runtime_conversion_input_changed")
	}
	active := map[string]Capture{}
	partial := map[string]Capture{}
	anchors := map[uint64]Capture{}
	for _, cap := range legacy.captures {
		if cap.Canonical && cap.Committed && anchored(cap) {
			if cap.Finality != "finalized" {
				active[cap.CaptureId] = cap
			} else if old, ok := anchors[cap.ChainId]; !ok || *cap.ToBlock > *old.ToBlock {
				anchors[cap.ChainId] = cap
			}
		}
		if runtimeRaw(cap) && cap.Status == "partial" && !strings.Contains(cap.Reason, repairAbandonedMarker) && firstUncovered(*cap.FromBlock, *cap.ToBlock, historyIntervals(legacy.captures, cap.ChainId, true)) <= *cap.ToBlock {
			partial[cap.CaptureId] = cap
		}
	}
	if !reflect.DeepEqual(active, p.State.Active) || !reflect.DeepEqual(partial, p.State.Partial) {
		return errors.New("runtime_conversion_finality_or_partial_mismatch")
	}
	for chain, ch := range p.State.Chains {
		if anchor, ok := anchors[chain]; ok {
			if ch.Anchor == nil || ID(*ch.Anchor) != ID(anchor) {
				return errors.New("runtime_conversion_anchor_mismatch")
			}
		} else if ch.Anchor != nil {
			return errors.New("runtime_conversion_extra_anchor")
		}
		for _, decoded := range []bool{false, true} {
			var ranges []RuntimeRange
			for _, r := range historyIntervals(legacy.captures, chain, decoded) {
				ranges = append(ranges, RuntimeRange{r.from, r.to})
			}
			if !reflect.DeepEqual(intervalSlice(mergeRuntimeRanges(ranges)), p.State.coverage(chain, decoded)) {
				return errors.New("runtime_conversion_coverage_mismatch")
			}
		}
	}
	return nil
}

// ConvertRuntime must be called with the process/maintenance lock held and all
// writers of this scope fenced/stopped. checkFrozen rechecks the exact database
// binding at each resume and immediately before publication. No domain facts
// are written, and no checkpoint is published until the old/new comparison
// succeeds. Interrupted work stays in the explicit .conversion directory.
func (c *Collector) ConvertRuntime(ctx context.Context, dir string, binding runtimecheckpoint.Binding, inputPath string, checkFrozen func(context.Context) (runtimecheckpoint.Binding, error), progress func(string)) (RuntimeConversionReport, error) {
	var report RuntimeConversionReport
	input, inputErr := runtimecheckpoint.ReadConversionInput(inputPath, binding, "across")
	if inputErr != nil {
		return report, inputErr
	}
	if c.runtime != nil || checkFrozen == nil {
		return report, errors.New("runtime_conversion_requires_frozen_exclusive_owner")
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		return report, errors.New("runtime_conversion_destination_exists")
	}
	pager, ok := c.Store.(runtimeCapturePager)
	if !ok {
		return report, errors.New("runtime_conversion_requires_bounded_capture_pages")
	}
	verifyBinding := func() error {
		actual, err := checkFrozen(ctx)
		if err != nil {
			return err
		}
		if actual != binding {
			return runtimecheckpoint.ErrConflict
		}
		return nil
	}
	if err := verifyBinding(); err != nil {
		return report, err
	}
	stageBinding := binding
	stageBinding.StateVersion += 1000
	stage := dir + ".conversion"
	p := conversionPlan{Version: 1, FrozenAt: Now(), Binding: binding, Phase: "headers", State: newRuntimeState(c.Manifest), HeaderDigests: map[string]string{}}
	var files *runtimecheckpoint.Store
	var err error
	if _, err = os.Stat(filepath.Join(stage, "checkpoint.current")); os.IsNotExist(err) {
		raw, e := runtimecheckpoint.MarshalTyped(p)
		if e != nil {
			return report, e
		}
		files, err = runtimecheckpoint.Create(stage, stageBinding, raw, nil, 64<<20)
	} else {
		files, err = runtimecheckpoint.Open(stage, stageBinding, 64<<20)
		if err == nil {
			err = runtimecheckpoint.UnmarshalTyped(files.State(), &p)
		}
	}
	if err != nil {
		if files != nil {
			files.Close()
		}
		return report, err
	}
	defer files.Close()
	p.Report.InputInventory, err = input.Persist(files, p.Report.InputInventory)
	if err != nil {
		return p.Report, err
	}
	if p.Version != 1 || p.Binding != binding || p.State.Manifest != c.Manifest.Hash {
		return report, runtimecheckpoint.ErrConflict
	}
	save := func() error {
		raw, err := runtimecheckpoint.MarshalTyped(p)
		if err != nil {
			return err
		}
		return files.Checkpoint(raw, append(append([]runtimecheckpoint.Reference(nil), p.Pages...), p.Report.InputInventory.References()...))
	}
	if err = save(); err != nil {
		return p.Report, err
	}

	for p.Phase == "headers" {
		if err = ctx.Err(); err != nil {
			return p.Report, err
		}
		page, next, err := pager.AcrossCapturePage(ctx, c.Manifest.Hash, p.Cursor, 500)
		if err != nil {
			return p.Report, err
		}
		if len(page) == 0 {
			p.Phase = "facts"
			if err = save(); err != nil {
				return p.Report, err
			}
			break
		}
		if len(next) != 32 || len(p.Cursor) > 0 && bytes.Compare(next, p.Cursor) <= 0 {
			return p.Report, errors.New("runtime_conversion_unstable_cursor")
		}
		for _, cap := range page {
			if cap.ManifestHash != c.Manifest.Hash {
				return p.Report, runtimecheckpoint.ErrConflict
			}
			if old, exists := p.HeaderDigests[cap.CaptureId]; exists && old != ID(cap) {
				return p.Report, errors.New("runtime_conversion_duplicate_identity")
			}
			p.HeaderDigests[cap.CaptureId] = ID(cap)
			if !cap.Committed {
				p.Report.Unreadable = append(p.Report.Unreadable, Hex(cap.CaptureId)+": uncommitted declaration")
				continue
			}
			if err = p.State.capture(cap); err != nil {
				return p.Report, err
			}
			p.Report.Headers++
		}
		raw, err := runtimecheckpoint.MarshalTyped(page)
		if err != nil {
			return p.Report, err
		}
		ref, err := files.PutObject(raw)
		if err != nil {
			return p.Report, err
		}
		p.Pages = append(p.Pages, ref)
		p.Cursor = append([]byte(nil), next...)
		if err = save(); err != nil {
			return p.Report, err
		}
		if progress != nil {
			progress(fmt.Sprintf("conversion_headers pages=%d captures=%d", len(p.Pages), p.Report.Headers))
		}
	}
	for p.Phase == "facts" && p.FactPage < len(p.Pages) {
		if err = ctx.Err(); err != nil {
			return p.Report, err
		}
		raw, err := files.ReadObject(p.Pages[p.FactPage])
		if err != nil {
			return p.Report, err
		}
		var page []Capture
		if err = runtimecheckpoint.UnmarshalTyped(raw, &page); err != nil {
			return p.Report, err
		}
		selected := []Capture{}
		for _, cap := range page {
			if cap.Canonical && cap.Committed && (cap.CaptureKind == "logs" || cap.CaptureKind == "receipts" || cap.CaptureKind == "receipt_transfers") {
				selected = append(selected, cap)
			}
		}
		for start := 0; start < len(selected); start += 64 {
			caps := selected[start:min(start+64, len(selected))]
			loaded, err := LoadBatches(ctx, c.Store, caps)
			if err != nil {
				return p.Report, err
			}
			for _, cap := range caps {
				if err = loaded.Errors[cap.CaptureId]; err != nil {
					p.Report.Unreadable = append(p.Report.Unreadable, Hex(cap.CaptureId)+": "+err.Error())
					if e := save(); e != nil {
						return p.Report, e
					}
					return p.Report, fmt.Errorf("runtime_conversion_unreadable_input: %w", err)
				}
				p.facts(loaded.Batches[cap.CaptureId], c.Manifest)
			}
		}
		p.FactPage++
		p.Report.FactPages++
		if err = save(); err != nil {
			return p.Report, err
		}
		if progress != nil {
			progress(fmt.Sprintf("conversion_facts pages=%d/%d batches=%d", p.FactPage, len(p.Pages), p.Report.FactBatches))
		}
	}
	if len(p.Report.Unreadable) > 0 {
		return p.Report, errors.New("runtime_conversion_unreadable_ranges_retained; explicit recovery required")
	}
	if p.Phase == "facts" {
		p.finish(c.Manifest)
		p.Phase = "verify"
		if err = save(); err != nil {
			return p.Report, err
		}
	}
	if p.Phase != "verify" {
		return p.Report, runtimecheckpoint.ErrRecovery
	}
	if err = p.compare(ctx, c); err != nil {
		return p.Report, err
	}
	if err = verifyBinding(); err != nil {
		return p.Report, err
	}
	p.Report.Version = 1
	p.Report.FrozenAt = p.FrozenAt
	p.Report.VerifiedAt = Now()
	p.Report.Binding = binding
	p.Report.InputDigest = conversionDigest(p.HeaderDigests)
	p.Report.RawCursors = maps.Clone(p.State.Watch.Cursors)
	p.Report.Active = len(p.State.Active)
	p.Report.Partial = len(p.State.Partial)
	p.Report.Orders = len(p.State.Watch.Orders)
	for _, r := range p.State.Receipts {
		if r.Pending {
			p.Report.Receipts++
		}
	}
	p.Report.Verified = true
	p.State.Conversion = p.Report
	ready := dir + ".ready"
	if _, err = os.Stat(ready); os.IsNotExist(err) {
		placeholder := newRuntimeState(c.Manifest)
		raw, marshalErr := runtimecheckpoint.MarshalTyped(placeholder)
		if marshalErr != nil {
			return p.Report, marshalErr
		}
		installed, createErr := runtimecheckpoint.Create(ready, binding, raw, nil, 0)
		if createErr != nil {
			return p.Report, createErr
		}
		if err = installed.Close(); err != nil {
			return p.Report, err
		}
	} else if err != nil {
		return p.Report, err
	}
	checked, err := runtimecheckpoint.Open(ready, binding, 0)
	if err != nil {
		return p.Report, err
	}
	if _, err = input.Persist(checked, p.Report.InputInventory); err != nil {
		checked.Close()
		return p.Report, err
	}
	var prepared runtimeState
	err = runtimecheckpoint.UnmarshalTyped(checked.State(), &prepared)
	if err == nil && !prepared.Conversion.Verified {
		// Rebuild an unpublished partial page image from the resumed frozen
		// conversion. This is never reachable through normal service startup.
		var raw []byte
		raw, err = runtimecheckpoint.MarshalTyped(p.State)
		if err == nil {
			err = runtimecheckpoint.UnmarshalTyped(raw, &prepared)
		}
		if err == nil {
			err = prepared.enableReceiptPages(checked)
		}
		if err == nil {
			err = prepared.enableCapturePages(checked)
		}
		if err == nil {
			err = prepared.enablePendingPages(checked)
		}
		var refs []runtimecheckpoint.Reference
		if err == nil {
			refs, err = prepared.receiptReferences()
		}
		if err == nil {
			var more []runtimecheckpoint.Reference
			more, err = prepared.captureReferences()
			refs = append(refs, more...)
		}
		if err == nil {
			var more []runtimecheckpoint.Reference
			more, err = prepared.pendingReferences()
			refs = append(refs, more...)
		}
		if err == nil {
			raw, err = runtimecheckpoint.MarshalTyped(prepared)
		}
		if err == nil {
			err = checked.Checkpoint(raw, refs)
		}
	}
	if err == nil {
		err = prepared.enableReceiptPages(checked)
	}
	if err == nil {
		err = prepared.enableCapturePages(checked)
	}
	if err == nil {
		err = prepared.enablePendingPages(checked)
	}
	if err == nil {
		err = prepared.validate(c.Manifest, binding)
	}
	if err == nil {
		// Only the explicit conversion materializes all pending receipts for
		// comparison. Normal restart keeps identity pages bounded in memory.
		plain := prepared
		plain.Receipts = map[string]runtimeReceipt{}
		prepared.eachReceipt(func(id string, r runtimeReceipt) { plain.Receipts[id] = r })
		plain.Active = map[string]Capture{}
		plain.Partial = map[string]Capture{}
		prepared.eachCapture(false, func(c Capture) { plain.Active[c.CaptureId] = c })
		prepared.eachCapture(true, func(c Capture) { plain.Partial[c.CaptureId] = c })
		plain.PartialRetry = map[string]runtimeRetry{}
		prepared.eachRetry(func(id string, r runtimeRetry) { plain.PartialRetry[id] = r })
		plain.Skipped = map[string]pendingOrder{}
		for after := ""; ; {
			keys, rows := prepared.skippedPage(after)
			if len(keys) == 0 {
				break
			}
			for i, key := range keys {
				plain.Skipped[key] = rows[i]
			}
			after = keys[len(keys)-1]
		}
		plain.RetryRoot, plain.SkippedRoot = runtimecheckpoint.PageRoot{}, runtimecheckpoint.PageRoot{}
		plain.retryPages, plain.skippedPages = nil, nil
		plain.pendingErr = nil
		err = prepared.ioError()
		plain.ReceiptRoot, plain.ReceiptScheduleRoot = runtimecheckpoint.PageRoot{}, runtimecheckpoint.PageRoot{}
		plain.receiptPages = nil
		plain.receiptSchedule = nil
		plain.receiptParents = nil
		plain.receiptErr = nil
		plain.ActiveRoot, plain.PartialRoot = runtimeCaptureRoot{}, runtimeCaptureRoot{}
		plain.activePages = nil
		plain.partialPages = nil
		plain.captureErr = nil
		for _, ch := range plain.Chains {
			ch.CurrentRaw = nil
			ch.CurrentDecoded = nil
		}
		plain.Conversion.VerifiedAt = p.State.Conversion.VerifiedAt
		if err == nil && !reflect.DeepEqual(plain, p.State) {
			err = runtimecheckpoint.ErrConflict
		}
	}
	if err == nil {
		err = checked.SealConversion()
	}
	p.Report.CheckpointBytes = len(checked.State())
	checked.Close()
	if err != nil {
		return p.Report, err
	}
	if err = verifyBinding(); err != nil {
		return p.Report, err
	}
	if err = os.Rename(ready, dir); err != nil {
		return p.Report, err
	}
	parent, err := os.Open(filepath.Dir(dir))
	if err != nil {
		return p.Report, err
	}
	err = parent.Sync()
	parent.Close()
	if err != nil {
		return p.Report, err
	}
	return p.Report, nil
}
