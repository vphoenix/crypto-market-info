package lst

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint"
)

type RuntimeConversionReport struct {
	InputInventory                      runtimecheckpoint.InputInventory
	Version                             uint32
	Binding                             runtimecheckpoint.Binding
	FrozenAt, VerifiedAt                time.Time
	Headers, FactBatches, Active, Seeds uint64
	InputDigest                         string
	Unreadable                          []string
	Verified                            bool
}
type runtimeCapturePager interface {
	LSTCapturePage(context.Context, string, []byte, int) ([]Capture, []byte, error)
}
type runtimeConversion struct {
	Version         uint32
	Binding         runtimecheckpoint.Binding
	Phase           string
	Cursor          []byte
	Inputs          map[string]string
	Facts           map[string]runtimecheckpoint.Reference
	OriginalCursors map[uint64]logProgress
	Pending         *runtimecheckpoint.Reference
	State           runtimeState
	Report          RuntimeConversionReport
}

func runtimeInputDigest(inputs map[string]string) string {
	keys := make([]string, 0, len(inputs))
	for key := range inputs {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	pairs := make([][2]string, 0, len(keys))
	for _, key := range keys {
		pairs = append(pairs, [2]string{key, inputs[key]})
	}
	return Hex(CanonicalHash(pairs))
}
func runtimeNeedsFacts(cap Capture, at time.Time) bool {
	return cap.CaptureKind == "market" && !cap.StartedAt.Before(at.Add(-31*24*time.Hour)) || cap.CaptureKind == "logs" && cap.Canonical && cap.Finality == "finalized" && cap.Status == "complete" && cap.ToBlockTime != nil && !cap.ToBlockTime.Before(at.UTC().Truncate(24*time.Hour).AddDate(0, 0, -30))
}
func (c *Collector) originalRuntimeCursors() (map[uint64]logProgress, error) {
	result := map[uint64]logProgress{}
	segments := []uint64{c.LiveLogsFromBlock}
	if c.LiveLogsFromBlock != 0 {
		segments = append(segments, 0)
	}
	for _, segment := range segments {
		old := *c
		old.LiveLogsFromBlock = segment
		var p logProgress
		if err := readGob(old.liveLogCursorPath(), &p); err != nil {
			if segment == 0 && segment != c.LiveLogsFromBlock && os.IsNotExist(err) {
				continue
			}
			return nil, fmt.Errorf("lst runtime requires original cursor %d: %w", segment, err)
		}
		if p.Manifest != c.Manifest.Hash || p.Start == 0 || p.Next < p.Start || segment != 0 && p.Start != segment || p.CoverageStartedAt.IsZero() {
			return nil, runtimecheckpoint.ErrRecovery
		}
		if p.RangeSize == 0 {
			p.RangeSize = c.initialLogBlocks()
		}
		p.RangeSize = min(p.RangeSize, c.liveLogBlocks())
		result[segment] = p
	}
	return result, nil
}
func (c *Collector) ConvertRuntime(ctx context.Context, dir string, binding runtimecheckpoint.Binding, inputPath string, check func(context.Context) (runtimecheckpoint.Binding, error), progress func(string)) (RuntimeConversionReport, error) {
	var empty RuntimeConversionReport
	input, inputErr := runtimecheckpoint.ReadConversionInput(inputPath, binding, "lst")
	if inputErr != nil {
		return empty, inputErr
	}
	if check == nil || binding.ScopeHash != RuntimeScopeHash(c) || c.runtime != nil {
		return empty, runtimecheckpoint.ErrConflict
	}
	verify := func() error {
		actual, err := check(ctx)
		if err != nil {
			return err
		}
		if actual != binding {
			return runtimecheckpoint.ErrConflict
		}
		return nil
	}
	if err := verify(); err != nil {
		return empty, err
	}
	if _, err := os.Stat(dir); err == nil {
		return empty, errors.New("lst_runtime_destination_exists")
	} else if !os.IsNotExist(err) {
		return empty, err
	}
	pager, ok := c.Store.(runtimeCapturePager)
	if !ok {
		return empty, errors.New("lst_runtime_requires_conversion_pager")
	}
	stageBinding := binding
	stageBinding.StateVersion += 1000
	stage := dir + ".conversion"
	var p runtimeConversion
	var files *runtimecheckpoint.Store
	var err error
	if _, err = os.Stat(filepath.Join(stage, "checkpoint.current")); os.IsNotExist(err) {
		cursors, e := c.originalRuntimeCursors()
		if e != nil {
			return empty, e
		}
		p = runtimeConversion{Version: 1, Binding: binding, Phase: "pending", Inputs: map[string]string{}, Facts: map[string]runtimecheckpoint.Reference{}, OriginalCursors: cursors, State: newRuntimeState(binding, c.Manifest.Hash), Report: RuntimeConversionReport{Version: 1, Binding: binding, FrozenAt: Now()}}
		for segment, cursor := range cursors {
			cursor.Next = cursor.Start
			p.State.Cursors[segment] = cursor
		}
		raw, e := runtimecheckpoint.MarshalTyped(p)
		if e != nil {
			return empty, e
		}
		files, err = runtimecheckpoint.Create(stage, stageBinding, raw, nil, 64<<20)
	} else if err == nil {
		files, err = runtimecheckpoint.Open(stage, stageBinding, 64<<20)
		if err == nil {
			err = runtimecheckpoint.UnmarshalTyped(files.State(), &p)
		}
	}
	if err != nil {
		if files != nil {
			files.Close()
		}
		return empty, err
	}
	defer files.Close()
	p.Report.InputInventory, err = input.Persist(files, p.Report.InputInventory)
	if err != nil {
		return p.Report, err
	}
	if p.Version != 1 || p.Binding != binding || p.Phase != "pending" && p.Phase != "headers" && p.Phase != "verified" {
		return p.Report, runtimecheckpoint.ErrRecovery
	}
	if err = p.State.open(files); err != nil {
		return p.Report, err
	}
	save := func() error {
		refs, e := p.State.references()
		if e != nil {
			return e
		}
		for _, ref := range p.Facts {
			refs = append(refs, ref)
		}
		if p.Pending != nil {
			refs = append(refs, *p.Pending)
		}
		raw, e := runtimecheckpoint.MarshalTyped(p)
		if e != nil {
			return e
		}
		return files.Checkpoint(raw, append(refs, p.Report.InputInventory.References()...))
	}
	if err = save(); err != nil {
		return p.Report, err
	}

	pendingFile := filepath.Join(c.StateDir, "pending-batch.gob")
	if p.Phase == "pending" {
		if p.Pending == nil {
			if _, e := os.Stat(pendingFile); e == nil {
				ref, e := files.LinkPending(pendingFile)
				if e != nil {
					return p.Report, e
				}
				p.Pending = &ref
				if e = save(); e != nil {
					return p.Report, e
				}
			} else if !os.IsNotExist(e) {
				return p.Report, e
			}
		}
		if p.Pending != nil {
			raw, e := files.ReadObject(*p.Pending)
			if e != nil {
				return p.Report, e
			}
			var b Batch
			if e = decodePersistent(bytes.NewReader(raw), &b); e != nil {
				return p.Report, e
			}
			if e = Validate(b); e != nil {
				return p.Report, e
			}
			if b.Capture.ManifestHash != c.Manifest.Hash {
				return p.Report, runtimecheckpoint.ErrConflict
			}
			if e = c.Store.WriteLST(ctx, b); e != nil {
				return p.Report, e
			}
			actual, e := c.Store.LSTBatch(ctx, b.Capture)
			if e != nil {
				return p.Report, e
			}
			if e = Validate(actual); e != nil {
				return p.Report, e
			}
			if CaptureIdentity(actual.Capture) != CaptureIdentity(b.Capture) {
				return p.Report, runtimecheckpoint.ErrConflict
			}
			p.State.ImportedPendingID = "capture:" + b.Capture.CaptureId.String()
			p.State.ImportedPendingHash = p.Pending.Hash
		}
		p.Phase = "headers"
		if err = save(); err != nil {
			return p.Report, err
		}
	}
	for p.Phase == "headers" {
		if err = ctx.Err(); err != nil {
			return p.Report, err
		}
		rows, next, e := pager.LSTCapturePage(ctx, c.Manifest.Hash, p.Cursor, 500)
		if e != nil {
			return p.Report, e
		}
		if len(rows) == 0 {
			break
		}
		if len(next) != 16 || bytes.Equal(next, p.Cursor) {
			return p.Report, runtimecheckpoint.ErrRecovery
		}
		for _, cap := range rows {
			if cap.ManifestHash != c.Manifest.Hash || !cap.Committed {
				return p.Report, runtimecheckpoint.ErrConflict
			}
			id, digest := cap.CaptureId.String(), CanonicalHash(cap)
			if old, ok := p.Inputs[id]; ok {
				if old != digest {
					return p.Report, runtimecheckpoint.ErrConflict
				}
				continue
			}
			if e = p.State.capture(cap); e != nil {
				return p.Report, e
			}
			if runtimeNeedsFacts(cap, p.Report.FrozenAt) {
				b, e := c.Store.LSTBatch(ctx, cap)
				if e == nil {
					e = Validate(b)
				}
				if e != nil {
					p.Report.Unreadable = append(p.Report.Unreadable, id)
					return p.Report, e
				}
				if CanonicalHash(b.Capture) != digest {
					return p.Report, runtimecheckpoint.ErrConflict
				}
				raw, e := runtimecheckpoint.MarshalTyped(b)
				if e != nil {
					return p.Report, e
				}
				ref, e := files.PutObject(raw)
				if e != nil {
					return p.Report, e
				}
				if e = p.State.batch(b, p.Report.FrozenAt); e != nil {
					return p.Report, e
				}
				p.Facts[id] = ref
				p.Report.FactBatches++
			}
			p.Inputs[id] = digest
			p.Report.Headers++
		}
		p.Cursor = append([]byte(nil), next...)
		if err = save(); err != nil {
			return p.Report, err
		}
		if progress != nil {
			progress(fmt.Sprintf("runtime_conversion headers=%d fact_batches=%d", p.Report.Headers, p.Report.FactBatches))
		}
	}
	if err = c.compareRuntimeConversion(ctx, &p, files); err != nil {
		return p.Report, err
	}
	p.Report.InputDigest = runtimeInputDigest(p.Inputs)
	p.Report.Verified = true
	p.Report.VerifiedAt = Now()
	p.Report.Active = p.State.active.Len()
	p.Report.Seeds = p.State.seeds.Len()
	p.State.Verified = true
	p.State.Conversion = p.Report
	p.Phase = "verified"
	if err = save(); err != nil {
		return p.Report, err
	}
	if err = verify(); err != nil {
		return p.Report, err
	}
	refs, err := p.State.references()
	if err != nil {
		return p.Report, err
	}
	ready := dir + ".ready"
	var target *runtimecheckpoint.Store
	if _, err = os.Stat(filepath.Join(ready, "checkpoint.current")); os.IsNotExist(err) {
		raw, e := runtimecheckpoint.MarshalTyped(newRuntimeState(binding, c.Manifest.Hash))
		if e != nil {
			return p.Report, e
		}
		target, err = runtimecheckpoint.Create(ready, binding, raw, nil, 0)
	} else if err == nil {
		target, err = runtimecheckpoint.Open(ready, binding, 0)
	}
	if err != nil {
		return p.Report, err
	}
	defer target.Close()
	for _, ref := range refs {
		raw, e := files.ReadObject(ref)
		if e != nil {
			return p.Report, e
		}
		got, e := target.PutObject(raw)
		if e != nil {
			return p.Report, e
		}
		if got != ref {
			return p.Report, runtimecheckpoint.ErrConflict
		}
	}
	raw, err := runtimecheckpoint.MarshalTyped(p.State)
	if err != nil {
		return p.Report, err
	}
	if err = target.Checkpoint(raw, refs); err != nil {
		return p.Report, err
	}
	var prepared runtimeState
	if err = runtimecheckpoint.UnmarshalTyped(target.State(), &prepared); err != nil {
		return p.Report, err
	}
	if err = prepared.open(target); err != nil {
		return p.Report, err
	}
	if err = prepared.validate(binding, c); err != nil {
		return p.Report, err
	}
	owner := runtimeOwner{state: prepared, files: target}
	if err = owner.capacity(); err != nil {
		return p.Report, err
	}
	if err = target.SealConversion(); err != nil {
		return p.Report, err
	}
	if err = verify(); err != nil {
		return p.Report, err
	}
	if err = target.Close(); err != nil {
		return p.Report, err
	}
	if err = os.Rename(ready, dir); err != nil {
		return p.Report, err
	}
	parent, err := os.Open(filepath.Dir(dir))
	if err != nil {
		return p.Report, err
	}
	defer parent.Close()
	return p.Report, parent.Sync()
}

// This is an explicit, stopped-service comparison. Full historical input is
// allowed here; none of these reads runs in Watch or normal restart.
func (c *Collector) compareRuntimeConversion(ctx context.Context, p *runtimeConversion, files *runtimecheckpoint.Store) error {
	original, err := c.originalRuntimeCursors()
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(original, p.OriginalCursors) {
		return errors.New("lst_runtime_original_cursor_changed")
	}
	pager := c.Store.(runtimeCapturePager)
	var caps []Capture
	inputs := map[string]string{}
	for after := []byte(nil); ; {
		rows, next, err := pager.LSTCapturePage(ctx, c.Manifest.Hash, after, 500)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			break
		}
		if len(next) != 16 || bytes.Equal(next, after) {
			return runtimecheckpoint.ErrRecovery
		}
		for _, cap := range rows {
			inputs[cap.CaptureId.String()] = CanonicalHash(cap)
		}
		caps = append(caps, rows...)
		after = next
	}
	if !reflect.DeepEqual(inputs, p.Inputs) {
		return errors.New("lst_runtime_conversion_input_changed")
	}
	sort.Slice(caps, func(i, j int) bool { return runtimeGasBefore(caps[i], caps[j]) })
	load := func(cap Capture) (Batch, error) {
		var b Batch
		ref, ok := p.Facts[cap.CaptureId.String()]
		if !ok {
			return b, runtimecheckpoint.ErrRecovery
		}
		raw, err := files.ReadObject(ref)
		if err != nil {
			return b, err
		}
		err = runtimecheckpoint.UnmarshalTyped(raw, &b)
		return b, err
	}
	expected, err := restoreFollowupsAt(caps, p.Report.FrozenAt, load)
	if err != nil {
		return err
	}
	got, err := p.State.followups(p.Report.FrozenAt)
	if err != nil {
		return err
	}
	// Legacy keeps done keys for seeds it no longer schedules. Compare all live
	// seed identities, each completion flag, and the exact next due order.
	if CanonicalHash(expected.Seeds) != CanonicalHash(got.Seeds) || expected.LastSeedDay != got.LastSeedDay || CanonicalHash(expected.due(p.Report.FrozenAt)) != CanonicalHash(got.due(p.Report.FrozenAt)) {
		return errors.New("lst_runtime_followup_comparison")
	}
	for _, seed := range expected.Seeds {
		for _, days := range []uint32{1, 3, 7, 14, 30} {
			key := followupKey(seed.QuoteId, days*86400)
			if expected.Done[key] != got.Done[key] {
				return errors.New("lst_runtime_followup_completion_comparison")
			}
		}
	}
	var gas []Batch
	active := map[string]Capture{}
	for _, cap := range caps {
		if cap.CaptureKind == "market" && cap.ToBlock != nil && cap.Finality != "finalized" && cap.Finality != "orphaned" {
			active[runtimeCaptureKey(cap)] = cap
		}
		if cap.CaptureKind == "logs" && runtimeNeedsFacts(cap, p.Report.FrozenAt) {
			b, e := load(cap)
			if e != nil {
				return e
			}
			gas = append(gas, b)
		}
	}
	actual := map[string]Capture{}
	for after := ""; ; {
		keys, rows, e := p.State.active.Scan(after, 128, false)
		if e != nil {
			return e
		}
		if len(keys) == 0 {
			break
		}
		for i, key := range keys {
			actual[key] = rows[i]
		}
		after = keys[len(keys)-1]
	}
	if !reflect.DeepEqual(active, actual) {
		return errors.New("lst_runtime_finality_comparison")
	}
	samples, err := gasSamples(gas, p.Report.FrozenAt)
	if err != nil {
		return err
	}
	selected, err := p.State.Gas.selectSamples(p.Report.FrozenAt, len(samples)+1)
	if err != nil {
		return err
	}
	if len(samples) != len(selected) {
		return errors.New("lst_runtime_gas_sample_count")
	}
	for i, v := range samples {
		got := selected[i]
		if got.Hash != v.hash || got.Block != v.block || !got.Day.Equal(v.day) || got.Parent.CaptureId != gas[v.batch].Capture.CaptureId {
			return errors.New("lst_runtime_gas_sample_identity")
		}
	}
	ordered := append([]Capture(nil), caps...)
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].FromBlock == nil {
			return false
		}
		if ordered[j].FromBlock == nil {
			return true
		}
		return *ordered[i].FromBlock < *ordered[j].FromBlock
	})
	for segment, cursor := range original {
		next := cursor.Start
		for _, cap := range ordered {
			if cap.ManifestHash == c.Manifest.Hash && cap.CaptureKind == "logs" && cap.Status == "complete" && cap.Committed && cap.Canonical && cap.Finality == "finalized" && cap.FromBlock != nil && cap.FromBlockTime != nil && cap.ToBlock != nil && *cap.FromBlock <= *cap.ToBlock && (cap.CaptureMode == "live" || cap.CaptureMode == "backfill") && *cap.FromBlock <= next && *cap.ToBlock+1 > next {
				next = *cap.ToBlock + 1
			}
		}
		if p.State.Cursors[segment].Next != next {
			return errors.New("lst_runtime_log_cursor_comparison")
		}
	}
	if p.Pending != nil {
		ref, err := files.LinkPending(filepath.Join(c.StateDir, "pending-batch.gob"))
		if err != nil {
			return err
		}
		if ref != *p.Pending {
			return runtimecheckpoint.ErrConflict
		}
	}
	return nil
}
