package reserve

import (
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
	InputInventory       runtimecheckpoint.InputInventory
	Version              uint32
	Binding              runtimecheckpoint.Binding
	FrozenAt, VerifiedAt time.Time
	Headers              uint64
	InputDigest          string
	Unreadable           []string
	Active, Pending      uint64
	Verified             bool
}
type runtimeConversion struct {
	Version uint32
	Binding runtimecheckpoint.Binding
	Scope   int
	Cursor  []byte
	Phase   string
	Inputs  map[string]string
	State   runtimeState
	Report  RuntimeConversionReport
}
type runtimeCapturePager interface {
	ReserveCapturePage(context.Context, string, []byte, int) ([]Capture, []byte, error)
}

func runtimeInputDigest(input map[string]string) string {
	keys := make([]string, 0, len(input))
	for key := range input {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	pairs := make([][2]string, 0, len(keys))
	for _, key := range keys {
		pairs = append(pairs, [2]string{key, input[key]})
	}
	return Hex(ID(pairs))
}
func (c *Collector) ConvertRuntime(ctx context.Context, dir string, binding runtimecheckpoint.Binding, inputPath string, check func(context.Context) (runtimecheckpoint.Binding, error), progress func(string)) (RuntimeConversionReport, error) {
	var empty RuntimeConversionReport
	input, inputErr := runtimecheckpoint.ReadConversionInput(inputPath, binding, "reserve")
	if inputErr != nil {
		return empty, inputErr
	}
	if binding.ScopeHash != RuntimeScopeHash(c.Manifest, c.RelatedManifests, c.StartBlock) || check == nil {
		return empty, runtimecheckpoint.ErrConflict
	}
	verify := func() error {
		now, err := check(ctx)
		if err != nil {
			return err
		}
		if now != binding {
			return runtimecheckpoint.ErrConflict
		}
		return nil
	}
	if err := verify(); err != nil {
		return empty, err
	}
	if _, err := os.Stat(dir); err == nil {
		return empty, errors.New("reserve_runtime_destination_exists")
	} else if !os.IsNotExist(err) {
		return empty, err
	}
	pager, ok := c.Store.(runtimeCapturePager)
	if !ok {
		return empty, errors.New("reserve_runtime_requires_conversion_pager")
	}
	scopes := runtimeScopeHashes(c.Manifest, c.RelatedManifests)
	stageBinding := binding
	stageBinding.StateVersion += 1000
	stage := dir + ".conversion"
	var files *runtimecheckpoint.Store
	var p runtimeConversion
	var err error
	if _, err = os.Stat(filepath.Join(stage, "checkpoint.current")); os.IsNotExist(err) {
		p = runtimeConversion{Version: 1, Binding: binding, Phase: "headers", Inputs: map[string]string{}, State: newRuntimeState(binding, scopes), Report: RuntimeConversionReport{Version: 1, Binding: binding, FrozenAt: time.Now().UTC()}}
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
	if p.Version != 1 || p.Binding != binding || p.Scope < 0 || p.Scope > len(scopes) || p.Phase != "headers" && p.Phase != "verified" {
		return p.Report, runtimecheckpoint.ErrRecovery
	}
	if err = p.State.openPages(files); err != nil {
		return p.Report, err
	}
	save := func() error {
		refs, e := p.State.references()
		if e != nil {
			return e
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

	for p.Phase == "headers" && p.Scope < len(scopes) {
		if err = ctx.Err(); err != nil {
			return p.Report, err
		}
		rows, next, e := pager.ReserveCapturePage(ctx, scopes[p.Scope], p.Cursor, 500)
		if e != nil {
			return p.Report, e
		}
		if len(rows) == 0 {
			p.Scope++
			p.Cursor = nil
			if err = save(); err != nil {
				return p.Report, err
			}
			continue
		}
		if len(next) != 64 || reflect.DeepEqual(next, p.Cursor) {
			return p.Report, runtimecheckpoint.ErrRecovery
		}
		for _, cap := range rows {
			if cap.ManifestHash != scopes[p.Scope] {
				return p.Report, runtimecheckpoint.ErrConflict
			}
			id := runtimeIdentity(cap)
			digest := ID(cap)
			if old, exists := p.Inputs[id]; exists && old != digest {
				return p.Report, runtimecheckpoint.ErrConflict
			}
			if _, exists := p.Inputs[id]; exists {
				continue
			}
			// Conversion reads a narrow registration page. Only unfinished payloads
			// remain referenced after final pruning; no historical batch clone is made.
			raw, e := runtimecheckpoint.MarshalTyped(runtimeWrite{Version: 1, Revision: &cap, Expected: cap.Revision - 1})
			if e != nil {
				return p.Report, e
			}
			ref, e := files.PutObject(raw)
			if e != nil {
				return p.Report, e
			}
			if e = p.State.capture(cap, ref, true); e != nil {
				p.Report.Unreadable = append(p.Report.Unreadable, id)
				return p.Report, e
			}
			p.Inputs[id] = digest
			p.Report.Headers++
		}
		p.Cursor = append([]byte(nil), next...)
		if err = save(); err != nil {
			return p.Report, err
		}
		if progress != nil {
			progress(fmt.Sprintf("runtime_conversion scope=%d/%d headers=%d", p.Scope+1, len(scopes), p.Report.Headers))
		}
	}
	if p.Phase == "headers" {
		p.State.each(p.State.active, "", func(h runtimeHeader) { p.State.resolveAttempt(h.Capture) })
		p.State.each(p.State.final, "", func(h runtimeHeader) { p.State.resolveAttempt(h.Capture) })
		if p.State.err != nil {
			return p.Report, p.State.err
		}
		if err = c.compareRuntimeConversion(ctx, &p); err != nil {
			return p.Report, err
		}
		p.Report.InputDigest = runtimeInputDigest(p.Inputs)
		p.Report.Verified = true
		p.Report.VerifiedAt = time.Now().UTC()
		p.Report.Active = p.State.active.Len()
		p.Report.Pending = p.State.pending.Len()
		p.State.Verified = true
		p.State.Conversion = p.Report
		p.Phase = "verified"
		if err = save(); err != nil {
			return p.Report, err
		}
	}
	if err = verify(); err != nil {
		return p.Report, err
	}
	// Resuming a verified conversion also checks frozen SQL input before publish.
	if err = c.compareRuntimeConversion(ctx, &p); err != nil {
		return p.Report, err
	}
	refs, err := p.State.references()
	if err != nil {
		return p.Report, err
	}
	ready := dir + ".ready"
	var target *runtimecheckpoint.Store
	if _, err = os.Stat(filepath.Join(ready, "checkpoint.current")); os.IsNotExist(err) {
		initial := newRuntimeState(binding, scopes)
		raw, e := runtimecheckpoint.MarshalTyped(initial)
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
	if err = prepared.openPages(target); err != nil {
		return p.Report, err
	}
	if err = prepared.validate(binding, scopes); err != nil {
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
	err = parent.Sync()
	return p.Report, err
}
func (c *Collector) compareRuntimeConversion(ctx context.Context, p *runtimeConversion) error {
	actual := map[string]string{}
	expectedPending := map[string]Capture{}
	expectedActive := map[string]Capture{}
	for _, scope := range runtimeScopeHashes(c.Manifest, c.RelatedManifests) {
		caps, err := c.Store.ReserveCaptures(ctx, scope)
		if err != nil {
			return err
		}
		sort.Slice(caps, func(i, j int) bool { return runtimeBefore(caps[i], caps[j]) })
		latest := map[string]Capture{}
		var anchor *Capture
		var raw, receipt, live []runtimeRange
		for _, cap := range caps {
			actual[runtimeIdentity(cap)] = ID(cap)
			runtimeCoverage(cap, &raw, &receipt, &live)
			if !cap.Canonical || !cap.Committed {
				continue
			}
			if cap.Finality != "finalized" {
				expectedActive[runtimeIdentity(cap)] = runtimeNarrow(cap)
			} else if anchor == nil || cap.ToBlock > anchor.ToBlock {
				v := runtimeNarrow(cap)
				anchor = &v
			}
			if cap.CaptureKind == "logs" {
				old, ok := latest[cap.CaptureId]
				if !ok || cap.AvailableAt.After(old.AvailableAt) {
					latest[cap.CaptureId] = cap
				}
			}
		}
		for _, v := range latest {
			if v.LogCoverage == "complete" && v.ReceiptCoverage != "complete" {
				expectedPending[runtimeAttemptKey(v)] = runtimeNarrow(v)
			}
		}
		ch := p.State.Scopes[scope]
		if !reflect.DeepEqual(raw, ch.CurrentRaw) || !reflect.DeepEqual(receipt, ch.CurrentReceipt) || !reflect.DeepEqual(live, ch.CurrentLive) || !reflect.DeepEqual(anchor, ch.Anchor) {
			return errors.New("reserve_runtime_conversion_coverage_or_finality_mismatch")
		}
		fallback := uint64(0)
		if c.StartBlock > 0 {
			fallback = c.StartBlock - 1
		}
		if ContiguousLiveCursor(caps, fallback, c.StartBlock == 0) != p.State.cursor(scope, fallback, c.StartBlock == 0) {
			return errors.New("reserve_runtime_conversion_cursor_mismatch")
		}
	}
	if len(actual) != len(p.Inputs) || runtimeInputDigest(actual) != runtimeInputDigest(p.Inputs) {
		return errors.New("reserve_runtime_conversion_input_changed")
	}
	gotActive, gotPending := map[string]Capture{}, map[string]Capture{}
	p.State.each(p.State.active, "", func(h runtimeHeader) { gotActive[runtimeIdentity(h.Capture)] = h.Capture })
	p.State.each(p.State.pending, "", func(h runtimeHeader) { gotPending[runtimeAttemptKey(h.Capture)] = h.Capture })
	if p.State.err != nil {
		return p.State.err
	}
	if !reflect.DeepEqual(expectedActive, gotActive) || !reflect.DeepEqual(expectedPending, gotPending) {
		return errors.New("reserve_runtime_conversion_attempts_mismatch")
	}
	return nil
}
