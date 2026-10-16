package justlendkeeper

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

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint"
)

type RuntimeConversionReport struct {
	InputInventory                      runtimecheckpoint.InputInventory
	Version                             uint32
	Binding                             runtimecheckpoint.Binding
	FrozenAt, VerifiedAt                time.Time
	Headers, Parents, Children, Windows uint64
	InputDigest                         string
	Unreadable                          []string
	Verified                            bool
}
type runtimeCapturePager interface {
	KeeperCapturePage(context.Context, string, []byte, int) ([]Capture, []byte, error)
}
type runtimeConversion struct {
	Version          uint32
	Binding          runtimecheckpoint.Binding
	Phase            string
	Cursor           []byte
	Original         runtimecheckpoint.Reference
	OriginalFileHash string
	Inputs           map[string]string
	PageFacts        map[string]runtimecheckpoint.Reference
	Work             runtimeWork
	Report           RuntimeConversionReport
}

func runtimeCaptureHash(cap Capture) (string, error) {
	raw, err := FactBytes(cap)
	return Hex(Hash(raw)), err
}
func runtimeConversionDigest(inputs map[string]string) string {
	keys := make([]string, 0, len(inputs))
	for key := range inputs {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var b bytes.Buffer
	for _, key := range keys {
		b.WriteString(key)
		b.WriteString(inputs[key])
	}
	return Hex(Hash(b.Bytes()))
}
func (c *Collector) ConvertRuntime(ctx context.Context, dir string, binding runtimecheckpoint.Binding, inputPath string, check func(context.Context) (runtimecheckpoint.Binding, error), progress func(string)) (RuntimeConversionReport, error) {
	var empty RuntimeConversionReport
	input, inputErr := runtimecheckpoint.ReadConversionInput(inputPath, binding, "keeper")
	if inputErr != nil {
		return empty, inputErr
	}
	if c.runtime != nil || c.State.Runtime != nil || check == nil || binding.ScopeHash != Hex(c.Config.Hash()) {
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
		return empty, errors.New("keeper_runtime_destination_exists")
	} else if !os.IsNotExist(err) {
		return empty, err
	}
	pager, ok := c.Store.(runtimeCapturePager)
	if !ok {
		return empty, errors.New("keeper_runtime_requires_conversion_pager")
	}
	pages, ok := c.Store.(IndexPageReader)
	if !ok {
		return empty, errors.New("keeper_runtime_requires_page_reader")
	}
	// An explicit converter holds both command and target locks. Resolve existing
	// frozen Ops through the old durable protocol before freezing the input. Never
	// perform their requests again or invent replacement IDs/timestamps.
	stage := dir + ".conversion"
	_, stageErr := os.Stat(filepath.Join(stage, "checkpoint.current"))
	if os.IsNotExist(stageErr) {
		if _, err := os.Stat(c.Path); err != nil {
			return empty, err
		}
		for i := 0; i < len(c.State.Ops); {
			if !c.State.Ops[i].Frozen {
				i++
				continue
			}
			if err := c.Finish(ctx, &c.State.Ops[i]); err != nil {
				return empty, err
			}
			c.State.Ops = append(c.State.Ops[:i], c.State.Ops[i+1:]...)
			if err := c.Save(); err != nil {
				return empty, err
			}
		}
	} else if stageErr != nil {
		return empty, stageErr
	}
	originalFile, err := os.ReadFile(c.Path)
	if err != nil {
		return empty, err
	}
	originalHash := Hex(Hash(originalFile))
	stageBinding := binding
	stageBinding.StateVersion += 1000
	var p runtimeConversion
	var files *runtimecheckpoint.Store
	if os.IsNotExist(stageErr) {
		p = runtimeConversion{Version: 1, Binding: binding, Phase: "parents", OriginalFileHash: originalHash, Inputs: map[string]string{}, PageFacts: map[string]runtimecheckpoint.Reference{}, Work: runtimeWork{Version: 1, Binding: binding}, Report: RuntimeConversionReport{Version: 1, Binding: binding, FrozenAt: UTC(c.Now())}}
		raw, e := runtimeCompress(p)
		if e != nil {
			return empty, e
		}
		files, err = runtimecheckpoint.Create(stage, stageBinding, raw, nil, maxCheckpointBytes)
	} else {
		files, err = runtimecheckpoint.Open(stage, stageBinding, maxCheckpointBytes)
		if err == nil {
			err = runtimeExpand(files.State(), &p)
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
	if p.Version != 1 || p.Binding != binding || p.OriginalFileHash != originalHash || p.Phase != "parents" && p.Phase != "children" && p.Phase != "verified" {
		return p.Report, runtimecheckpoint.ErrRecovery
	}
	if err = p.Work.open(files); err != nil {
		return p.Report, err
	}
	save := func() error {
		refs, e := p.Work.references()
		if e != nil {
			return e
		}
		if p.Original.Hash != "" {
			refs = append(refs, p.Original)
		}
		for _, ref := range p.PageFacts {
			refs = append(refs, ref)
		}
		raw, e := runtimeCompress(p)
		if e != nil {
			return e
		}
		return files.Checkpoint(raw, append(refs, p.Report.InputInventory.References()...))
	}
	if err = save(); err != nil {
		return p.Report, err
	}

	if p.Original.Hash == "" {
		raw, e := runtimeCompress(*c.State)
		if e != nil {
			return p.Report, e
		}
		p.Original, err = files.PutObject(raw)
		if err != nil {
			return p.Report, err
		}
		if err = save(); err != nil {
			return p.Report, err
		}
	}
	var original State
	raw, err := files.ReadObject(p.Original)
	if err != nil {
		return p.Report, err
	}
	if err = runtimeExpand(raw, &original); err != nil {
		return p.Report, err
	}
	if !runtimeStateEqual(original, *c.State) {
		return p.Report, errors.New("keeper_runtime_legacy_state_changed")
	}
	for p.Phase != "verified" {
		if err = ctx.Err(); err != nil {
			return p.Report, err
		}
		caps, next, e := pager.KeeperCapturePage(ctx, c.Config.Hash(), p.Cursor, 500)
		if e != nil {
			return p.Report, e
		}
		if len(caps) == 0 {
			if p.Phase == "parents" {
				p.Phase = "children"
				p.Cursor = nil
				if err = save(); err != nil {
					return p.Report, err
				}
				continue
			}
			break
		}
		if len(next) != 16 || bytes.Compare(next, p.Cursor) <= 0 {
			return p.Report, runtimecheckpoint.ErrRecovery
		}
		var wanted []Capture
		if p.Phase == "parents" {
			for _, cap := range caps {
				if cap.CaptureKind == "event_index" && cap.Status == "complete" && (cap.CaptureMode == "live" || cap.CaptureMode == "catchup") && cap.RequestedFrom != nil && cap.RequestedTo != nil && cap.RequestedTo.After(original.EvidenceCursors[cap.EventKind]) {
					wanted = append(wanted, cap)
				}
			}
		}
		found := map[uuid.UUID]IndexPage{}
		for start := 0; start < len(wanted); start += 256 {
			group := wanted[start:min(start+256, len(wanted))]
			got, e := pages.KeeperIndexPages(ctx, group)
			if e != nil {
				return p.Report, e
			}
			for id, page := range got {
				found[id] = page
			}
		}
		for _, cap := range caps {
			if cap.ConfigHash != c.Config.Hash() || !cap.Committed {
				return p.Report, runtimecheckpoint.ErrConflict
			}
			id := cap.CaptureId.String()
			digest, e := runtimeCaptureHash(cap)
			if e != nil {
				return p.Report, e
			}
			if p.Phase == "parents" {
				if _, exists := p.Inputs[id]; exists {
					return p.Report, runtimecheckpoint.ErrConflict
				}
				var page *IndexPage
				if value, exists := found[cap.CaptureId]; exists {
					page = &value
					raw, e := runtimecheckpoint.MarshalTyped(value)
					if e != nil {
						return p.Report, e
					}
					ref, e := files.PutObject(raw)
					if e != nil {
						return p.Report, e
					}
					p.PageFacts[id] = ref
				}
				if e = p.Work.parent(cap, page, original.EvidenceCursors); e != nil {
					p.Report.Unreadable = append(p.Report.Unreadable, id)
					return p.Report, e
				}
				p.Inputs[id] = digest
				p.Report.Headers++
			} else {
				if p.Inputs[id] != digest {
					return p.Report, runtimecheckpoint.ErrConflict
				}
				if e = p.Work.child(cap); e != nil {
					return p.Report, e
				}
			}
		}
		p.Cursor = append([]byte(nil), next...)
		if err = save(); err != nil {
			return p.Report, err
		}
		if progress != nil {
			progress(fmt.Sprintf("runtime_conversion phase=%s headers=%d parents=%d windows=%d", p.Phase, p.Report.Headers, p.Work.parents.Len(), p.Work.windows.Len()))
		}
	}
	if err = c.compareRuntimeConversion(ctx, &p, files, original); err != nil {
		return p.Report, err
	}
	p.Work.Verified = true
	p.Report.Verified = true
	p.Report.VerifiedAt = UTC(c.Now())
	p.Report.InputDigest = runtimeConversionDigest(p.Inputs)
	p.Report.Parents = p.Work.parents.Len()
	p.Report.Children = p.Work.order.Len()
	p.Report.Windows = p.Work.windows.Len()
	p.Work.Conversion = p.Report
	p.Phase = "verified"
	if err = save(); err != nil {
		return p.Report, err
	}
	if err = verify(); err != nil {
		return p.Report, err
	}
	if raw, err = os.ReadFile(c.Path); err != nil {
		return p.Report, err
	} else if Hex(Hash(raw)) != p.OriginalFileHash {
		return p.Report, errors.New("keeper_runtime_legacy_file_changed")
	}
	refs, err := p.Work.references()
	if err != nil {
		return p.Report, err
	}
	ready := dir + ".ready"
	var target *runtimecheckpoint.Store
	if _, err = os.Stat(filepath.Join(ready, "checkpoint.current")); os.IsNotExist(err) {
		target, err = runtimecheckpoint.Create(ready, binding, []byte("conversion-not-published"), nil, maxCheckpointBytes)
	} else if err == nil {
		target, err = runtimecheckpoint.Open(ready, binding, maxCheckpointBytes)
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
	original.Runtime = &p.Work
	if err = p.Work.open(target); err != nil {
		return p.Report, err
	}
	owner := runtimeOwner{files: target}
	if err = owner.save(&original); err != nil {
		return p.Report, err
	}
	if err = p.Work.validate(binding, c.Config.Hash()); err != nil {
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
func runtimeStateEqual(a, b State) bool {
	normalize := func(s State) (State, error) {
		raw, e := runtimecheckpoint.MarshalTyped(s)
		if e != nil {
			return State{}, e
		}
		var v State
		e = runtimecheckpoint.UnmarshalTyped(raw, &v)
		return v, e
	}
	x, e := normalize(a)
	y, f := normalize(b)
	return e == nil && f == nil && reflect.DeepEqual(x, y)
}

// Only explicit conversion/rollback reconstructs the old selection from frozen
// captures. Normal startup and loops never call this complete input traversal.
func (c *Collector) compareRuntimeConversion(ctx context.Context, p *runtimeConversion, files *runtimecheckpoint.Store, original State) error {
	pager := c.Store.(runtimeCapturePager)
	var caps []Capture
	seen := map[string]bool{}
	for after := []byte(nil); ; {
		rows, next, err := pager.KeeperCapturePage(ctx, c.Config.Hash(), after, 500)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			break
		}
		if len(next) != 16 || bytes.Compare(next, after) <= 0 {
			return runtimecheckpoint.ErrRecovery
		}
		for _, cap := range rows {
			id := cap.CaptureId.String()
			hash, e := runtimeCaptureHash(cap)
			if e != nil {
				return e
			}
			if seen[id] || p.Inputs[id] != hash {
				return errors.New("keeper_runtime_frozen_input_changed")
			}
			seen[id] = true
			caps = append(caps, cap)
		}
		after = next
	}
	if len(seen) != len(p.Inputs) {
		return errors.New("keeper_runtime_frozen_input_missing")
	}
	pages := map[uuid.UUID]IndexPage{}
	for id, ref := range p.PageFacts {
		raw, e := files.ReadObject(ref)
		if e != nil {
			return e
		}
		var page IndexPage
		if e = runtimecheckpoint.UnmarshalTyped(raw, &page); e != nil {
			return e
		}
		if page.CaptureId.String() != id {
			return runtimecheckpoint.ErrConflict
		}
		pages[page.CaptureId] = page
	}
	// Verify mutable/migratable pagination rows still match the frozen proof.
	var wanted []Capture
	for _, cap := range caps {
		if _, ok := pages[cap.CaptureId]; ok {
			wanted = append(wanted, cap)
		}
	}
	reader := c.Store.(IndexPageReader)
	for start := 0; start < len(wanted); start += 256 {
		group := wanted[start:min(start+256, len(wanted))]
		actual, e := reader.KeeperIndexPages(ctx, group)
		if e != nil {
			return e
		}
		for _, cap := range group {
			if actual[cap.CaptureId] != pages[cap.CaptureId] {
				return errors.New("keeper_runtime_page_changed")
			}
		}
	}
	children := map[uuid.UUID][]Capture{}
	for _, cap := range caps {
		if cap.CaptureKind == "enrichment" && cap.ParentCaptureId != nil {
			children[*cap.ParentCaptureId] = append(children[*cap.ParentCaptureId], cap)
		}
	}
	expected := map[string]runtimeParent{}
	windows := map[string][]Capture{}
	for _, cap := range caps {
		if cap.CaptureKind != "event_index" || cap.Status != "complete" || cap.CaptureMode == "bootstrap" {
			continue
		}
		parent := runtimeParent{Capture: cap}
		for _, child := range children[cap.CaptureId] {
			parent.Attempts++
			if child.AvailableAt.After(parent.LastAttempt) {
				parent.LastAttempt = child.AvailableAt
			}
			parent.Complete = parent.Complete || child.Status == "complete"
		}
		if !parent.Complete && parent.Attempts < 3 {
			expected[cap.CaptureId.String()] = parent
		}
		if (cap.CaptureMode == "live" || cap.CaptureMode == "catchup") && cap.RequestedFrom != nil && cap.RequestedTo != nil && cap.RequestedTo.After(original.EvidenceCursors[cap.EventKind]) {
			key := runtimeWindowID(cap)
			windows[key] = append(windows[key], cap)
		}
	}
	if uint64(len(expected)) != p.Work.order.Len() || uint64(len(windows)) != p.Work.windows.Len() {
		return errors.New("keeper_runtime_work_set_mismatch")
	}
	for id, parent := range expected {
		actual, ok, e := p.Work.parents.Get(id)
		if e != nil {
			return e
		}
		if !ok || !captureEqual(parent.Capture, actual.Capture) || actual.Attempts != parent.Attempts || !actual.LastAttempt.Equal(parent.LastAttempt) || actual.Complete {
			return errors.New("keeper_runtime_parent_mismatch")
		}
		found, ok, e := p.Work.order.Get(runtimeParentOrder(parent.Capture))
		if e != nil {
			return e
		}
		if !ok || found != id {
			return errors.New("keeper_runtime_order_mismatch")
		}
	}
	var eligible []Capture
	for _, parent := range expected {
		if !parent.LastAttempt.After(p.Report.FrozenAt.Add(-30 * time.Minute)) {
			eligible = append(eligible, parent.Capture)
		}
	}
	sort.Slice(eligible, func(i, j int) bool { return runtimeParentOrder(eligible[i]) < runtimeParentOrder(eligible[j]) })
	next, e := p.Work.next(p.Report.FrozenAt)
	if e != nil {
		return e
	}
	if len(eligible) == 0 && next != nil || len(eligible) > 0 && (next == nil || !captureEqual(*next, eligible[0])) {
		return errors.New("keeper_runtime_next_mismatch")
	}
	for key, parents := range windows {
		complete, e := indexWindowCompletePages(parents, pages)
		if e != nil {
			return e
		}
		done := uint64(0)
		for _, parent := range parents {
			for _, child := range children[parent.CaptureId] {
				if child.Status == "complete" {
					done++
					break
				}
			}
		}
		actual, ok, e := p.Work.windows.Get(key)
		if e != nil {
			return e
		}
		if !ok || actual.Count != uint64(len(parents)) || actual.ChildrenDone != done || complete != (!actual.Invalid && actual.Ended && actual.Visited == actual.Count) {
			return errors.New("keeper_runtime_window_mismatch")
		}
	}
	return nil
}
