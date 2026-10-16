package justlendkeeper

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint"
)

// These are actual unfinished source windows and enrichment tasks, not a copy
// of historical capture rows. Their root descriptors live with the existing
// State/Ops checkpoint; only bounded pages are resident.
type runtimeParent struct {
	Capture     Capture
	Attempts    uint32
	LastAttempt time.Time
	Children    []uuid.UUID
	Complete    bool
	Window      string
	PageKey     string
}
type runtimeWindow struct {
	Kind                         string
	From, To                     time.Time
	ScanID                       string
	Count, Visited, ChildrenDone uint64
	Next                         string
	Ended, Invalid               bool
}
type runtimePage struct {
	Capture            Capture
	Page               IndexPage
	Visited, ChildDone bool
}
type runtimeWork struct {
	Version                                     uint32
	Binding                                     runtimecheckpoint.Binding
	Verified                                    bool
	ParentRoot, OrderRoot, WindowRoot, PageRoot runtimecheckpoint.PageRoot
	Conversion                                  RuntimeConversionReport
	parents                                     *runtimecheckpoint.PagedMap[runtimeParent]
	order                                       *runtimecheckpoint.PagedMap[string]
	windows                                     *runtimecheckpoint.PagedMap[runtimeWindow]
	pages                                       *runtimecheckpoint.PagedMap[runtimePage]
	err                                         error
}

func runtimeTime(at time.Time) string    { return at.UTC().Format("2006-01-02T15:04:05.000000000Z") }
func runtimeParentID(cap Capture) string { return cap.CaptureId.String() }
func runtimeWindowID(cap Capture) string {
	if cap.RequestedFrom == nil || cap.RequestedTo == nil {
		return ""
	}
	return cap.EventKind + "/" + runtimeTime(*cap.RequestedFrom) + "/" + runtimeTime(*cap.RequestedTo)
}
func runtimePageID(window, token string) string { return window + "/" + Hex(Hash([]byte(token))) }
func runtimeParentOrder(cap Capture) string {
	priority := "1"
	if cap.CaptureMode == "live" || cap.CaptureMode == "catchup" {
		priority = "0"
	}
	from := time.Time{}
	if cap.RequestedFrom != nil {
		from = *cap.RequestedFrom
	}
	return priority + "/" + runtimeTime(from) + "/" + runtimeTime(cap.CaptureStartedAt) + "/" + cap.CaptureId.String()
}
func (w *runtimeWork) open(files *runtimecheckpoint.Store) error {
	var err error
	w.parents, err = runtimecheckpoint.OpenPagedMap[runtimeParent](files, w.ParentRoot, 2<<20, func(p runtimeParent) bool { return !p.Complete && p.Attempts < 3 }, func(p runtimeParent) time.Time { return p.Capture.CaptureStartedAt })
	if err != nil {
		return err
	}
	w.order, err = runtimecheckpoint.OpenPagedMap[string](files, w.OrderRoot, 512<<10, func(string) bool { return true })
	if err != nil {
		return err
	}
	w.windows, err = runtimecheckpoint.OpenPagedMap[runtimeWindow](files, w.WindowRoot, 1<<20, func(runtimeWindow) bool { return true }, func(v runtimeWindow) time.Time { return v.From })
	if err != nil {
		return err
	}
	w.pages, err = runtimecheckpoint.OpenPagedMap[runtimePage](files, w.PageRoot, 2<<20, func(runtimePage) bool { return true })
	return err
}
func (w *runtimeWork) references() ([]runtimecheckpoint.Reference, error) {
	if w.err != nil {
		return nil, w.err
	}
	var refs, more []runtimecheckpoint.Reference
	var err error
	w.ParentRoot, refs, err = w.parents.Snapshot()
	if err != nil {
		return nil, err
	}
	w.OrderRoot, more, err = w.order.Snapshot()
	if err != nil {
		return nil, err
	}
	refs = append(refs, more...)
	w.WindowRoot, more, err = w.windows.Snapshot()
	if err != nil {
		return nil, err
	}
	refs = append(refs, more...)
	w.PageRoot, more, err = w.pages.Snapshot()
	return append(append(refs, more...), w.Conversion.InputInventory.References()...), err
}
func (w *runtimeWork) parent(cap Capture, page *IndexPage, evidence map[string]time.Time) error {
	if !cap.Committed || cap.CaptureKind != "event_index" || cap.Status != "complete" || cap.CaptureMode == "bootstrap" {
		return nil
	}
	id := runtimeParentID(cap)
	old, exists, err := w.parents.Get(id)
	if err != nil {
		return err
	}
	if exists {
		if !captureEqual(old.Capture, cap) {
			return runtimecheckpoint.ErrConflict
		}
		return nil
	}
	p := runtimeParent{Capture: cap}
	// Only these source modes participate in the original evidence frontier.
	if (cap.CaptureMode == "live" || cap.CaptureMode == "catchup") && cap.RequestedFrom != nil && cap.RequestedTo != nil && cap.RequestedTo.After(evidence[cap.EventKind]) {
		if page == nil {
			return errors.New("keeper_runtime_index_page_missing")
		}
		if err = validateIndexPage(cap, *page); err != nil {
			return err
		}
		p.Window = runtimeWindowID(cap)
		win, ok, e := w.windows.Get(p.Window)
		if e != nil {
			return e
		}
		if !ok {
			win = runtimeWindow{Kind: cap.EventKind, From: *cap.RequestedFrom, To: *cap.RequestedTo, ScanID: page.ScanID}
		}
		if win.ScanID != page.ScanID {
			win.Invalid = true
		}
		key := runtimePageID(p.Window, page.FingerprintIn)
		p.PageKey = key
		previous, duplicate, e := w.pages.Get(key)
		if e != nil {
			return e
		}
		if duplicate {
			// Distinct captures with the same input token invalidate the original
			// complete-window proof, even if their response bodies are identical.
			if previous.Capture.CaptureId != cap.CaptureId {
				win.Invalid = true
			} else if !captureEqual(previous.Capture, cap) || previous.Page != *page {
				return runtimecheckpoint.ErrConflict
			}
		} else {
			if e = w.pages.Set(key, runtimePage{Capture: cap, Page: *page}); e != nil {
				return e
			}
		}
		win.Count++
		if e = w.walk(p.Window, &win); e != nil {
			return e
		}
		if e = w.windows.Set(p.Window, win); e != nil {
			return e
		}
	}
	if err = w.parents.Set(id, p); err != nil {
		return err
	}
	return w.order.Set(runtimeParentOrder(cap), id)
}
func (w *runtimeWork) walk(key string, win *runtimeWindow) error {
	if win.Invalid || win.Ended {
		return nil
	}
	for {
		pageKey := runtimePageID(key, win.Next)
		page, ok, err := w.pages.Get(pageKey)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		if page.Page.FingerprintIn != win.Next || page.Visited {
			win.Invalid = true
			return nil
		}
		page.Visited = true
		win.Visited++
		if err = w.pages.Set(pageKey, page); err != nil {
			return err
		}
		win.Next = page.Page.FingerprintOut
		if win.Next == "" {
			win.Ended = true
			return nil
		}
	}
}
func (w *runtimeWork) child(cap Capture) error {
	if !cap.Committed || cap.CaptureKind != "enrichment" || cap.ParentCaptureId == nil {
		return nil
	}
	id := cap.ParentCaptureId.String()
	p, ok, err := w.parents.Get(id)
	if err != nil {
		return err
	}
	// A replay after the corresponding window was already retired cannot create
	// new work. Pending source parents are always loaded before conversion children.
	if !ok {
		return nil
	}
	for _, previous := range p.Children {
		if previous == cap.CaptureId {
			return nil
		}
	}
	if p.Complete {
		return nil
	}
	p.Attempts++
	if len(p.Children) < 3 {
		p.Children = append(p.Children, cap.CaptureId)
	}
	if cap.AvailableAt.After(p.LastAttempt) {
		p.LastAttempt = cap.AvailableAt
	}
	if cap.Status == "complete" {
		p.Complete = true
		if p.Window != "" {
			win, ok, e := w.windows.Get(p.Window)
			if e != nil {
				return e
			}
			if !ok {
				return runtimecheckpoint.ErrRecovery
			}
			// Invalid windows remain unresolved; their parent completion is still
			// retained and they cannot accidentally advance on a later child.
			win.ChildrenDone++
			if e = w.windows.Set(p.Window, win); e != nil {
				return e
			}
			// Parent identity retains its exact page key, so a child result does
			// not walk other pages in this still-unfinished source window.
			row, exists, e := w.pages.Get(p.PageKey)
			if e != nil {
				return e
			}
			if !exists {
				return runtimecheckpoint.ErrRecovery
			}
			if row.Capture.CaptureId == p.Capture.CaptureId {
				row.ChildDone = true
				if e = w.pages.Set(p.PageKey, row); e != nil {
					return e
				}
			} else if !win.Invalid {
				return runtimecheckpoint.ErrConflict
			}
		}
	}
	if p.Complete || p.Attempts >= 3 {
		if err = w.order.Delete(runtimeParentOrder(p.Capture)); err != nil {
			return err
		}
	}
	// Keep terminal parent identity only while a still-unresolved evidence window
	// needs it. History/bootstrap completions have no future working dependency.
	if p.Window == "" && (p.Complete || p.Attempts >= 3) {
		return w.parents.Delete(id)
	}
	return w.parents.Set(id, p)
}
func (w *runtimeWork) next(now time.Time) (*Capture, error) {
	for after := ""; ; {
		keys, ids, err := w.order.Scan(after, 128, false)
		if err != nil {
			return nil, err
		}
		if len(keys) == 0 {
			return nil, nil
		}
		for i, id := range ids {
			p, ok, err := w.parents.Get(id)
			if err != nil {
				return nil, err
			}
			if !ok || runtimeParentOrder(p.Capture) != keys[i] {
				return nil, runtimecheckpoint.ErrRecovery
			}
			if !p.Complete && p.Attempts < 3 && !p.LastAttempt.After(now.Add(-30*time.Minute)) {
				cap := p.Capture
				return &cap, nil
			}
		}
		after = keys[len(keys)-1]
	}
}
func (w *runtimeWork) advance(kind string, from, to time.Time) (time.Time, error) {
	if from.IsZero() || !from.Before(to) {
		return from, nil
	}
	for after := kind + "/"; ; {
		keys, rows, err := w.windows.Scan(after, 128, false)
		if err != nil {
			return from, err
		}
		if len(keys) == 0 {
			return from, nil
		}
		for i, win := range rows {
			if win.Kind != kind || win.From.After(from) {
				return from, nil
			}
			if !win.To.After(from) || win.To.After(to) {
				continue
			}
			// Match the SQL interface's earliest overlapping window. Do not skip a
			// broken smaller window in favor of a later, wider successful one.
			if win.Invalid || !win.Ended || win.Visited != win.Count || win.ChildrenDone != win.Count {
				return from, nil
			}
			if err = w.retire(keys[i]); err != nil {
				return from, err
			}
			return win.To, nil
		}
		after = keys[len(keys)-1]
	}
}
func (w *runtimeWork) retire(window string) error {
	// Remove only closed pages; any incomplete enrichment is preserved separately.
	for after := window + "/"; ; {
		keys, rows, err := w.pages.Scan(after, 128, false)
		if err != nil {
			return err
		}
		if len(keys) == 0 {
			break
		}
		end := false
		for i, key := range keys {
			if len(key) < len(window)+1 || key[:len(window)+1] != window+"/" {
				end = true
				break
			}
			p := rows[i]
			if !p.ChildDone {
				return runtimecheckpoint.ErrRecovery
			}
			if err = w.parents.Delete(runtimeParentID(p.Capture)); err != nil {
				return err
			}
			if err = w.pages.Delete(key); err != nil {
				return err
			}
		}
		if end {
			break
		}
		after = keys[len(keys)-1]
	}
	return w.windows.Delete(window)
}
func (w *runtimeWork) memoryBytes() int64 {
	return runtimecheckpoint.EstimateTypedBytes(*w) + w.parents.MemoryBytes() + w.order.MemoryBytes() + w.windows.MemoryBytes() + w.pages.MemoryBytes()
}
func (w *runtimeWork) validate(binding runtimecheckpoint.Binding, config string) error {
	if w.Version != 1 || !w.Verified || w.Binding != binding || binding.ScopeHash != Hex(config) {
		return runtimecheckpoint.ErrRecovery
	}
	for after := ""; ; {
		keys, parents, err := w.parents.Scan(after, 128, false)
		if err != nil {
			return err
		}
		if len(keys) == 0 {
			break
		}
		for i, p := range parents {
			if keys[i] != runtimeParentID(p.Capture) || p.Capture.ConfigHash != config || !p.Capture.Committed || p.Capture.Status != "complete" || p.Capture.CaptureKind != "event_index" {
				return runtimecheckpoint.ErrRecovery
			}
		}
		after = keys[len(keys)-1]
	}
	return w.err
}
func (w *runtimeWork) summary() string {
	return fmt.Sprintf("windows=%d enrichment_pending=%d oldest_parent=%s", w.windows.Len(), w.order.Len(), w.parents.Oldest().Format(time.RFC3339Nano))
}
