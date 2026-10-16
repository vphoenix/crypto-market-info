package reserve

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/dex"
	"github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint"
)

type runtimeRange struct{ From, To uint64 }
type runtimeScope struct {
	Raw, Receipt, Live                      []runtimeRange
	CurrentRaw, CurrentReceipt, CurrentLive []runtimeRange
	Anchor                                  *Capture
	LiveStarted                             bool
	LiveStart                               uint64
}

// ReceiptRefs can be large. The scheduling header excludes them; the original
// typed operation retains the complete header and payload under this reference.
type runtimeHeader struct {
	Capture Capture
	Source  runtimecheckpoint.Reference
}
type runtimeObject struct {
	Ref   runtimecheckpoint.Reference
	Users uint64
}
type runtimeCadence struct {
	LastQuote, LastSimulation, NextLogAttempt time.Time
	LastReconcile, LastReceiptRepair          time.Time
	Active, PendingSnapshot, PendingReconcile bool
	BurstUntil, LogChunk                      uint64
}
type runtimeState struct {
	Version                            uint32
	Binding                            runtimecheckpoint.Binding
	Verified                           bool
	Scopes                             map[string]*runtimeScope
	ActiveRoot, FinalRoot, PendingRoot runtimecheckpoint.PageRoot
	Objects                            map[string]runtimeObject
	Cadence                            runtimeCadence
	Conversion                         RuntimeConversionReport
	active, final, pending             *runtimecheckpoint.PagedMap[runtimeHeader]
	files                              *runtimecheckpoint.Store
	err                                error
}

func runtimeScopeHashes(m Manifest, related []dex.Hash) []string {
	hashes := []string{Hash(m.Hash)}
	for _, h := range related {
		hashes = append(hashes, Hash(h))
	}
	sort.Strings(hashes)
	return hashes
}
func RuntimeScopeHash(m Manifest, related []dex.Hash, start uint64) string {
	return Hex(ID(struct {
		Scopes []string
		Start  uint64
	}{runtimeScopeHashes(m, related), start}))
}
func newRuntimeState(binding runtimecheckpoint.Binding, scopes []string) runtimeState {
	s := runtimeState{Version: 1, Binding: binding, Scopes: map[string]*runtimeScope{}, Objects: map[string]runtimeObject{}}
	for _, h := range scopes {
		s.Scopes[h] = &runtimeScope{}
	}
	s.Cadence.LogChunk = 512
	return s
}
func runtimeIdentity(c Capture) string {
	return Hex(c.ManifestHash) + Hex(c.CaptureId) + Hex(c.BatchId)
}
func runtimeAttemptKey(c Capture) string { return Hex(c.ManifestHash) + Hex(c.CaptureId) }
func runtimeNarrow(c Capture) Capture    { c.ReceiptRefs = nil; return c }
func runtimeFrozen(c Capture) string {
	c.Canonical = false
	c.Finality = ""
	c.Revision = 0
	c.Reason = ""
	c.PayloadHash = ""
	return ID(c)
}
func runtimeBefore(a, b Capture) bool {
	if a.ToBlock != b.ToBlock {
		return a.ToBlock < b.ToBlock
	}
	return a.BatchId < b.BatchId
}
func runtimeLatest(a, b Capture) bool {
	if !a.AvailableAt.Equal(b.AvailableAt) {
		return a.AvailableAt.After(b.AvailableAt)
	}
	return runtimeBefore(a, b)
}
func runtimeMerge(in []runtimeRange) []runtimeRange {
	sort.Slice(in, func(i, j int) bool {
		if in[i].From != in[j].From {
			return in[i].From < in[j].From
		}
		return in[i].To < in[j].To
	})
	out := in[:0]
	for _, r := range in {
		if len(out) == 0 || out[len(out)-1].To != math.MaxUint64 && r.From > out[len(out)-1].To+1 {
			out = append(out, r)
		} else {
			out[len(out)-1].To = max(out[len(out)-1].To, r.To)
		}
	}
	return out
}
func runtimeUnion(a, b []runtimeRange) []runtimeRange {
	return runtimeMerge(append(append([]runtimeRange(nil), a...), b...))
}
func runtimeThrough(ranges []runtimeRange, fallback uint64) uint64 {
	n := fallback
	for _, r := range ranges {
		if r.From > n+1 {
			break
		}
		n = max(n, r.To)
	}
	return n
}
func runtimeCoverage(c Capture, raw, receipts, live *[]runtimeRange) {
	if !c.Canonical || !c.Committed || c.CaptureKind != "logs" || c.LogCoverage != "complete" {
		return
	}
	r := runtimeRange{c.FromBlock, c.ToBlock}
	*raw = runtimeUnion(*raw, []runtimeRange{r})
	if c.ReceiptCoverage == "complete" {
		*receipts = runtimeUnion(*receipts, []runtimeRange{r})
	}
	if c.CaptureMode == "live" {
		*live = runtimeUnion(*live, []runtimeRange{r})
	}
}
func (s *runtimeState) openPages(files *runtimecheckpoint.Store) error {
	s.files = files
	eligible := func(runtimeHeader) bool { return true }
	oldest := func(h runtimeHeader) time.Time { return h.Capture.AvailableAt }
	var err error
	s.active, err = runtimecheckpoint.OpenPagedMap[runtimeHeader](files, s.ActiveRoot, 2<<20, eligible, oldest)
	if err != nil {
		return err
	}
	s.final, err = runtimecheckpoint.OpenPagedMap[runtimeHeader](files, s.FinalRoot, 1<<20, eligible, oldest)
	if err != nil {
		return err
	}
	s.pending, err = runtimecheckpoint.OpenPagedMap[runtimeHeader](files, s.PendingRoot, 1<<20, eligible, oldest)
	return err
}
func (s *runtimeState) get(p *runtimecheckpoint.PagedMap[runtimeHeader], key string) (runtimeHeader, bool) {
	if s.err != nil {
		return runtimeHeader{}, false
	}
	v, ok, e := p.Get(key)
	s.err = e
	return v, ok
}
func (s *runtimeState) retain(ref runtimecheckpoint.Reference) {
	if ref.Hash == "" {
		return
	}
	v := s.Objects[ref.Hash]
	v.Ref = ref
	v.Users++
	s.Objects[ref.Hash] = v
}
func (s *runtimeState) release(ref runtimecheckpoint.Reference) {
	if ref.Hash == "" {
		return
	}
	v, ok := s.Objects[ref.Hash]
	if !ok || v.Users == 0 {
		s.err = runtimecheckpoint.ErrRecovery
		return
	}
	if v.Users == 1 {
		delete(s.Objects, ref.Hash)
	} else {
		v.Users--
		s.Objects[ref.Hash] = v
	}
}
func (s *runtimeState) set(p *runtimecheckpoint.PagedMap[runtimeHeader], key string, h runtimeHeader) {
	if s.err != nil {
		return
	}
	old, exists := s.get(p, key)
	if s.err != nil {
		return
	}
	if exists && ID(old) == ID(h) {
		return
	}
	if err := p.Set(key, h); err != nil {
		s.err = err
		return
	}
	if exists {
		s.release(old.Source)
	}
	s.retain(h.Source)
}
func (s *runtimeState) remove(p *runtimecheckpoint.PagedMap[runtimeHeader], key string) {
	if s.err != nil {
		return
	}
	old, exists := s.get(p, key)
	if s.err != nil || !exists {
		return
	}
	if err := p.Delete(key); err != nil {
		s.err = err
		return
	}
	s.release(old.Source)
}
func (s *runtimeState) each(p *runtimecheckpoint.PagedMap[runtimeHeader], prefix string, fn func(runtimeHeader)) {
	after := prefix
	for s.err == nil {
		keys, rows, err := p.Scan(after, 128, false)
		if err != nil {
			s.err = err
			return
		}
		if len(keys) == 0 {
			return
		}
		for i, key := range keys {
			if prefix != "" && !strings.HasPrefix(key, prefix) {
				return
			}
			fn(rows[i])
			if s.err != nil {
				return
			}
		}
		after = keys[len(keys)-1]
	}
}
func (s *runtimeState) resolveAttempt(cap Capture) {
	key := runtimeAttemptKey(cap)
	chosen, have := s.get(s.final, key)
	active := false
	s.each(s.active, key, func(h runtimeHeader) {
		active = true
		if h.Capture.CaptureKind == "logs" && (!have || runtimeLatest(h.Capture, chosen.Capture)) {
			chosen, have = h, true
		}
	})
	if have && chosen.Capture.LogCoverage == "complete" && chosen.Capture.ReceiptCoverage != "complete" {
		s.set(s.pending, key, chosen)
	} else {
		s.remove(s.pending, key)
	}
	if h, ok := s.get(s.final, key); ok && !active && (h.Capture.LogCoverage != "complete" || h.Capture.ReceiptCoverage == "complete") {
		s.remove(s.final, key)
	}
}
func (s *runtimeState) capture(cap Capture, source runtimecheckpoint.Reference, converting bool) error {
	ch := s.Scopes[cap.ManifestHash]
	if ch == nil || cap.ChainId != 1 || len(cap.CaptureId) != 32 || len(cap.BatchId) != 32 || cap.FromBlock == 0 || cap.FromBlock > cap.ToBlock || cap.ToBlock == math.MaxUint64 || len(cap.FromHash) != 32 || len(cap.ToHash) != 32 || !cap.Committed || cap.Revision == 0 {
		return errors.New("reserve_runtime_capture_identity")
	}
	h := runtimeHeader{runtimeNarrow(cap), source}
	id := runtimeIdentity(cap)
	old, existed := s.get(s.active, id)
	if existed {
		if cap.Revision < old.Capture.Revision {
			return nil
		}
		if cap.Revision == old.Capture.Revision && ID(h.Capture) != ID(old.Capture) {
			return runtimecheckpoint.ErrConflict
		}
	}
	if cap.Canonical && cap.Finality != "finalized" {
		s.set(s.active, id, h)
	} else {
		s.remove(s.active, id)
	}
	if cap.Canonical && cap.Finality == "finalized" {
		runtimeCoverage(cap, &ch.Raw, &ch.Receipt, &ch.Live)
		if ch.Anchor == nil || cap.ToBlock > ch.Anchor.ToBlock || cap.ToBlock == ch.Anchor.ToBlock && runtimeBefore(cap, *ch.Anchor) {
			v := runtimeNarrow(cap)
			ch.Anchor = &v
		}
		if cap.CaptureKind == "logs" {
			key := runtimeAttemptKey(cap)
			old, ok := s.get(s.final, key)
			if !ok || runtimeLatest(cap, old.Capture) {
				s.set(s.final, key, h)
			}
		}
	}
	if cap.Canonical && cap.CaptureKind == "logs" && cap.CaptureMode == "live" && cap.LogCoverage == "complete" {
		if !ch.LiveStarted || cap.FromBlock < ch.LiveStart {
			ch.LiveStarted = true
			ch.LiveStart = cap.FromBlock
		}
	}
	if cap.Canonical {
		runtimeCoverage(cap, &ch.CurrentRaw, &ch.CurrentReceipt, &ch.CurrentLive)
	} else if existed {
		s.rebuild(cap.ManifestHash)
	}
	if !converting && cap.CaptureKind == "logs" {
		s.resolveAttempt(cap)
	}
	return s.err
}
func (s *runtimeState) rebuild(scope string) {
	ch := s.Scopes[scope]
	ch.CurrentRaw = append([]runtimeRange(nil), ch.Raw...)
	ch.CurrentReceipt = append([]runtimeRange(nil), ch.Receipt...)
	ch.CurrentLive = append([]runtimeRange(nil), ch.Live...)
	s.each(s.active, Hex(scope), func(h runtimeHeader) { runtimeCoverage(h.Capture, &ch.CurrentRaw, &ch.CurrentReceipt, &ch.CurrentLive) })
}
func (s *runtimeState) cursor(scope string, fallback uint64, discover bool) uint64 {
	ch := s.Scopes[scope]
	if discover && ch.LiveStarted {
		fallback = ch.LiveStart - 1
	}
	return runtimeThrough(ch.CurrentLive, fallback)
}
func (s *runtimeState) receiptCandidate(scope string) (runtimeHeader, bool) {
	var selected runtimeHeader
	found := false
	s.each(s.pending, Hex(scope), func(h runtimeHeader) {
		if !found || h.Capture.AvailableAt.Before(selected.Capture.AvailableAt) || h.Capture.AvailableAt.Equal(selected.Capture.AvailableAt) && runtimeBefore(h.Capture, selected.Capture) {
			selected = h
			found = true
		}
	})
	return selected, found
}
func (s *runtimeState) references() ([]runtimecheckpoint.Reference, error) {
	if s.err != nil {
		return nil, s.err
	}
	var refs, more []runtimecheckpoint.Reference
	var err error
	s.ActiveRoot, refs, err = s.active.Snapshot()
	if err != nil {
		return nil, err
	}
	s.FinalRoot, more, err = s.final.Snapshot()
	if err != nil {
		return nil, err
	}
	refs = append(refs, more...)
	s.PendingRoot, more, err = s.pending.Snapshot()
	if err != nil {
		return nil, err
	}
	refs = append(refs, more...)
	for _, v := range s.Objects {
		refs = append(refs, v.Ref)
	}
	return append(refs, s.Conversion.InputInventory.References()...), nil
}
func (s *runtimeState) full(h runtimeHeader) (Capture, error) {
	raw, err := s.files.ReadObject(h.Source)
	if err != nil {
		return Capture{}, err
	}
	var w runtimeWrite
	if err = runtimecheckpoint.UnmarshalTyped(raw, &w); err != nil {
		return Capture{}, err
	}
	var c Capture
	if w.Batch != nil {
		c = w.Batch.Capture
	} else if w.Revision != nil {
		c = *w.Revision
	} else {
		return c, runtimecheckpoint.ErrRecovery
	}
	if ID(runtimeNarrow(c)) != ID(h.Capture) {
		return c, runtimecheckpoint.ErrConflict
	}
	return c, nil
}
func (s *runtimeState) applyCadence(b Batch) {
	cap := b.Capture
	if cap.CaptureMode != "live" {
		return
	}
	if cap.CaptureKind == "logs" {
		if cap.Reason == "rpc_archive_auth_required" {
			s.Cadence.NextLogAttempt = dex.Now().Add(5 * time.Minute)
		}
		s.Cadence.LogChunk = nextLogChunk(s.Cadence.LogChunk, cap.ToBlock-cap.FromBlock+1, cap.LogCoverage == "complete", cap.Reason)
		s.Cadence.PendingSnapshot = s.Cadence.PendingSnapshot || SnapshotEvent(b.Logs)
	}
	if cap.CaptureKind == "snapshot" {
		s.Cadence.PendingSnapshot = cap.StateCoverage != "complete"
		s.Cadence.Active = false
		for _, v := range b.States {
			if AuctionMonitor(v) {
				s.Cadence.Active = true
			}
		}
		for _, q := range b.Quotes {
			if (q.RouteKind == "mint" || q.RouteKind == "redeem") && q.Quality != "incomplete" && q.AmountInRaw != nil && q.AmountOutRaw != nil && q.AmountOutRaw.Cmp(q.AmountInRaw) > 0 {
				s.Cadence.BurstUntil = max(s.Cadence.BurstUntil, cap.ToBlock+10)
			}
		}
		s.Cadence.LastQuote = dex.Now()
	}
}
func (s *runtimeState) validate(binding runtimecheckpoint.Binding, scopes []string) error {
	if s.Version != 1 || !s.Verified || s.Binding != binding || len(s.Scopes) != len(scopes) || s.Objects == nil {
		return runtimecheckpoint.ErrRecovery
	}
	for _, scope := range scopes {
		ch := s.Scopes[scope]
		if ch == nil {
			return runtimecheckpoint.ErrRecovery
		}
		for _, ranges := range [][]runtimeRange{ch.Raw, ch.Receipt, ch.Live, ch.CurrentRaw, ch.CurrentReceipt, ch.CurrentLive} {
			for i, r := range ranges {
				if r.From == 0 || r.From > r.To || r.To == math.MaxUint64 || i > 0 && ranges[i-1].To+1 >= r.From {
					return fmt.Errorf("reserve coverage: %w", runtimecheckpoint.ErrRecovery)
				}
			}
		}
		before := [][]runtimeRange{ch.CurrentRaw, ch.CurrentReceipt, ch.CurrentLive}
		s.rebuild(scope)
		if !reflect.DeepEqual(before, [][]runtimeRange{ch.CurrentRaw, ch.CurrentReceipt, ch.CurrentLive}) {
			return fmt.Errorf("reserve cached coverage: %w", runtimecheckpoint.ErrRecovery)
		}
	}
	objects := map[string]runtimeObject{}
	for i, pages := range []*runtimecheckpoint.PagedMap[runtimeHeader]{s.active, s.final, s.pending} {
		s.each(pages, "", func(h runtimeHeader) {
			cap := h.Capture
			key := runtimeAttemptKey(cap)
			if i == 0 {
				key = runtimeIdentity(cap)
			}
			stored, ok := s.get(pages, key)
			if !ok || ID(stored) != ID(h) || s.Scopes[cap.ManifestHash] == nil || !cap.Canonical || !cap.Committed || len(cap.ReceiptRefs) != 0 || h.Source.Hash == "" || i == 0 && cap.Finality == "finalized" || i == 1 && cap.Finality != "finalized" || i == 2 && (cap.CaptureKind != "logs" || cap.LogCoverage != "complete" || cap.ReceiptCoverage == "complete") {
				s.err = fmt.Errorf("reserve working identity: %w", runtimecheckpoint.ErrRecovery)
				return
			}
			v := objects[h.Source.Hash]
			v.Ref = h.Source
			v.Users++
			objects[h.Source.Hash] = v
		})
	}
	if !reflect.DeepEqual(objects, s.Objects) {
		return fmt.Errorf("reserve pending references: %w", runtimecheckpoint.ErrRecovery)
	}
	return s.err
}
