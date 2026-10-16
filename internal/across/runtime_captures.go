package across

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint"
)

type runtimeCaptureRoot struct{ Records, Order runtimecheckpoint.PageRoot }
type runtimeInvalidation struct {
	Chain  uint64
	Bad    []uint64
	Reason string
}
type runtimeCapturePages struct {
	records *runtimecheckpoint.PagedMap[Capture]
	order   *runtimecheckpoint.PagedMap[string]
	partial bool
}

func captureOrder(c Capture, partial bool) string {
	at := c.StartedAt.UTC().Format("2006-01-02T15:04:05.000000000Z")
	if partial {
		return at + "/" + c.CaptureId
	}
	return fmt.Sprintf("%020d/%020d/%s/%s", c.ChainId, *c.ToBlock, at, c.CaptureId)
}
func openCapturePages(files *runtimecheckpoint.Store, root runtimeCaptureRoot, partial bool) (*runtimeCapturePages, error) {
	records, err := runtimecheckpoint.OpenPagedMap[Capture](files, root.Records, 1<<20, func(Capture) bool { return true }, func(c Capture) time.Time { return c.StartedAt })
	if err != nil {
		return nil, err
	}
	order, err := runtimecheckpoint.OpenPagedMap[string](files, root.Order, 256<<10, func(string) bool { return true })
	if err != nil {
		return nil, err
	}
	if records.Len() != order.Len() {
		return nil, runtimecheckpoint.ErrRecovery
	}
	return &runtimeCapturePages{records: records, order: order, partial: partial}, nil
}
func (p *runtimeCapturePages) set(c Capture) error {
	if !anchored(c) || len(c.CaptureId) != 32 {
		return runtimecheckpoint.ErrRecovery
	}
	old, ok, err := p.records.Get(c.CaptureId)
	if err != nil {
		return err
	}
	if ok && captureOrder(old, p.partial) != captureOrder(c, p.partial) {
		if err = p.order.Delete(captureOrder(old, p.partial)); err != nil {
			return err
		}
	}
	if err = p.records.Set(c.CaptureId, c); err != nil {
		return err
	}
	return p.order.Set(captureOrder(c, p.partial), c.CaptureId)
}
func (p *runtimeCapturePages) remove(id string) error {
	old, ok, err := p.records.Get(id)
	if err != nil || !ok {
		return err
	}
	if err = p.order.Delete(captureOrder(old, p.partial)); err != nil {
		return err
	}
	return p.records.Delete(id)
}
func (p *runtimeCapturePages) scan(after string, limit int) ([]string, []Capture, error) {
	keys, ids, err := p.order.Scan(after, limit, false)
	if err != nil {
		return nil, nil, err
	}
	rows := make([]Capture, 0, len(ids))
	for i, id := range ids {
		c, ok, err := p.records.Get(id)
		if err != nil {
			return nil, nil, err
		}
		if !ok || !anchored(c) || c.CaptureId != id || captureOrder(c, p.partial) != keys[i] {
			return nil, nil, runtimecheckpoint.ErrRecovery
		}
		rows = append(rows, c)
	}
	return keys, rows, nil
}
func (p *runtimeCapturePages) snapshot() (runtimeCaptureRoot, []runtimecheckpoint.Reference, error) {
	a, refs, err := p.records.Snapshot()
	if err != nil {
		return runtimeCaptureRoot{}, nil, err
	}
	b, more, err := p.order.Snapshot()
	return runtimeCaptureRoot{a, b}, append(refs, more...), err
}
func (s *runtimeState) captureGet(id string, partial bool) (Capture, bool) {
	p := s.activePages
	plain := s.Active
	if partial {
		p = s.partialPages
		plain = s.Partial
	}
	if p == nil {
		c, ok := plain[id]
		return c, ok
	}
	c, ok, err := p.records.Get(id)
	if err != nil {
		s.captureErr = err
	}
	return c, ok
}
func (s *runtimeState) captureSet(c Capture, partial bool) {
	if s.captureErr != nil {
		return
	}
	p := s.activePages
	plain := s.Active
	if partial {
		p = s.partialPages
		plain = s.Partial
	}
	if p == nil {
		plain[c.CaptureId] = c
		return
	}
	s.captureErr = p.set(c)
}
func (s *runtimeState) captureDelete(id string, partial bool) {
	if s.captureErr != nil {
		return
	}
	p := s.activePages
	plain := s.Active
	if partial {
		p = s.partialPages
		plain = s.Partial
	}
	if p == nil {
		delete(plain, id)
		return
	}
	s.captureErr = p.remove(id)
}
func (s *runtimeState) eachCapture(partial bool, fn func(Capture)) {
	if s.captureErr != nil {
		return
	}
	p := s.activePages
	plain := s.Active
	if partial {
		p = s.partialPages
		plain = s.Partial
	}
	if p == nil {
		for _, c := range plain {
			fn(c)
		}
		return
	}
	after := ""
	for s.captureErr == nil {
		keys, rows, err := p.scan(after, 128)
		if err != nil {
			s.captureErr = err
			return
		}
		if len(keys) == 0 {
			return
		}
		for _, c := range rows {
			fn(c)
			if s.captureErr != nil {
				return
			}
		}
		after = keys[len(keys)-1]
	}
}
func (s *runtimeState) captureCount(partial bool) int {
	p := s.activePages
	plain := s.Active
	if partial {
		p = s.partialPages
		plain = s.Partial
	}
	if p == nil {
		return len(plain)
	}
	return int(p.records.Len())
}
func (s *runtimeState) ioError() error {
	if s.captureErr != nil {
		return s.captureErr
	}
	if s.pendingErr != nil {
		return s.pendingErr
	}
	return s.receiptErr
}
func (s *runtimeState) invalidated(c Capture) bool {
	if !anchored(c) {
		return false
	}
	for _, job := range s.Invalidations {
		if job.Chain == c.ChainId {
			for _, n := range job.Bad {
				if *c.FromBlock <= n && n <= *c.ToBlock {
					return true
				}
			}
		}
	}
	return false
}
func (s *runtimeState) rebuildCoverage(chain uint64) {
	ch := s.Chains[chain]
	if ch == nil {
		return
	}
	raw := append([]RuntimeRange(nil), ch.Raw...)
	decoded := append([]RuntimeRange(nil), ch.Decoded...)
	s.eachCapture(false, func(c Capture) {
		if c.ChainId != chain || !runtimeRaw(c) || s.invalidated(c) {
			return
		}
		raw = append(raw, RuntimeRange{*c.FromBlock, *c.ToBlock})
		if runtimeDecoded(c) {
			decoded = append(decoded, RuntimeRange{*c.FromBlock, *c.ToBlock})
		}
	})
	ch.CurrentRaw, ch.CurrentDecoded = mergeRuntimeRanges(raw), mergeRuntimeRanges(decoded)
	if ch.Started {
		next := firstUncovered(ch.Start, ch.End, intervalSlice(ch.CurrentRaw))
		if next > 0 {
			s.Watch.Cursors[chain] = next - 1
		} else {
			s.Watch.Cursors[chain] = 0
		}
	}
}
func (s *runtimeState) enableCapturePages(files *runtimecheckpoint.Store) error {
	if s.activePages != nil {
		return s.captureErr
	}
	var err error
	s.activePages, err = openCapturePages(files, s.ActiveRoot, false)
	if err != nil {
		return err
	}
	s.partialPages, err = openCapturePages(files, s.PartialRoot, true)
	if err != nil {
		return err
	}
	if s.activePages.records.Len() != 0 && len(s.Active) != 0 || s.partialPages.records.Len() != 0 && len(s.Partial) != 0 {
		return runtimecheckpoint.ErrRecovery
	}
	for _, partial := range []bool{false, true} {
		plain := s.Active
		if partial {
			plain = s.Partial
		}
		keys := make([]string, 0, len(plain))
		for id := range plain {
			keys = append(keys, id)
		}
		sort.Strings(keys)
		for _, id := range keys {
			if plain[id].CaptureId != id {
				return runtimecheckpoint.ErrRecovery
			}
			s.captureSet(plain[id], partial)
		}
	}
	s.Active = map[string]Capture{}
	s.Partial = map[string]Capture{}
	for chain, ch := range s.Chains {
		raw, decoded := ch.CurrentRaw, ch.CurrentDecoded
		cursor := s.Watch.Cursors[chain]
		s.rebuildCoverage(chain)
		if cursor != s.Watch.Cursors[chain] || s.ActiveRoot.Records.Version != 0 && (!reflect.DeepEqual(raw, ch.CurrentRaw) || !reflect.DeepEqual(decoded, ch.CurrentDecoded)) {
			return runtimecheckpoint.ErrRecovery
		}
	}
	return s.captureErr
}
func (s *runtimeState) captureReferences() ([]runtimecheckpoint.Reference, error) {
	if s.captureErr != nil {
		return nil, s.captureErr
	}
	if s.activePages == nil {
		return nil, nil
	}
	a, refs, err := s.activePages.snapshot()
	if err != nil {
		return nil, err
	}
	b, more, err := s.partialPages.snapshot()
	if err != nil {
		return nil, err
	}
	s.ActiveRoot, s.PartialRoot = a, b
	return append(refs, more...), nil
}
func (s *runtimeState) reconcileCaptures(chain uint64, limit int) []Capture {
	var out []Capture
	if ch := s.Chains[chain]; ch != nil && ch.Anchor != nil {
		out = append(out, *ch.Anchor)
	}
	if s.activePages == nil {
		var rows []Capture
		for _, c := range s.Active {
			if c.ChainId == chain {
				rows = append(rows, c)
			}
		}
		sort.Slice(rows, func(i, j int) bool { return captureOrder(rows[i], false) < captureOrder(rows[j], false) })
		return append(out, rows[:min(limit, len(rows))]...)
	}
	prefix := fmt.Sprintf("%020d/", chain)
	after := prefix
	for limit > 0 {
		keys, rows, err := s.activePages.scan(after, min(limit, 128))
		if err != nil {
			s.captureErr = err
			return nil
		}
		if len(keys) == 0 {
			break
		}
		for i, c := range rows {
			if !strings.HasPrefix(keys[i], prefix) {
				return out
			}
			out = append(out, c)
			limit--
		}
		after = keys[len(keys)-1]
	}
	return out
}

func (s *runtimeState) affectedCaptures(job runtimeInvalidation, limit int) []Capture {
	rows := make([]Capture, 0, limit)
	add := func(cap Capture) {
		if cap.ChainId != job.Chain {
			return
		}
		for _, n := range job.Bad {
			if *cap.FromBlock <= n && n <= *cap.ToBlock {
				rows = append(rows, cap)
				break
			}
		}
	}
	if s.activePages == nil {
		for _, cap := range s.Active {
			add(cap)
			if len(rows) >= limit {
				break
			}
		}
		return rows
	}
	minimum := ^uint64(0)
	for _, n := range job.Bad {
		minimum = min(minimum, n)
	}
	prefix := fmt.Sprintf("%020d/", job.Chain)
	after := fmt.Sprintf("%s%020d/", prefix, minimum)
	for {
		keys, page, err := s.activePages.scan(after, 128)
		if err != nil {
			s.captureErr = err
			return nil
		}
		if len(page) == 0 {
			return rows
		}
		for i, cap := range page {
			if !strings.HasPrefix(keys[i], prefix) {
				return rows
			}
			add(cap)
			if len(rows) >= limit {
				return rows
			}
		}
		after = keys[len(keys)-1]
	}
}
