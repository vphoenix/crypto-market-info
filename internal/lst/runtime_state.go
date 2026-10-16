package lst

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint"
)

type runtimeRange struct{ From, To uint64 }
type runtimeSeed struct {
	Quote     Quote
	StartedAt time.Time
	Canonical bool
}
type runtimeSchedule struct {
	NextLogs, NextOldGap, NextGas, NextMarket, NextMetadata time.Time
	Funding                                                 fundingSchedule
}
type runtimeState struct {
	Version                                uint32
	Binding                                runtimecheckpoint.Binding
	Manifest                               string
	Verified                               bool
	Coverage                               []runtimeRange
	Cursors                                map[uint64]logProgress
	ActiveRoot, SeedRoot, DoneRoot         runtimecheckpoint.PageRoot
	Gas                                    runtimeGasState
	Schedule                               runtimeSchedule
	ImportedPendingID, ImportedPendingHash string
	Conversion                             RuntimeConversionReport
	active                                 *runtimecheckpoint.PagedMap[Capture]
	seeds                                  *runtimecheckpoint.PagedMap[runtimeSeed]
	done                                   *runtimecheckpoint.PagedMap[time.Time]
	cleanupAfter                           string
	err                                    error
}

func RuntimeScopeHash(c *Collector) string {
	return Hex(CanonicalHash(struct {
		Manifest       string
		LiveStart      uint64
		LogMode        string
		MarketInterval time.Duration
		EntryRoutes    int
		PauseLiveLogs  bool
	}{c.Manifest.Hash, c.LiveLogsFromBlock, c.LogMode, c.MarketInterval, c.EntryRoutesPerRound, c.PauseLiveLogs}))
}
func newRuntimeState(binding runtimecheckpoint.Binding, manifest string) runtimeState {
	return runtimeState{Version: 1, Binding: binding, Manifest: manifest, Cursors: map[uint64]logProgress{}}
}
func runtimeCaptureKey(cap Capture) string {
	return cap.StartedAt.UTC().Format("2006-01-02T15:04:05.000000000Z") + "/" + cap.CaptureId.String()
}
func runtimeSeedKey(seed runtimeSeed) string {
	return seed.Quote.AvailableAt.UTC().Format("2006-01-02T15:04:05.000000000Z") + "/" + Hex(seed.Quote.QuoteId) + "/" + seed.Quote.CaptureId.String()
}
func (s *runtimeState) open(files *runtimecheckpoint.Store) error {
	var err error
	s.active, err = runtimecheckpoint.OpenPagedMap[Capture](files, s.ActiveRoot, 1<<20, func(Capture) bool { return true }, func(cap Capture) time.Time { return cap.StartedAt })
	if err != nil {
		return err
	}
	s.seeds, err = runtimecheckpoint.OpenPagedMap[runtimeSeed](files, s.SeedRoot, 1<<20, func(seed runtimeSeed) bool { return seed.Canonical }, func(seed runtimeSeed) time.Time { return seed.Quote.AvailableAt })
	if err != nil {
		return err
	}
	s.done, err = runtimecheckpoint.OpenPagedMap[time.Time](files, s.DoneRoot, 256<<10, func(time.Time) bool { return true })
	if err != nil {
		return err
	}
	return s.Gas.open(files)
}
func (s *runtimeState) eachSeed(fn func(string, runtimeSeed)) {
	for after := ""; s.err == nil; {
		keys, rows, err := s.seeds.Scan(after, 128, false)
		if err != nil {
			s.err = err
			return
		}
		if len(keys) == 0 {
			return
		}
		for i, seed := range rows {
			fn(keys[i], seed)
			if s.err != nil {
				return
			}
		}
		after = keys[len(keys)-1]
	}
}
func (s *runtimeState) foldCoverage(cap Capture) error {
	if cap.CaptureKind != "logs" || !cap.Canonical || !cap.Committed || cap.Status != "complete" || cap.Finality != "finalized" || (cap.CaptureMode != "live" && cap.CaptureMode != "backfill") {
		return nil
	}
	if cap.FromBlock == nil || cap.FromBlockTime == nil || cap.ToBlock == nil || *cap.FromBlock > *cap.ToBlock || *cap.ToBlock == math.MaxUint64 {
		return errors.New("lst_runtime_log_anchor")
	}
	s.Coverage = append(s.Coverage, runtimeRange{*cap.FromBlock, *cap.ToBlock})
	sort.Slice(s.Coverage, func(i, j int) bool {
		if s.Coverage[i].From != s.Coverage[j].From {
			return s.Coverage[i].From < s.Coverage[j].From
		}
		return s.Coverage[i].To < s.Coverage[j].To
	})
	merged := s.Coverage[:0]
	for _, r := range s.Coverage {
		if len(merged) == 0 || r.From > merged[len(merged)-1].To+1 {
			merged = append(merged, r)
		} else {
			merged[len(merged)-1].To = max(merged[len(merged)-1].To, r.To)
		}
	}
	s.Coverage = merged
	for segment, p := range s.Cursors {
		p.Next = s.continuous(p.Start)
		s.Cursors[segment] = p
	}
	return nil
}
func (s *runtimeState) continuous(start uint64) uint64 {
	next := start
	for _, r := range s.Coverage {
		if r.From > next {
			break
		}
		if r.To >= next {
			next = r.To + 1
		}
	}
	return next
}
func (s *runtimeState) capture(cap Capture) error {
	if cap.ManifestHash != s.Manifest || !cap.Committed {
		return runtimecheckpoint.ErrConflict
	}
	if cap.CaptureKind == "market" {
		key := runtimeCaptureKey(cap)
		old, exists, err := s.active.Get(key)
		if err != nil {
			return err
		}
		if exists && old.Revision > cap.Revision {
			return nil
		}
		if exists && old.Revision == cap.Revision && !reflect.DeepEqual(old, cap) {
			return runtimecheckpoint.ErrConflict
		}
		if cap.ToBlock != nil && cap.Finality != "finalized" && cap.Finality != "orphaned" {
			if err = s.active.Set(key, cap); err != nil {
				return err
			}
		} else {
			if err = s.active.Delete(key); err != nil {
				return err
			}
		}
		s.eachSeed(func(key string, seed runtimeSeed) {
			if seed.Quote.CaptureId == cap.CaptureId && seed.Canonical != cap.Canonical {
				seed.Canonical = cap.Canonical
				s.err = s.seeds.Set(key, seed)
			}
		})
	}
	if err := s.foldCoverage(cap); err != nil {
		return err
	}
	return s.err
}
func (s *runtimeState) batch(b Batch, now time.Time) error {
	if err := s.capture(b.Capture); err != nil {
		return err
	}
	cut := now.Add(-31 * 24 * time.Hour)
	if b.Capture.CaptureKind == "market" && !b.Capture.StartedAt.Before(cut) {
		for _, q := range b.Quotes {
			if q.IsFollowupSeed {
				seed := runtimeSeed{q, b.Capture.StartedAt, b.Capture.Canonical}
				if err := s.seeds.Set(runtimeSeedKey(seed), seed); err != nil {
					return err
				}
			}
			if q.QuoteRole == "followup" && q.ReferenceQuoteId != nil && q.TargetDelaySeconds != nil {
				key := followupKey(*q.ReferenceQuoteId, *q.TargetDelaySeconds)
				old, ok, err := s.done.Get(key)
				if err != nil {
					return err
				}
				if !ok || b.Capture.StartedAt.After(old) {
					if err = s.done.Set(key, b.Capture.StartedAt); err != nil {
						return err
					}
				}
			}
		}
		s.Schedule.Funding.observe(b, b.Capture.AvailableAt)
	}
	if b.Capture.CaptureKind == "funding" {
		s.Schedule.Funding.result(b.Capture.AvailableAt, b, nil)
	}
	return s.Gas.add(b, now)
}
func (s *runtimeState) followups(now time.Time) (followupState, error) {
	out := followupState{Done: map[string]bool{}}
	cut := now.Add(-31 * 24 * time.Hour)
	s.eachSeed(func(key string, seed runtimeSeed) {
		if seed.StartedAt.Before(cut) {
			s.err = s.seeds.Delete(key)
			return
		}
		if !seed.Canonical {
			return
		}
		q := seed.Quote
		out.Seeds = append(out.Seeds, q)
		if len(out.Seeds) > 30 {
			out.Seeds = out.Seeds[1:]
		}
		day := q.AvailableAt.Format("2006-01-02")
		if day > out.LastSeedDay {
			out.LastSeedDay = day
		}
	})
	for _, q := range out.Seeds {
		for _, days := range []uint32{1, 3, 7, 14, 30} {
			key := followupKey(q.QuoteId, days*86400)
			at, ok, err := s.done.Get(key)
			if err != nil {
				return out, err
			}
			if ok && !at.Before(cut) {
				out.Done[key] = true
			}
		}
	}
	return out, s.err
}
func (s *runtimeState) prune(now time.Time) error {
	cut := now.Add(-31 * 24 * time.Hour)
	keys, rows, err := s.done.Scan(s.cleanupAfter, 128, false)
	if err != nil {
		return err
	}
	for i, key := range keys {
		if rows[i].Before(cut) {
			if err = s.done.Delete(key); err != nil {
				return err
			}
		}
	}
	if len(keys) == 0 {
		s.cleanupAfter = ""
	} else {
		s.cleanupAfter = keys[len(keys)-1]
	}
	return s.Gas.prune(now)
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
	s.SeedRoot, more, err = s.seeds.Snapshot()
	if err != nil {
		return nil, err
	}
	refs = append(refs, more...)
	s.DoneRoot, more, err = s.done.Snapshot()
	if err != nil {
		return nil, err
	}
	refs = append(refs, more...)
	more, err = s.Gas.references()
	return append(append(refs, more...), s.Conversion.InputInventory.References()...), err
}
func (s *runtimeState) validate(binding runtimecheckpoint.Binding, c *Collector) error {
	if s.Version != 1 || !s.Verified || s.Binding != binding || s.Manifest != c.Manifest.Hash || s.Cursors == nil || binding.ScopeHash != RuntimeScopeHash(c) {
		return runtimecheckpoint.ErrRecovery
	}
	for i, r := range s.Coverage {
		if r.From > r.To || r.To == math.MaxUint64 || i > 0 && r.From <= s.Coverage[i-1].To+1 {
			return runtimecheckpoint.ErrRecovery
		}
	}
	for segment, p := range s.Cursors {
		if p.Manifest != s.Manifest || p.Start == 0 || p.Next != s.continuous(p.Start) || p.RangeSize == 0 || segment != 0 && p.Start != segment {
			return fmt.Errorf("lst runtime cursor %d: %w", segment, runtimecheckpoint.ErrRecovery)
		}
	}
	for after := ""; ; {
		keys, rows, err := s.active.Scan(after, 128, false)
		if err != nil {
			return err
		}
		if len(keys) == 0 {
			break
		}
		for i, cap := range rows {
			if runtimeCaptureKey(cap) != keys[i] || cap.ManifestHash != s.Manifest || !cap.Committed || cap.CaptureKind != "market" || cap.ToBlock == nil || cap.Finality == "finalized" || cap.Finality == "orphaned" {
				return runtimecheckpoint.ErrRecovery
			}
		}
		after = keys[len(keys)-1]
	}
	if _, ok := s.Cursors[c.LiveLogsFromBlock]; !ok {
		return errors.New("lst_runtime_requires_verified_original_cursor")
	}
	return s.err
}
