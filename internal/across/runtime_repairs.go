package across

import (
	"context"
	"errors"
	"math"
	"sort"
	"strings"
	"time"
)

type runtimePartialChoice struct {
	Found         bool
	Window        HistoryRange
	Key, Finality string
	Retry         runtimeRetry
}

// Enumerate only actual unresolved partials, in the same capture/time/member
// order as the legacy function. Two bounded passes select cursor % count
// without constructing a slice proportional to the number of missing windows.
func (s *runtimeState) selectPartial(readers map[uint64]*Reader, cursor int, at time.Time) runtimePartialChoice {
	visit := func(fn func(runtimePartialChoice)) {
		process := func(cap Capture) {
			if !runtimeRaw(cap) || cap.Status != "partial" || readers[cap.ChainId] == nil || strings.Contains(cap.Reason, repairAbandonedMarker) {
				return
			}
			decoded := s.coverage(cap.ChainId, true)
			for from := *cap.FromBlock; from <= *cap.ToBlock; {
				window, ok := historyUncovered(from, *cap.ToBlock, decoded)
				if !ok {
					break
				}
				window.ChainID = cap.ChainId
				key := partialKey(cap, window)
				retry := s.retryGet(key)
				if !retry.Attempted && !at.Before(retry.At) {
					finality := "head"
					if cap.Finality == "finalized" {
						finality = "finalized"
					}
					fn(runtimePartialChoice{true, window, key, finality, retry})
				}
				if window.To == math.MaxUint64 {
					break
				}
				from = window.To + 1
			}
		}
		if s.partialPages != nil {
			s.eachCapture(true, process)
			return
		}
		rows := make([]Capture, 0, len(s.Partial))
		for _, cap := range s.Partial {
			rows = append(rows, cap)
		}
		sort.Slice(rows, func(i, j int) bool { return captureOrder(rows[i], true) < captureOrder(rows[j], true) })
		for _, cap := range rows {
			process(cap)
		}
	}
	count := 0
	visit(func(runtimePartialChoice) { count++ })
	if count == 0 || s.captureErr != nil {
		return runtimePartialChoice{}
	}
	index := cursor % count
	if index < 0 {
		index = 0
	}
	position := 0
	var selected runtimePartialChoice
	visit(func(choice runtimePartialChoice) {
		if position == index {
			selected = choice
		}
		position++
	})
	return selected
}
func (c *Collector) runtimeRepairPartialStep(ctx context.Context, networkContext ...context.Context) error {
	if c.partialRepair == nil {
		c.partialRepair = &partialRepairState{attempted: map[string]bool{}, retryAt: map[string]time.Time{}, failures: map[string]uint{}}
	}
	var choice runtimePartialChoice
	err := c.runtime.view(ctx, func(s *runtimeState) (any, error) {
		v := s.selectPartial(c.Readers, c.partialRepair.cursor, Now())
		return v, s.ioError()
	}, &choice)
	if err != nil || !choice.Found {
		return err
	}
	c.partialRepair.cursor++
	c.partialRepair.attempted = map[string]bool{choice.Key: choice.Retry.Attempted}
	c.partialRepair.retryAt = map[string]time.Time{choice.Key: choice.Retry.At}
	c.partialRepair.failures = map[string]uint{choice.Key: choice.Retry.Failures}
	c.runtimePartial = &runtimePartialTransition{Window: choice.Window, Key: choice.Key, Failures: choice.Retry.Failures}
	defer func() { c.runtimePartial = nil }()
	err = c.runPartialCandidate(ctx, partialCandidate{choice.Window, choice.Key, choice.Finality}, networkContext...)
	if err != nil {
		return err
	}
	// At most the selected parent and its new attempt. Committed children are
	// already recoverable from the frozen operation if this callback is lost.
	delta := map[string]runtimeRetry{}
	for key, value := range c.partialRepair.attempted {
		r := delta[key]
		r.Window = choice.Window
		r.Attempted = value
		delta[key] = r
	}
	for key, value := range c.partialRepair.retryAt {
		r := delta[key]
		r.Window = choice.Window
		r.At = value
		delta[key] = r
	}
	for key, value := range c.partialRepair.failures {
		r := delta[key]
		r.Window = choice.Window
		r.Failures = value
		delta[key] = r
	}
	return c.runtime.change(ctx, func(s *runtimeState) (any, error) {
		for key, r := range delta {
			s.retrySet(key, r)
		}
		return len(delta) > 0, s.ioError()
	}, false)
}

func (c *Collector) runtimeMarkInvalidation(ctx context.Context, chain uint64, bad []uint64, reason string) error {
	job := runtimeInvalidation{Chain: chain, Bad: append([]uint64(nil), bad...), Reason: reason}
	id := ID(job)
	err := c.runtime.change(ctx, func(s *runtimeState) (any, error) {
		ch := s.Chains[chain]
		if ch == nil {
			return false, errors.New("runtime_reorg_chain")
		}
		for _, n := range bad {
			if ch.Anchor != nil && n <= *ch.Anchor.ToBlock {
				return false, errors.New("finalized_hash_conflict")
			}
		}
		if _, ok := s.Invalidations[id]; ok {
			return false, nil
		}
		s.Invalidations[id] = job
		s.rebuildCoverage(chain)
		return true, s.ioError()
	}, true)
	if err != nil {
		return err
	}
	return c.runtimeResumeInvalidations(ctx)
}

// The discovered reorg and original proof reason are durable before revising
// its first capture. A crash between pages resumes the remaining identities
// from local state, without another RPC or a scan of historical SQL rows.
func (c *Collector) runtimeResumeInvalidations(ctx context.Context) error {
	for {
		var jobs map[string]runtimeInvalidation
		if err := c.runtime.view(ctx, func(s *runtimeState) (any, error) { return s.Invalidations, nil }, &jobs); err != nil {
			return err
		}
		if len(jobs) == 0 {
			return nil
		}
		ids := make([]string, 0, len(jobs))
		for id := range jobs {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		id := ids[0]
		job := jobs[id]
		for {
			var page []Capture
			err := c.runtime.view(ctx, func(s *runtimeState) (any, error) {
				rows := s.affectedCaptures(job, 24)
				return rows, s.ioError()
			}, &page)
			if err != nil {
				return err
			}
			if len(page) == 0 {
				break
			}
			for _, cap := range page {
				expected := cap.Revision
				cap.Revision++
				cap.Canonical = false
				cap.Finality = "orphaned"
				cap.Reason = "reorg_invalidated; " + job.Reason
				actual, err := c.runtime.writeRevision(ctx, c.Store, expected, cap)
				if err != nil {
					return err
				}
				for i := range c.captures {
					if c.captures[i].CaptureId == actual.CaptureId {
						c.captures[i] = actual
					}
				}
			}
		}
		if err := c.runtime.change(ctx, func(s *runtimeState) (any, error) {
			delete(s.Invalidations, id)
			s.rebuildCoverage(job.Chain)
			return true, s.ioError()
		}, true); err != nil {
			return err
		}
	}
}
