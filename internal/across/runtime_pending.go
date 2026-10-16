package across

import (
	"errors"
	"sort"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint"
)

type runtimePartialTransition struct {
	Window   HistoryRange
	Key      string
	Failures uint
}

func (s *runtimeState) retryGet(key string) runtimeRetry {
	if s.pendingErr != nil {
		return runtimeRetry{}
	}
	if s.retryPages == nil {
		return s.PartialRetry[key]
	}
	v, _, err := s.retryPages.Get(key)
	if err != nil {
		s.pendingErr = err
	}
	return v
}
func (s *runtimeState) retrySet(key string, r runtimeRetry) {
	if s.pendingErr != nil {
		return
	}
	if r.Window.ChainID != 0 && firstUncovered(r.Window.From, r.Window.To, s.coverage(r.Window.ChainID, true)) > r.Window.To {
		if s.retryPages == nil {
			delete(s.PartialRetry, key)
		} else {
			s.pendingErr = s.retryPages.Delete(key)
		}
		return
	}
	if s.retryPages == nil {
		s.PartialRetry[key] = r
	} else {
		s.pendingErr = s.retryPages.Set(key, r)
	}
}
func (s *runtimeState) eachRetry(fn func(string, runtimeRetry)) {
	if s.retryPages == nil {
		for key, r := range s.PartialRetry {
			fn(key, r)
		}
		return
	}
	after := ""
	for s.pendingErr == nil {
		keys, values, err := s.retryPages.Scan(after, 128, false)
		if err != nil {
			s.pendingErr = err
			return
		}
		if len(keys) == 0 {
			return
		}
		for i, key := range keys {
			fn(key, values[i])
		}
		after = keys[len(keys)-1]
	}
}
func (s *runtimeState) skippedSet(key string, p pendingOrder) {
	if s.pendingErr != nil {
		return
	}
	if s.skippedPages == nil {
		s.Skipped[key] = p
	} else {
		s.pendingErr = s.skippedPages.Set(key, p)
	}
}
func (s *runtimeState) skippedDelete(key string) {
	if s.pendingErr != nil {
		return
	}
	if s.skippedPages == nil {
		delete(s.Skipped, key)
	} else {
		s.pendingErr = s.skippedPages.Delete(key)
	}
}
func (s *runtimeState) skippedPage(after string) ([]string, []pendingOrder) {
	if s.skippedPages != nil {
		keys, values, err := s.skippedPages.Scan(after, 128, false)
		if err != nil {
			s.pendingErr = err
		}
		return keys, values
	}
	keys := make([]string, 0, len(s.Skipped))
	for key := range s.Skipped {
		if key > after {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	if len(keys) > 128 {
		keys = keys[:128]
	}
	values := make([]pendingOrder, len(keys))
	for i, key := range keys {
		values[i] = s.Skipped[key]
	}
	return keys, values
}
func (s *runtimeState) enablePendingPages(files *runtimecheckpoint.Store) error {
	if s.retryPages != nil {
		return s.pendingErr
	}
	var err error
	s.retryPages, err = runtimecheckpoint.OpenPagedMap[runtimeRetry](files, s.RetryRoot, 512<<10, func(runtimeRetry) bool { return true })
	if err != nil {
		return err
	}
	s.skippedPages, err = runtimecheckpoint.OpenPagedMap[pendingOrder](files, s.SkippedRoot, 512<<10, func(pendingOrder) bool { return true }, func(p pendingOrder) time.Time { return p.Deposit.AvailableAt })
	if err != nil {
		return err
	}
	if s.retryPages.Len() != 0 && len(s.PartialRetry) != 0 || s.skippedPages.Len() != 0 && len(s.Skipped) != 0 {
		return runtimecheckpoint.ErrRecovery
	}
	for key, r := range s.PartialRetry {
		s.retrySet(key, r)
	}
	keys := make([]string, 0, len(s.Skipped))
	for key := range s.Skipped {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		s.skippedSet(key, s.Skipped[key])
	}
	s.PartialRetry = map[string]runtimeRetry{}
	s.Skipped = map[string]pendingOrder{}
	return s.pendingErr
}
func (s *runtimeState) pendingReferences() ([]runtimecheckpoint.Reference, error) {
	if s.pendingErr != nil {
		return nil, s.pendingErr
	}
	if s.retryPages == nil {
		return nil, nil
	}
	r, refs, err := s.retryPages.Snapshot()
	if err != nil {
		return nil, err
	}
	p, more, err := s.skippedPages.Snapshot()
	if err != nil {
		return nil, err
	}
	s.RetryRoot, s.SkippedRoot = r, p
	return append(refs, more...), nil
}
func (s *runtimeState) pruneRetries() bool {
	if s.retryPages == nil {
		return false
	}
	keys, values, err := s.retryPages.Scan(s.RetryCleanupAfter, 128, false)
	if err != nil {
		s.pendingErr = err
		return false
	}
	if len(keys) == 0 {
		s.RetryCleanupAfter = ""
		return false
	}
	changed := false
	for i, key := range keys {
		r := values[i]
		if r.Window.ChainID != 0 && firstUncovered(r.Window.From, r.Window.To, s.coverage(r.Window.ChainID, true)) > r.Window.To {
			s.pendingErr = s.retryPages.Delete(key)
			changed = true
			if s.pendingErr != nil {
				return changed
			}
		}
	}
	s.RetryCleanupAfter = keys[len(keys)-1]
	return changed
}
func (s *runtimeState) partialCommitted(meta runtimePartialTransition, cap Capture) {
	r := runtimeRetry{Window: meta.Window, Failures: meta.Failures}
	if cap.Status == "error" {
		if historyFatal(errors.New(cap.Reason)) {
			return
		}
		delay, _ := historyRetryDelay(errors.New(cap.Reason), meta.Failures)
		r.Failures++
		r.At = Now().Add(delay)
		s.retrySet(meta.Key, r)
		return
	}
	if partialRetryable(cap) {
		delay, _ := historyRetryDelay(errors.New(cap.Reason), meta.Failures)
		r.Failures++
		r.At = Now().Add(delay)
	} else {
		r.Attempted = true
	}
	s.retrySet(meta.Key, r)
	if key := partialKey(cap, meta.Window); key != meta.Key {
		r.Failures = 0
		s.retrySet(key, r)
	}
}
