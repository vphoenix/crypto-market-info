package across

import (
	"fmt"
	"maps"
	"sort"
	"strings"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint"
)

func receiptScheduleKey(id string, r runtimeReceipt) string {
	urgent := 1
	if r.Refund && !r.Transfers {
		urgent = 0
	}
	var stamp int64
	if !r.LastAttempt.IsZero() {
		stamp = r.LastAttempt.UnixNano()
	}
	return fmt.Sprintf("%d/%020d/%s", urgent, stamp, id)
}
func (s *runtimeState) receiptGet(id string) (runtimeReceipt, bool) {
	if s.receiptErr != nil {
		return runtimeReceipt{}, false
	}
	if s.receiptPages == nil {
		r, ok := s.Receipts[id]
		r.Parents = maps.Clone(r.Parents)
		return r, ok
	}
	r, ok, err := s.receiptPages.Get(id)
	if err != nil {
		s.receiptErr = err
	}
	r.Parents = maps.Clone(r.Parents)
	return r, ok
}
func (s *runtimeState) receiptSet(id string, r runtimeReceipt) {
	if s.receiptErr != nil {
		return
	}
	old, exists := s.receiptGet(id)
	if s.receiptPages == nil {
		s.Receipts[id] = r
		return
	}
	if exists && old.Pending {
		s.receiptErr = s.receiptSchedule.Delete(receiptScheduleKey(id, old))
		if s.receiptErr != nil {
			return
		}
	}
	if r.Pending {
		s.receiptErr = s.receiptSchedule.Set(receiptScheduleKey(id, r), id)
		if s.receiptErr != nil {
			return
		}
	}
	s.receiptErr = s.receiptPages.Set(id, r)
	for parent := range old.Parents {
		if parent != "" {
			delete(s.receiptParents[parent], id)
			if len(s.receiptParents[parent]) == 0 {
				delete(s.receiptParents, parent)
			}
		}
	}
	for parent := range r.Parents {
		if parent != "" {
			if s.receiptParents[parent] == nil {
				s.receiptParents[parent] = map[string]bool{}
			}
			s.receiptParents[parent][id] = true
		}
	}
}
func (s *runtimeState) receiptDelete(id string) {
	if s.receiptErr != nil {
		return
	}
	old, ok := s.receiptGet(id)
	if !ok {
		return
	}
	if s.receiptPages == nil {
		delete(s.Receipts, id)
		return
	}
	if old.Pending {
		s.receiptErr = s.receiptSchedule.Delete(receiptScheduleKey(id, old))
		if s.receiptErr != nil {
			return
		}
	}
	s.receiptErr = s.receiptPages.Delete(id)
	for parent := range old.Parents {
		delete(s.receiptParents[parent], id)
		if len(s.receiptParents[parent]) == 0 {
			delete(s.receiptParents, parent)
		}
	}
}
func (s *runtimeState) eachReceipt(fn func(string, runtimeReceipt)) {
	if s.receiptErr != nil {
		return
	}
	if s.receiptPages == nil {
		for id, r := range s.Receipts {
			r.Parents = maps.Clone(r.Parents)
			fn(id, r)
		}
		return
	}
	after := ""
	for s.receiptErr == nil {
		keys, values, err := s.receiptPages.Scan(after, 128, false)
		if err != nil {
			s.receiptErr = err
			return
		}
		if len(keys) == 0 {
			return
		}
		for i, id := range keys {
			r := values[i]
			r.Parents = maps.Clone(r.Parents)
			fn(id, r)
			if s.receiptErr != nil {
				return
			}
		}
		after = keys[len(keys)-1]
	}
}
func (s *runtimeState) parentReceipts(parent string) []string {
	var ids []string
	if s.receiptPages == nil {
		for id, r := range s.Receipts {
			if _, ok := r.Parents[parent]; ok {
				ids = append(ids, id)
			}
		}
	} else {
		for id := range s.receiptParents[parent] {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}
func (s *runtimeState) receiptCount() int {
	if s.receiptPages != nil {
		return int(s.receiptPages.Len())
	}
	return len(s.Receipts)
}

func (s *runtimeState) enableReceiptPages(files *runtimecheckpoint.Store) error {
	if s.receiptPages != nil {
		return s.receiptErr
	}
	pages, err := runtimecheckpoint.OpenPagedMap[runtimeReceipt](files, s.ReceiptRoot, 2<<20, func(r runtimeReceipt) bool { return r.Pending || r.FeeComplete }, func(r runtimeReceipt) time.Time {
		if r.Pending {
			return r.FirstObserved
		}
		return time.Time{}
	})
	if err != nil {
		return err
	}
	schedule, err := runtimecheckpoint.OpenPagedMap[string](files, s.ReceiptScheduleRoot, 512<<10, func(string) bool { return true })
	if err != nil {
		return err
	}
	if pages.Len() != 0 && len(s.Receipts) != 0 {
		return runtimecheckpoint.ErrRecovery
	}
	s.receiptPages, s.receiptSchedule = pages, schedule
	s.receiptParents = map[string]map[string]bool{}
	keys := make([]string, 0, len(s.Receipts))
	for key := range s.Receipts {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		s.receiptSet(key, s.Receipts[key])
		if s.receiptErr != nil {
			return s.receiptErr
		}
	}
	s.Receipts = map[string]runtimeReceipt{}
	// Only true unfinished/uncertain state is paged here, never ended history.
	// Rebuild the small branch-dependency index and verify scheduling identity.
	var pending uint64
	s.eachReceipt(func(id string, r runtimeReceipt) {
		for parent := range r.Parents {
			if parent != "" {
				if s.receiptParents[parent] == nil {
					s.receiptParents[parent] = map[string]bool{}
				}
				s.receiptParents[parent][id] = true
			}
		}
		if r.Pending {
			pending++
			key := receiptScheduleKey(id, r)
			actual, ok, e := schedule.Get(key)
			if e != nil {
				s.receiptErr = e
			} else if !ok || actual != id {
				s.receiptErr = runtimecheckpoint.ErrRecovery
			}
		}
	})
	if s.receiptErr != nil {
		return s.receiptErr
	}
	if schedule.Len() != pending {
		return runtimecheckpoint.ErrRecovery
	}
	return nil
}
func (s *runtimeState) receiptReferences() ([]runtimecheckpoint.Reference, error) {
	if s.receiptErr != nil {
		return nil, s.receiptErr
	}
	if s.receiptPages == nil {
		return nil, nil
	}
	root, refs, err := s.receiptPages.Snapshot()
	if err != nil {
		return nil, err
	}
	schedule, more, err := s.receiptSchedule.Snapshot()
	if err != nil {
		return nil, err
	}
	s.ReceiptRoot, s.ReceiptScheduleRoot = root, schedule
	return append(append(refs, more...), s.Conversion.InputInventory.References()...), nil
}

// Retain the original urgent/oldest-attempt/identity ordering while loading
// only schedulable work. A large unknown-fee backlog stays in identity pages.
func (s *runtimeState) selectReceipt(readers map[uint64]*Reader, at time.Time) (string, runtimeReceipt, bool) {
	if s.receiptPages == nil {
		key := ""
		var chosen runtimeReceipt
		for id, r := range s.Receipts {
			if !r.Pending || readers[r.Task.ChainID] == nil || !r.LastAttempt.IsZero() && at.Sub(r.LastAttempt) < time.Minute {
				continue
			}
			if key == "" || receiptScheduleKey(id, r) < receiptScheduleKey(key, chosen) {
				key, chosen = id, r
			}
		}
		return key, chosen, key != ""
	}
	for _, prefix := range []string{"0/", "1/"} {
		after := prefix
		for s.receiptErr == nil {
			keys, ids, err := s.receiptSchedule.Scan(after, 1, false)
			if err != nil {
				s.receiptErr = err
				break
			}
			if len(keys) == 0 || !strings.HasPrefix(keys[0], prefix) {
				break
			}
			id := ids[0]
			r, ok := s.receiptGet(id)
			if !ok || !r.Pending || receiptScheduleKey(id, r) != keys[0] {
				s.receiptErr = runtimecheckpoint.ErrRecovery
				break
			}
			if !r.LastAttempt.IsZero() && at.Sub(r.LastAttempt) < time.Minute {
				break
			}
			if readers[r.Task.ChainID] != nil {
				return id, r, true
			}
			after = keys[0]
		}
	}
	return "", runtimeReceipt{}, false
}

// Completed receipts are reclaimed incrementally; uncertain fee records remain
// recoverable. Selection skips all pages with no pending or complete receipts.
func (s *runtimeState) pruneReceipts() bool {
	changed := false
	prune := func(id string, r runtimeReceipt) {
		if !r.Pending && r.FeeComplete && s.finalizedDecodedBlock(r.Task.ChainID, r.Block) {
			s.receiptDelete(id)
			delete(s.ReceiptAttempts, id)
			changed = true
		}
	}
	if s.receiptPages == nil {
		s.eachReceipt(prune)
		return changed
	}
	keys, values, err := s.receiptPages.Scan(s.ReceiptCleanupAfter, 128, true)
	if err != nil {
		s.receiptErr = err
		return false
	}
	if len(keys) == 0 {
		s.ReceiptCleanupAfter = ""
		return false
	}
	for i, id := range keys {
		prune(id, values[i])
	}
	s.ReceiptCleanupAfter = keys[len(keys)-1]
	return changed
}
