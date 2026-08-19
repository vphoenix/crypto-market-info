package funding

import (
	"fmt"
	"sync"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/model"
)

type estimateKey struct {
	instrumentID uint32
	fundingMS    int64
}

// EstimateStore retains the latest observation for each instrument/target
// pair. Keeping the previous target is necessary at a settlement boundary:
// the first post-settlement push already points at the next funding period.
type EstimateStore struct {
	mu        sync.RWMutex
	values    map[estimateKey]model.FundingEstimate
	available map[uint32]bool
	lastPrune time.Time
}

func NewEstimateStore() *EstimateStore {
	return &EstimateStore{values: make(map[estimateKey]model.FundingEstimate), available: make(map[uint32]bool)}
}

func (s *EstimateStore) Put(estimate model.FundingEstimate) error {
	if s == nil {
		return fmt.Errorf("funding estimate store is nil")
	}
	if err := estimate.Validate(); err != nil {
		return err
	}
	estimate.FundingTime = estimate.FundingTime.UTC()
	estimate.SourceTime = estimate.SourceTime.UTC()
	key := estimateKey{instrumentID: estimate.InstrumentID, fundingMS: estimate.FundingTime.UnixMilli()}
	s.mu.Lock()
	defer s.mu.Unlock()
	if previous, exists := s.values[key]; exists && previous.SourceTime.After(estimate.SourceTime) {
		return nil
	}
	s.values[key] = estimate
	s.available[estimate.InstrumentID] = true
	if s.lastPrune.IsZero() || estimate.SourceTime.After(s.lastPrune.Add(time.Hour)) {
		for candidateKey, candidate := range s.values {
			if candidate.SourceTime.Before(estimate.SourceTime.Add(-48 * time.Hour)) {
				delete(s.values, candidateKey)
			}
		}
		s.lastPrune = estimate.SourceTime
	}
	return nil
}

func (s *EstimateStore) MarkUnavailable(instrumentIDs []uint32) {
	if s == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, instrumentID := range instrumentIDs {
		if instrumentID != 0 {
			s.available[instrumentID] = false
		}
	}
}

// At returns the freshest observation that existed at cutoff. At a funding
// boundary, an observation targeting that hour wins over one that already
// points at a later period.
func (s *EstimateStore) At(instrumentID uint32, cutoff time.Time, maxAge time.Duration) (model.FundingEstimate, bool) {
	if s == nil || instrumentID == 0 || maxAge <= 0 {
		return model.FundingEstimate{}, false
	}
	cutoff = cutoff.UTC()
	var latest model.FundingEstimate
	var settlement model.FundingEstimate
	settlementExpected := false
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !s.available[instrumentID] {
		return model.FundingEstimate{}, false
	}
	for key, candidate := range s.values {
		if key.instrumentID != instrumentID {
			continue
		}
		targetsCutoffHour := candidate.FundingTime.UTC().Truncate(time.Hour).Equal(cutoff.Truncate(time.Hour))
		if targetsCutoffHour {
			settlementExpected = true
		}
		if candidate.SourceTime.After(cutoff) {
			continue
		}
		age := cutoff.Sub(candidate.SourceTime)
		if age < 0 || age > maxAge {
			continue
		}
		if latest.SourceTime.IsZero() || candidate.SourceTime.After(latest.SourceTime) {
			latest = candidate
		}
		if targetsCutoffHour &&
			(settlement.SourceTime.IsZero() || candidate.SourceTime.After(settlement.SourceTime)) {
			settlement = candidate
		}
	}
	if settlementExpected {
		return settlement, !settlement.SourceTime.IsZero()
	}
	return latest, !latest.SourceTime.IsZero()
}
