package app

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/exchange"
	"github.com/vphoenix/crypto-market-info/internal/funding"
	"github.com/vphoenix/crypto-market-info/internal/model"
)

// One parent-lifetime timer owns paired-market hourly writes. Generation
// replacement must never cancel a partially persisted hour or skip an hour
// merely because the new generation becomes effective exactly on that hour.
type pairedFundingScheduler struct {
	mu               sync.Mutex
	current, pending *funding.Scheduler
	effective        time.Time
	sink             funding.Sink
	logger           *slog.Logger
	now              func() time.Time
}

func (s *pairedFundingScheduler) schedule(instruments []model.Instrument, estimates *funding.EstimateStore, effective time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC()
	if s.now != nil {
		now = s.now().UTC()
	}
	// A generation can become active and be replaced before the next hour.
	// Preserve that active owner before replacing a still-pending slot.
	if s.pending != nil && !now.Before(s.effective) {
		s.current = s.pending
	}
	s.pending = &funding.Scheduler{Instruments: append([]model.Instrument(nil), instruments...), Estimates: estimates, Sink: s.sink, Logger: s.logger}
	s.effective = effective.UTC()
}
func (s *pairedFundingScheduler) collectHour(ctx context.Context, hour time.Time) {
	s.mu.Lock()
	if s.pending != nil && !hour.Before(s.effective) {
		s.current = s.pending
		s.pending = nil
	}
	current := s.current
	s.mu.Unlock()
	if current != nil && len(current.Instruments) > 0 {
		current.CollectHour(ctx, hour)
	}
}
func (s *pairedFundingScheduler) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		hour := time.Now().UTC().Truncate(time.Hour).Add(time.Hour)
		if !exchange.Wait(ctx, time.Until(hour)) {
			return nil
		}
		s.collectHour(ctx, hour)
	}
	return nil
}
