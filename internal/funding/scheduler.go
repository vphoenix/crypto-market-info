package funding

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/model"
)

type Sink interface {
	UpsertFundingRate(context.Context, model.FundingRate) error
}

type Scheduler struct {
	Instruments    []model.Instrument
	Estimates      *EstimateStore
	Sink           Sink
	MaxEstimateAge time.Duration
	Logger         *slog.Logger
}

func (s *Scheduler) Run(ctx context.Context) error {
	if err := s.defaults(); err != nil {
		return err
	}
	for {
		now := time.Now().UTC()
		nextHour := now.Truncate(time.Hour).Add(time.Hour)
		if !wait(ctx, time.Until(nextHour)) {
			return nil
		}
		s.CollectHour(ctx, nextHour)
	}
}

func (s *Scheduler) defaults() error {
	if s.Sink == nil || s.Estimates == nil || len(s.Instruments) == 0 {
		return fmt.Errorf("funding scheduler requires instruments, estimates and sink")
	}
	if s.MaxEstimateAge <= 0 {
		s.MaxEstimateAge = 2 * time.Minute
	}
	if s.Logger == nil {
		s.Logger = slog.Default()
	}
	return nil
}

func (s *Scheduler) CollectHour(ctx context.Context, hour time.Time) {
	if s.Logger == nil {
		s.Logger = slog.Default()
	}
	if s.MaxEstimateAge <= 0 {
		s.MaxEstimateAge = 2 * time.Minute
	}
	hour = hour.UTC().Truncate(time.Hour)
	for _, instrument := range s.Instruments {
		estimate, found := s.Estimates.At(instrument.ID, hour, s.MaxEstimateAge)
		if !found {
			s.Logger.Warn("fresh funding estimate unavailable", "exchange", instrument.Exchange, "symbol", instrument.ExchangeSymbol, "hour", hour)
			continue
		}
		rate := model.FundingRate{
			InstrumentID: instrument.ID,
			HourTime:     hour,
			FundingTime:  estimate.FundingTime,
			Rate:         estimate.Rate,
			IsActual:     false,
		}
		if err := s.Sink.UpsertFundingRate(ctx, rate); err != nil {
			s.Logger.Error("estimated funding write failed", "instrument_id", instrument.ID, "hour", hour, "error", err)
		}
	}
}

func wait(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
