package funding

import (
	"context"
	"fmt"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/model"
)

const StartupBackfillWindow = 24 * time.Hour

type ConfirmationScheduler interface {
	Schedule(context.Context, model.Instrument, time.Time) error
}

func ScheduleStartupBackfill(ctx context.Context, pending []model.PendingFundingConfirmation, instruments []model.Instrument, workers map[string]ConfirmationScheduler) error {
	byID := make(map[uint32]model.Instrument, len(instruments))
	for _, instrument := range instruments {
		if instrument.ID != 0 && instrument.MarketType == model.MarketPerpetual {
			byID[instrument.ID] = instrument
		}
	}
	for _, item := range pending {
		if err := item.Validate(); err != nil {
			return err
		}
		instrument, configured := byID[item.InstrumentID]
		if !configured {
			continue
		}
		worker, available := workers[instrument.Exchange]
		if !available || worker == nil {
			return fmt.Errorf("no funding confirmation worker for exchange %q", instrument.Exchange)
		}
		if err := worker.Schedule(ctx, instrument, item.FundingTime.UTC()); err != nil {
			return fmt.Errorf("schedule startup funding confirmation for instrument %d: %w", item.InstrumentID, err)
		}
	}
	return nil
}
