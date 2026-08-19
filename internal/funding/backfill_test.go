package funding

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/model"
)

type backfillCall struct {
	instrument model.Instrument
	target     time.Time
}

type backfillScheduler struct {
	calls []backfillCall
	err   error
}

func (s *backfillScheduler) Schedule(_ context.Context, instrument model.Instrument, target time.Time) error {
	s.calls = append(s.calls, backfillCall{instrument: instrument, target: target})
	return s.err
}

func TestScheduleStartupBackfillRoutesConfiguredPerpetualsWithExactTime(t *testing.T) {
	binance := fundingInstrument(1, "Binance")
	okx := fundingInstrument(2, "OKX")
	targetA := time.UnixMilli(1787097600123).UTC()
	targetB := time.UnixMilli(1787126400456).UTC()
	binanceWorker := &backfillScheduler{}
	okxWorker := &backfillScheduler{}
	err := ScheduleStartupBackfill(context.Background(), []model.PendingFundingConfirmation{
		{InstrumentID: binance.ID, FundingTime: targetA},
		{InstrumentID: 999, FundingTime: targetA},
		{InstrumentID: okx.ID, FundingTime: targetB},
	}, []model.Instrument{binance, okx}, map[string]ConfirmationScheduler{"Binance": binanceWorker, "OKX": okxWorker})
	if err != nil {
		t.Fatal(err)
	}
	if len(binanceWorker.calls) != 1 || binanceWorker.calls[0].target.UnixMilli() != targetA.UnixMilli() {
		t.Fatalf("Binance calls=%+v", binanceWorker.calls)
	}
	if len(okxWorker.calls) != 1 || okxWorker.calls[0].target.UnixMilli() != targetB.UnixMilli() {
		t.Fatalf("OKX calls=%+v", okxWorker.calls)
	}
}

func TestScheduleStartupBackfillPropagatesWorkerFailure(t *testing.T) {
	instrument := fundingInstrument(1, "Binance")
	want := errors.New("queue unavailable")
	err := ScheduleStartupBackfill(context.Background(), []model.PendingFundingConfirmation{{InstrumentID: instrument.ID, FundingTime: time.UnixMilli(1787097600123).UTC()}}, []model.Instrument{instrument}, map[string]ConfirmationScheduler{"Binance": &backfillScheduler{err: want}})
	if !errors.Is(err, want) {
		t.Fatalf("error=%v, want %v", err, want)
	}
}
