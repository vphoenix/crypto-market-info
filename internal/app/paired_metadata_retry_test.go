package app

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/orderbook"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
)

type pairedTransient struct{}

func (pairedTransient) Error() string   { return "lock wait timeout" }
func (pairedTransient) Temporary() bool { return true }

func TestPairedMetadataRetryPreservesSamplingAndFrozenIdentity(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	book, _ := orderbook.New(1, 400)
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := book.ApplySnapshot(model.BookSnapshot{InstrumentID: 1, SourceTime: at, Sequence: 1,
		Bids: []model.Level{{PriceTick: 100, QtyLot: 1}}, Asks: []model.Level{{PriceTick: 101, QtyLot: 1}}}); err != nil {
		t.Fatal(err)
	}
	frozen := model.OKXPairObservation{ID: uuid.New(), ObservedAt: at, EffectiveMinute: at.Add(time.Minute)}
	identity := frozen.ID
	calls := 0
	var delays []time.Duration
	err := retryPairedMetadataWait(ctx, logger, "catalog fixture", func(attempt context.Context) error {
		calls++
		if frozen.ID != identity || !frozen.ObservedAt.Equal(at) {
			t.Fatal("retry fabricated observation")
		}
		if _, ok := attempt.Deadline(); !ok {
			t.Fatal("unbounded attempt")
		}
		if calls < 7 {
			return pairedTransient{}
		}
		return nil
	}, func(waitCtx context.Context, delay time.Duration) bool {
		delays = append(delays, delay)
		if waitCtx.Err() != nil {
			t.Fatal("sampling context canceled")
		}
		if _, valid := book.Snapshot(5); !valid {
			t.Fatal("active book invalidated by metadata failure")
		}
		return true
	})
	if err != nil || calls != 7 || !reflect.DeepEqual(delays, []time.Duration{5 * time.Second, 10 * time.Second, 20 * time.Second, 40 * time.Second, time.Minute, time.Minute}) {
		t.Fatal(calls, delays, err)
	}
}

func TestPairedMetadataConflictAndCancellationStopRetry(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx, cancel := context.WithCancel(context.Background())
	waits := 0
	err := retryPairedMetadataWait(ctx, logger, "conflict", func(context.Context) error { return contracts.ErrConflict }, func(context.Context, time.Duration) bool { waits++; return true })
	if !errors.Is(err, contracts.ErrConflict) || waits != 0 {
		t.Fatal("conflict retried", err, waits)
	}
	err = retryPairedMetadataWait(ctx, logger, "cancel", func(context.Context) error { return pairedTransient{} }, func(context.Context, time.Duration) bool { cancel(); return false })
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
