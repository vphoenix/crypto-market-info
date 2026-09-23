package app

import (
	"context"
	"errors"
	"github.com/vphoenix/crypto-market-info/internal/config"
	"github.com/vphoenix/crypto-market-info/internal/dex"
	"io"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"
)

func TestDEXStaleHeadRecoversButFinalizedConflictPauses(t *testing.T) {
	checkpoint := dex.Block{Anchor: dex.Anchor{Number: 100, Hash: dex.ObjectHash(uint64(100))}}
	old := dex.Block{Anchor: dex.Anchor{Number: 99, Hash: dex.ObjectHash(uint64(99))}}
	if e := checkDEXHead(old, &checkpoint, &checkpoint); e == nil || errors.Is(e, errDEXFinality) {
		t.Fatal("stale lower head treated as finality contradiction", e)
	}
	recovered := dex.Block{Anchor: dex.Anchor{Number: 101, Hash: dex.ObjectHash(uint64(101))}}
	if e := checkDEXHead(recovered, &checkpoint, &checkpoint); e != nil {
		t.Fatal("recovery prevented", e)
	}
	conflict := checkpoint
	conflict.Hash = dex.Digest([]byte("other chain"))
	if e := checkDEXHead(conflict, &checkpoint, &checkpoint); !errors.Is(e, errDEXFinality) {
		t.Fatal("same-height contradiction accepted", e)
	}
}
func TestDEXSetupFailureDoesNotStopOtherCollectors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	otherStarted := make(chan struct{})
	done := make(chan error, 1)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cfg := config.Config{DEXRPCURL: "://invalid", DEXEvidenceDir: t.TempDir()}
	go func() {
		done <- runComponents(ctx, []component{{name: "DEX", run: func(ctx context.Context) error { return runDEX(ctx, cfg, nil, logger) }}, {name: "existing CEX", run: func(ctx context.Context) error { close(otherStarted); <-ctx.Done(); return nil }}})
	}()
	<-otherStarted
	select {
	case e := <-done:
		t.Fatal("DEX error stopped other collector", e)
	case <-time.After(30 * time.Millisecond):
	}
	cancel()
	select {
	case e := <-done:
		if e != nil {
			t.Fatal(e)
		}
	case <-time.After(time.Second):
		t.Fatal("DEX branch did not stop")
	}
}

func TestDEXLivePollingContinuesWhileMaintenanceBlocks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered := make(chan struct{})
	var samples atomic.Int32
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	done := make(chan error, 1)
	go func() {
		done <- runDEXLoops(ctx, logger, func(context.Context) error {
			samples.Add(1)
			return nil
		}, func(ctx context.Context) error {
			close(entered)
			<-ctx.Done()
			return ctx.Err()
		}, 2*time.Millisecond)
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("maintenance did not start")
	}
	before := samples.Load()
	deadline := time.After(time.Second)
	for samples.Load() < before+3 {
		select {
		case <-deadline:
			t.Fatal("blocked maintenance stopped live polling")
		case <-time.After(time.Millisecond):
		}
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("DEX loops did not stop after cancellation")
	}
}

func TestDEXMaintenanceFinalityConflictStopsLiveLoop(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	err := runDEXLoops(context.Background(), logger, func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}, func(context.Context) error { return errDEXFinality }, time.Millisecond)
	if !errors.Is(err, errDEXFinality) {
		t.Fatalf("finalized contradiction was not propagated: %v", err)
	}
}

func TestDEXTransientLiveFailureRetriesAtPollInterval(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var calls atomic.Int32
	retried := make(chan struct{})
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	done := make(chan error, 1)
	go func() {
		done <- runDEXLoops(ctx, logger, func(ctx context.Context) error {
			if calls.Add(1) == 1 {
				return errors.New("temporary RPC failure")
			}
			close(retried)
			<-ctx.Done()
			return ctx.Err()
		}, func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		}, 2*time.Millisecond)
	}()
	select {
	case <-retried:
	case <-time.After(time.Second):
		t.Fatal("live RPC failure added a long delay before retry")
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
