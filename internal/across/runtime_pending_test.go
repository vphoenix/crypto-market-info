package across

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint"
)

func TestRuntimePartialUnknownIsRetriedOncePerProcess(t *testing.T) {
	o, db, dir, binding, _, m := runtimeOwnerFixture(t)
	cap := runtimeCapture(m, "old-unknown", 10, 20, "finalized")
	cap.Status, cap.UnknownEventCount = "partial", 1
	window := HistoryRange{ChainID: cap.ChainId, From: 10, To: 20}
	key := partialKey(cap, window)
	err := o.change(context.Background(), func(s *runtimeState) (any, error) {
		if err := s.capture(cap); err != nil {
			return false, err
		}
		s.retrySet(key, runtimeRetry{Window: window, Attempted: true})
		return true, s.ioError()
	}, true)
	if err != nil {
		t.Fatal(err)
	}
	if err = o.close(); err != nil {
		t.Fatal(err)
	}
	files, err := runtimecheckpoint.Open(dir, binding, 0)
	if err != nil {
		t.Fatal(err)
	}
	next, err := newRuntimeOwner(context.Background(), files, m, db, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer next.close()
	var got runtimePartialChoice
	err = next.view(context.Background(), func(s *runtimeState) (any, error) {
		return s.selectPartial(map[uint64]*Reader{cap.ChainId: {}}, 0, Now()), s.ioError()
	}, &got)
	if err != nil || !got.Found || got.Key != key || got.Retry.Attempted {
		t.Fatal("old unresolved work was suppressed after restart", got, err)
	}
	if db.historyReads != 0 {
		t.Fatal("restart scanned history")
	}
}

func TestRuntimePartialChildSurvivesLostCommitNotification(t *testing.T) {
	o, db, dir, binding, b, m := runtimeOwnerFixture(t)
	b.Capture.Status, b.Capture.Reason = "partial", "rpc_rate_limited"
	Seal(&b)
	window := HistoryRange{ChainID: b.Capture.ChainId, From: *b.Capture.FromBlock, To: *b.Capture.ToBlock}
	meta := runtimePartialTransition{Window: window, Key: "old-attempt", Failures: 2}
	db.loseReturn = true
	if err := o.writeBatchEffects(context.Background(), db, runtimeWrite{Version: 1, Batch: &b, Partial: &meta}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if err := o.close(); err != nil {
		t.Fatal(err)
	}
	files, err := runtimecheckpoint.Open(dir, binding, 0)
	if err != nil {
		t.Fatal(err)
	}
	next, err := newRuntimeOwner(context.Background(), files, m, db, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer next.close()
	var retry runtimeRetry
	err = next.view(context.Background(), func(s *runtimeState) (any, error) { return s.retryGet(meta.Key), s.ioError() }, &retry)
	if err != nil || retry.Failures != 3 || retry.At.Before(Now()) || retry.Window != window {
		t.Fatal("committed parent lost retry child", retry, err)
	}
	err = next.view(context.Background(), func(s *runtimeState) (any, error) { return s.retryGet(partialKey(b.Capture, window)), s.ioError() }, &retry)
	if err != nil || retry.Failures != 0 || retry.At.Before(Now()) {
		t.Fatal("new attempt bypassed original cooldown", retry, err)
	}
	if db.historyReads != 0 || db.writes != 2 {
		t.Fatal("recovery changed write identity", db.historyReads, db.writes)
	}
}

func TestRuntimePagedSkippedProbesKeepEveryTaskAndOriginalOrder(t *testing.T) {
	o, _, _, _, b, m := runtimeOwnerFixture(t)
	ctx := context.Background()
	err := o.change(ctx, func(s *runtimeState) (any, error) {
		for i := 999; i >= 0; i-- {
			s.skippedSet(fmt.Sprintf("%04d", i), pendingOrder{Deposit: b.Deposits[0]})
		}
		return true, s.ioError()
	}, true)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 150; i++ {
		var got runtimeProbeWork
		err = o.view(ctx, func(s *runtimeState) (any, error) { return s.selectProbe(m, time.Now(), nil), s.ioError() }, &got)
		if err != nil || !got.Exists || got.Transition.Task.Key != fmt.Sprintf("%04d", i) || !got.Transition.Skipped {
			t.Fatal("paged order lost", i, got, err)
		}
		err = o.change(ctx, func(s *runtimeState) (any, error) {
			return true, s.probeCommitted(got.Transition, OrderProbe{ProbeStatus: "skipped_budget"})
		}, false)
		if err != nil {
			t.Fatal(err)
		}
	}
	var count uint64
	err = o.view(ctx, func(s *runtimeState) (any, error) {
		if len(s.Skipped) != 0 || s.skippedPages.Metrics().ResidentBytes > 512<<10 {
			return nil, errors.New("unbounded pending cache")
		}
		return s.skippedPages.Len(), s.ioError()
	}, &count)
	if err != nil || count != 850 {
		t.Fatal(count, err)
	}
}
