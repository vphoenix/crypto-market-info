package across

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
	"github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint"
)

type runtimeTestDB struct {
	mu                   sync.Mutex
	rows                 map[string]Batch
	historyReads, writes int
	loseReturn           bool
	entered, release     chan struct{}
	onWrite              func(Batch)
}

func (d *runtimeTestDB) AcrossCaptures(context.Context, string) ([]Capture, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.historyReads++
	return nil, errors.New("normal runtime must not reconstruct historical captures")
}
func (d *runtimeTestDB) AcrossBatch(_ context.Context, c Capture) (Batch, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	b, ok := d.rows[c.CaptureId]
	if !ok {
		return b, errors.New("not committed")
	}
	return b, nil
}
func (d *runtimeTestDB) AcrossCapture(ctx context.Context, want Capture) (Capture, error) {
	b, e := d.AcrossBatch(ctx, want)
	return b.Capture, e
}
func (d *runtimeTestDB) WriteAcrossBatch(_ context.Context, b Batch) error {
	if d.entered != nil {
		d.entered <- struct{}{}
		<-d.release
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.writes++
	if d.onWrite != nil {
		d.onWrite(b)
	}
	if old, ok := d.rows[b.Capture.CaptureId]; ok {
		if runtimeCaptureMembers(old.Capture) != runtimeCaptureMembers(b.Capture) || ID(old.Deposits) != ID(b.Deposits) {
			return errors.New("frozen content changed")
		}
	} else {
		d.rows[b.Capture.CaptureId] = b
	}
	if d.loseReturn {
		d.loseReturn = false
		return context.DeadlineExceeded
	}
	return nil
}
func (d *runtimeTestDB) WriteAcrossRevision(_ context.Context, c Capture) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	b := d.rows[c.CaptureId]
	if c.Revision >= b.Capture.Revision {
		b.Capture = c
		d.rows[c.CaptureId] = b
	}
	return nil
}

func runtimeOwnerFixture(t *testing.T) (*runtimeOwner, *runtimeTestDB, string, runtimecheckpoint.Binding, Batch, Manifest) {
	t.Helper()
	b, _ := coreBatch(t)
	m := reportTestManifest()
	m.Hash = b.Capture.ManifestHash
	m.MaxPending = 100
	s := newRuntimeState(m)
	raw, err := runtimecheckpoint.MarshalTyped(s)
	if err != nil {
		t.Fatal(err)
	}
	binding := runtimecheckpoint.Binding{Service: "across", Database: "test", ServerUUID: "server", ScopeHash: Hex(m.Hash), SchemaHash: "schema", Producer: "across", Holder: "holder", RecoveryEpoch: "epoch", ProducerGeneration: 1, StateVersion: 1}
	dir := t.TempDir()
	files, err := runtimecheckpoint.Create(dir, binding, raw, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	db := &runtimeTestDB{rows: map[string]Batch{}}
	o, err := newRuntimeOwner(context.Background(), files, m, db, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		select {
		case <-o.done:
		default:
			o.close()
		}
	})
	return o, db, dir, binding, b, m
}

func TestRuntimeOwnerUnknownCommitRecoversSameIdentityWithoutHistory(t *testing.T) {
	o, db, dir, binding, b, m := runtimeOwnerFixture(t)
	db.loseReturn = true
	if err := o.writeBatch(context.Background(), db, b); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	var before []Capture
	if err := o.view(context.Background(), func(s *runtimeState) (any, error) { return s.captureView(), nil }, &before); err != nil || len(before) != 0 {
		t.Fatal("unknown commit advanced local state", before, err)
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
	var got []Capture
	if err = next.view(context.Background(), func(s *runtimeState) (any, error) { return s.captureView(), nil }, &got); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || ID(got[0]) != ID(b.Capture) || db.writes != 2 || db.historyReads != 0 {
		t.Fatal("recovery changed identity or scanned history", len(got), db.writes, db.historyReads)
	}
	if err = next.writeBatch(context.Background(), db, b); err != nil {
		t.Fatal("duplicate notification/retry", err)
	}
	var duplicate []Capture
	if err = next.view(context.Background(), func(s *runtimeState) (any, error) { return s.captureView(), nil }, &duplicate); err != nil || !reflect.DeepEqual(duplicate, got) {
		t.Fatal("duplicate applied twice", err)
	}
}

func TestRuntimeOwnerDoesNotHoldStateAcrossSQLAndViewsAreCopies(t *testing.T) {
	o, db, _, _, b, _ := runtimeOwnerFixture(t)
	db.entered = make(chan struct{}, 1)
	db.release = make(chan struct{})
	done := make(chan error, 1)
	go func() { done <- o.writeBatch(context.Background(), db, b) }()
	select {
	case <-db.entered:
	case <-time.After(time.Second):
		t.Fatal("write not started")
	}
	viewDone := make(chan error, 1)
	go func() {
		var v []Capture
		viewDone <- o.view(context.Background(), func(s *runtimeState) (any, error) { return s.captureView(), nil }, &v)
	}()
	select {
	case err := <-viewDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		close(db.release)
		t.Fatal("SQL blocked state owner")
	}
	close(db.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	var view []Capture
	if err := o.view(context.Background(), func(s *runtimeState) (any, error) { return s.captureView(), nil }, &view); err != nil {
		t.Fatal(err)
	}
	*view[0].FromBlock = 999
	if err := o.view(context.Background(), func(s *runtimeState) (any, error) { return s.captureView(), nil }, &view); err != nil || *view[0].FromBlock != 10 {
		t.Fatal("worker mutated owner state", err)
	}
}

func TestRuntimeOwnerStaleRevisionUsesConfirmedStorageRow(t *testing.T) {
	o, db, _, _, b, _ := runtimeOwnerFixture(t)
	if err := o.writeBatch(context.Background(), db, b); err != nil {
		t.Fatal(err)
	}
	newer := b.Capture
	newer.Revision = 5
	newer.Finality = "finalized"
	if err := db.WriteAcrossRevision(context.Background(), newer); err != nil {
		t.Fatal(err)
	}
	stale := b.Capture
	stale.Revision = 2
	actual, err := o.writeRevision(context.Background(), db, 1, stale)
	if err != nil {
		t.Fatal(err)
	}
	if actual.Revision != 5 || actual.Finality != "finalized" {
		t.Fatal("stale SQL no-op returned as applied", actual)
	}
	var view []Capture
	if err = o.view(context.Background(), func(s *runtimeState) (any, error) { return s.captureView(), nil }, &view); err != nil || len(view) != 1 || view[0].Revision != 5 {
		t.Fatal("state reverted", view, err)
	}
}

func TestRuntimeRecoveryCaptureAlreadyRevisedAndTailReplaysOnce(t *testing.T) {
	o, db, dir, binding, b, m := runtimeOwnerFixture(t)
	if err := o.writeBatch(context.Background(), db, b); err != nil {
		t.Fatal(err)
	}
	next := b.Capture
	next.Revision++
	next.Canonical = false
	next.Finality = "orphaned"
	raw, err := runtimecheckpoint.MarshalTyped(runtimeWrite{Version: 1, Revision: &next, Expected: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = o.request(context.Background(), runtimeRequest{begin: raw}); err != nil {
		t.Fatal(err)
	}
	if err = db.WriteAcrossRevision(context.Background(), next); err != nil {
		t.Fatal(err)
	}
	// Fail the owner before a checkpoint, as a lost process would. Both
	// original operation records remain; SQL now exposes the newer revision.
	_, _ = o.request(context.Background(), runtimeRequest{view: func(*runtimeState) (any, error) { return nil, errors.New("injected process loss") }})
	_ = o.close()
	files, err := runtimecheckpoint.Open(dir, binding, 0)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := newRuntimeOwner(context.Background(), files, m, db, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.close()
	var gap struct {
		Window HistoryRange
		Found  bool
	}
	err = restored.view(context.Background(), func(s *runtimeState) (any, error) {
		w, ok := s.newestRawGap(b.Capture.ChainId)
		return struct {
			Window HistoryRange
			Found  bool
		}{w, ok}, nil
	}, &gap)
	if err != nil || !gap.Found || gap.Window.From != 10 || gap.Window.To != 10 {
		t.Fatal("orphan recovery lost original coverage boundary", gap, err)
	}
	if db.historyReads != 0 {
		t.Fatal("recovery scanned history")
	}
}

func TestRuntimeWatchAndMaintenanceNeverLoadHistory(t *testing.T) {
	for _, once := range []bool{true, false} {
		t.Run(fmt.Sprint(once), func(t *testing.T) {
			o, db, _, _, _, m := runtimeOwnerFixture(t)
			m.MaxLogBlocks = 512
			m.MaxLogs = 1000
			m.PollMillis = 10
			m.FreshMillis = 180000
			m.FutureMillis = 2000
			readers := map[uint64]*Reader{}
			for _, chain := range m.Chains {
				readers[chain.ChainID] = protocolChainReader(t, chain, nil)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			logs := 0
			db.onWrite = func(b Batch) {
				if b.Capture.CaptureKind == "logs" && b.Capture.CompletedTasks == 1 {
					logs++
					if !once && logs >= 2 {
						cancel()
					}
				}
			}
			c := Collector{Manifest: m, Readers: readers, Store: db, Archive: ethereum.Archive{Dir: t.TempDir()}, runtime: o}
			err := c.Watch(ctx, once, nil)
			if err != nil && !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if logs < 2 || db.historyReads != 0 {
				t.Fatal("runtime still depended on history", logs, db.historyReads)
			}
		})
	}
}

func TestRuntimeBatchUnknownCommitRetriesInSameOwnerAndUsesActualRevision(t *testing.T) {
	o, db, _, _, b, _ := runtimeOwnerFixture(t)
	db.loseReturn = true
	if err := o.writeBatch(context.Background(), db, b); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	actual := b.Capture
	actual.Revision += 10
	actual.Canonical = false
	actual.Finality = "orphaned"
	if err := db.WriteAcrossRevision(context.Background(), actual); err != nil {
		t.Fatal(err)
	}
	if err := o.writeBatch(context.Background(), db, b); err != nil {
		t.Fatal("same-owner original retry", err)
	}
	var rows []Capture
	if err := o.view(context.Background(), func(s *runtimeState) (any, error) { return s.captureView(), nil }, &rows); err != nil {
		t.Fatal(err)
	}
	for _, cap := range rows {
		if cap.CaptureId == actual.CaptureId && (cap.Revision != actual.Revision || cap.Canonical) {
			t.Fatal("stale proposal installed", cap)
		}
	}
	if db.writes != 2 || db.historyReads != 0 {
		t.Fatal(db.writes, db.historyReads)
	}
}

func TestRuntimeCloseReleasesLockBeforeAcknowledgement(t *testing.T) {
	o, db, dir, binding, _, m := runtimeOwnerFixture(t)
	for i := 0; i < 64; i++ {
		if err := o.close(); err != nil {
			t.Fatal(err)
		}
		files, err := runtimecheckpoint.Open(dir, binding, 0)
		if err != nil {
			t.Fatal(i, err)
		}
		o, err = newRuntimeOwner(context.Background(), files, m, db, nil)
		if err != nil {
			files.Close()
			t.Fatal(err)
		}
	}
	if err := o.close(); err != nil {
		t.Fatal(err)
	}
}
