package dexruntime

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	runtimeinputtest "github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint/testutil"
	"io"
	"log/slog"
	"math/big"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/dex"
	"github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
)

func fixtureBlock(n uint64) dex.Block {
	at := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC).Add(time.Duration(n) * time.Second)
	return dex.Block{Anchor: dex.Anchor{ChainID: 1, Number: n, Hash: dex.Digest([]byte(fmt.Sprint(n))), Manifest: dex.Digest([]byte("manifest")), Batch: dex.Digest([]byte(fmt.Sprint("batch", n))), Payload: dex.Digest([]byte("payload")), Time: at}, BaseFee: new(big.Int), ReceivedAt: at, AvailableAt: at, Canonical: true, Finality: "head", Revision: 1, Committed: true}
}
func fixtureBinding() runtimecheckpoint.Binding {
	return runtimecheckpoint.Binding{Service: "collector", Database: "test", ServerUUID: "server", ScopeHash: fixtureBlock(1).Manifest.String(), SchemaHash: "schema", Producer: "collector", Holder: "holder", RecoveryEpoch: "epoch", ProducerGeneration: 1, StateVersion: 1}
}

type memoryDB struct {
	contracts.DEXStore
	mu                               sync.Mutex
	rows                             map[string]dex.Block
	writes, history, pages, failPage int
	lose                             bool
}

func (d *memoryDB) WriteDEXBatch(ctx context.Context, b dex.Batch) error {
	return d.WriteDEXBlock(ctx, b.Block)
}
func (d *memoryDB) CompleteDEXLogs(ctx context.Context, b dex.Block, _ []dex.Log) error {
	return d.WriteDEXBlock(ctx, b)
}
func (d *memoryDB) WriteDEXBlock(_ context.Context, b dex.Block) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.writes++
	old, ok := d.rows[key(b)]
	if !ok || b.Revision > old.Revision {
		if old.Finality == "finalized" && (!b.Canonical || b.Finality != "finalized") {
			return runtimecheckpoint.ErrConflict
		}
		d.rows[key(b)] = b
	}
	if d.lose {
		d.lose = false
		return errors.New("COMMIT response lost")
	}
	return nil
}
func (d *memoryDB) DEXRuntimeBlock(_ context.Context, b dex.Block) (dex.Block, bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	v, ok := d.rows[key(b)]
	return v, ok, nil
}
func (d *memoryDB) DEXRuntimePage(_ context.Context, m dex.Hash, after []byte, limit int) ([]dex.Block, []byte, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.pages++
	if d.pages == d.failPage {
		return nil, nil, errors.New("conversion interruption")
	}
	var out []dex.Block
	for _, b := range d.rows {
		cursor := make([]byte, 40)
		binary.BigEndian.PutUint64(cursor[:8], b.Number)
		copy(cursor[8:], b.Hash[:])
		if b.Manifest == m && b.Committed && bytes.Compare(cursor, after) > 0 {
			out = append(out, b)
		}
	}
	sort.Slice(out, func(i, j int) bool { return key(out[i]) < key(out[j]) })
	out = out[:min(limit, len(out))]
	if len(out) == 0 {
		return nil, nil, nil
	}
	last := out[len(out)-1]
	cursor := make([]byte, 40)
	binary.BigEndian.PutUint64(cursor[:8], last.Number)
	copy(cursor[8:], last.Hash[:])
	return out, cursor, nil
}
func (d *memoryDB) DEXGapStart(context.Context, dex.Hash) (uint64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.history++
	return 0, errors.New("normal history forbidden")
}
func fixtureRuntime(t *testing.T) (*Store, *memoryDB, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "runtime")
	binding := fixtureBinding()
	s := state{Version: 1, Binding: binding, Manifest: fixtureBlock(1).Manifest, Verified: true}
	raw, err := runtimecheckpoint.MarshalTyped(s)
	if err != nil {
		t.Fatal(err)
	}
	files, err := runtimecheckpoint.Create(dir, binding, raw, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	files.Close()
	db := &memoryDB{rows: map[string]dex.Block{}}
	store, err := Open(context.Background(), dir, binding, s.Manifest, db, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.CloseRuntime() })
	return store, db, dir
}
func TestIntervalsRemoveOverflowAndDuplicateHeights(t *testing.T) {
	var ranges []interval
	for _, n := range []uint64{10, 12, 11, 10, 1, 3, 2, ^uint64(0), ^uint64(0) - 1} {
		ranges = add(ranges, n)
	}
	want := []interval{{1, 3}, {10, 12}, {^uint64(0) - 1, ^uint64(0)}}
	if !reflect.DeepEqual(ranges, want) || !validIntervals(ranges) {
		t.Fatal(ranges)
	}
	ranges = remove(ranges, 11)
	ranges = remove(ranges, 1)
	ranges = remove(ranges, ^uint64(0))
	want = []interval{{2, 3}, {10, 10}, {12, 12}, {^uint64(0) - 1, ^uint64(0) - 1}}
	if !reflect.DeepEqual(ranges, want) || !validIntervals(ranges) {
		t.Fatal(ranges)
	}
}
func TestRuntimeUnknownCommitRecoveryAndIdleNoHistory(t *testing.T) {
	s, db, dir := fixtureRuntime(t)
	ctx := context.Background()
	manifest := fixtureBlock(1).Manifest
	for _, n := range []uint64{100, 102} {
		if err := s.WriteDEXBatch(ctx, dex.Batch{Block: fixtureBlock(n)}); err != nil {
			t.Fatal(err)
		}
	}
	if gap, err := s.DEXGapStart(ctx, manifest); err != nil || gap != 101 {
		t.Fatal(gap, err)
	}
	if _, err := s.Inspect(ctx, ""); err != nil {
		t.Fatal(err)
	}
	db.lose = true
	original := fixtureBlock(101)
	if err := s.WriteDEXBatch(ctx, dex.Batch{Block: original}); err == nil {
		t.Fatal("lost response not propagated")
	}
	if gap, err := s.DEXGapStart(ctx, manifest); err != nil || gap != 101 {
		t.Fatal("unknown commit advanced", gap, err)
	}
	_, _ = s.owner.call(ctx, func(*owner) (any, error) { return nil, errors.New("crash before checkpoint") })
	if err := s.CloseRuntime(); err == nil {
		t.Fatal("crash checkpoint acknowledged")
	}
	s, err := Open(ctx, dir, fixtureBinding(), manifest, db, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.CloseRuntime()
	for i := 0; i < 10; i++ {
		if gap, e := s.DEXGapStart(ctx, manifest); e != nil || gap != 0 {
			t.Fatal(gap, e)
		}
	}
	if db.writes != 4 || db.history != 0 || db.pages != 0 || len(db.rows) != 3 || !reflect.DeepEqual(db.rows[key(original)], original) {
		t.Fatal("recovery changed original or queried history", db.writes, db.history, db.pages)
	}
}
func TestRuntimeUnknownRevisionResolvedBeforeNewPlanAndStaleSQL(t *testing.T) {
	s, db, _ := fixtureRuntime(t)
	ctx := context.Background()
	b := fixtureBlock(100)
	if err := s.WriteDEXBatch(ctx, dex.Batch{Block: b}); err != nil {
		t.Fatal(err)
	}
	orphan := b
	orphan.Revision = 2
	orphan.Canonical = false
	orphan.Finality = "orphaned"
	db.lose = true
	if _, err := s.WriteRuntimeBlock(ctx, orphan, 1); err == nil {
		t.Fatal("expected lost commit")
	}
	final := b
	final.Revision = 4
	final.Finality = "finalized"
	if _, err := s.WriteRuntimeBlock(ctx, final, 1); !errors.Is(err, ErrStalePlan) {
		t.Fatal("new proposal bypassed recovered revision", err)
	}
	if db.writes != 3 {
		t.Fatal("stale proposal reached SQL", db.writes)
	}
	actual, err := s.WriteRuntimeBlock(ctx, final, 2)
	if err != nil || actual.Finality != "finalized" {
		t.Fatal(actual, err)
	}
	// A success/no-op for an old proposal must return the actual current revision.
	stale := b
	stale.Revision = 3
	stale.Canonical = false
	stale.Finality = "orphaned"
	actual, err = s.WriteRuntimeBlock(ctx, stale, 1)
	if err != nil || actual.Revision != 4 || !actual.Canonical || actual.Finality != "finalized" {
		t.Fatal("planned stale result replaced actual", actual, err)
	}
}
func TestRuntimeConcurrentIndependentBlocksAndCompactedFinalHistory(t *testing.T) {
	s, db, _ := fixtureRuntime(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	errs := make(chan error, 40)
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func(n uint64) {
			defer wg.Done()
			b := fixtureBlock(n)
			b.Finality = "finalized"
			errs <- s.WriteDEXBatch(ctx, dex.Batch{Block: b})
		}(uint64(i + 100))
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	value, err := s.owner.call(ctx, func(o *owner) (any, error) {
		if o.state.active.Len() != 0 || o.state.heights.Len() != 0 || len(o.state.Coverage) != 1 || len(o.state.Finalized) != 1 {
			return nil, errors.New("finished history retained")
		}
		return o.state.Coverage[0], nil
	})
	if err != nil || value.(interval) != (interval{100, 139}) {
		t.Fatal(value, err)
	}
	if db.history != 0 {
		t.Fatal("queried historical coverage")
	}
}
func TestRuntimeConversionResumeCompleteInputAndFrozenComparison(t *testing.T) {
	db := &memoryDB{rows: map[string]dex.Block{}, failPage: 2}
	for i := uint64(100); i < 802; i++ {
		if i == 300 {
			continue
		}
		b := fixtureBlock(i)
		if i < 780 {
			b.Finality = "finalized"
		}
		db.rows[key(b)] = b
	}
	dir := filepath.Join(t.TempDir(), "runtime")
	binding := fixtureBinding()
	check := func(context.Context) (runtimecheckpoint.Binding, error) { return binding, nil }
	ctx := context.Background()
	m := fixtureBlock(1).Manifest
	if _, err := Convert(ctx, dir, binding, runtimeinputtest.Inventory(t, binding, "dex"), m, db, check, nil); err == nil {
		t.Fatal("interruption ignored")
	}
	db.failPage = 0
	report, err := Convert(ctx, dir, binding, runtimeinputtest.Inventory(t, binding, "dex"), m, db, check, nil)
	if err != nil || !report.Verified || report.Headers != 701 || report.Unfinalized != 22 || report.FirstGap != 300 {
		t.Fatal(report, err)
	}
	pages := db.pages
	s, err := Open(ctx, dir, binding, m, db, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer s.CloseRuntime()
	if gap, err := s.DEXGapStart(ctx, m); err != nil || gap != 300 {
		t.Fatal(gap, err)
	}
	if db.pages != pages || db.history != 0 || db.writes != 0 {
		t.Fatal("normal restart scanned history")
	}
}

func TestRuntimeCloseIsRepeatableWithFullClosedOwnerQueue(t *testing.T) {
	s, _, _ := fixtureRuntime(t)
	if err := s.CloseRuntime(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		for i := 0; i < 64; i++ {
			if err := s.CloseRuntime(); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("second close blocked on abandoned queue")
	}
}

func TestRuntimeFullNotificationChannelDoesNotDropAcceptedResults(t *testing.T) {
	s, _, _ := fixtureRuntime(t)
	ctx := context.Background()
	entered := make(chan struct{})
	release := make(chan struct{})
	blocker := make(chan error, 1)
	go func() {
		_, err := s.owner.call(ctx, func(*owner) (any, error) { close(entered); <-release; return true, nil })
		blocker <- err
	}()
	<-entered
	replies := make([]chan response, cap(s.owner.requests))
	applied := 0
	for i := range replies {
		replies[i] = make(chan response, 1)
		s.owner.requests <- request{run: func(*owner) (any, error) { applied++; return true, nil }, reply: replies[i]}
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := s.owner.call(cancelled, func(*owner) (any, error) { t.Error("cancelled unaccepted request executed"); return nil, nil }); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	close(release)
	if err := <-blocker; err != nil {
		t.Fatal(err)
	}
	for _, reply := range replies {
		if got := <-reply; got.err != nil || got.value != true {
			t.Fatal(got)
		}
	}
	value, err := s.owner.call(ctx, func(*owner) (any, error) { return applied, nil })
	if err != nil || value.(int) != len(replies) {
		t.Fatal("accepted notification dropped", value, err)
	}
}
