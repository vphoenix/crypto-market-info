package lst

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	runtimeinputtest "github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint/testutil"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint"
)

type runtimeTestDB struct {
	rows                   map[uuid.UUID]Batch
	lose                   bool
	history, writes, pages int
	failPage               int
}

func (d *runtimeTestDB) WriteLST(_ context.Context, b Batch) error {
	d.writes++
	if old, ok := d.rows[b.Capture.CaptureId]; ok {
		if CaptureIdentity(old.Capture) != CaptureIdentity(b.Capture) {
			return runtimecheckpoint.ErrConflict
		}
	} else {
		d.rows[b.Capture.CaptureId] = b
	}
	if d.lose {
		d.lose = false
		return context.DeadlineExceeded
	}
	return nil
}
func (d *runtimeTestDB) WriteLSTRevision(_ context.Context, cap Capture) error {
	b := d.rows[cap.CaptureId]
	if cap.Revision > b.Capture.Revision {
		b.Capture = cap
		d.rows[cap.CaptureId] = b
	}
	return nil
}
func (d *runtimeTestDB) LSTBatch(_ context.Context, cap Capture) (Batch, error) {
	b, ok := d.rows[cap.CaptureId]
	if !ok {
		return b, errors.New("missing")
	}
	return b, nil
}
func (d *runtimeTestDB) LSTCapture(ctx context.Context, cap Capture) (Capture, error) {
	b, e := d.LSTBatch(ctx, cap)
	return b.Capture, e
}
func (d *runtimeTestDB) LSTCaptures(context.Context, string) ([]Capture, error) {
	d.history++
	return nil, errors.New("normal history reconstruction forbidden")
}
func (d *runtimeTestDB) LSTCapturePage(_ context.Context, manifest string, after []byte, limit int) ([]Capture, []byte, error) {
	d.pages++
	if d.failPage == d.pages {
		return nil, nil, errors.New("conversion interrupted")
	}
	var out []Capture
	for _, b := range d.rows {
		if b.Capture.ManifestHash == manifest && bytes.Compare(b.Capture.CaptureId[:], after) > 0 {
			out = append(out, b.Capture)
		}
	}
	sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i].CaptureId[:], out[j].CaptureId[:]) < 0 })
	out = out[:min(limit, len(out))]
	if len(out) == 0 {
		return out, nil, nil
	}
	id := out[len(out)-1].CaptureId
	return out, append([]byte(nil), id[:]...), nil
}
func runtimeFixture(t *testing.T) (*Collector, *runtimeTestDB, string, runtimecheckpoint.Binding, Batch) {
	t.Helper()
	b := gasTestBatch(t, Now().Truncate(24*time.Hour).Add(-24*time.Hour))
	db := &runtimeTestDB{rows: map[uuid.UUID]Batch{}}
	c := &Collector{Manifest: Manifest{Hash: b.Capture.ManifestHash, MaxLogBlocks: 8}, Store: db, StateDir: t.TempDir(), LiveLogsFromBlock: 100, LogMode: "range"}
	binding := runtimecheckpoint.Binding{Service: "lst", Database: "test", ServerUUID: "server", ScopeHash: RuntimeScopeHash(c), SchemaHash: "schema", Producer: "lst", Holder: "holder", RecoveryEpoch: "epoch", ProducerGeneration: 1, StateVersion: 1}
	s := newRuntimeState(binding, c.Manifest.Hash)
	s.Verified = true
	s.Cursors[100] = logProgress{Manifest: c.Manifest.Hash, Start: 100, Next: 100, RangeSize: 8, CoverageStartedAt: *b.Capture.FromBlockTime}
	raw, e := runtimecheckpoint.MarshalTyped(s)
	if e != nil {
		t.Fatal(e)
	}
	dir := t.TempDir()
	files, e := runtimecheckpoint.Create(dir, binding, raw, nil, 0)
	if e != nil {
		t.Fatal(e)
	}
	files.Close()
	if e = c.OpenRuntime(context.Background(), dir, binding, nil); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { c.CloseRuntime() })
	return c, db, dir, binding, b
}
func runtimeCrash(t *testing.T, c *Collector) {
	t.Helper()
	crash := errors.New("injected process loss")
	_ = c.runtime.request(context.Background(), runtimeRequest{Apply: func(*runtimeOwner) (any, error) { return nil, crash }}, nil)
	if !errors.Is(c.CloseRuntime(), crash) {
		t.Fatal("checkpoint unexpectedly acknowledged")
	}
}
func TestRuntimePendingUnknownCommitRetainsOriginalAndDerivesOnce(t *testing.T) {
	for _, restart := range []bool{false, true} {
		t.Run(fmt.Sprint(restart), func(t *testing.T) {
			c, db, dir, binding, b := runtimeFixture(t)
			ctx := context.Background()
			db.lose = true
			if e := c.Commit(ctx, b); !errors.Is(e, context.DeadlineExceeded) {
				t.Fatal(e)
			}
			p, e := c.logProgress(ctx)
			if e != nil || p.Next != 100 {
				t.Fatal("unknown commit advanced", p, e)
			}
			var op runtimecheckpoint.Operation
			if e = c.runtime.request(ctx, runtimeRequest{Apply: func(o *runtimeOwner) (any, error) { return o.files.Pending(0, 1)[0], nil }}, &op); e != nil {
				t.Fatal(e)
			}
			original, e := os.Stat(filepath.Join(c.StateDir, "pending-batch.gob"))
			if e != nil {
				t.Fatal(e)
			}
			linked, e := os.Stat(filepath.Join(dir, "objects", op.Payload.Hash+".blob"))
			if e != nil || !os.SameFile(original, linked) {
				t.Fatal("payload copied instead of reused", e)
			}
			if restart {
				runtimeCrash(t, c)
				if e = c.OpenRuntime(ctx, dir, binding, nil); e != nil {
					t.Fatal(e)
				}
			}
			if e = c.FlushPending(ctx); e != nil {
				t.Fatal(e)
			}
			p, e = c.logProgress(ctx)
			if e != nil || p.Next != 121 || db.history != 0 || len(db.rows) != 1 {
				t.Fatal(p, e, db.history)
			}
			if _, e = os.Stat(filepath.Join(c.StateDir, "pending-batch.gob")); !os.IsNotExist(e) {
				t.Fatal("pending not cleaned", e)
			}
			if _, e = os.Stat(filepath.Join(dir, "objects", op.Payload.Hash+".blob")); e != nil {
				t.Fatal("last recovery payload deleted before checkpoint", e)
			}
			runtimeCrash(t, c)
			if e = c.OpenRuntime(ctx, dir, binding, nil); e != nil {
				t.Fatal(e)
			}
			var count uint64
			if e = c.runtime.view(ctx, func(s *runtimeState) (any, error) { return s.Gas.headers.Len(), s.err }, &count); e != nil || count != 1 {
				t.Fatal("recovered children duplicated", count, e)
			}
		})
	}
}
func TestRuntimePendingAlreadyCoveredAndOldRevision(t *testing.T) {
	c, db, dir, binding, b := runtimeFixture(t)
	ctx := context.Background()
	file := filepath.Join(c.StateDir, "pending-batch.gob")
	if e := writeGob(file, b); e != nil {
		t.Fatal(e)
	}
	if e := c.runtimeFlushPending(ctx, file, b); e != nil {
		t.Fatal(e)
	}
	if e := c.CloseRuntime(); e != nil {
		t.Fatal(e)
	}
	if e := c.OpenRuntime(ctx, dir, binding, nil); e != nil {
		t.Fatal(e)
	}
	var before uint64
	_ = c.runtime.request(ctx, runtimeRequest{Apply: func(o *runtimeOwner) (any, error) { return o.files.Metrics().Uncheckpointed, nil }}, &before)
	if e := c.FlushPending(ctx); e != nil {
		t.Fatal(e)
	}
	var after uint64
	_ = c.runtime.request(ctx, runtimeRequest{Apply: func(o *runtimeOwner) (any, error) { return o.files.Metrics().Uncheckpointed, nil }}, &after)
	if after != before {
		t.Fatal("checkpointed pending reapplied", before, after)
	}
	market := lstBatchFixture()
	market.Capture.ManifestHash = c.Manifest.Hash
	market.Capture.ChainId = Ptr(uint64(1))
	market.Capture.FromBlock = Ptr(uint64(110))
	market.Capture.ToBlock = Ptr(uint64(110))
	market.Capture.FromBlockHash = Ptr(gasTestHash(110))
	market.Capture.ToBlockHash = Ptr(gasTestHash(110))
	market.Capture.FromBlockTime = Ptr(market.Capture.StartedAt.Add(-time.Second))
	market.Capture.ToBlockTime = Ptr(market.Capture.StartedAt.Add(-time.Second))
	if e := market.Seal(); e != nil {
		t.Fatal(e)
	}
	if e := c.Commit(ctx, market); e != nil {
		t.Fatal(e)
	}
	actual := market.Capture
	actual.Revision = 3
	actual.Finality = "orphaned"
	actual.Canonical = false
	row := db.rows[actual.CaptureId]
	row.Capture = actual
	db.rows[actual.CaptureId] = row
	stale := market.Capture
	stale.Revision = 2
	stale.Finality = "finalized"
	if e := c.runtimeWriteRevision(ctx, market.Capture.Revision, stale); e != nil {
		t.Fatal(e)
	}
	var active uint64
	if e := c.runtime.view(ctx, func(s *runtimeState) (any, error) { return s.active.Len(), s.err }, &active); e != nil || active != 0 {
		t.Fatal("stale revision resurrected work", active, e)
	}
}
func TestRuntimeConversionResumeAllPagesPreservesPendingAndOriginalStart(t *testing.T) {
	c, db, _, binding, b := runtimeFixture(t)
	if e := c.CloseRuntime(); e != nil {
		t.Fatal(e)
	}
	original := logProgress{Manifest: c.Manifest.Hash, Start: 100, Next: 9999, RangeSize: 4, CoverageStartedAt: *b.Capture.FromBlockTime, BlockedSource: "provider", BlockedReason: "unsupported"}
	if e := writeGob(c.liveLogCursorPath(), original); e != nil {
		t.Fatal(e)
	}
	// Ended registrations still contribute coverage even beyond the first page.
	for i := 0; i < 501; i++ {
		v := b
		v.Capture.CaptureId = uuid.New()
		v.Capture.FromBlock = Ptr(uint64(100 + i))
		v.Capture.ToBlock = Ptr(uint64(100 + i))
		v.Capture.ToBlockTime = Ptr(Now().Add(-40 * 24 * time.Hour))
		v.Capture.StartedAt = *v.Capture.ToBlockTime
		db.rows[v.Capture.CaptureId] = v
	}
	if e := writeGob(filepath.Join(c.StateDir, "pending-batch.gob"), b); e != nil {
		t.Fatal(e)
	}
	dir := filepath.Join(t.TempDir(), "converted")
	check := func(context.Context) (runtimecheckpoint.Binding, error) { return binding, nil }
	db.failPage = 2
	if _, e := c.ConvertRuntime(context.Background(), dir, binding, runtimeinputtest.Inventory(t, binding, "lst"), check, nil); e == nil {
		t.Fatal("missing injected conversion interruption")
	}
	if _, e := os.Stat(dir); !os.IsNotExist(e) {
		t.Fatal("partial state published")
	}
	db.failPage = 0
	report, e := c.ConvertRuntime(context.Background(), dir, binding, runtimeinputtest.Inventory(t, binding, "lst"), check, nil)
	if e != nil {
		t.Fatal(e)
	}
	if !report.Verified || report.Headers != 502 || report.FactBatches != 1 {
		t.Fatal(report)
	}
	if e = c.OpenRuntime(context.Background(), dir, binding, nil); e != nil {
		t.Fatal(e)
	}
	if e = c.FlushPending(context.Background()); e != nil {
		t.Fatal(e)
	}
	p, e := c.logProgress(context.Background())
	if e != nil || p.Start != 100 || p.Next != 601 || p.BlockedSource != original.BlockedSource || p.RangeSize != 4 {
		t.Fatal(p, e)
	}
	if db.history != 0 {
		t.Fatal("conversion called unpaged legacy database reader")
	}
}
func TestRuntimeIdleJobsDoNotReadHistory(t *testing.T) {
	c, db, _, _, _ := runtimeFixture(t)
	c.RPC = &RPC{}
	if _, e := c.restoreFollowups(context.Background()); e != nil {
		t.Fatal(e)
	}
	if e := c.FinalizePending(context.Background()); e != nil {
		t.Fatal(e)
	}
	if e := c.GasPass(context.Background(), 10); e != nil {
		t.Fatal(e)
	}
	if _, e := c.logProgress(context.Background()); e != nil {
		t.Fatal(e)
	}
	if db.history != 0 || db.pages != 0 || db.writes != 0 {
		t.Fatal("idle runtime accessed SQL", db)
	}
}

func TestRuntimeLostCommitRestoresFollowupAndPreserves31DayBoundary(t *testing.T) {
	c, db, dir, binding, _ := runtimeFixture(t)
	ctx := context.Background()
	b := reportGoodMarket(t)
	b.Capture.ManifestHash = c.Manifest.Hash
	if err := b.Seal(); err != nil {
		t.Fatal(err)
	}
	db.lose = true
	if err := c.Commit(ctx, b); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	runtimeCrash(t, c)
	if err := c.OpenRuntime(ctx, dir, binding, nil); err != nil {
		t.Fatal(err)
	}
	got, err := c.restoreFollowups(ctx)
	if err != nil || len(got.Seeds) != 1 || got.Seeds[0].QuoteId != b.Quotes[0].QuoteId {
		t.Fatal("lost committed followup seed", got, err)
	}
	at := Now()
	var expected followupState
	err = c.runtime.request(ctx, runtimeRequest{Apply: func(o *runtimeOwner) (any, error) {
		var history []Capture
		facts := map[uuid.UUID]Batch{}
		// Include the already committed seed in the same frozen comparison input.
		history = append(history, b.Capture)
		facts[b.Capture.CaptureId] = b
		for i := 0; i < 35; i++ {
			v := lstBatchFixture()
			v.Capture.ManifestHash = c.Manifest.Hash
			v.Capture.Committed = true
			v.Capture.Revision = 1
			v.Capture.StartedAt = at.Add(-time.Duration(i) * 24 * time.Hour)
			v.Capture.AvailableAt = v.Capture.StartedAt
			v.Quotes[0].AvailableAt = v.Capture.StartedAt
			v.Quotes[0].QuoteId = CanonicalHash(i)
			v.Quotes[0].IsFollowupSeed = true
			if e := o.state.batch(v, at); e != nil {
				return nil, e
			}
			history = append(history, v.Capture)
			facts[v.Capture.CaptureId] = v
		}
		old, e := restoreFollowupsAt(history, at, func(cap Capture) (Batch, error) { return facts[cap.CaptureId], nil })
		if e != nil {
			return nil, e
		}
		actual, e := o.state.followups(at)
		if e != nil {
			return nil, e
		}
		if CanonicalHash(old.Seeds) != CanonicalHash(actual.Seeds) || CanonicalHash(old.due(at)) != CanonicalHash(actual.due(at)) {
			return nil, errors.New("31 day selection differs from original")
		}
		o.dirty = true
		return actual, nil
	}}, &expected)
	if err != nil || len(expected.Seeds) != 30 {
		t.Fatal(len(expected.Seeds), err)
	}
}

func TestRuntimeCloseReleasesLockBeforeAcknowledgement(t *testing.T) {
	c, _, dir, binding, _ := runtimeFixture(t)
	for i := 0; i < 64; i++ {
		if err := c.CloseRuntime(); err != nil {
			t.Fatal(err)
		}
		if err := c.OpenRuntime(context.Background(), dir, binding, nil); err != nil {
			t.Fatal(i, err)
		}
	}
}
