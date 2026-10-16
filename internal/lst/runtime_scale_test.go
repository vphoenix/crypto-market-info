package lst

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint"
	testutil "github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint/testutil"
	"path/filepath"
	"testing"
	"time"
)

func TestRuntimeHistoryScale(t *testing.T) {
	var results []testutil.ScaleResult
	for _, n := range []int{100, 1000} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			c, db, _, binding, b := runtimeFixture(t)
			if err := c.CloseRuntime(); err != nil {
				t.Fatal(err)
			}
			original := logProgress{Manifest: c.Manifest.Hash, Start: 100, Next: 100, RangeSize: 4, CoverageStartedAt: *b.Capture.FromBlockTime}
			if err := writeGob(c.liveLogCursorPath(), original); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < n; i++ {
				v := b
				v.Capture.CaptureId = uuid.New()
				v.Capture.FromBlock = Ptr(uint64(100 + i))
				v.Capture.ToBlock = Ptr(uint64(100 + i))
				v.Capture.ToBlockTime = Ptr(Now().Add(-40 * 24 * time.Hour))
				v.Capture.StartedAt = *v.Capture.ToBlockTime
				db.rows[v.Capture.CaptureId] = v
			}
			// Same recent gas work in both inputs; old completed rows need no facts.
			db.rows[b.Capture.CaptureId] = b
			ctx := context.Background()
			dir := filepath.Join(t.TempDir(), "state")
			at := time.Now()
			report, err := c.ConvertRuntime(ctx, dir, binding, testutil.Inventory(t, binding, "lst"), func(context.Context) (runtimecheckpoint.Binding, error) { return binding, nil }, nil)
			if err != nil || !report.Verified {
				t.Fatal(report, err)
			}
			conversion := time.Since(at)
			pages, history := db.pages, db.history
			at = time.Now()
			if err = c.OpenRuntime(ctx, dir, binding, nil); err != nil {
				t.Fatal(err)
			}
			restart := time.Since(at)
			r := testutil.ScaleResult{Domain: "lst", HistoryRows: n, ConversionNS: int64(conversion), RestartNS: int64(restart)}
			if err = c.runtime.request(ctx, runtimeRequest{Apply: func(o *runtimeOwner) (any, error) {
				r.HotBytes = o.hotBytes()
				r.Metrics = o.files.Metrics()
				r.Active = o.state.Gas.headers.Len() + o.state.active.Len()
				return nil, nil
			}}, nil); err != nil {
				t.Fatal(err)
			}
			query := func() error { _, err := c.logProgress(ctx); return err }
			results = append(results, testutil.MeasureScale(t, r, query))
			if db.pages != pages || db.history != history {
				t.Fatal("steady/restart rebuilt history")
			}
			if err = c.runtime.request(ctx, runtimeRequest{Apply: func(o *runtimeOwner) (any, error) {
				m := o.files.Metrics()
				if m.WrittenBytes != r.Metrics.WrittenBytes || m.FileSyncs != r.Metrics.FileSyncs {
					return nil, fmt.Errorf("idle persisted state")
				}
				return nil, nil
			}}, nil); err != nil {
				t.Fatal(err)
			}
		})
	}
	if len(results) == 2 {
		testutil.CompareScale(t, results[0], results[1])
	}
}
