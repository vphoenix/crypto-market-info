package across

import (
	"context"
	"fmt"
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
			o, db, _, binding, active, m := runtimeOwnerFixture(t)
			if err := o.close(); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < n; i++ {
				b := batchLoadFixture(fmt.Sprint(i))
				b.Capture.ManifestHash = m.Hash
				db.rows[b.Capture.CaptureId] = b
			}
			db.rows[active.Capture.CaptureId] = active
			c := Collector{Manifest: m, Store: &runtimeConversionDB{db}}
			ctx := context.Background()
			dir := filepath.Join(t.TempDir(), "state")
			at := time.Now()
			report, err := c.ConvertRuntime(ctx, dir, binding, testutil.Inventory(t, binding, "across"), func(context.Context) (runtimecheckpoint.Binding, error) { return binding, nil }, nil)
			if err != nil || !report.Verified {
				t.Fatal(report, err)
			}
			conversion := time.Since(at)
			history := db.historyReads
			at = time.Now()
			if err = c.OpenRuntime(ctx, dir, binding, nil); err != nil {
				t.Fatal(err)
			}
			restart := time.Since(at)
			defer c.CloseRuntime()
			r := testutil.ScaleResult{Domain: "across", HistoryRows: n, ConversionNS: int64(conversion), RestartNS: int64(restart)}
			if _, err = c.runtime.request(ctx, runtimeRequest{view: func(s *runtimeState) (any, error) {
				r.HotBytes = c.runtime.hotBytes()
				r.Metrics = c.runtime.store.Metrics()
				r.Active = uint64(s.captureCount(false))
				return true, nil
			}}); err != nil {
				t.Fatal(err)
			}
			query := func() error {
				var cursor uint64
				return c.runtime.view(ctx, func(s *runtimeState) (any, error) { return s.Watch.Cursors[active.Capture.ChainId], s.ioError() }, &cursor)
			}
			results = append(results, testutil.MeasureScale(t, r, query))
			if db.historyReads != history {
				t.Fatal("steady/restart rebuilt history")
			}
			if _, err = c.runtime.request(ctx, runtimeRequest{view: func(*runtimeState) (any, error) {
				m := c.runtime.store.Metrics()
				if m.WrittenBytes != r.Metrics.WrittenBytes || m.FileSyncs != r.Metrics.FileSyncs {
					return nil, fmt.Errorf("idle persisted state")
				}
				return true, nil
			}}); err != nil {
				t.Fatal(err)
			}
		})
	}
	if len(results) == 2 {
		testutil.CompareScale(t, results[0], results[1])
	}
}
