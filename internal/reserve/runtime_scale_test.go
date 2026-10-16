package reserve

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
			o, db, _, binding, m := runtimeFixture(t)
			if err := o.close(); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < n; i++ {
				b := runtimeBatch(m.Hash, fmt.Sprint(i), uint64(i*2+1), uint64(i*2+2), "finalized")
				db.rows[runtimeIdentity(b.Capture)] = b
			}
			active := runtimeBatch(m.Hash, "active", 3000, 3001, "head")
			db.rows[runtimeIdentity(active.Capture)] = active
			c := Collector{Manifest: m, Store: db, StartBlock: 1}
			ctx := context.Background()
			dir := filepath.Join(t.TempDir(), "state")
			at := time.Now()
			report, err := c.ConvertRuntime(ctx, dir, binding, testutil.Inventory(t, binding, "reserve"), func(context.Context) (runtimecheckpoint.Binding, error) { return binding, nil }, nil)
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
			defer c.CloseRuntime()
			r := testutil.ScaleResult{Domain: "reserve", HistoryRows: n, ConversionNS: int64(conversion), RestartNS: int64(restart)}
			if err = c.runtime.request(ctx, runtimeRequest{Apply: func(o *runtimeOwner) (any, error) {
				r.HotBytes = o.hotBytes()
				r.Metrics = o.files.Metrics()
				r.Active = o.state.active.Len()
				return nil, nil
			}}, nil); err != nil {
				t.Fatal(err)
			}
			query := func() error {
				var cursor uint64
				return c.runtime.request(ctx, runtimeRequest{Apply: func(o *runtimeOwner) (any, error) { return o.state.cursor(Hash(m.Hash), 1, true), o.state.err }}, &cursor)
			}
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
