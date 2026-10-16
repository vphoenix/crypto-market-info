package dexruntime

import (
	"context"
	"fmt"
	"github.com/vphoenix/crypto-market-info/internal/dex"
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
			db := &memoryDB{rows: map[string]dex.Block{}}
			for i := 1; i <= n; i++ {
				b := fixtureBlock(uint64(i))
				b.Finality = "finalized"
				db.rows[key(b)] = b
			}
			b := fixtureBlock(3000)
			db.rows[key(b)] = b
			ctx := context.Background()
			dir := filepath.Join(t.TempDir(), "state")
			binding := fixtureBinding()
			at := time.Now()
			report, err := Convert(ctx, dir, binding, testutil.Inventory(t, binding, "dex"), b.Manifest, db, func(context.Context) (runtimecheckpoint.Binding, error) { return binding, nil }, nil)
			if err != nil || !report.Verified {
				t.Fatal(report, err)
			}
			conversion := time.Since(at)
			pages, history := db.pages, db.history
			at = time.Now()
			s, err := Open(ctx, dir, binding, b.Manifest, db, nil)
			if err != nil {
				t.Fatal(err)
			}
			restart := time.Since(at)
			defer s.CloseRuntime()
			r := testutil.ScaleResult{Domain: "dex", HistoryRows: n, ConversionNS: int64(conversion), RestartNS: int64(restart)}
			if _, err = s.owner.call(ctx, func(o *owner) (any, error) {
				r.Metrics = o.files.Metrics()
				r.HotBytes = o.state.memoryBytes() + r.Metrics.HotBytes + r.Metrics.VerifiedMetadataBytes
				r.Active = o.state.active.Len()
				return nil, nil
			}); err != nil {
				t.Fatal(err)
			}
			query := func() error { _, err := s.DEXGapStart(ctx, b.Manifest); return err }
			results = append(results, testutil.MeasureScale(t, r, query))
			if db.pages != pages || db.history != history {
				t.Fatal("steady/restart rebuilt history")
			}
			if _, err = s.owner.call(ctx, func(o *owner) (any, error) {
				m := o.files.Metrics()
				if m.WrittenBytes != r.Metrics.WrittenBytes || m.FileSyncs != r.Metrics.FileSyncs {
					return nil, fmt.Errorf("idle persisted state")
				}
				return nil, nil
			}); err != nil {
				t.Fatal(err)
			}
		})
	}
	if len(results) == 2 {
		testutil.CompareScale(t, results[0], results[1])
	}
}
