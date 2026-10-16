package justlendkeeper

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
			c, _, now := testCollector(t)
			db := &runtimeKeeperStore{rows: map[uuid.UUID]Batch{}}
			c.Store = db
			from, to := now.Add(-time.Hour), now.Add(-time.Hour+time.Minute)
			for i := 0; i < n; i++ {
				b := keeperRuntimePage(c, from, to, "", "", "history")
				child := Batch{Capture: keeperRuntimeChild(c, b.Capture, "complete")}
				Seal(&child)
				db.rows[b.Capture.CaptureId] = b
				db.rows[child.Capture.CaptureId] = child
			}
			active := keeperRuntimePage(c, from, to, "", "", "history")
			db.rows[active.Capture.CaptureId] = active
			if err := c.Save(); err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			dir := filepath.Join(t.TempDir(), "state")
			binding := keeperRuntimeBinding(c)
			at := time.Now()
			report, err := c.ConvertRuntime(ctx, dir, binding, testutil.Inventory(t, binding, "keeper"), func(context.Context) (runtimecheckpoint.Binding, error) { return binding, nil }, nil)
			if err != nil || !report.Verified {
				t.Fatal(report, err)
			}
			conversion := time.Since(at)
			pages, history := db.pages, db.history
			at = time.Now()
			if err = c.OpenRuntime(ctx, dir, binding); err != nil {
				t.Fatal(err)
			}
			restart := time.Since(at)
			defer c.CloseRuntime()
			m := c.runtime.files.Metrics()
			r := testutil.ScaleResult{Domain: "keeper", HistoryRows: n * 2, ConversionNS: int64(conversion), RestartNS: int64(restart), Active: c.State.Runtime.parents.Len(), HotBytes: c.State.Runtime.memoryBytes() + m.HotBytes - m.CheckpointBytes + m.VerifiedMetadataBytes, Metrics: m}
			query := func() error {
				parent, err := c.State.Runtime.next(c.Now().UTC())
				if err == nil && (parent == nil || parent.CaptureId != active.Capture.CaptureId) {
					return fmt.Errorf("wrong active parent")
				}
				return err
			}
			results = append(results, testutil.MeasureScale(t, r, query))
			if db.pages != pages || db.history != history {
				t.Fatal("steady/restart rebuilt history")
			}
			after := c.runtime.files.Metrics()
			if after.WrittenBytes != m.WrittenBytes || after.FileSyncs != m.FileSyncs {
				t.Fatal("idle persisted state")
			}
		})
	}
	if len(results) == 2 {
		testutil.CompareScale(t, results[0], results[1])
	}
}
