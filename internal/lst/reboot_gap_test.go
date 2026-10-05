package lst

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestManualGapCoverageRestoresLiveCursorWithoutCrossingUnfilledBlocks(t *testing.T) {
	m, err := LoadManifest("../../config/lst-lido-ethereum.json")
	if err != nil {
		t.Fatal(err)
	}
	store := &runnerMemoryStore{batches: map[uuid.UUID]Batch{}}
	c := Collector{Manifest: m, Store: store, StateDir: t.TempDir(), LiveLogsFromBlock: 100, LogMode: "range"}
	at := Now().Add(-time.Hour)
	p := logProgress{Manifest: m.Hash, Start: 100, Next: 108, RangeSize: 8, CoverageStartedAt: at}
	if err = writeGob(c.liveLogCursorPath(), p); err != nil {
		t.Fatal(err)
	}
	add := func(from, to uint64, status, finality, mode, manifest string, canonical, committed bool) {
		id := uuid.New()
		store.batches[id] = Batch{Capture: Capture{CaptureId: id, ManifestHash: manifest, CaptureKind: "logs", CaptureMode: mode, Status: status, Finality: finality, Canonical: canonical, Committed: committed, FromBlock: Ptr(from), ToBlock: Ptr(to), FromBlockTime: Ptr(at)}}
	}
	// The database must also prove the prefix referenced by the saved cursors.
	add(50, 59, "complete", "finalized", "live", m.Hash, true, true)
	add(100, 107, "complete", "finalized", "live", m.Hash, true, true)
	add(108, 119, "complete", "finalized", "backfill", m.Hash, true, true)
	add(121, 130, "complete", "finalized", "backfill", m.Hash, true, true)
	add(120, 130, "failed", "finalized", "backfill", m.Hash, true, true)
	add(120, 130, "complete", "head", "backfill", m.Hash, true, true)
	add(120, 130, "complete", "finalized", "backfill", m.Hash, false, true)
	add(120, 130, "complete", "finalized", "backfill", m.Hash, true, false)
	add(120, 130, "complete", "finalized", "research", m.Hash, true, true)
	add(120, 130, "complete", "finalized", "backfill", "other_manifest", true, true)
	add(120, 119, "complete", "finalized", "backfill", m.Hash, true, true)
	got, err := c.logProgress(context.Background())
	if err != nil || got.Next != 120 || got.Start != 100 || got.RangeSize != 8 {
		t.Fatal("manual repair was ignored or crossed a real gap", got, err)
	}
	add(120, 120, "complete", "finalized", "backfill", m.Hash, true, true)
	got, err = c.logProgress(context.Background())
	if err != nil || got.Next != 131 {
		t.Fatal("committed bridge did not advance through adjacent repaired ranges", got, err)
	}
	var persisted logProgress
	if err = readGob(c.liveLogCursorPath(), &persisted); err != nil || persisted.Next != 131 {
		t.Fatal("recovered cursor was not persisted", persisted, err)
	}
	oldPath := filepath.Join(c.StateDir, "live-logs.gob")
	old := logProgress{Manifest: m.Hash, Start: 50, Next: 60, RangeSize: 8, CoverageStartedAt: at}
	if err = writeGob(oldPath, old); err != nil {
		t.Fatal(err)
	}
	legacy := c
	legacy.LiveLogsFromBlock = 0
	got, err = legacy.logProgress(context.Background())
	if err != nil || got.Next != 60 {
		t.Fatal("repair of new segment skipped the older gap", got, err)
	}
	if _, err = os.Stat(c.liveLogCursorPath()); err != nil {
		t.Fatal(err)
	}
}
