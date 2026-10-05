package lst

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestLiveCursorRebuildsMissingDatabaseTailAndPreservesSegment(t *testing.T) {
	m, err := LoadManifest("../../config/lst-lido-ethereum.json")
	if err != nil {
		t.Fatal(err)
	}
	tr, _ := transportFixture(t, func(*http.Request) (*http.Response, error) { t.Fatal("recovery must not contact RPC"); return nil, nil })
	s := &runnerMemoryStore{batches: map[uuid.UUID]Batch{}}
	c := Collector{Manifest: m, Store: s, RPC: &RPC{Transport: tr, URL: "https://rpc.example"}, StateDir: t.TempDir(), LiveLogsFromBlock: 100, LogMode: "range"}
	at := Now().Add(-time.Hour)
	p := logProgress{Manifest: m.Hash, Start: 100, Next: 140, RangeSize: 8, CoverageStartedAt: at, BlockedSource: "retained-source", BlockedReason: "retained-reason"}
	if err = writeGob(c.liveLogCursorPath(), p); err != nil {
		t.Fatal(err)
	}
	oldFile, oldBytes := mvp9OldCursor(t, &c)
	add := func(from, to uint64, status string, canonical, committed bool, finality, mode, manifest string) {
		id := uuid.New()
		s.batches[id] = Batch{Capture: Capture{CaptureId: id, ManifestHash: manifest, CaptureKind: "logs", CaptureMode: mode, Status: status, Canonical: canonical, Committed: committed, Finality: finality, FromBlock: Ptr(from), ToBlock: Ptr(to), FromBlockTime: Ptr(at)}}
	}
	add(100, 107, "complete", true, true, "finalized", "live", m.Hash)
	add(116, 139, "complete", true, true, "finalized", "live", m.Hash)
	for _, v := range []struct {
		status                   string
		canonical, committed     bool
		finality, mode, manifest string
	}{
		{"failed", true, true, "finalized", "live", m.Hash},
		{"complete", false, true, "finalized", "live", m.Hash},
		{"complete", true, false, "finalized", "live", m.Hash},
		{"complete", true, true, "head", "live", m.Hash},
		{"complete", true, true, "finalized", "restart", m.Hash},
		{"complete", true, true, "finalized", "live", string(make([]byte, 32))},
	} {
		add(108, 115, v.status, v.canonical, v.committed, v.finality, v.mode, v.manifest)
	}
	r, err := c.logProgress(context.Background())
	if err != nil || r.Start != 100 || r.Next != 108 || r.RangeSize != 8 || !r.CoverageStartedAt.Equal(at) || r.BlockedSource != p.BlockedSource || r.BlockedReason != p.BlockedReason {
		t.Fatal("persisted Next skipped missing committed coverage or reset state", r, err)
	}
	add(108, 115, "complete", true, true, "finalized", "live", m.Hash)
	r, err = c.logProgress(context.Background())
	if err != nil || r.Next != 140 {
		t.Fatal("verified bridge failed to recover later ranges", r, err)
	}
	r, err = c.logProgress(context.Background())
	if err != nil || r.Next != 140 {
		t.Fatal("repeated recovery changed the prefix", r, err)
	}
	oldNow, err := os.ReadFile(oldFile)
	if err != nil || !bytes.Equal(oldBytes, oldNow) {
		t.Fatal("new-segment recovery rewrote old segment", err)
	}
}

type cursorReadFailureStore struct{ *runnerMemoryStore }

func (*cursorReadFailureStore) LSTCaptures(context.Context, string) ([]Capture, error) {
	return nil, errors.New("database_read_failed")
}

func TestLiveCursorDatabaseReadFailureLeavesPersistedProgressIntact(t *testing.T) {
	m, err := LoadManifest("../../config/lst-lido-ethereum.json")
	if err != nil {
		t.Fatal(err)
	}
	c := Collector{Manifest: m, Store: &cursorReadFailureStore{&runnerMemoryStore{}}, StateDir: t.TempDir(), LiveLogsFromBlock: 100, LogMode: "range"}
	p := logProgress{Manifest: m.Hash, Start: 100, Next: 140, RangeSize: 8, CoverageStartedAt: Now()}
	if err = writeGob(c.liveLogCursorPath(), p); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(c.liveLogCursorPath())
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.logProgress(context.Background())
	if err == nil || err.Error() != "database_read_failed" {
		t.Fatal("database failure was hidden", err)
	}
	after, err := os.ReadFile(c.liveLogCursorPath())
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("read failure overwrote persisted progress", err)
	}
}
