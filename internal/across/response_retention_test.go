package across

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
)

func responseFixture(t *testing.T, a ethereum.Archive, name string) string {
	t.Helper()
	h, err := a.PutObject(struct{ Source, Symbol, Response string }{"public", "USDCUSDT", name})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(a.Dir, h.String()[2:4], h.String()[2:]+".json.gz")
	old := time.Now().Add(-time.Hour)
	if err := os.Chtimes(path, old, old); err != nil {
		t.Fatal(err)
	}
	return string(h[:])
}
func responseExists(a ethereum.Archive, ref string) bool {
	h := Hex(ref)[2:]
	_, err := os.Stat(filepath.Join(a.Dir, h[:2], h+".json.gz"))
	return err == nil
}
func TestResponseCleanupAlsoDeletesUnreferencedOldRequests(t *testing.T) {
	a := ethereum.Archive{Dir: t.TempDir()}
	unique, shared, orphan := responseFixture(t, a, "parsed"), responseFixture(t, a, "not decoded"), responseFixture(t, a, "old poll")
	complete, partial := batchLoadFixture("complete"), batchLoadFixture("partial")
	partial.Capture.Status = "partial"
	partial.Capture.UnknownEventCount = 1
	// An unanchored partial attempt has no complete replacement.
	for _, item := range []struct {
		batch *Batch
		refs  []string
	}{{&complete, []string{unique, shared}}, {&partial, []string{shared}}} {
		if err := ArchiveBatch(a, item.batch, nil, item.refs); err != nil {
			t.Fatal(err)
		}
	}
	store := reportTestStore{caps: []Capture{complete.Capture, partial.Capture}, batches: map[string]Batch{complete.Capture.CaptureId: complete, partial.Capture.CaptureId: partial}}
	stats, err := PruneStoredResponses(context.Background(), store, Manifest{Hash: complete.Capture.ManifestHash}, a)
	if err != nil || stats.FilesDeleted != 2 || stats.ProtectedRefs != 1 {
		t.Fatal(stats, err)
	}
	if responseExists(a, unique) || responseExists(a, orphan) || !responseExists(a, shared) {
		t.Fatal("wrong file deleted")
	}
	for _, cap := range store.caps {
		if _, err := ReadCaptureEvidence(a, cap); err != nil {
			t.Fatal(err)
		}
	}
}
func TestResponseCleanupChecksDatabaseAndMetadataBeforeDeleting(t *testing.T) {
	a := ethereum.Archive{Dir: t.TempDir()}
	ref := responseFixture(t, a, "unused")
	b := batchLoadFixture("missing metadata")
	store := reportTestStore{caps: []Capture{b.Capture}, batches: map[string]Batch{b.Capture.CaptureId: b}}
	if _, err := PruneStoredResponses(context.Background(), store, Manifest{Hash: b.Capture.ManifestHash}, a); !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	if !responseExists(a, ref) {
		t.Fatal("deleted before validation")
	}
}
