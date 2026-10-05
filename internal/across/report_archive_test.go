package across

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
)

func archiveReviewCaptures(t *testing.T, archive ethereum.Archive, refs []string) []Capture {
	t.Helper()
	captures := make([]Capture, len(refs))
	for i, ref := range refs {
		b := batchLoadFixture("parallel-archive-" + string(rune('a'+i)))
		if err := ArchiveBatch(archive, &b, nil, []string{ref}); err != nil {
			t.Fatal(err)
		}
		captures[i] = b.Capture
	}
	return captures
}

func TestReportReadsCommittedCapturesWithoutSourceBodies(t *testing.T) {
	archive := ethereum.Archive{Dir: t.TempDir()}
	captures := archiveReviewCaptures(t, archive, []string{ID("absent raw")})
	rows, err := newReportArchiveValidator(archive).check(context.Background(), captures, nil)
	if err != nil || rows[0].reason != "" {
		t.Fatal(rows, err)
	}
	hash := Hex(captures[0].EvidenceHash)[2:]
	if err := os.Remove(filepath.Join(archive.Dir, hash[:2], hash+".json.gz")); err != nil {
		t.Fatal(err)
	}
	rows, err = newReportArchiveValidator(archive).check(context.Background(), captures, nil)
	if err != nil || rows[0].reason != "missing_or_invalid_capture_evidence" {
		t.Fatal(rows, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := newReportArchiveValidator(archive).check(ctx, captures, nil); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestReportFailureLeavesReadablePhaseProgress(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	b := batchLoadFixture("cancel-report")
	store := reportTestStore{caps: []Capture{b.Capture}, batches: map[string]Batch{b.Capture.CaptureId: b}}
	out := t.TempDir()
	err := BuildReport(ctx, store, reportTestManifest(), ethereum.Archive{Dir: t.TempDir()}, time.Now().Add(-time.Hour), time.Now(), out)
	if !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(out, "phase-progress.jsonl"))
	if err != nil || !strings.Contains(string(raw), `"phase":"capture_metadata_started"`) || !strings.Contains(string(raw), `"phase":"report_failed"`) {
		t.Fatal(string(raw), err)
	}
	if _, err = os.Stat(filepath.Join(out, "summary.json")); !os.IsNotExist(err) {
		t.Fatal("failed report produced summary", err)
	}
}
