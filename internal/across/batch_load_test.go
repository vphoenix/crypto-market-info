package across

import (
	"context"
	"errors"
	"testing"
)

func batchLoadFixture(name string) Batch {
	now := Now()
	b := Batch{Capture: Capture{ManifestHash: ID("manifest"), CaptureId: ID(name), ChainId: 8453, CaptureKind: "logs", CaptureMode: "backfill", StartedAt: now, AvailableAt: now, SourceId: "fixture", Status: "complete", EvidenceHash: ID("evidence"), Canonical: true, Finality: "finalized"}}
	Seal(&b)
	return b
}

type bulkTestStore struct {
	reportTestStore
	bulkCalls, singleCalls int
	requests               [][]Capture
	mutate                 func(*BatchLoad)
}

func (s *bulkTestStore) AcrossBatch(ctx context.Context, c Capture) (Batch, error) {
	s.singleCalls++
	return s.reportTestStore.AcrossBatch(ctx, c)
}
func (s *bulkTestStore) AcrossBatches(_ context.Context, caps []Capture) (BatchLoad, error) {
	s.bulkCalls++
	s.requests = append(s.requests, append([]Capture{}, caps...))
	out := BatchLoad{Batches: map[string]Batch{}, Errors: map[string]error{}, Stats: BatchLoadStats{Mode: "fixture_bulk", SQLQueries: Ptr(uint64(6))}}
	for _, c := range caps {
		b, ok := s.batches[c.CaptureId]
		if !ok {
			out.Errors[c.CaptureId] = errors.New("fixture_missing_capture")
			continue
		}
		b.Capture = c
		out.Batches[c.CaptureId] = b
	}
	if s.mutate != nil {
		s.mutate(&out)
	}
	return out, nil
}

func TestLoadBatchesFallbackAndOptionalLoaderKeepExactMembers(t *testing.T) {
	a, b := batchLoadFixture("a"), batchLoadFixture("b")
	base := reportTestStore{batches: map[string]Batch{a.Capture.CaptureId: a, b.Capture.CaptureId: b}}
	got, e := LoadBatches(context.Background(), base, []Capture{a.Capture, b.Capture})
	if e != nil || len(got.Batches) != 2 || len(got.Errors) != 0 || got.Stats.SQLQueries != nil {
		t.Fatal(got, e)
	}
	bulk := &bulkTestStore{reportTestStore: base}
	got, e = LoadBatches(context.Background(), bulk, []Capture{a.Capture, b.Capture})
	if e != nil || bulk.bulkCalls != 1 || bulk.singleCalls != 0 || len(got.Batches) != 2 || got.Stats.SQLQueries == nil || *got.Stats.SQLQueries != 6 {
		t.Fatal(got, e, bulk)
	}
	bulk.mutate = func(out *BatchLoad) {
		bad := out.Batches[a.Capture.CaptureId]
		bad.Capture.Revision++
		out.Batches[a.Capture.CaptureId] = bad
		out.Errors[b.Capture.CaptureId] = errors.New("incomplete_members")
	}
	got, e = LoadBatches(context.Background(), bulk, []Capture{a.Capture, b.Capture})
	if e != nil || len(got.Batches) != 0 || len(got.Errors) != 2 {
		t.Fatal("bad capture rev or partial members accepted", got, e)
	}
	bulk.mutate = func(out *BatchLoad) { delete(out.Batches, a.Capture.CaptureId) }
	got, e = LoadBatches(context.Background(), bulk, []Capture{a.Capture})
	if e != nil || len(got.Errors) != 1 {
		t.Fatal("missing batch accepted", got, e)
	}
}

func TestRestoreUsesOneBulkLoadAndLatestRevisionBeforeFiltering(t *testing.T) {
	a, b := batchLoadFixture("a"), batchLoadFixture("receipts")
	b.Capture.CaptureKind = "receipts"
	b.Capture.ExpectedTasks = 1
	b.Capture.CompletedTasks = 1
	orphan := a.Capture
	orphan.Revision++
	orphan.Canonical = false
	orphan.Finality = "orphaned"
	store := &bulkTestStore{reportTestStore: reportTestStore{caps: []Capture{orphan, a.Capture, b.Capture}, batches: map[string]Batch{a.Capture.CaptureId: a, b.Capture.CaptureId: b}}}
	c := Collector{Store: store, Manifest: Manifest{Hash: a.Capture.ManifestHash}}
	if _, e := c.restore(context.Background()); e != nil {
		t.Fatal(e)
	}
	if store.bulkCalls != 1 || store.singleCalls != 0 || len(store.requests[0]) != 1 || store.requests[0][0].CaptureId != b.Capture.CaptureId {
		t.Fatal("restore reloaded captures or resurrected orphan", store)
	}
}
