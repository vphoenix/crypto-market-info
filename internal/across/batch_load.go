package across

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"
)

func latestReadCaptures(captures []Capture) ([]Capture, error) {
	latest := map[string]Capture{}
	for _, cap := range captures {
		if old, ok := latest[cap.CaptureId]; !ok || cap.Revision > old.Revision {
			latest[cap.CaptureId] = cap
		} else if cap.Revision == old.Revision && ID(cap) != ID(old) {
			return nil, errors.New("conflicting_capture_revision")
		}
	}
	out := make([]Capture, 0, len(latest))
	for _, cap := range latest {
		out = append(out, cap)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].StartedAt.Equal(out[j].StartedAt) {
			return out[i].CaptureId < out[j].CaptureId
		}
		return out[i].StartedAt.Before(out[j].StartedAt)
	})
	return out, nil
}

// BatchLoader is optional so existing in-memory stores remain usable. A loader
// returns per-capture integrity errors separately from database/query failures.
type BatchLoader interface {
	AcrossBatches(context.Context, []Capture) (BatchLoad, error)
}

type BatchLoad struct {
	Batches map[string]Batch
	Errors  map[string]error
	Stats   BatchLoadStats
}

type BatchLoadStats struct {
	Mode        string  `json:"mode"`
	Captures    uint64  `json:"captures"`
	Chunks      uint64  `json:"chunks"`
	SQLQueries  *uint64 `json:"sql_queries"` // nil: the Store does not expose SQL counts.
	SQLMicros   *int64  `json:"sql_read_microseconds"`
	TotalMicros int64   `json:"load_and_validate_microseconds"`
}

// LoadBatches never changes row identity, timestamps, digests or revision. Its
// caller selects latest revisions before canonical/committed filtering. Even an
// optimized backend must return the exact requested capture and member set.
func LoadBatches(ctx context.Context, store Store, captures []Capture) (out BatchLoad, err error) {
	started := time.Now()
	defer func() { out.Stats.TotalMicros = time.Since(started).Microseconds() }()
	requested := make(map[string]Capture, len(captures))
	for _, cap := range captures {
		if _, exists := requested[cap.CaptureId]; exists {
			return out, errors.New("duplicate_capture_in_batch_load")
		}
		requested[cap.CaptureId] = cap
	}
	if loader, ok := store.(BatchLoader); ok {
		out, err = loader.AcrossBatches(ctx, captures)
		if err != nil {
			return out, err
		}
	} else {
		out = BatchLoad{Batches: map[string]Batch{}, Errors: map[string]error{}, Stats: BatchLoadStats{Mode: "store_fallback", Captures: uint64(len(captures))}}
		for _, cap := range captures {
			if err = ctx.Err(); err != nil {
				return out, err
			}
			batch, loadErr := store.AcrossBatch(ctx, cap)
			if loadErr != nil {
				out.Errors[cap.CaptureId] = loadErr
			} else {
				out.Batches[cap.CaptureId] = batch
			}
		}
	}
	if out.Errors == nil {
		out.Errors = map[string]error{}
	}
	if out.Batches == nil {
		out.Batches = map[string]Batch{}
	}
	for id := range out.Batches {
		if _, ok := requested[id]; !ok {
			return out, errors.New("unexpected_capture_in_batch_load")
		}
	}
	for id := range out.Errors {
		if _, ok := requested[id]; !ok {
			return out, errors.New("unexpected_capture_error_in_batch_load")
		}
	}
	for id, cap := range requested {
		if loadErr := out.Errors[id]; loadErr != nil {
			delete(out.Batches, id)
			continue
		}
		batch, exists := out.Batches[id]
		if !exists {
			out.Errors[id] = errors.New("capture_missing_from_batch_load")
			continue
		}
		if ID(batch.Capture) != ID(cap) {
			out.Errors[id] = errors.New("batch_load_capture_revision_mismatch")
			delete(out.Batches, id)
			continue
		}
		if validateErr := Validate(batch); validateErr != nil {
			out.Errors[id] = fmt.Errorf("batch_load_members: %w", validateErr)
			delete(out.Batches, id)
		}
	}
	out.Stats.Captures = uint64(len(captures))
	return out, nil
}
