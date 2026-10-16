package justlendkeeper

import (
	"context"
	"errors"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint"
)

type RuntimeInspection struct {
	Files                               runtimecheckpoint.Inspection
	Conversion                          RuntimeConversionReport
	EventCursors, EvidenceCursors       map[string]time.Time
	Parents, Windows, EnrichmentPending uint64
}

func (c *Collector) InspectRuntime(ctx context.Context, exportDir string) (RuntimeInspection, error) {
	var out RuntimeInspection
	if err := ctx.Err(); err != nil {
		return out, err
	}
	if c.runtime == nil {
		return out, errors.New("keeper_runtime_not_open")
	}
	for _, op := range c.State.Ops {
		if op.Frozen {
			return out, errors.New("keeper_runtime_frozen_operation_pending")
		}
	}
	if err := c.Save(); err != nil {
		return out, err
	}
	if exportDir != "" {
		if err := c.runtime.files.Export(exportDir); err != nil {
			return out, err
		}
	}
	files, err := c.runtime.files.Inspect()
	if err != nil {
		return out, err
	}
	if files.Pending != 0 {
		return out, errors.New("keeper_runtime_uncovered_operation_pending")
	}
	w := c.State.Runtime
	return RuntimeInspection{Files: files, Conversion: w.Conversion, EventCursors: c.State.EventCursors, EvidenceCursors: c.State.EvidenceCursors, Parents: w.parents.Len(), Windows: w.windows.Len(), EnrichmentPending: w.order.Len()}, nil
}

// The old collector already rebuilds windows/enrichment from MySQL. Export the
// current chain and current State/Ops, never an upgrade-time backup. Refuse a
// lossy legacy encoding (for example, a known-zero pointer in an unfinished Op).
func (c *Collector) RollbackRuntime(ctx context.Context, exportDir string) (RuntimeInspection, error) {
	var out RuntimeInspection
	if exportDir == "" {
		return out, errors.New("keeper_runtime_rollback_requires_export")
	}
	out, err := c.InspectRuntime(ctx, exportDir)
	if err != nil {
		return out, err
	}
	old := *c.State
	old.Runtime = nil
	raw, err := Freeze(old)
	if err != nil {
		return out, err
	}
	var roundtrip State
	if err = Thaw(raw, &roundtrip); err != nil {
		return out, err
	}
	if !runtimeStateEqual(old, roundtrip) {
		return out, errors.New("keeper_runtime_legacy_encoding_would_lose_pending_state")
	}
	// Old SQL reconstruction is explicit here. Check all working parents/pages
	// against today's immutable facts, without changing their original identities.
	r, ok := c.Store.(CollectionReader)
	if !ok {
		return out, errors.New("keeper_runtime_rollback_reader_required")
	}
	for after := ""; ; {
		keys, parents, e := c.State.Runtime.parents.Scan(after, 128, false)
		if e != nil {
			return out, e
		}
		if len(keys) == 0 {
			break
		}
		for _, parent := range parents {
			b, e := r.KeeperBatch(ctx, parent.Capture)
			if e != nil {
				return out, e
			}
			if !captureEqual(parent.Capture, b.Capture) {
				return out, runtimecheckpoint.ErrConflict
			}
			if e = Validate(b); e != nil {
				return out, e
			}
		}
		after = keys[len(keys)-1]
	}
	if err = SaveState(c.Path, old); err != nil {
		return out, err
	}
	actual, err := LoadState(c.Path, old.Target, c.Config)
	if err != nil {
		return out, err
	}
	if !runtimeStateEqual(old, actual) {
		return out, runtimecheckpoint.ErrConflict
	}
	return out, nil
}
