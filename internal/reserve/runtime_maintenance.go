package reserve

import (
	"context"
	"errors"
	"reflect"

	"github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint"
)

type RuntimeInspection struct {
	Files           runtimecheckpoint.Inspection
	Conversion      RuntimeConversionReport
	LiveCursors     map[string]uint64
	Active, Pending uint64
}

func (c *Collector) InspectRuntime(ctx context.Context, exportDir string) (RuntimeInspection, error) {
	var result RuntimeInspection
	if c.runtime == nil {
		return result, errors.New("reserve_runtime_not_open")
	}
	if err := c.runtime.request(ctx, runtimeRequest{Save: true}, nil); err != nil {
		return result, err
	}
	err := c.runtime.request(ctx, runtimeRequest{Apply: func(o *runtimeOwner) (any, error) {
		if len(o.inflight) > 0 {
			return nil, errors.New("reserve_runtime_inflight")
		}
		if exportDir != "" {
			if err := o.files.Export(exportDir); err != nil {
				return nil, err
			}
		}
		files, err := o.files.Inspect()
		if err != nil {
			return nil, err
		}
		cursors := map[string]uint64{}
		fallback := uint64(0)
		if c.StartBlock > 0 {
			fallback = c.StartBlock - 1
		}
		for _, scope := range runtimeScopeHashes(c.Manifest, c.RelatedManifests) {
			cursors[Hex(scope)] = o.state.cursor(scope, fallback, c.StartBlock == 0)
		}
		return RuntimeInspection{files, o.state.Conversion, cursors, o.state.active.Len(), o.state.pending.Len()}, nil
	}}, &result)
	return result, err
}
func (c *Collector) RollbackRuntime(ctx context.Context, exportDir string) (RuntimeInspection, error) {
	if exportDir == "" {
		return RuntimeInspection{}, errors.New("reserve_runtime_rollback_requires_export")
	}
	result, err := c.InspectRuntime(ctx, exportDir)
	if err != nil {
		return result, err
	}
	expected := map[string]Capture{}
	for _, scope := range runtimeScopeHashes(c.Manifest, c.RelatedManifests) {
		caps, e := c.Store.ReserveCaptures(ctx, scope)
		if e != nil {
			return result, e
		}
		fallback := uint64(0)
		if c.StartBlock > 0 {
			fallback = c.StartBlock - 1
		}
		if ContiguousLiveCursor(caps, fallback, c.StartBlock == 0) != result.LiveCursors[Hex(scope)] {
			return result, errors.New("reserve_legacy_rebuild_would_change_live_cursor")
		}
		latest := map[string]Capture{}
		for _, cap := range caps {
			if !cap.Canonical || !cap.Committed || cap.CaptureKind != "logs" {
				continue
			}
			old, ok := latest[cap.CaptureId]
			if !ok || runtimeLatest(cap, old) {
				latest[cap.CaptureId] = cap
			}
		}
		for _, cap := range latest {
			if cap.LogCoverage == "complete" && cap.ReceiptCoverage != "complete" {
				expected[runtimeAttemptKey(cap)] = runtimeNarrow(cap)
			}
		}
	}
	var pending map[string]Capture
	err = c.runtime.view(ctx, func(s *runtimeState) (any, error) {
		got := map[string]Capture{}
		s.each(s.pending, "", func(h runtimeHeader) { got[runtimeAttemptKey(h.Capture)] = h.Capture })
		return got, s.err
	}, &pending)
	if err == nil && !reflect.DeepEqual(pending, expected) {
		err = errors.New("reserve_legacy_rebuild_would_change_receipt_work")
	}
	return result, err
}
