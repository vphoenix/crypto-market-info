package lst

import (
	"context"
	"errors"
	"os"
	"path/filepath"

	"github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint"
)

type RuntimeInspection struct {
	Files         runtimecheckpoint.Inspection
	Conversion    RuntimeConversionReport
	Cursors       map[uint64]logProgress
	Active, Seeds uint64
}

func (c *Collector) InspectRuntime(ctx context.Context, exportDir string) (RuntimeInspection, error) {
	var out RuntimeInspection
	if c.runtime == nil {
		return out, errors.New("lst_runtime_not_open")
	}
	if err := c.runtime.request(ctx, runtimeRequest{Save: true}, nil); err != nil {
		return out, err
	}
	err := c.runtime.request(ctx, runtimeRequest{Apply: func(o *runtimeOwner) (any, error) {
		if len(o.inflight) != 0 {
			return nil, errors.New("lst_runtime_inflight")
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
		return RuntimeInspection{files, o.state.Conversion, o.state.Cursors, o.state.active.Len(), o.state.seeds.Len()}, nil
	}}, &out)
	return out, err
}

// Rollback exports today's recovery chain, confirms current database work using
// the original reconstruction rules, then writes the current legacy cursors.
// It never restores a checkpoint made before the upgrade.
func (c *Collector) RollbackRuntime(ctx context.Context, exportDir string) (RuntimeInspection, error) {
	var out RuntimeInspection
	if exportDir == "" {
		return out, errors.New("lst_runtime_rollback_requires_export")
	}
	if err := c.FlushPending(ctx); err != nil {
		return out, err
	}
	out, err := c.InspectRuntime(ctx, exportDir)
	if err != nil {
		return out, err
	}
	// A disposable comparison copy keeps the exported chain unchanged. No normal
	// runner calls this controlled historical reconstruction.
	scratch, err := os.MkdirTemp(filepath.Dir(exportDir), ".lst-rollback-")
	if err != nil {
		return out, err
	}
	defer os.RemoveAll(scratch)
	compareDir := filepath.Join(scratch, "state")
	if _, err = c.InspectRuntime(ctx, compareDir); err != nil {
		return out, err
	}
	var binding runtimecheckpoint.Binding
	if err = c.runtime.view(ctx, func(s *runtimeState) (any, error) { return s.Binding, s.err }, &binding); err != nil {
		return out, err
	}
	files, err := runtimecheckpoint.Open(compareDir, binding, 64<<20)
	if err != nil {
		return out, err
	}
	defer files.Close()
	var state runtimeState
	if err = runtimecheckpoint.UnmarshalTyped(files.State(), &state); err != nil {
		return out, err
	}
	if err = state.open(files); err != nil {
		return out, err
	}
	original, err := c.originalRuntimeCursors()
	if err != nil {
		return out, err
	}
	p := runtimeConversion{State: state, OriginalCursors: original, Inputs: map[string]string{}, Facts: map[string]runtimecheckpoint.Reference{}, Report: RuntimeConversionReport{FrozenAt: Now()}}
	pager, ok := c.Store.(runtimeCapturePager)
	if !ok {
		return out, errors.New("lst_runtime_requires_conversion_pager")
	}
	for after := []byte(nil); ; {
		caps, next, e := pager.LSTCapturePage(ctx, c.Manifest.Hash, after, 500)
		if e != nil {
			return out, e
		}
		if len(caps) == 0 {
			break
		}
		for _, cap := range caps {
			id := cap.CaptureId.String()
			p.Inputs[id] = CanonicalHash(cap)
			if runtimeNeedsFacts(cap, p.Report.FrozenAt) {
				b, e := c.Store.LSTBatch(ctx, cap)
				if e != nil {
					return out, e
				}
				if e = Validate(b); e != nil {
					return out, e
				}
				raw, e := runtimecheckpoint.MarshalTyped(b)
				if e != nil {
					return out, e
				}
				ref, e := files.PutObject(raw)
				if e != nil {
					return out, e
				}
				p.Facts[id] = ref
			}
		}
		after = next
	}
	if err = c.compareRuntimeConversion(ctx, &p, files); err != nil {
		return out, err
	}
	for segment, cursor := range out.Cursors {
		old := *c
		old.LiveLogsFromBlock = segment
		if err = writeGob(old.liveLogCursorPath(), cursor); err != nil {
			return out, err
		}
	}
	return out, nil
}
