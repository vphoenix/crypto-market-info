package across

import (
	"context"
	"errors"

	"github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint"
)

type RuntimeInspection struct {
	Conversion                                    RuntimeConversionReport
	Files                                         runtimecheckpoint.Inspection
	RawCursors                                    map[uint64]uint64
	Active, Partial, Receipts, Orders, ProbeTasks int
}

// InspectRuntime is for a stopped service under its maintenance lock, after
// OpenRuntime has retried and verified every uncovered original operation.
func (c *Collector) InspectRuntime(ctx context.Context, exportDir string) (RuntimeInspection, error) {
	var out RuntimeInspection
	if c.runtime == nil {
		return out, errors.New("runtime_not_open")
	}
	if _, err := c.runtime.request(ctx, runtimeRequest{checkpoint: true}); err != nil {
		return out, err
	}
	err := c.runtime.view(ctx, func(s *runtimeState) (any, error) {
		if len(c.runtime.inflight) != 0 {
			return nil, errors.New("runtime_maintenance_has_inflight_work")
		}
		if exportDir != "" {
			if err := c.runtime.store.Export(exportDir); err != nil {
				return nil, err
			}
		}
		files, err := c.runtime.store.Inspect()
		return RuntimeInspection{Conversion: s.Conversion, Files: files, RawCursors: s.Watch.Cursors, Active: s.captureCount(false), Partial: s.captureCount(true), Receipts: s.receiptCount(), Orders: len(s.Watch.Orders), ProbeTasks: len(s.Watch.Tasks)}, err
	}, &out)
	return out, err
}

type RuntimeRollbackReport struct {
	Current                                           RuntimeInspection
	RebuiltOrders, RebuiltProbeTasks, RebuiltReceipts int
	RebuiltRawCursors                                 map[uint64]uint64
	Status                                            string
}

// RollbackRuntime validates a controlled reconstruction from the CURRENT
// committed database, after recovering frozen pending operations. It does not
// restore an old local image or erase the new state. The caller holds the same
// process/generation fence used for conversion throughout this procedure.
func (c *Collector) RollbackRuntime(ctx context.Context, exportDir string) (RuntimeRollbackReport, error) {
	var out RuntimeRollbackReport
	if exportDir == "" {
		return out, errors.New("runtime_rollback_requires_current_state_export")
	}
	current, err := c.InspectRuntime(ctx, exportDir)
	if err != nil {
		return out, err
	}
	legacy := &Collector{Manifest: c.Manifest, Store: c.Store}
	w, err := legacy.restore(ctx)
	if err != nil {
		return out, err
	}
	if err = legacy.PrepareRepair(ctx); err != nil {
		return out, err
	}
	out = RuntimeRollbackReport{Current: current, RebuiltOrders: len(w.Orders), RebuiltProbeTasks: len(w.Tasks), RebuiltReceipts: len(legacy.receiptQueue), RebuiltRawCursors: w.Cursors, Status: "current committed input verified for explicit legacy rebuild; source observations during downtime remain missing"}
	return out, nil
}
