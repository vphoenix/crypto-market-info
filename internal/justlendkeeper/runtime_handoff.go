package justlendkeeper

import (
	"context"
	"errors"
	"github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint"
)

func (c *Collector) applyRuntimeImport(ctx context.Context, op runtimecheckpoint.Operation) error {
	files := c.runtime.files
	raw, err := files.ReadObject(op.Payload)
	if err != nil {
		return err
	}
	var b Batch
	if err = runtimecheckpoint.UnmarshalTyped(raw, &b); err != nil {
		return err
	}
	if op.Kind != "keeper_import" || op.ID != "import:"+b.Capture.CaptureId.String() || b.Capture.ConfigHash != c.Config.Hash() {
		return runtimecheckpoint.ErrConflict
	}
	if err = Validate(b); err != nil {
		return err
	}
	if err = c.Store.WriteKeeperBatch(ctx, b); err != nil {
		return err
	}
	reader, ok := c.Store.(CollectionReader)
	if !ok {
		return errors.New("keeper_import_requires_exact_reader")
	}
	actual, err := reader.KeeperBatch(ctx, b.Capture)
	if err != nil {
		return err
	}
	if err = Validate(actual); err != nil {
		return err
	}
	if !captureEqual(b.Capture, actual.Capture) {
		return runtimecheckpoint.ErrConflict
	}
	applied, err := files.IsApplied(op)
	if err != nil {
		return err
	}
	if !applied {
		if err = c.runtimeCommitted(actual); err != nil {
			return err
		}
		if actual.Capture.CaptureKind == "enrichment" {
			c.ApplyEvents(actual.Events)
		}
		if err = c.advanceEvidence(ctx, actual.Capture.EventKind); err != nil {
			return err
		}
	}
	if err = files.Applied(op); err != nil {
		return err
	}
	return c.Save()
}
func (c *Collector) HandoffRuntime(ctx context.Context, path string) (runtimecheckpoint.HandoffStatus, error) {
	if c.runtime == nil {
		return runtimecheckpoint.HandoffStatus{}, errors.New("runtime_not_open")
	}
	if len(c.State.Ops) != 0 {
		return runtimecheckpoint.HandoffStatus{}, errors.New("keeper_import_requires_existing_operations_drained")
	}
	files := c.runtime.files
	access := func(fn func(*runtimecheckpoint.Store) (any, error)) (any, error) {
		if c.runtime.poison != nil {
			return nil, c.runtime.poison
		}
		return fn(files)
	}
	plannedParents := map[string]bool{}
	prepare := func(raw []byte) (runtimecheckpoint.HandoffWork, error) {
		var b Batch
		if err := runtimecheckpoint.UnmarshalTyped(raw, &b); err != nil {
			return runtimecheckpoint.HandoffWork{}, err
		}
		if b.Capture.ConfigHash != c.Config.Hash() || (b.Capture.CaptureKind != "event_index" && b.Capture.CaptureKind != "enrichment") {
			return runtimecheckpoint.HandoffWork{}, runtimecheckpoint.ErrConflict
		}
		if err := Validate(b); err != nil {
			return runtimecheckpoint.HandoffWork{}, err
		}
		if b.Capture.CaptureKind == "event_index" && (b.Capture.CaptureMode == "live" || b.Capture.CaptureMode == "catchup") && b.Capture.RequestedFrom != nil && b.Capture.RequestedFrom.Before(c.State.EvidenceCursors[b.Capture.EventKind]) {
			return runtimecheckpoint.HandoffWork{}, errors.New("keeper_import_overlaps_completed_frontier_requires_explicit_rebuild")
		}
		if b.Capture.CaptureKind == "enrichment" {
			if b.Capture.ParentCaptureId == nil {
				return runtimecheckpoint.HandoffWork{}, runtimecheckpoint.ErrConflict
			}
			key := b.Capture.ParentCaptureId.String()
			if !plannedParents[key] {
				parent, exists, err := c.State.Runtime.parents.Get(key)
				if err != nil {
					return runtimecheckpoint.HandoffWork{}, err
				}
				if !exists || parent.Complete {
					return runtimecheckpoint.HandoffWork{}, errors.New("keeper_import_requires_unfinished_parent_before_child")
				}
			}
		} else {
			plannedParents[b.Capture.CaptureId.String()] = true
		}
		reader, ok := c.Store.(interface {
			KeeperCaptureExists(context.Context, Capture) (bool, error)
		})
		if !ok {
			return runtimecheckpoint.HandoffWork{}, errors.New("runtime_handoff_requires_exact_existence_reader")
		}
		exists, e := reader.KeeperCaptureExists(ctx, b.Capture)
		if e != nil {
			return runtimecheckpoint.HandoffWork{}, e
		}
		if exists {
			return runtimecheckpoint.HandoffWork{}, errors.New("runtime_handoff_existing_batch_requires_original_plan_or_explicit_rebuild")
		}
		payload := raw
		return runtimecheckpoint.HandoffWork{ID: "import:" + b.Capture.CaptureId.String(), Kind: "keeper_import", Payload: payload}, nil
	}
	return runtimecheckpoint.ExecuteHandoff(ctx, path, files.Binding(), prepare, access, c.Save, func(op runtimecheckpoint.Operation) error { return c.applyRuntimeImport(ctx, op) })
}

func EncodeRuntimeHandoffBatch(b Batch) ([]byte, error) { return runtimecheckpoint.MarshalTyped(b) }
