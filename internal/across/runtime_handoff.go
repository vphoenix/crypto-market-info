package across

import (
	"context"
	"errors"
	"github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint"
)

func (c *Collector) HandoffRuntime(ctx context.Context, path string) (runtimecheckpoint.HandoffStatus, error) {
	if c.runtime == nil {
		return runtimecheckpoint.HandoffStatus{}, errors.New("runtime_not_open")
	}
	o := c.runtime
	access := func(fn func(*runtimecheckpoint.Store) (any, error)) (any, error) {
		var value any
		_, err := o.request(ctx, runtimeRequest{view: func(*runtimeState) (any, error) {
			if len(o.inflight) != 0 {
				return nil, errors.New("runtime_handoff_inflight")
			}
			var e error
			value, e = fn(o.store)
			return true, e
		}})
		return value, err
	}
	save := func() error { _, err := o.request(ctx, runtimeRequest{checkpoint: true}); return err }
	planned := map[string]Capture{}
	completed := func(chain, block uint64) bool {
		if o.state.finalizedDecodedBlock(chain, block) {
			return true
		}
		for _, cap := range planned {
			if cap.ChainId == chain && cap.Finality == "finalized" && runtimeDecoded(cap) && anchored(cap) && *cap.FromBlock <= block && block <= *cap.ToBlock {
				return true
			}
		}
		return false
	}
	prepare := func(raw []byte) (runtimecheckpoint.HandoffWork, error) {
		var w runtimeWrite
		if err := runtimecheckpoint.UnmarshalTyped(raw, &w); err != nil {
			return runtimecheckpoint.HandoffWork{}, err
		}
		if w.Version != 1 {
			return runtimecheckpoint.HandoffWork{}, runtimecheckpoint.ErrConflict
		}
		if w.Probe != nil || w.Partial != nil {
			return runtimecheckpoint.HandoffWork{}, runtimecheckpoint.ErrConflict
		}
		id, kind, err := w.identity()
		if err != nil {
			return runtimecheckpoint.HandoffWork{}, err
		}
		cap := w.Revision
		if w.Batch != nil {
			cap = &w.Batch.Capture
			if err = Validate(*w.Batch); err != nil {
				return runtimecheckpoint.HandoffWork{}, err
			}
		}
		if cap.ManifestHash != c.Manifest.Hash {
			return runtimecheckpoint.HandoffWork{}, runtimecheckpoint.ErrConflict
		}
		if w.Batch != nil {
			reader, ok := c.Store.(interface {
				AcrossCaptureExists(context.Context, Capture) (bool, error)
			})
			if !ok {
				return runtimecheckpoint.HandoffWork{}, errors.New("runtime_handoff_requires_exact_existence_reader")
			}
			exists, e := reader.AcrossCaptureExists(ctx, *cap)
			if e != nil {
				return runtimecheckpoint.HandoffWork{}, e
			}
			if exists {
				return runtimecheckpoint.HandoffWork{}, errors.New("runtime_handoff_existing_batch_requires_original_plan_or_explicit_rebuild")
			}
		}
		// Validate the finite list in order, including a predecessor imported by
		// an earlier item. No SQL has been admitted during this pass.
		old, known := planned[cap.CaptureId]
		_, err = access(func(*runtimecheckpoint.Store) (any, error) {
			if o.state.Chains[cap.ChainId] == nil {
				return nil, runtimecheckpoint.ErrConflict
			}
			if w.Batch != nil {
				for _, d := range w.Batch.Deposits {
					if completed(d.ChainId, d.BlockNumber) {
						return nil, errors.New("runtime_handoff_completed_range_requires_explicit_rebuild")
					}
				}
				for _, f := range w.Batch.Fills {
					if completed(f.ChainId, f.BlockNumber) {
						return nil, errors.New("runtime_handoff_completed_range_requires_explicit_rebuild")
					}
				}
				for _, r := range w.Batch.Refunds {
					if completed(r.ChainId, r.BlockNumber) {
						return nil, errors.New("runtime_handoff_completed_range_requires_explicit_rebuild")
					}
				}
			} else if !known {
				old, known = o.state.captureGet(cap.CaptureId, false)
				if !known {
					old, known = o.state.captureGet(cap.CaptureId, true)
				}
				if !known {
					old, known = o.state.Retired[cap.CaptureId]
				}
				if ch := o.state.Chains[cap.ChainId]; !known && ch.Anchor != nil && ch.Anchor.CaptureId == cap.CaptureId {
					old, known = *ch.Anchor, true
				}
			}
			return nil, o.state.ioError()
		})
		if err != nil {
			return runtimecheckpoint.HandoffWork{}, err
		}
		if w.Revision != nil {
			if !known || old.Revision < w.Expected || runtimeCaptureMembers(old) != runtimeCaptureMembers(*cap) || old.Finality == "finalized" && (!cap.Canonical || cap.Finality != "finalized") || old.Revision == cap.Revision && ID(old) != ID(*cap) {
				return runtimecheckpoint.HandoffWork{}, runtimecheckpoint.ErrConflict
			}
		}
		if !known || cap.Revision > old.Revision {
			planned[cap.CaptureId] = *cap
		} else {
			planned[cap.CaptureId] = old
		}
		payload := raw
		revision := uint64(0)
		if w.Revision != nil {
			revision = cap.Revision
		}
		return runtimecheckpoint.HandoffWork{Task: cap.CaptureId, ID: id, Kind: kind, Payload: payload, ExpectedRevision: w.Expected, NewRevision: revision}, err
	}
	commit := func(op runtimecheckpoint.Operation) error {
		var w runtimeWrite
		_, err := access(func(files *runtimecheckpoint.Store) (any, error) { var e error; w, e = o.frozen(op); return nil, e })
		if err != nil {
			return err
		}
		cap := w.Revision
		if w.Batch != nil {
			cap = &w.Batch.Capture
			err = c.Store.WriteAcrossBatch(ctx, *w.Batch)
		} else {
			err = c.Store.WriteAcrossRevision(ctx, *cap)
		}
		if err != nil {
			return err
		}
		actual, err := runtimeCurrentCapture(ctx, c.Store, *cap)
		if err != nil {
			return err
		}
		_, err = o.request(context.WithoutCancel(ctx), runtimeRequest{complete: &op, confirmed: &actual})
		return err
	}
	return runtimecheckpoint.ExecuteHandoff(ctx, path, o.store.Binding(), prepare, access, save, commit)
}

// EncodeRuntimeHandoffBatch preserves binary string hashes and known-zero fields.
func EncodeRuntimeHandoffBatch(b Batch) ([]byte, error) {
	return runtimecheckpoint.MarshalTyped(runtimeWrite{Version: 1, Batch: &b})
}
func EncodeRuntimeHandoffRevision(c Capture, expected uint64) ([]byte, error) {
	return runtimecheckpoint.MarshalTyped(runtimeWrite{Version: 1, Revision: &c, Expected: expected})
}
