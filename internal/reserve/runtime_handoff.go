package reserve

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
		err := o.request(ctx, runtimeRequest{Apply: func(o *runtimeOwner) (any, error) {
			if len(o.inflight) != 0 {
				return nil, errors.New("runtime_handoff_inflight")
			}
			var e error
			value, e = fn(o.files)
			return nil, e
		}}, nil)
		return value, err
	}
	save := func() error { return o.request(ctx, runtimeRequest{Save: true}, nil) }
	planned := map[string]Capture{}
	prepare := func(raw []byte) (runtimecheckpoint.HandoffWork, error) {
		var w runtimeWrite
		if err := runtimecheckpoint.UnmarshalTyped(raw, &w); err != nil {
			return runtimecheckpoint.HandoffWork{}, err
		}
		if w.Version != 1 {
			return runtimecheckpoint.HandoffWork{}, runtimecheckpoint.ErrConflict
		}
		id, kind, err := w.identity()
		if err != nil {
			return runtimecheckpoint.HandoffWork{}, err
		}
		cap := w.Revision
		if w.Batch != nil {
			cap = &w.Batch.Capture
		}
		known := false
		for _, scope := range runtimeScopeHashes(c.Manifest, c.RelatedManifests) {
			if cap.ManifestHash == scope {
				known = true
			}
		}
		if !known {
			return runtimecheckpoint.HandoffWork{}, runtimecheckpoint.ErrConflict
		}
		if w.Batch != nil {
			reader, ok := c.Store.(interface {
				ReserveCaptureExists(context.Context, Capture) (bool, error)
			})
			if !ok {
				return runtimecheckpoint.HandoffWork{}, errors.New("runtime_handoff_requires_exact_existence_reader")
			}
			exists, e := reader.ReserveCaptureExists(ctx, *cap)
			if e != nil {
				return runtimecheckpoint.HandoffWork{}, e
			}
			if exists {
				return runtimecheckpoint.HandoffWork{}, errors.New("runtime_handoff_existing_batch_requires_original_plan_or_explicit_rebuild")
			}
		}
		key := runtimeIdentity(*cap)
		old, known := planned[key]
		if w.Revision != nil && !known {
			_, err = access(func(*runtimecheckpoint.Store) (any, error) {
				header, ok := o.state.get(o.state.active, key)
				old, known = header.Capture, ok
				return nil, o.state.err
			})
			if err != nil {
				return runtimecheckpoint.HandoffWork{}, err
			}
		}
		if cap.ChainId != 1 || w.Revision != nil && (!known || old.Revision < w.Expected || runtimeFrozen(runtimeNarrow(old)) != runtimeFrozen(runtimeNarrow(*cap)) || old.Finality == "finalized" && (!cap.Canonical || cap.Finality != "finalized") || old.Revision == cap.Revision && ID(runtimeNarrow(old)) != ID(runtimeNarrow(*cap))) {
			return runtimecheckpoint.HandoffWork{}, runtimecheckpoint.ErrConflict
		}
		if !known || cap.Revision > old.Revision {
			planned[key] = *cap
		} else {
			planned[key] = old
		}
		payload := raw
		revision := uint64(0)
		if w.Revision != nil {
			revision = cap.Revision
		}
		return runtimecheckpoint.HandoffWork{Task: runtimeIdentity(*cap), ID: id, Kind: kind, Payload: payload, ExpectedRevision: w.Expected, NewRevision: revision}, err
	}
	commit := func(op runtimecheckpoint.Operation) error {
		var w runtimeWrite
		_, err := access(func(*runtimecheckpoint.Store) (any, error) { var e error; w, e = o.frozen(op); return nil, e })
		if err != nil {
			return err
		}
		cap := w.Revision
		if w.Batch != nil {
			cap = &w.Batch.Capture
			err = c.Store.WriteReserveBatch(ctx, *w.Batch)
		} else {
			err = c.Store.WriteReserveRevision(ctx, *cap)
		}
		if err != nil {
			return err
		}
		actual, err := runtimeCurrent(ctx, c.Store, *cap)
		if err != nil {
			return err
		}
		return o.request(context.WithoutCancel(ctx), runtimeRequest{Apply: func(o *runtimeOwner) (any, error) { return nil, o.apply(op, &actual) }}, nil)
	}
	return runtimecheckpoint.ExecuteHandoff(ctx, path, o.files.Binding(), prepare, access, save, commit)
}

func EncodeRuntimeHandoffBatch(b Batch) ([]byte, error) {
	return runtimecheckpoint.MarshalTyped(runtimeWrite{Version: 1, Batch: &b})
}
func EncodeRuntimeHandoffRevision(c Capture, expected uint64) ([]byte, error) {
	return runtimecheckpoint.MarshalTyped(runtimeWrite{Version: 1, Revision: &c, Expected: expected})
}
