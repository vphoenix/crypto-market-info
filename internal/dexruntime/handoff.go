package dexruntime

import (
	"context"
	"errors"
	"github.com/vphoenix/crypto-market-info/internal/dex"
	"github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint"
)

func (s *Store) Handoff(ctx context.Context, path string) (runtimecheckpoint.HandoffStatus, error) {
	o := s.owner
	access := func(fn func(*runtimecheckpoint.Store) (any, error)) (any, error) {
		return o.call(ctx, func(o *owner) (any, error) {
			if len(o.inflight) != 0 {
				return nil, ErrBusy
			}
			return fn(o.files)
		})
	}
	save := func() error {
		_, err := o.call(ctx, func(o *owner) (any, error) { return nil, o.checkpoint() })
		return err
	}
	planned := map[string]dex.Block{}
	prepare := func(raw []byte) (runtimecheckpoint.HandoffWork, error) {
		var w frozenWrite
		if err := runtimecheckpoint.UnmarshalTyped(raw, &w); err != nil {
			return runtimecheckpoint.HandoffWork{}, err
		}
		if w.Version != 1 {
			return runtimecheckpoint.HandoffWork{}, runtimecheckpoint.ErrConflict
		}

		id, err := w.identity()
		if err != nil {
			return runtimecheckpoint.HandoffWork{}, err
		}
		if w.Block.Manifest != o.state.Manifest || w.Block.ChainID != 1 {
			return runtimecheckpoint.HandoffWork{}, runtimecheckpoint.ErrConflict
		}
		if w.Kind == "batch" {
			_, exists, e := s.DEXStore.(exactReader).DEXRuntimeBlock(ctx, w.Block)
			if e != nil {
				return runtimecheckpoint.HandoffWork{}, e
			}
			if exists {
				return runtimecheckpoint.HandoffWork{}, errors.New("runtime_handoff_existing_batch_requires_original_plan_or_explicit_rebuild")
			}
		}
		current, known := planned[key(w.Block)]
		if w.Kind != "batch" {
			if !known {
				var e error
				current, known, e = s.DEXStore.(exactReader).DEXRuntimeBlock(ctx, w.Block)
				if e != nil {
					return runtimecheckpoint.HandoffWork{}, e
				}
			}
			if !known || w.Expected == 0 || current.Revision != w.Expected && current.Revision < w.Block.Revision {
				return runtimecheckpoint.HandoffWork{}, ErrStalePlan
			}
		}
		if !known || w.Block.Revision > current.Revision {
			planned[key(w.Block)] = w.Block
		} else {
			planned[key(w.Block)] = current
		}
		payload := raw
		return runtimecheckpoint.HandoffWork{ID: id, Kind: w.Kind, Payload: payload, ExpectedRevision: w.Expected, NewRevision: w.Block.Revision}, err
	}
	commit := func(op runtimecheckpoint.Operation) error {
		value, err := access(func(*runtimecheckpoint.Store) (any, error) { return o.frozen(op) })
		if err != nil {
			return err
		}
		actual, err := writeOriginal(ctx, s.DEXStore, value.(frozenWrite))
		if err != nil {
			return err
		}
		_, err = o.call(context.WithoutCancel(ctx), func(o *owner) (any, error) {
			applied, e := o.files.IsApplied(op)
			if e != nil {
				return nil, e
			}
			if !applied {
				if e = o.state.apply(actual); e != nil {
					return nil, e
				}
			}
			if e = o.files.Applied(op); e != nil {
				return nil, e
			}
			o.dirty = true
			return nil, o.capacity()
		})
		return err
	}
	return runtimecheckpoint.ExecuteHandoff(ctx, path, o.files.Binding(), prepare, access, save, commit)
}

func EncodeHandoffBatch(b dex.Batch) ([]byte, error) {
	return runtimecheckpoint.MarshalTyped(frozenWrite{Version: 1, Kind: "batch", Block: b.Block, Batch: &b})
}
func EncodeHandoffRevision(b dex.Block, expected uint64) ([]byte, error) {
	return runtimecheckpoint.MarshalTyped(frozenWrite{Version: 1, Kind: "block", Block: b, Expected: expected})
}
func EncodeHandoffLogs(b dex.Block, logs []dex.Log, expected uint64) ([]byte, error) {
	return runtimecheckpoint.MarshalTyped(frozenWrite{Version: 1, Kind: "logs", Block: b, Logs: logs, Expected: expected})
}
