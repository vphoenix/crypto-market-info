package lst

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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
		var batch Batch
		if err := decodePersistent(bytes.NewReader(raw), &batch); err == nil {
			if batch.Capture.ManifestHash != c.Manifest.Hash {
				return runtimecheckpoint.HandoffWork{}, runtimecheckpoint.ErrConflict
			}
			if err = Validate(batch); err != nil {
				return runtimecheckpoint.HandoffWork{}, err
			}
			reader, ok := c.Store.(interface {
				LSTCaptureExists(context.Context, Capture) (bool, error)
			})
			if !ok {
				return runtimecheckpoint.HandoffWork{}, errors.New("runtime_handoff_requires_exact_existence_reader")
			}
			exists, e := reader.LSTCaptureExists(ctx, batch.Capture)
			if e != nil {
				return runtimecheckpoint.HandoffWork{}, e
			}
			if exists {
				return runtimecheckpoint.HandoffWork{}, errors.New("runtime_handoff_existing_batch_requires_original_plan_or_explicit_rebuild")
			}
			planned[runtimeCaptureKey(batch.Capture)] = batch.Capture
			return runtimecheckpoint.HandoffWork{Task: batch.Capture.CaptureId.String(), ID: "capture:" + batch.Capture.CaptureId.String(), Kind: "capture", Payload: raw, NewRevision: batch.Capture.Revision}, nil
		}
		var rev runtimeRevision
		if err := runtimecheckpoint.UnmarshalTyped(raw, &rev); err != nil {
			return runtimecheckpoint.HandoffWork{}, err
		}
		if rev.Version != 1 || rev.Capture.ManifestHash != c.Manifest.Hash || rev.Capture.Revision <= rev.Expected {
			return runtimecheckpoint.HandoffWork{}, runtimecheckpoint.ErrConflict
		}
		key := runtimeCaptureKey(rev.Capture)
		old, known := planned[key]
		if !known {
			_, err := access(func(*runtimecheckpoint.Store) (any, error) {
				var e error
				old, known, e = o.state.active.Get(key)
				return nil, e
			})
			if err != nil {
				return runtimecheckpoint.HandoffWork{}, err
			}
		}
		if !known || old.Revision < rev.Expected || CaptureIdentity(old) != CaptureIdentity(rev.Capture) || old.Finality == "finalized" && (!rev.Capture.Canonical || rev.Capture.Finality != "finalized") || old.Revision == rev.Capture.Revision && CanonicalHash(old) != CanonicalHash(rev.Capture) {
			return runtimecheckpoint.HandoffWork{}, runtimecheckpoint.ErrConflict
		}
		if rev.Capture.Revision > old.Revision {
			planned[key] = rev.Capture
		} else {
			planned[key] = old
		}
		return runtimecheckpoint.HandoffWork{Task: rev.Capture.CaptureId.String(), ID: fmt.Sprintf("revision:%s:%d", rev.Capture.CaptureId, rev.Capture.Revision), Kind: "revision", Payload: raw, ExpectedRevision: rev.Expected, NewRevision: rev.Capture.Revision}, nil
	}
	commit := func(op runtimecheckpoint.Operation) error {
		var b Batch
		var rev runtimeRevision
		_, err := access(func(*runtimecheckpoint.Store) (any, error) {
			var e error
			if op.Kind == "capture" {
				b, e = o.frozenBatch(op)
			} else {
				rev, e = o.frozenRevision(op)
			}
			return nil, e
		})
		if err != nil {
			return err
		}
		cap := rev.Capture
		if op.Kind == "capture" {
			cap = b.Capture
			err = c.Store.WriteLST(ctx, b)
		} else {
			err = c.Store.WriteLSTRevision(ctx, cap)
		}
		if err != nil {
			return err
		}
		actual, err := runtimeCurrent(ctx, c.Store, cap)
		if err != nil {
			return err
		}
		return o.request(context.WithoutCancel(ctx), runtimeRequest{Apply: func(o *runtimeOwner) (any, error) { return nil, o.apply(op, actual) }}, nil)
	}
	return runtimecheckpoint.ExecuteHandoff(ctx, path, o.files.Binding(), prepare, access, save, commit)
}

func EncodeRuntimeHandoffBatch(b Batch) ([]byte, error) {
	var raw bytes.Buffer
	err := encodePersistent(&raw, b)
	return raw.Bytes(), err
}
func EncodeRuntimeHandoffRevision(c Capture, expected uint64) ([]byte, error) {
	return runtimecheckpoint.MarshalTyped(runtimeRevision{1, expected, c})
}
