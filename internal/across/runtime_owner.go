package across

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint"
)

// runtimeWrite is a typed recovery operation. Source observations and evidence
// are frozen before Begin returns; retries use these bytes without new IDs or
// renewed observation times. Payloads are not retained in the hot state.
type runtimeWrite struct {
	Version  uint32
	Batch    *Batch
	Revision *Capture
	Expected uint64
	Probe    *runtimeProbeTransition
	Partial  *runtimePartialTransition
}

func (w runtimeWrite) identity() (string, string, error) {
	if w.Version != 1 || (w.Batch == nil) == (w.Revision == nil) || w.Probe != nil && (w.Batch == nil || len(w.Batch.Probes) != 1) {
		return "", "", errors.New("runtime_operation_shape")
	}
	if w.Partial != nil && (w.Batch == nil || w.Probe != nil || w.Batch.Capture.CaptureKind != "logs" || w.Partial.Key == "" || w.Partial.Window.ChainID != w.Batch.Capture.ChainId || w.Partial.Window.From > w.Partial.Window.To) {
		return "", "", errors.New("runtime_partial_operation_shape")
	}
	if w.Revision != nil && w.Revision.Revision <= w.Expected {
		return "", "", errors.New("runtime_revision_not_newer")
	}
	if w.Batch != nil {
		return "capture:" + Hex(w.Batch.Capture.CaptureId), "capture", nil
	}
	return fmt.Sprintf("revision:%s:%d", Hex(w.Revision.CaptureId), w.Revision.Revision), "revision", nil
}
func (w runtimeWrite) digest() string {
	return ID(w)
}

type runtimeResponse struct {
	raw []byte
	op  runtimecheckpoint.Operation
	err error
}
type runtimeRequest struct {
	begin             []byte
	complete          *runtimecheckpoint.Operation
	confirmed         *Capture
	view              func(*runtimeState) (any, error)
	change            func(*runtimeState) (any, error)
	checkpoint, close bool
	result            chan runtimeResponse
}
type runtimeOwner struct {
	requests  chan runtimeRequest
	done      chan struct{}
	store     *runtimecheckpoint.Store
	state     runtimeState
	manifest  Manifest
	inflight  map[string]bool
	poison    error
	dirty     bool
	progress  func(string)
	wake      chan struct{}
	leased    map[string]bool
	telemetry runtimeTelemetry
}

// newRuntimeOwner takes ownership of files. Recovery is finished before worker
// admission; there is no concurrent goroutine while original transactions are
// retried and verified. The owner loop itself never calls SQL/RPC or holds a
// mutex while syncing files.
func newRuntimeOwner(ctx context.Context, files *runtimecheckpoint.Store, m Manifest, db Store, progress func(string)) (*runtimeOwner, error) {
	o := &runtimeOwner{requests: make(chan runtimeRequest, 16), done: make(chan struct{}), store: files, manifest: m, inflight: map[string]bool{}, progress: progress}
	o.wake = make(chan struct{}, 1)
	o.leased = map[string]bool{}
	o.telemetry.At = time.Now().UTC()
	if err := runtimecheckpoint.UnmarshalTyped(files.State(), &o.state); err != nil {
		return nil, err
	}
	if o.state.Version != 1 || o.state.Manifest != m.Hash {
		return nil, runtimecheckpoint.ErrRecovery
	}
	if err := o.state.enableReceiptPages(files); err != nil {
		return nil, err
	}
	if err := o.state.enableCapturePages(files); err != nil {
		return nil, err
	}
	if err := o.state.enablePendingPages(files); err != nil {
		return nil, err
	}
	if o.state.Conversion.Version != 0 {
		if err := o.state.Conversion.InputInventory.Verify(files); err != nil {
			return nil, err
		}
	}
	if o.state.Conversion.Verified {
		if err := o.state.validate(m, files.Binding()); err != nil {
			return nil, err
		}
	}
	for after := uint64(0); ; {
		page := files.Pending(after, 16)
		if len(page) == 0 {
			break
		}
		for _, op := range page {
			w, err := o.frozen(op)
			if err != nil {
				return nil, err
			}
			var actual *Capture
			if w.Batch != nil {
				// An absent initial read cannot prove a timed-out transaction
				// ended. The original idempotent transaction pins its identity.
				if err = db.WriteAcrossBatch(ctx, *w.Batch); err != nil {
					return nil, err
				}
				verified, err := db.AcrossBatch(ctx, w.Batch.Capture)
				if err != nil {
					return nil, err
				}
				if err = Validate(verified); err != nil {
					return nil, err
				}
				actual = &verified.Capture
			} else {
				if err = db.WriteAcrossRevision(ctx, *w.Revision); err != nil {
					return nil, err
				}
				cap, err := runtimeCurrentCapture(ctx, db, *w.Revision)
				if err != nil {
					return nil, err
				}
				actual = &cap
			}
			if err = o.apply(op, actual); err != nil {
				return nil, err
			}
			after = op.Sequence
			if files.Due() {
				if err = o.checkpoint(); err != nil {
					return nil, err
				}
			}
		}
	}
	if files.Due() {
		if err := o.checkpoint(); err != nil {
			return nil, err
		}
	}
	// The existing startup rule retries a successfully fetched unknown fee
	// once per run. Keep its unknown status and identity in the checkpoint.
	o.state.eachReceipt(func(key string, receipt runtimeReceipt) {
		if receipt.Known && !receipt.FeeComplete && !receipt.Pending {
			receipt.Pending = true
			receipt.RetryUnknown = true
			o.state.receiptSet(key, receipt)
			o.dirty = true
		}
	})
	o.state.eachRetry(func(key string, r runtimeRetry) {
		if r.Attempted {
			r.Attempted = false
			o.state.retrySet(key, r)
			o.dirty = true
		}
	})
	if o.state.ioError() != nil {
		return nil, o.state.ioError()
	}
	if err := o.capacity(); err != nil {
		return nil, err
	}
	go o.run()
	return o, nil
}

// Accurate key lookup is optional for legacy in-memory stores. MySQL supplies
// the narrow header lookup; the fallback reads exactly one validated batch.
type runtimeCaptureReader interface {
	AcrossCapture(context.Context, Capture) (Capture, error)
}

func runtimeCurrentCapture(ctx context.Context, db Store, want Capture) (Capture, error) {
	if reader, ok := db.(runtimeCaptureReader); ok {
		return reader.AcrossCapture(ctx, want)
	}
	b, err := db.AcrossBatch(ctx, want)
	return b.Capture, err
}

func (o *runtimeOwner) frozen(op runtimecheckpoint.Operation) (runtimeWrite, error) {
	var w runtimeWrite
	raw, err := o.store.ReadObject(op.Payload)
	if err != nil {
		return w, err
	}
	if err = runtimecheckpoint.UnmarshalTyped(raw, &w); err != nil {
		return w, err
	}
	id, kind, err := w.identity()
	if err != nil || id != op.ID || kind != op.Kind {
		return w, runtimecheckpoint.ErrConflict
	}
	if w.Batch != nil {
		if err = Validate(*w.Batch); err != nil {
			return w, err
		}
	}
	return w, nil
}
func (o *runtimeOwner) begin(raw []byte) (runtimecheckpoint.Operation, error) {
	var w runtimeWrite
	if err := runtimecheckpoint.UnmarshalTyped(raw, &w); err != nil {
		return runtimecheckpoint.Operation{}, err
	}
	id, kind, err := w.identity()
	if err != nil {
		return runtimecheckpoint.Operation{}, err
	}
	if err = o.capacity(); err != nil {
		return runtimecheckpoint.Operation{}, err
	}
	cap := w.Revision
	if w.Batch != nil {
		cap = &w.Batch.Capture
	}
	if cap.ManifestHash != o.manifest.Hash || o.state.Chains[cap.ChainId] == nil {
		return runtimecheckpoint.Operation{}, runtimecheckpoint.ErrConflict
	}
	if o.inflight[id] {
		return runtimecheckpoint.Operation{}, runtimecheckpoint.ErrBusy
	}
	if old, ok := o.store.Find(id); ok {
		frozen, err := o.frozen(old)
		if err != nil {
			return old, err
		}
		if frozen.digest() != w.digest() {
			return old, runtimecheckpoint.ErrConflict
		}
		o.inflight[id] = true
		return old, nil
	}
	if w.Batch != nil {
		if err = Validate(*w.Batch); err != nil {
			return runtimecheckpoint.Operation{}, err
		}
	}
	if o.store.Due() {
		if err = o.checkpoint(); err != nil {
			return runtimecheckpoint.Operation{}, err
		}
	}
	ref, err := o.store.PutObject(raw)
	if err != nil {
		return runtimecheckpoint.Operation{}, err
	}
	var revision uint64
	if w.Revision != nil {
		revision = w.Revision.Revision
	}
	op, err := o.store.Begin(id, kind, ref, w.Expected, revision, cap.CaptureId)
	if err != nil {
		return op, err
	}
	o.inflight[id] = true
	return op, nil
}
func (o *runtimeOwner) apply(op runtimecheckpoint.Operation, actual *Capture) error {
	already, err := o.store.IsApplied(op)
	if err != nil {
		return err
	}
	if already {
		delete(o.inflight, op.ID)
		return o.store.Applied(op)
	}
	w, err := o.frozen(op)
	if err != nil {
		return err
	}
	if w.Batch != nil {
		b := *w.Batch
		if actual != nil {
			if runtimeCaptureMembers(*actual) != runtimeCaptureMembers(b.Capture) || actual.Revision < b.Capture.Revision {
				return runtimecheckpoint.ErrConflict
			}
			if actual.Revision == b.Capture.Revision && ID(*actual) != ID(b.Capture) {
				return runtimecheckpoint.ErrConflict
			}
		}
		o.state.Retired[b.Capture.CaptureId] = b.Capture
		if err = o.state.batch(b, o.manifest, false); err != nil {
			return err
		}
		if w.Probe != nil {
			if err = o.state.probeCommitted(*w.Probe, b.Probes[0]); err != nil {
				return err
			}
		}
		if w.Partial != nil {
			o.state.partialCommitted(*w.Partial, b.Capture)
		}
		if actual != nil && actual.Revision > b.Capture.Revision {
			if err = o.state.revise(b.Capture.Revision, *actual); err != nil {
				return err
			}
			o.state.Retired[b.Capture.CaptureId] = *actual
		}
	} else {
		if actual == nil || actual.CaptureId != w.Revision.CaptureId || actual.Revision < w.Revision.Revision || runtimeCaptureMembers(*actual) != runtimeCaptureMembers(*w.Revision) {
			return runtimecheckpoint.ErrConflict
		}
		if err = o.state.revise(w.Expected, *actual); err != nil {
			return err
		}
		o.state.Retired[actual.CaptureId] = *actual
	}
	if o.state.ioError() != nil {
		return o.state.ioError()
	}
	if err = o.capacity(); err != nil {
		return err
	}
	if err = o.store.Applied(op); err != nil {
		return err
	}
	o.dirty = true
	delete(o.inflight, op.ID)
	select {
	case o.wake <- struct{}{}:
	default:
	}
	if o.store.Due() {
		return o.checkpoint()
	}
	return nil
}
func runtimeCaptureMembers(c Capture) string {
	c.Canonical = false
	c.Finality = ""
	c.Reason = ""
	c.Revision = 0
	return ID(c)
}
func (o *runtimeOwner) checkpoint() error {
	if !o.dirty && !o.store.Due() {
		return nil
	}
	retained := o.store.Retained()
	for id := range o.state.Retired {
		keep := false
		key := Hex(id)
		for _, op := range retained {
			if op.ID == "capture:"+key || strings.HasPrefix(op.ID, "revision:"+key+":") {
				keep = true
				break
			}
		}
		if !keep {
			delete(o.state.Retired, id)
		}
	}
	refs, err := o.state.receiptReferences()
	if err != nil {
		return err
	}
	captureRefs, err := o.state.captureReferences()
	if err != nil {
		return err
	}
	refs = append(refs, captureRefs...)
	pendingRefs, err := o.state.pendingReferences()
	if err != nil {
		return err
	}
	refs = append(refs, pendingRefs...)
	raw, err := runtimecheckpoint.MarshalTyped(o.state)
	if err != nil {
		return err
	}
	if err = o.store.Checkpoint(raw, refs); err != nil {
		return err
	}
	o.dirty = false
	return nil
}
func (o *runtimeOwner) run() {
	defer close(o.done)
	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()
	for {
		select {
		case req := <-o.requests:
			response := runtimeResponse{}
			if o.poison != nil {
				response.err = o.poison
			} else {
				switch {
				case req.begin != nil:
					response.op, response.err = o.begin(req.begin)
				case req.complete != nil:
					response.err = o.apply(*req.complete, req.confirmed)
				case req.view != nil:
					var value any
					value, response.err = req.view(&o.state)
					if response.err == nil {
						response.raw, response.err = runtimecheckpoint.MarshalTyped(value)
					}
				case req.change != nil:
					var value any
					value, response.err = req.change(&o.state)
					if response.err == nil {
						changed, known := value.(bool)
						if !known || changed {
							o.dirty = true
						}
						response.raw, response.err = runtimecheckpoint.MarshalTyped(value)
					}
					if response.err == nil && req.checkpoint {
						response.err = o.checkpoint()
					}
				case req.checkpoint || req.close:
					response.err = o.checkpoint()
				}
			}
			if response.err == nil {
				response.err = o.state.ioError()
			}
			if response.err != nil && !errors.Is(response.err, context.Canceled) && !errors.Is(response.err, runtimecheckpoint.ErrBusy) {
				o.poison = response.err
			}
			if req.close {
				response.err = errors.Join(response.err, o.store.Close())
				req.result <- response
				return
			}
			req.result <- response
		case <-tick.C:
			if o.poison == nil && (o.store.Due() || o.dirty && time.Since(o.store.Metrics().CheckpointAt) >= 30*time.Second) {
				o.poison = o.checkpoint()
			}
			if o.poison == nil && o.state.pruneReceipts() {
				o.dirty = true
			}
			if o.poison == nil && o.state.pruneRetries() {
				o.dirty = true
			}
			if o.poison == nil {
				o.poison = o.state.ioError()
			}
			o.logRuntimeMetrics(time.Now().UTC())
		}
	}
}

func (o *runtimeOwner) request(ctx context.Context, r runtimeRequest) (runtimeResponse, error) {
	r.result = make(chan runtimeResponse, 1)
	select {
	case o.requests <- r:
	case <-ctx.Done():
		return runtimeResponse{}, ctx.Err()
	case <-o.done:
		return runtimeResponse{}, errors.New("runtime_owner_closed")
	}
	// Once accepted, wait for the bounded local result. Caller cancellation may
	// not silently discard an accepted completion or allow an unsynced SQL write.
	select {
	case response := <-r.result:
		return response, response.err
	case <-o.done:
		select {
		case response := <-r.result:
			return response, response.err
		default:
			return runtimeResponse{}, errors.New("runtime_owner_closed")
		}
	}
}
func (o *runtimeOwner) release(id string) {
	_, _ = o.request(context.Background(), runtimeRequest{view: func(*runtimeState) (any, error) {
		delete(o.inflight, id)
		return true, nil
	}})
}

func (o *runtimeOwner) writeBatch(ctx context.Context, db Store, b Batch, probe ...runtimeProbeTransition) error {
	w := runtimeWrite{Version: 1, Batch: &b}
	if len(probe) != 0 {
		w.Probe = &probe[0]
	}
	return o.writeBatchEffects(ctx, db, w)
}
func (o *runtimeOwner) writeBatchEffects(ctx context.Context, db Store, w runtimeWrite) error {
	raw, err := runtimecheckpoint.MarshalTyped(w)
	if err != nil {
		return err
	}
	r, err := o.request(ctx, runtimeRequest{begin: raw})
	if err != nil {
		return err
	}
	defer o.release(r.op.ID)
	if err = db.WriteAcrossBatch(ctx, *w.Batch); err != nil {
		return err
	}
	actual, err := runtimeCurrentCapture(ctx, db, w.Batch.Capture)
	if err != nil {
		return err
	}
	_, err = o.request(context.WithoutCancel(ctx), runtimeRequest{complete: &r.op, confirmed: &actual})
	return err
}
func (o *runtimeOwner) writeRevision(ctx context.Context, db Store, expected uint64, c Capture) (Capture, error) {
	raw, err := runtimecheckpoint.MarshalTyped(runtimeWrite{Version: 1, Revision: &c, Expected: expected})
	if err != nil {
		return Capture{}, err
	}
	r, err := o.request(ctx, runtimeRequest{begin: raw})
	if err != nil {
		return Capture{}, err
	}
	defer o.release(r.op.ID)
	if err = db.WriteAcrossRevision(ctx, c); err != nil {
		return Capture{}, err
	}
	actual, err := runtimeCurrentCapture(ctx, db, c)
	if err != nil {
		return Capture{}, err
	}
	_, err = o.request(context.WithoutCancel(ctx), runtimeRequest{complete: &r.op, confirmed: &actual})
	return actual, err
}
func (o *runtimeOwner) view(ctx context.Context, selectView func(*runtimeState) (any, error), dst any) error {
	r, err := o.request(ctx, runtimeRequest{view: selectView})
	if err != nil {
		return err
	}
	return runtimecheckpoint.UnmarshalTyped(r.raw, dst)
}
func (o *runtimeOwner) close() error {
	_, err := o.request(context.Background(), runtimeRequest{close: true})
	return err
}

func (o *runtimeOwner) change(ctx context.Context, fn func(*runtimeState) (any, error), save bool) error {
	_, err := o.request(ctx, runtimeRequest{change: fn, checkpoint: save})
	return err
}

// OpenRuntime requires a previously verified explicit conversion. Normal
// startup cannot synthesize an empty state or scan history as a fallback.
func (c *Collector) OpenRuntime(ctx context.Context, dir string, binding runtimecheckpoint.Binding, progress func(string)) error {
	if c.runtime != nil {
		return errors.New("runtime_already_open")
	}
	files, err := runtimecheckpoint.OpenContext(ctx, dir, binding, 0)
	if err != nil {
		return err
	}
	var initial runtimeState
	if err = runtimecheckpoint.UnmarshalTyped(files.State(), &initial); err == nil {
		err = initial.validateConversion(c.Manifest, binding)
	}
	if err != nil {
		files.Close()
		return err
	}
	o, err := newRuntimeOwner(ctx, files, c.Manifest, c.Store, progress)
	if err != nil {
		files.Close()
		return err
	}
	c.runtime = o
	if err = c.runtimeResumeInvalidations(ctx); err != nil {
		c.CloseRuntime()
		return err
	}
	return nil
}
func (c *Collector) CloseRuntime() error {
	if c.runtime == nil {
		return nil
	}
	err := c.runtime.close()
	c.runtime = nil
	return err
}
