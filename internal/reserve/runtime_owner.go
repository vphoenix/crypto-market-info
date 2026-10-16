package reserve

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint"
)

type runtimeWrite struct {
	Version  uint32
	Batch    *Batch
	Revision *Capture
	Expected uint64
}

func (w runtimeWrite) identity() (string, string, error) {
	if w.Version != 1 || (w.Batch == nil) == (w.Revision == nil) {
		return "", "", errors.New("reserve_runtime_operation_shape")
	}
	if w.Batch != nil {
		return "capture:" + runtimeIdentity(w.Batch.Capture), "capture", Validate(*w.Batch)
	}
	if w.Revision.Revision <= w.Expected {
		return "", "", errors.New("reserve_runtime_revision_not_newer")
	}
	return fmt.Sprintf("revision:%s:%d", runtimeIdentity(*w.Revision), w.Revision.Revision), "revision", nil
}

type runtimeResult struct {
	Raw []byte
	Err error
}
type runtimeRequest struct {
	Apply               func(*runtimeOwner) (any, error)
	Change, Save, Close bool
	Result              chan runtimeResult
}
type runtimeOwner struct {
	state                        runtimeState
	files                        *runtimecheckpoint.Store
	requests                     chan runtimeRequest
	done                         chan struct{}
	dirty                        bool
	poison                       error
	inflight                     map[string]bool
	progress                     func(string)
	metricsAt                    time.Time
	written, fileSyncs, dirSyncs uint64
}

func newRuntimeOwner(ctx context.Context, files *runtimecheckpoint.Store, binding runtimecheckpoint.Binding, scopes []string, db Store, progress func(string)) (*runtimeOwner, error) {
	o := &runtimeOwner{files: files, requests: make(chan runtimeRequest, 16), done: make(chan struct{}), inflight: map[string]bool{}, progress: progress, metricsAt: time.Now()}
	if err := runtimecheckpoint.UnmarshalTyped(files.State(), &o.state); err != nil {
		return nil, err
	}
	if o.state.Conversion.Version != 0 {
		if err := o.state.Conversion.InputInventory.Verify(files); err != nil {
			return nil, err
		}
	}
	if err := o.state.openPages(files); err != nil {
		return nil, err
	}
	if err := o.state.validate(binding, scopes); err != nil {
		return nil, err
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
			var actual Capture
			if w.Batch != nil {
				if err = db.WriteReserveBatch(ctx, *w.Batch); err != nil {
					return nil, err
				}
				b, e := db.ReserveBatch(ctx, w.Batch.Capture)
				if e != nil {
					return nil, e
				}
				if e = Validate(b); e != nil {
					return nil, e
				}
				actual = b.Capture
			} else {
				if err = db.WriteReserveRevision(ctx, *w.Revision); err != nil {
					return nil, err
				}
				actual, err = runtimeCurrent(ctx, db, *w.Revision)
				if err != nil {
					return nil, err
				}
			}
			if err = o.apply(op, &actual); err != nil {
				return nil, err
			}
			after = op.Sequence
		}
	}
	if err := o.capacity(); err != nil {
		return nil, err
	}
	go o.run()
	return o, nil
}

type runtimeCaptureReader interface {
	ReserveCapture(context.Context, Capture) (Capture, error)
}

func runtimeCurrent(ctx context.Context, db Store, want Capture) (Capture, error) {
	if r, ok := db.(runtimeCaptureReader); ok {
		return r.ReserveCapture(ctx, want)
	}
	b, err := db.ReserveBatch(ctx, want)
	return b.Capture, err
}
func (o *runtimeOwner) frozen(op runtimecheckpoint.Operation) (runtimeWrite, error) {
	var w runtimeWrite
	raw, err := o.files.ReadObject(op.Payload)
	if err != nil {
		return w, err
	}
	if err = runtimecheckpoint.UnmarshalTyped(raw, &w); err != nil {
		return w, err
	}
	id, kind, err := w.identity()
	if err != nil {
		return w, err
	}
	if id != op.ID || kind != op.Kind {
		return w, runtimecheckpoint.ErrConflict
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
	if cap.ChainId != 1 || o.state.Scopes[cap.ManifestHash] == nil {
		return runtimecheckpoint.Operation{}, runtimecheckpoint.ErrConflict
	}
	if o.inflight[id] {
		return runtimecheckpoint.Operation{}, runtimecheckpoint.ErrBusy
	}
	if op, exists := o.files.Find(id); exists {
		frozen, err := o.frozen(op)
		if err != nil {
			return op, err
		}
		if ID(frozen) != ID(w) {
			return op, runtimecheckpoint.ErrConflict
		}
		o.inflight[id] = true
		return op, nil
	}
	if o.files.Due() {
		if err = o.checkpoint(); err != nil {
			return runtimecheckpoint.Operation{}, err
		}
	}
	if w.Revision != nil {
		old, ok := o.state.get(o.state.active, runtimeIdentity(*w.Revision))
		if !ok || old.Capture.Revision < w.Expected || runtimeFrozen(old.Capture) != runtimeFrozen(runtimeNarrow(*w.Revision)) {
			return runtimecheckpoint.Operation{}, runtimecheckpoint.ErrConflict
		}
	}
	ref, err := o.files.PutObject(raw)
	if err != nil {
		return runtimecheckpoint.Operation{}, err
	}
	revision := uint64(0)
	if w.Revision != nil {
		revision = w.Revision.Revision
	}
	op, err := o.files.Begin(id, kind, ref, w.Expected, revision, runtimeIdentity(*cap))
	if err == nil {
		o.inflight[id] = true
	}
	return op, err
}
func (o *runtimeOwner) apply(op runtimecheckpoint.Operation, actual *Capture) error {
	already, err := o.files.IsApplied(op)
	if err != nil {
		return err
	}
	if already {
		delete(o.inflight, op.ID)
		return o.files.Applied(op)
	}
	w, err := o.frozen(op)
	if err != nil {
		return err
	}
	var cap Capture
	if w.Batch != nil {
		cap = w.Batch.Capture
	} else {
		cap = *w.Revision
	}
	ref := op.Payload
	if actual != nil {
		if actual.Revision < cap.Revision || runtimeFrozen(*actual) != runtimeFrozen(cap) || actual.Revision == cap.Revision && ID(*actual) != ID(cap) {
			return runtimecheckpoint.ErrConflict
		}
		if cap.Finality == "finalized" && (actual.Finality != "finalized" || !actual.Canonical) {
			return errors.New("finalized_hash_conflict")
		}
		if actual.Revision > cap.Revision {
			raw, e := runtimecheckpoint.MarshalTyped(runtimeWrite{Version: 1, Revision: actual, Expected: cap.Revision})
			if e != nil {
				return e
			}
			ref, e = o.files.PutObject(raw)
			if e != nil {
				return e
			}
			cap = *actual
		}
	}
	if err = o.state.capture(cap, ref, false); err != nil {
		return err
	}
	if w.Batch != nil {
		if cap.Canonical {
			o.state.applyCadence(*w.Batch)
		} else {
			o.state.Cadence.PendingSnapshot = true
		}
	}
	if err = o.capacity(); err != nil {
		return err
	}
	if err = o.files.Applied(op); err != nil {
		return err
	}
	o.dirty = true
	delete(o.inflight, op.ID)
	if o.files.Due() {
		return o.checkpoint()
	}
	return nil
}
func (o *runtimeOwner) checkpoint() error {
	if !o.dirty && !o.files.Due() {
		return nil
	}
	refs, err := o.state.references()
	if err != nil {
		return err
	}
	raw, err := runtimecheckpoint.MarshalTyped(o.state)
	if err != nil {
		return err
	}
	if err = o.files.Checkpoint(raw, refs); err != nil {
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
		case r := <-o.requests:
			var value any
			err := o.poison
			if err == nil && r.Apply != nil {
				value, err = r.Apply(o)
				if r.Change && err == nil {
					changed, ok := value.(bool)
					if !ok || changed {
						o.dirty = true
					}
				}
			}
			if err == nil {
				err = o.state.err
			}
			if err == nil && (r.Save || r.Close) {
				err = o.checkpoint()
			}
			var raw []byte
			if err == nil && value != nil {
				raw, err = runtimecheckpoint.MarshalTyped(value)
			}
			if err != nil && !errors.Is(err, runtimecheckpoint.ErrBusy) {
				o.poison = err
			}
			if r.Close {
				err = errors.Join(err, o.files.Close())
				r.Result <- runtimeResult{raw, err}
				return
			}
			r.Result <- runtimeResult{raw, err}
		case at := <-tick.C:
			if o.poison == nil && (o.files.Due() || o.dirty && at.Sub(o.files.Metrics().CheckpointAt) >= 30*time.Second) {
				o.poison = o.checkpoint()
			}
			o.log(at)
		}
	}
}
func (o *runtimeOwner) request(ctx context.Context, r runtimeRequest, dst any) error {
	r.Result = make(chan runtimeResult, 1)
	select {
	case o.requests <- r:
	case <-ctx.Done():
		return ctx.Err()
	case <-o.done:
		return errors.New("reserve_runtime_closed")
	}
	select {
	case result := <-r.Result:
		if result.Err != nil {
			return result.Err
		}
		if dst != nil {
			return runtimecheckpoint.UnmarshalTyped(result.Raw, dst)
		}
		return nil
	case <-o.done:
		select {
		case result := <-r.Result:
			return result.Err
		default:
			return errors.New("reserve_runtime_closed")
		}
	}
}
func (o *runtimeOwner) view(ctx context.Context, fn func(*runtimeState) (any, error), dst any) error {
	return o.request(ctx, runtimeRequest{Apply: func(o *runtimeOwner) (any, error) { return fn(&o.state) }}, dst)
}
func (o *runtimeOwner) close() error {
	return o.request(context.Background(), runtimeRequest{Close: true}, nil)
}
func (o *runtimeOwner) release(id string) {
	_ = o.request(context.Background(), runtimeRequest{Apply: func(o *runtimeOwner) (any, error) {
		delete(o.inflight, id)
		return nil, nil
	}}, nil)
}

func (o *runtimeOwner) write(ctx context.Context, db Store, w runtimeWrite) error {
	raw, err := runtimecheckpoint.MarshalTyped(w)
	if err != nil {
		return err
	}
	var op runtimecheckpoint.Operation
	err = o.request(ctx, runtimeRequest{Apply: func(o *runtimeOwner) (any, error) { return o.begin(raw) }}, &op)
	if err != nil {
		return err
	}
	defer o.release(op.ID)
	var actual *Capture
	if w.Batch != nil {
		err = db.WriteReserveBatch(ctx, *w.Batch)
		if err == nil {
			row, e := runtimeCurrent(ctx, db, w.Batch.Capture)
			err = e
			actual = &row
		}
	} else {
		err = db.WriteReserveRevision(ctx, *w.Revision)
		if err == nil {
			row, e := runtimeCurrent(ctx, db, *w.Revision)
			err = e
			actual = &row
		}
	}
	if err != nil {
		return err
	}
	return o.request(context.WithoutCancel(ctx), runtimeRequest{Apply: func(o *runtimeOwner) (any, error) { return nil, o.apply(op, actual) }}, nil)
}
func (c *Collector) writeBatch(ctx context.Context, b Batch) error {
	if c.runtime == nil {
		return c.Store.WriteReserveBatch(ctx, b)
	}
	return c.runtime.write(ctx, c.Store, runtimeWrite{Version: 1, Batch: &b})
}
func (c *Collector) writeRevision(ctx context.Context, expected uint64, cap Capture) error {
	if c.runtime == nil {
		return c.Store.WriteReserveRevision(ctx, cap)
	}
	return c.runtime.write(ctx, c.Store, runtimeWrite{Version: 1, Revision: &cap, Expected: expected})
}
func (c *Collector) OpenRuntime(ctx context.Context, dir string, binding runtimecheckpoint.Binding, progress func(string)) error {
	if c.runtime != nil {
		return errors.New("reserve_runtime_already_open")
	}
	if binding.ScopeHash != RuntimeScopeHash(c.Manifest, c.RelatedManifests, c.StartBlock) {
		return runtimecheckpoint.ErrConflict
	}
	files, err := runtimecheckpoint.OpenContext(ctx, dir, binding, 0)
	if err != nil {
		return err
	}
	o, err := newRuntimeOwner(ctx, files, binding, runtimeScopeHashes(c.Manifest, c.RelatedManifests), c.Store, progress)
	if err != nil {
		files.Close()
		return err
	}
	c.runtime = o
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
func (c *Collector) saveCadence(ctx context.Context, next runtimeCadence) error {
	if c.runtime == nil {
		return nil
	}
	return c.runtime.request(ctx, runtimeRequest{Change: true, Apply: func(o *runtimeOwner) (any, error) {
		if reflect.DeepEqual(o.state.Cadence, next) {
			return false, nil
		}
		o.state.Cadence = next
		return true, nil
	}}, nil)
}
func (o *runtimeOwner) log(at time.Time) {
	if o.progress == nil {
		return
	}
	m := o.files.Metrics()
	var oldest time.Time
	cache := int64(0)
	reads, writes := uint64(0), uint64(0)
	for _, p := range []*runtimecheckpoint.PagedMap[runtimeHeader]{o.state.active, o.state.final, o.state.pending} {
		v := p.Metrics()
		cache += v.ResidentBytes
		reads += v.PagesRead
		writes += v.PagesWritten
		x := p.Oldest()
		if oldest.IsZero() || !x.IsZero() && x.Before(oldest) {
			oldest = x
		}
	}
	age := int64(0)
	if !oldest.IsZero() {
		age = max(int64(0), at.Sub(oldest).Milliseconds())
	}
	elapsed := at.Sub(o.metricsAt)
	rate := uint64(0)
	if elapsed > 0 {
		rate = (m.WrittenBytes - o.written) * uint64(time.Second) / uint64(elapsed)
	}
	o.progress(fmt.Sprintf("runtime_state active=%d receipts_pending=%d inflight=%d oldest_pending_age_ms=%d checkpoint_age_ms=%d checkpoint_bytes=%d uncheckpointed=%d state_estimate_bytes=%d total_hot_estimate_bytes=%d page_cache_bytes=%d recovery_metadata_bytes=%d recovery_bytes=%d recovery_peak_bytes=%d written_bytes_per_second=%d file_syncs=%d directory_syncs=%d page_reads=%d page_writes=%d error=%v", o.state.active.Len(), o.state.pending.Len(), len(o.inflight), age, at.Sub(m.CheckpointAt).Milliseconds(), m.CheckpointBytes, m.Uncheckpointed, runtimecheckpoint.EstimateTypedBytes(o.state), o.hotBytes(), cache, m.HotBytes, m.RecoveryBytes, m.PeakRecoveryBytes, rate, m.FileSyncs-o.fileSyncs, m.DirectorySyncs-o.dirSyncs, reads, writes, o.poison))
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	var rss uint64
	if raw, err := os.ReadFile("/proc/self/statm"); err == nil {
		fields := strings.Fields(string(raw))
		if len(fields) > 1 {
			pages, _ := strconv.ParseUint(fields[1], 10, 64)
			rss = pages * uint64(os.Getpagesize())
		}
	}
	o.progress(fmt.Sprintf("runtime_resources heap_bytes=%d rss_bytes=%d", memory.HeapAlloc, rss))
	scopes := make([]string, 0, len(o.state.Scopes))
	for key := range o.state.Scopes {
		scopes = append(scopes, key)
	}
	sort.Strings(scopes)
	for _, key := range scopes {
		scope := o.state.Scopes[key]
		o.progress(fmt.Sprintf("runtime_scope manifest=%s live_started=%t original_live_start=%d through_live=%d raw_intervals=%d receipt_intervals=%d live_intervals=%d", Hex(key), scope.LiveStarted, scope.LiveStart, o.state.cursor(key, scope.LiveStart, true), len(scope.CurrentRaw), len(scope.CurrentReceipt), len(scope.CurrentLive)))
	}
	o.metricsAt, o.written, o.fileSyncs, o.dirSyncs = at, m.WrittenBytes, m.FileSyncs, m.DirectorySyncs
}
