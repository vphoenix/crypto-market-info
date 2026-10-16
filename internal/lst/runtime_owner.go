package lst

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint"
)

type runtimeRevision struct {
	Version  uint32
	Expected uint64
	Capture  Capture
}
type runtimeReply struct {
	Raw []byte
	Err error
}
type runtimeRequest struct {
	Apply       func(*runtimeOwner) (any, error)
	Save, Close bool
	Result      chan runtimeReply
}
type runtimeOwner struct {
	state                        runtimeState
	files                        *runtimecheckpoint.Store
	requests                     chan runtimeRequest
	done                         chan struct{}
	inflight                     map[string]bool
	dirty                        bool
	poison                       error
	progress                     func(string)
	metricsAt                    time.Time
	written, fileSyncs, dirSyncs uint64
}

func newRuntimeOwner(ctx context.Context, files *runtimecheckpoint.Store, binding runtimecheckpoint.Binding, c *Collector, progress func(string)) (*runtimeOwner, error) {
	o := &runtimeOwner{files: files, requests: make(chan runtimeRequest, 16), done: make(chan struct{}), inflight: map[string]bool{}, progress: progress, metricsAt: time.Now()}
	if err := runtimecheckpoint.UnmarshalTyped(files.State(), &o.state); err != nil {
		return nil, err
	}
	if o.state.Conversion.Version != 0 {
		if err := o.state.Conversion.InputInventory.Verify(files); err != nil {
			return nil, err
		}
	}
	if err := o.state.open(files); err != nil {
		return nil, err
	}
	if err := o.state.validate(binding, c); err != nil {
		return nil, err
	}
	for after := uint64(0); ; {
		page := files.Pending(after, 16)
		if len(page) == 0 {
			break
		}
		for _, op := range page {
			var actual Capture
			if op.Kind == "capture" {
				b, err := o.frozenBatch(op)
				if err != nil {
					return nil, err
				}
				if err = c.Store.WriteLST(ctx, b); err != nil {
					return nil, err
				}
				verified, err := c.Store.LSTBatch(ctx, b.Capture)
				if err != nil {
					return nil, err
				}
				if err = Validate(verified); err != nil {
					return nil, err
				}
				actual = verified.Capture
			} else {
				rev, err := o.frozenRevision(op)
				if err != nil {
					return nil, err
				}
				if err = c.Store.WriteLSTRevision(ctx, rev.Capture); err != nil {
					return nil, err
				}
				actual, err = runtimeCurrent(ctx, c.Store, rev.Capture)
				if err != nil {
					return nil, err
				}
			}
			if err := o.apply(op, actual); err != nil {
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
	LSTCapture(context.Context, Capture) (Capture, error)
}

func runtimeCurrent(ctx context.Context, db Store, want Capture) (Capture, error) {
	if r, ok := db.(runtimeCaptureReader); ok {
		return r.LSTCapture(ctx, want)
	}
	b, err := db.LSTBatch(ctx, want)
	return b.Capture, err
}
func (o *runtimeOwner) frozenBatch(op runtimecheckpoint.Operation) (Batch, error) {
	var b Batch
	raw, err := o.files.ReadObject(op.Payload)
	if err != nil {
		return b, err
	}
	if err = decodePersistent(bytes.NewReader(raw), &b); err != nil {
		return b, err
	}
	if op.Kind != "capture" || op.ID != "capture:"+b.Capture.CaptureId.String() {
		return b, runtimecheckpoint.ErrConflict
	}
	return b, nil
}
func (o *runtimeOwner) frozenRevision(op runtimecheckpoint.Operation) (runtimeRevision, error) {
	var rev runtimeRevision
	raw, err := o.files.ReadObject(op.Payload)
	if err != nil {
		return rev, err
	}
	if err = runtimecheckpoint.UnmarshalTyped(raw, &rev); err != nil {
		return rev, err
	}
	if rev.Version != 1 || rev.Capture.Revision <= rev.Expected || op.Kind != "revision" || op.ID != fmt.Sprintf("revision:%s:%d", rev.Capture.CaptureId, rev.Capture.Revision) {
		return rev, runtimecheckpoint.ErrConflict
	}
	return rev, nil
}
func (o *runtimeOwner) beginPending(path string) (runtimecheckpoint.Operation, error) {
	if err := o.capacity(); err != nil {
		return runtimecheckpoint.Operation{}, err
	}
	if o.files.Due() {
		if err := o.checkpoint(); err != nil {
			return runtimecheckpoint.Operation{}, err
		}
	}
	ref, err := o.files.LinkPending(path)
	if err != nil {
		return runtimecheckpoint.Operation{}, err
	}
	raw, err := o.files.ReadObject(ref)
	if err != nil {
		return runtimecheckpoint.Operation{}, err
	}
	var b Batch
	if err = decodePersistent(bytes.NewReader(raw), &b); err != nil {
		return runtimecheckpoint.Operation{}, err
	}
	if b.Capture.ManifestHash != o.state.Manifest {
		return runtimecheckpoint.Operation{}, runtimecheckpoint.ErrConflict
	}
	id := "capture:" + b.Capture.CaptureId.String()
	if o.inflight[id] {
		return runtimecheckpoint.Operation{}, runtimecheckpoint.ErrBusy
	}
	if op, ok := o.files.Find(id); ok {
		if op.Payload != ref {
			return op, runtimecheckpoint.ErrConflict
		}
		o.inflight[id] = true
		return op, nil
	}
	op, err := o.files.Begin(id, "capture", ref, 0, b.Capture.Revision, b.Capture.CaptureId.String())
	if err == nil {
		o.inflight[id] = true
	}
	return op, err
}
func (o *runtimeOwner) beginRevision(rev runtimeRevision) (runtimecheckpoint.Operation, error) {
	if err := o.capacity(); err != nil {
		return runtimecheckpoint.Operation{}, err
	}
	if rev.Version != 1 || rev.Capture.ManifestHash != o.state.Manifest || rev.Capture.Revision <= rev.Expected {
		return runtimecheckpoint.Operation{}, runtimecheckpoint.ErrConflict
	}
	id := fmt.Sprintf("revision:%s:%d", rev.Capture.CaptureId, rev.Capture.Revision)
	if o.inflight[id] {
		return runtimecheckpoint.Operation{}, runtimecheckpoint.ErrBusy
	}
	if op, ok := o.files.Find(id); ok {
		old, err := o.frozenRevision(op)
		if err != nil {
			return op, err
		}
		if CanonicalHash(old) != CanonicalHash(rev) {
			return op, runtimecheckpoint.ErrConflict
		}
		o.inflight[id] = true
		return op, nil
	}
	old, ok, err := o.state.active.Get(runtimeCaptureKey(rev.Capture))
	if err != nil {
		return runtimecheckpoint.Operation{}, err
	}
	if !ok || old.Revision < rev.Expected || CaptureIdentity(old) != CaptureIdentity(rev.Capture) {
		return runtimecheckpoint.Operation{}, runtimecheckpoint.ErrConflict
	}
	if o.files.Due() {
		if err = o.checkpoint(); err != nil {
			return runtimecheckpoint.Operation{}, err
		}
	}
	raw, err := runtimecheckpoint.MarshalTyped(rev)
	if err != nil {
		return runtimecheckpoint.Operation{}, err
	}
	ref, err := o.files.PutObject(raw)
	if err != nil {
		return runtimecheckpoint.Operation{}, err
	}
	op, err := o.files.Begin(id, "revision", ref, rev.Expected, rev.Capture.Revision, rev.Capture.CaptureId.String())
	if err == nil {
		o.inflight[id] = true
	}
	return op, err
}
func (o *runtimeOwner) apply(op runtimecheckpoint.Operation, actual Capture) error {
	applied, err := o.files.IsApplied(op)
	if err != nil {
		return err
	}
	if applied {
		delete(o.inflight, op.ID)
		return o.files.Applied(op)
	}
	if op.Kind == "capture" {
		b, e := o.frozenBatch(op)
		if e != nil {
			return e
		}
		if CaptureIdentity(actual) != CaptureIdentity(b.Capture) || actual.Revision < b.Capture.Revision || actual.Revision == b.Capture.Revision && CanonicalHash(actual) != CanonicalHash(b.Capture) {
			return runtimecheckpoint.ErrConflict
		}
		b.Capture = actual
		if err = o.state.batch(b, Now()); err != nil {
			return err
		}
	} else {
		rev, e := o.frozenRevision(op)
		if e != nil {
			return e
		}
		if CaptureIdentity(actual) != CaptureIdentity(rev.Capture) || actual.Revision < rev.Capture.Revision {
			return runtimecheckpoint.ErrConflict
		}
		if err = o.state.capture(actual); err != nil {
			return err
		}
	}
	if err = o.capacity(); err != nil {
		return err
	}
	if err = o.files.Applied(op); err != nil {
		return err
	}
	if op.Kind == "capture" {
		o.state.ImportedPendingID, o.state.ImportedPendingHash = op.ID, op.Payload.Hash
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
func (o *runtimeOwner) gcIdentity() string {
	return CanonicalHash(struct {
		Seeds, Done, Headers, Transactions uint64
		Cut                                time.Time
		Buckets                            int
	}{o.state.seeds.Len(), o.state.done.Len(), o.state.Gas.headers.Len(), o.state.Gas.transactions.Len(), o.state.Gas.Cut, len(o.state.Gas.Buckets)})
}
func (o *runtimeOwner) run() {
	defer close(o.done)
	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()
	for {
		select {
		case req := <-o.requests:
			var result any
			err := o.poison
			before := o.gcIdentity()
			if err == nil && req.Apply != nil {
				result, err = req.Apply(o)
			}
			if o.gcIdentity() != before {
				o.dirty = true
			}
			if err == nil {
				err = o.state.err
			}
			if err == nil && (req.Save || req.Close) {
				err = o.checkpoint()
			}
			var raw []byte
			if err == nil && result != nil {
				raw, err = runtimecheckpoint.MarshalTyped(result)
			}
			if err != nil && !errors.Is(err, runtimecheckpoint.ErrBusy) {
				o.poison = err
			}
			if req.Close {
				err = errors.Join(err, o.files.Close())
				req.Result <- runtimeReply{raw, err}
				return
			}
			req.Result <- runtimeReply{raw, err}
		case at := <-tick.C:
			if o.poison == nil {
				before := o.gcIdentity()
				o.poison = o.state.prune(at)
				if before != o.gcIdentity() {
					o.dirty = true
				}
			}
			if o.poison == nil && (o.files.Due() || o.dirty && at.Sub(o.files.Metrics().CheckpointAt) >= 30*time.Second) {
				o.poison = o.checkpoint()
			}
			o.log(at)
		}
	}
}
func (o *runtimeOwner) request(ctx context.Context, req runtimeRequest, dst any) error {
	req.Result = make(chan runtimeReply, 1)
	select {
	case o.requests <- req:
	case <-ctx.Done():
		return ctx.Err()
	case <-o.done:
		return errors.New("lst_runtime_closed")
	}
	select {
	case result := <-req.Result:
		if result.Err != nil {
			return result.Err
		}
		if dst != nil {
			return runtimecheckpoint.UnmarshalTyped(result.Raw, dst)
		}
		return nil
	case <-o.done:
		select {
		case result := <-req.Result:
			return result.Err
		default:
			return errors.New("lst_runtime_closed")
		}
	}
}
func (o *runtimeOwner) view(ctx context.Context, fn func(*runtimeState) (any, error), dst any) error {
	return o.request(ctx, runtimeRequest{Apply: func(o *runtimeOwner) (any, error) { return fn(&o.state) }}, dst)
}
func (o *runtimeOwner) close() error {
	return o.request(context.Background(), runtimeRequest{Close: true}, nil)
}
func (o *runtimeOwner) hotBytes() int64 {
	m := o.files.Metrics()
	return runtimecheckpoint.EstimateTypedBytes(o.state) + m.HotBytes + m.VerifiedMetadataBytes + o.state.active.MemoryBytes() + o.state.seeds.MemoryBytes() + o.state.done.MemoryBytes() + o.state.Gas.headers.MemoryBytes() + o.state.Gas.boundaries.MemoryBytes() + o.state.Gas.transactions.MemoryBytes()
}
func (o *runtimeOwner) capacity() error {
	if n := o.hotBytes(); n > 16<<20 {
		return fmt.Errorf("lst runtime estimated hot bytes=%d: %w", n, runtimecheckpoint.ErrCapacity)
	}
	return nil
}
func (o *runtimeOwner) log(at time.Time) {
	if o.progress == nil {
		return
	}
	m := o.files.Metrics()
	elapsed := at.Sub(o.metricsAt)
	rate := uint64(0)
	if elapsed > 0 {
		rate = (m.WrittenBytes - o.written) * uint64(time.Second) / uint64(elapsed)
	}
	o.progress(fmt.Sprintf("runtime_state finality_pending=%d followup_seeds=%d inflight=%d checkpoint_age_ms=%d checkpoint_bytes=%d uncheckpointed=%d hot_estimate_bytes=%d recovery_bytes=%d recovery_peak_bytes=%d written_bytes_per_second=%d file_syncs=%d directory_syncs=%d error=%v", o.state.active.Len(), o.state.seeds.Len(), len(o.inflight), at.Sub(m.CheckpointAt).Milliseconds(), m.CheckpointBytes, m.Uncheckpointed, o.hotBytes(), m.RecoveryBytes, m.PeakRecoveryBytes, rate, m.FileSyncs-o.fileSyncs, m.DirectorySyncs-o.dirSyncs, o.poison))
	var process runtime.MemStats
	runtime.ReadMemStats(&process)
	var rss uint64
	if raw, err := os.ReadFile("/proc/self/statm"); err == nil {
		parts := strings.Fields(string(raw))
		if len(parts) > 1 {
			pages, _ := strconv.ParseUint(parts[1], 10, 64)
			rss = pages * uint64(os.Getpagesize())
		}
	}
	pages := []runtimecheckpoint.PageMetrics{o.state.active.Metrics(), o.state.seeds.Metrics(), o.state.done.Metrics(), o.state.Gas.headers.Metrics(), o.state.Gas.boundaries.Metrics(), o.state.Gas.transactions.Metrics()}
	var cached int64
	var reads, writes uint64
	for _, page := range pages {
		cached += page.ResidentBytes
		reads += page.PagesRead
		writes += page.PagesWritten
	}
	o.progress(fmt.Sprintf("runtime_resources page_cache_bytes=%d page_reads=%d page_writes=%d process_heap_bytes=%d process_rss_bytes=%d oldest_finality=%s oldest_tracked_seed=%s", cached, reads, writes, process.HeapAlloc, rss, o.state.active.Oldest().Format(time.RFC3339Nano), o.state.seeds.Oldest().Format(time.RFC3339Nano)))
	segments := make([]uint64, 0, len(o.state.Cursors))
	for segment := range o.state.Cursors {
		segments = append(segments, segment)
	}
	sort.Slice(segments, func(i, j int) bool { return segments[i] < segments[j] })
	for _, segment := range segments {
		p := o.state.Cursors[segment]
		o.progress(fmt.Sprintf("runtime_log_scope segment=%d original_start=%d next=%d coverage_intervals=%d source_blocked=%t", segment, p.Start, p.Next, len(o.state.Coverage), p.BlockedSource != ""))
	}
	o.metricsAt, o.written, o.fileSyncs, o.dirSyncs = at, m.WrittenBytes, m.FileSyncs, m.DirectorySyncs
}
func (c *Collector) OpenRuntime(ctx context.Context, dir string, binding runtimecheckpoint.Binding, progress func(string)) error {
	if c.runtime != nil {
		return errors.New("lst_runtime_already_open")
	}
	files, err := runtimecheckpoint.OpenContext(ctx, dir, binding, 0)
	if err != nil {
		return err
	}
	o, err := newRuntimeOwner(ctx, files, binding, c, progress)
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
func (c *Collector) runtimeFlushPending(ctx context.Context, file string, b Batch) error {
	// Linking preserves the exact original pending format without a second
	// complete payload. SQL is admitted only after the original operation is durable.
	type admission struct {
		Op      runtimecheckpoint.Operation
		Covered bool
	}
	var admitted admission
	err := c.runtime.request(ctx, runtimeRequest{Apply: func(o *runtimeOwner) (any, error) {
		ref, err := o.files.LinkPending(file)
		if err != nil {
			return nil, err
		}
		id := "capture:" + b.Capture.CaptureId.String()
		if o.state.ImportedPendingID == id {
			if o.state.ImportedPendingHash != ref.Hash {
				return nil, runtimecheckpoint.ErrConflict
			}
			return admission{Covered: true}, nil
		}
		op, err := o.beginPending(file)
		return admission{Op: op}, err
	}}, &admitted)
	if err != nil {
		return err
	}
	op := admitted.Op
	defer c.runtime.release(op.ID)
	if err = c.Store.WriteLST(ctx, b); err != nil {
		return err
	}
	actual, err := runtimeCurrent(ctx, c.Store, b.Capture)
	if err != nil {
		return err
	}
	if CaptureIdentity(actual) != CaptureIdentity(b.Capture) || actual.Revision < b.Capture.Revision {
		return runtimecheckpoint.ErrConflict
	}
	if admitted.Covered {
		return nil
	}
	return c.runtime.request(context.WithoutCancel(ctx), runtimeRequest{Apply: func(o *runtimeOwner) (any, error) { return nil, o.apply(op, actual) }}, nil)
}
func (o *runtimeOwner) release(id string) {
	if id == "" {
		return
	}
	_ = o.request(context.Background(), runtimeRequest{Apply: func(o *runtimeOwner) (any, error) { delete(o.inflight, id); return nil, nil }}, nil)
}
func (c *Collector) runtimeWriteRevision(ctx context.Context, old uint64, next Capture) error {
	if c.runtime == nil {
		return c.Store.WriteLSTRevision(ctx, next)
	}
	rev := runtimeRevision{1, old, next}
	var op runtimecheckpoint.Operation
	if err := c.runtime.request(ctx, runtimeRequest{Apply: func(o *runtimeOwner) (any, error) { return o.beginRevision(rev) }}, &op); err != nil {
		return err
	}
	defer c.runtime.release(op.ID)
	if err := c.Store.WriteLSTRevision(ctx, next); err != nil {
		return err
	}
	actual, err := runtimeCurrent(ctx, c.Store, next)
	if err != nil {
		return err
	}
	return c.runtime.request(context.WithoutCancel(ctx), runtimeRequest{Apply: func(o *runtimeOwner) (any, error) { return nil, o.apply(op, actual) }}, nil)
}
func removeRuntimePending(path string) error {
	if err := os.Remove(path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
