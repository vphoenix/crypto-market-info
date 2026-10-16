package dexruntime

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/dex"
	"github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
)

var ErrBusy = errors.New("DEX runtime block already in flight")

type exactReader interface {
	DEXRuntimeBlock(context.Context, dex.Block) (dex.Block, bool, error)
}
type frozenWrite struct {
	Version  uint32
	Kind     string
	Block    dex.Block
	Batch    *dex.Batch
	Logs     []dex.Log
	Expected uint64
}

func (w frozenWrite) identity() (string, error) {
	if w.Version != 1 || w.Block.Revision == 0 {
		return "", runtimecheckpoint.ErrConflict
	}
	switch w.Kind {
	case "batch":
		if w.Batch == nil || !reflect.DeepEqual(w.Batch.Block, w.Block) || len(w.Logs) != 0 {
			return "", runtimecheckpoint.ErrConflict
		}
	case "block":
		if w.Batch != nil || len(w.Logs) != 0 {
			return "", runtimecheckpoint.ErrConflict
		}
	case "logs":
		if w.Batch != nil {
			return "", runtimecheckpoint.ErrConflict
		}
	default:
		return "", runtimecheckpoint.ErrConflict
	}
	return fmt.Sprintf("%s:%s:%d:%x", w.Kind, key(w.Block), w.Block.Revision, w.Block.Batch[:]), nil
}

type request struct {
	run   func(*owner) (any, error)
	reply chan response
	close bool
}
type response struct {
	value any
	err   error
}
type owner struct {
	files                        *runtimecheckpoint.Store
	state                        state
	requests                     chan request
	done                         chan struct{}
	inflight                     map[string]bool
	pending                      map[string]string
	poison                       error
	dirty                        bool
	logger                       *slog.Logger
	telemetry                    time.Time
	written, fileSyncs, dirSyncs uint64
}

// Store preserves the original DEXStore API. Live and maintenance network
// workers remain independent; only immutable publications cross the owner.
type Store struct {
	contracts.DEXStore
	owner *owner
}

func (o *owner) capacity() error {
	m := o.files.Metrics()
	if s := o.state.memoryBytes() + m.HotBytes + m.VerifiedMetadataBytes; s > 16<<20 {
		return fmt.Errorf("DEX runtime hot bytes %d: %w", s, runtimecheckpoint.ErrCapacity)
	}
	return nil
}
func (o *owner) checkpoint() error {
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
	if err = o.capacity(); err != nil {
		return err
	}
	if err = o.files.Checkpoint(raw, refs); err != nil {
		return err
	}
	o.dirty = false
	return nil
}
func (o *owner) call(ctx context.Context, fn func(*owner) (any, error)) (any, error) {
	q := request{run: fn, reply: make(chan response, 1)}
	select {
	case o.requests <- q:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-o.done:
		return nil, errors.New("DEX runtime closed")
	}
	// Once accepted, consume the owner's result even if the caller is cancelled;
	// durable admission must not leave an invisible in-flight lease.
	select {
	case result := <-q.reply:
		return result.value, result.err
	case <-o.done:
		select {
		case result := <-q.reply:
			return result.value, result.err
		default:
			return nil, errors.New("DEX runtime closed")
		}
	}
}
func (o *owner) loop() {
	defer close(o.done)
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case q := <-o.requests:
			var result response
			if o.poison != nil {
				result.err = o.poison
			} else if q.close {
				result.err = o.checkpoint()
			} else {
				result.value, result.err = q.run(o)
			}
			if result.err != nil && !errors.Is(result.err, ErrBusy) {
				o.poison = result.err
			}
			if q.close {
				result.err = errors.Join(result.err, o.files.Close())
				q.reply <- result
				return
			}
			q.reply <- result
		case <-ticker.C:
			if o.poison == nil && o.files.Due() {
				if err := o.checkpoint(); err != nil {
					o.poison = err
				}
			}
			o.metrics()
		}
	}
}
func (o *owner) metrics() {
	now := time.Now()
	m := o.files.Metrics()
	elapsed := now.Sub(o.telemetry)
	var rate uint64
	if elapsed > 0 {
		rate = (m.WrittenBytes - o.written) * uint64(time.Second) / uint64(elapsed)
	}
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	var rss uint64
	if raw, e := os.ReadFile("/proc/self/statm"); e == nil {
		fields := strings.Fields(string(raw))
		if len(fields) > 1 {
			pages, _ := strconv.ParseUint(fields[1], 10, 64)
			rss = pages * uint64(os.Getpagesize())
		}
	}
	o.logger.Info("DEX runtime_state", "coverage_intervals", len(o.state.Coverage), "first_gap", o.state.gap(), "unfinalized", o.state.active.Len(), "inflight", len(o.inflight), "pending", m.Pending, "uncheckpointed", m.Uncheckpointed, "oldest", o.state.active.Oldest(), "checkpoint_age_ms", now.Sub(m.CheckpointAt).Milliseconds(), "checkpoint_bytes", m.CheckpointBytes, "working_hot_bytes", o.state.memoryBytes(), "heap_bytes", mem.HeapAlloc, "rss_bytes", rss, "recovery_bytes", m.RecoveryBytes, "recovery_peak_bytes", m.PeakRecoveryBytes, "written_bytes_per_second", rate, "file_syncs", m.FileSyncs-o.fileSyncs, "directory_syncs", m.DirectorySyncs-o.dirSyncs, "error", o.poison)
	o.telemetry, o.written, o.fileSyncs, o.dirSyncs = now, m.WrittenBytes, m.FileSyncs, m.DirectorySyncs
}
func (o *owner) frozen(op runtimecheckpoint.Operation) (frozenWrite, error) {
	var w frozenWrite
	raw, err := o.files.ReadObject(op.Payload)
	if err != nil {
		return w, err
	}
	if err = runtimecheckpoint.UnmarshalTyped(raw, &w); err != nil {
		return w, err
	}
	id, err := w.identity()
	if err != nil {
		return w, err
	}
	if id != op.ID || op.Kind != w.Kind || op.ExpectedRevision != w.Expected || op.NewRevision != w.Block.Revision || w.Block.Manifest != o.state.Manifest {
		return w, runtimecheckpoint.ErrConflict
	}
	return w, nil
}
func writeOriginal(ctx context.Context, db contracts.DEXStore, w frozenWrite) (dex.Block, error) {
	var err error
	switch w.Kind {
	case "batch":
		err = db.WriteDEXBatch(ctx, *w.Batch)
	case "block":
		err = db.WriteDEXBlock(ctx, w.Block)
	case "logs":
		err = db.CompleteDEXLogs(ctx, w.Block, w.Logs)
	default:
		err = runtimecheckpoint.ErrConflict
	}
	if err != nil {
		return dex.Block{}, err
	}
	// A stale planned revision can be a successful SQL no-op. Install only the
	// actual immutable key/current revision, after the original transaction.
	r, ok := db.(exactReader)
	if !ok {
		return dex.Block{}, errors.New("DEX runtime requires exact recovery reader")
	}
	actual, ok, err := r.DEXRuntimeBlock(ctx, w.Block)
	if err == nil && !ok {
		err = runtimecheckpoint.ErrConflict
	}
	return actual, err
}
func Open(ctx context.Context, dir string, binding runtimecheckpoint.Binding, manifest dex.Hash, db contracts.DEXStore, logger *slog.Logger) (*Store, error) {
	if logger == nil {
		logger = slog.Default()
	}
	if binding.ScopeHash != manifest.String() {
		return nil, runtimecheckpoint.ErrConflict
	}
	if _, ok := db.(exactReader); !ok {
		return nil, errors.New("DEX runtime requires exact recovery reader")
	}
	files, err := runtimecheckpoint.OpenContext(ctx, dir, binding, 0)
	if err != nil {
		return nil, err
	}
	o := &owner{files: files, requests: make(chan request, 16), done: make(chan struct{}), inflight: map[string]bool{}, pending: map[string]string{}, logger: logger, telemetry: time.Now()}
	fail := func(err error) (*Store, error) { files.Close(); return nil, err }
	if err = runtimecheckpoint.UnmarshalTyped(files.State(), &o.state); err != nil {
		return fail(err)
	}
	if o.state.Conversion.Version != 0 {
		if err := o.state.Conversion.InputInventory.Verify(files); err != nil {
			return fail(err)
		}
	}
	if err = o.state.open(files); err != nil {
		return fail(err)
	}
	if err = o.state.validate(binding, manifest); err != nil {
		return fail(err)
	}
	for after := uint64(0); ; {
		ops := files.Pending(after, 16)
		if len(ops) == 0 {
			break
		}
		for _, op := range ops {
			w, e := o.frozen(op)
			if e != nil {
				return fail(e)
			}
			actual, e := writeOriginal(ctx, db, w)
			if e != nil {
				return fail(e)
			}
			if e = o.state.apply(actual); e != nil {
				return fail(e)
			}
			if e = files.Applied(op); e != nil {
				return fail(e)
			}
			o.dirty = true
			if files.Due() {
				if e = o.checkpoint(); e != nil {
					return fail(e)
				}
			}
			after = op.Sequence
		}
	}
	if err = o.checkpoint(); err != nil {
		return fail(err)
	}
	if err = o.capacity(); err != nil {
		return fail(err)
	}
	go o.loop()
	return &Store{DEXStore: db, owner: o}, nil
}
func (s *Store) CloseRuntime() error {
	o := s.owner
	if o == nil {
		return nil
	}
	q := request{close: true, reply: make(chan response, 1)}
	select {
	case o.requests <- q:
	case <-o.done:
		return nil
	}
	select {
	case result := <-q.reply:
		<-o.done
		return result.err
	case <-o.done:
		select {
		case result := <-q.reply:
			return result.err
		default:
			return nil
		}
	}
}
func (s *Store) DEXGapStart(ctx context.Context, m dex.Hash) (uint64, error) {
	value, err := s.owner.call(ctx, func(o *owner) (any, error) {
		if m != o.state.Manifest {
			return nil, runtimecheckpoint.ErrConflict
		}
		return o.state.gap(), nil
	})
	if err != nil {
		return 0, err
	}
	return value.(uint64), nil
}

type admission struct {
	Operation runtimecheckpoint.Operation
	Frozen    *frozenWrite
}

var ErrStalePlan = errors.New("DEX planned revision changed; reload current block")

func (s *Store) execute(ctx context.Context, w frozenWrite) (dex.Block, error) {
	// Make a private typed copy before any worker/owner exchange.
	raw, err := runtimecheckpoint.MarshalTyped(w)
	if err != nil {
		return dex.Block{}, err
	}
	var owned frozenWrite
	if err = runtimecheckpoint.UnmarshalTyped(raw, &owned); err != nil {
		return dex.Block{}, err
	}
	id, err := owned.identity()
	if err != nil {
		return dex.Block{}, err
	}
	k := key(owned.Block)
	for {
		value, err := s.owner.call(ctx, func(o *owner) (any, error) {
			if owned.Block.Manifest != o.state.Manifest || owned.Block.ChainID != 1 {
				return nil, runtimecheckpoint.ErrConflict
			}
			if o.inflight[k] {
				return nil, ErrBusy
			}
			if err := o.capacity(); err != nil {
				return nil, err
			}
			previousID := o.pending[k]
			if previousID == "" {
				previousID = id
			}
			var next admission
			if op, ok := o.files.Find(previousID); ok {
				previous, e := o.frozen(op)
				if e != nil {
					return nil, e
				}
				if previousID == id {
					candidate := owned
					candidate.Expected = previous.Expected
					if !reflect.DeepEqual(previous, candidate) {
						return nil, runtimecheckpoint.ErrConflict
					}
				}
				next = admission{Operation: op, Frozen: &previous}
			}
			o.inflight[k] = true
			return next, nil
		})
		if err != nil {
			return dex.Block{}, err
		}
		next := value.(admission)
		selected := owned
		if next.Frozen != nil {
			selected = *next.Frozen
		} else {
			// The lease excludes another local publication while the exact revision
			// is checked. No SQL write has been admitted yet, so stale work can simply
			// release its lease and be replanned without a fictitious Applied record.
			current, exists, e := s.DEXStore.(exactReader).DEXRuntimeBlock(ctx, selected.Block)
			if e == nil && selected.Kind != "batch" {
				if !exists {
					e = runtimecheckpoint.ErrConflict
				} else if selected.Expected == 0 {
					selected.Expected = current.Revision
				} else if current.Revision != selected.Expected && current.Revision < selected.Block.Revision {
					e = ErrStalePlan
				}
			}
			if e != nil {
				_, releaseErr := s.owner.call(context.Background(), func(o *owner) (any, error) { delete(o.inflight, k); return nil, nil })
				return dex.Block{}, errors.Join(e, releaseErr)
			}
			frozen, e := runtimecheckpoint.MarshalTyped(selected)
			if e != nil {
				_, releaseErr := s.owner.call(context.Background(), func(o *owner) (any, error) { delete(o.inflight, k); return nil, nil })
				return dex.Block{}, errors.Join(e, releaseErr)
			}
			value, e = s.owner.call(ctx, func(o *owner) (any, error) {
				if o.files.Due() {
					if e := o.checkpoint(); e != nil {
						return nil, e
					}
				}
				ref, e := o.files.PutObject(frozen)
				if e != nil {
					return nil, e
				}
				op, e := o.files.Begin(id, selected.Kind, ref, selected.Expected, selected.Block.Revision)
				if e != nil {
					return nil, e
				}
				o.pending[k] = id
				return op, nil
			})
			if e != nil {
				_, releaseErr := s.owner.call(context.Background(), func(o *owner) (any, error) { delete(o.inflight, k); return nil, nil })
				return dex.Block{}, errors.Join(e, releaseErr)
			}
			next.Operation = value.(runtimecheckpoint.Operation)
		}
		actual, sqlErr := writeOriginal(ctx, s.DEXStore, selected)
		_, err = s.owner.call(context.Background(), func(o *owner) (any, error) {
			delete(o.inflight, k)
			if sqlErr != nil {
				o.pending[k] = next.Operation.ID
				return nil, nil
			}
			if e := o.state.apply(actual); e != nil {
				return nil, e
			}
			if e := o.files.Applied(next.Operation); e != nil {
				return nil, e
			}
			delete(o.pending, k)
			o.dirty = true
			if e := o.capacity(); e != nil {
				return nil, e
			}
			if o.files.Due() {
				return nil, o.checkpoint()
			}
			return nil, nil
		})
		if e := errors.Join(sqlErr, err); e != nil {
			return dex.Block{}, e
		}
		if next.Operation.ID == id {
			return actual, nil
		}
		// An earlier unknown operation for this key is now proved/applied. Only
		// then reserve the new proposal and validate its original expected revision.
	}
}
func (s *Store) WriteDEXBatch(ctx context.Context, b dex.Batch) error {
	_, err := s.execute(ctx, frozenWrite{Version: 1, Kind: "batch", Block: b.Block, Batch: &b})
	return err
}
func (s *Store) WriteDEXBlock(ctx context.Context, b dex.Block) error {
	_, err := s.WriteRuntimeBlock(ctx, b, 0)
	return err
}
func (s *Store) WriteRuntimeBlock(ctx context.Context, b dex.Block, expected uint64) (dex.Block, error) {
	return s.execute(ctx, frozenWrite{Version: 1, Kind: "block", Block: b, Expected: expected})
}
func (s *Store) CompleteDEXLogs(ctx context.Context, b dex.Block, logs []dex.Log) error {
	return s.CompleteRuntimeLogs(ctx, b, logs, 0)
}
func (s *Store) CompleteRuntimeLogs(ctx context.Context, b dex.Block, logs []dex.Log, expected uint64) error {
	_, err := s.execute(ctx, frozenWrite{Version: 1, Kind: "logs", Block: b, Logs: logs, Expected: expected})
	return err
}
