package justlendkeeper

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint"
)

// Keeper already has a single sequential owner (Collector/Step). Extend its
// State/Ops checkpoint instead of introducing a second owner or pending queue.
type runtimeSnapshot struct {
	Version    uint32
	State      State
	Candidates runtimecheckpoint.Reference
}
type runtimeOwner struct {
	files                        *runtimecheckpoint.Store
	last                         *runtimeSnapshot
	candidates                   []Candidate
	poison                       error
	metricsAt                    time.Time
	written, fileSyncs, dirSyncs uint64
}

func runtimeCompress(v any) ([]byte, error) {
	raw, err := runtimecheckpoint.MarshalTyped(v)
	if err != nil {
		return nil, err
	}
	if len(raw) > maxCheckpointBytes {
		return nil, errors.New("checkpoint_size_limit")
	}
	var b bytes.Buffer
	z, err := gzip.NewWriterLevel(&b, gzip.BestSpeed)
	if err != nil {
		return nil, err
	}
	if _, err = z.Write(raw); err != nil {
		return nil, err
	}
	if err = z.Close(); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}
func runtimeExpand(raw []byte, v any) error {
	z, err := gzip.NewReader(bytes.NewReader(raw))
	if err != nil {
		return err
	}
	defer z.Close()
	body, err := io.ReadAll(io.LimitReader(z, maxCheckpointBytes+1))
	if err != nil {
		return err
	}
	if len(body) > maxCheckpointBytes {
		return errors.New("checkpoint_size_limit")
	}
	return runtimecheckpoint.UnmarshalTyped(body, v)
}
func runtimeDecode(raw []byte, files *runtimecheckpoint.Store, loadCandidates bool) (runtimeSnapshot, error) {
	var cp runtimeSnapshot
	if err := runtimeExpand(raw, &cp); err != nil {
		return cp, err
	}
	if cp.Version != 1 || cp.State.Runtime == nil || len(cp.State.Candidates) != 0 {
		return cp, runtimecheckpoint.ErrRecovery
	}
	if loadCandidates {
		raw, err := files.ReadObject(cp.Candidates)
		if err != nil {
			return cp, err
		}
		if err = runtimeExpand(raw, &cp.State.Candidates); err != nil {
			return cp, err
		}
	}
	return cp, nil
}
func (o *runtimeOwner) save(s *State) error {
	if o.poison != nil {
		return o.poison
	}
	refs, err := s.Runtime.references()
	if err != nil {
		return o.fail(err)
	}
	raw, err := runtimecheckpoint.MarshalTyped(*s)
	if err != nil {
		return o.fail(err)
	}
	var normalized State
	if err = runtimecheckpoint.UnmarshalTyped(raw, &normalized); err != nil {
		return o.fail(err)
	}
	candidates := normalized.Candidates
	normalized.Candidates = nil
	cp := runtimeSnapshot{Version: 1, State: normalized}
	if o.last == nil || !reflect.DeepEqual(candidates, o.candidates) {
		raw, err := runtimeCompress(candidates)
		if err != nil {
			return o.fail(err)
		}
		cp.Candidates, err = o.files.PutObject(raw)
		if err != nil {
			return o.fail(err)
		}
	} else {
		cp.Candidates = o.last.Candidates
	}
	refs = append(refs, cp.Candidates)
	if o.last != nil && reflect.DeepEqual(cp, *o.last) && !o.files.Due() {
		return nil
	}
	raw, err = runtimeCompress(cp)
	if err != nil {
		return o.fail(err)
	}
	if err = o.capacity(s); err != nil {
		return o.fail(err)
	}
	if err = o.files.Checkpoint(raw, refs); err != nil {
		return o.fail(err)
	}
	o.last, o.candidates = &cp, candidates
	return nil
}
func (o *runtimeOwner) fail(err error) error {
	if err != nil {
		o.poison = err
	}
	return err
}
func (o *runtimeOwner) capacity(s *State) error {
	// Existing State/Ops/business payload is accounted separately, just as the
	// design separates it from the newly added hot working-state budget.
	m := o.files.Metrics()
	added := s.Runtime.memoryBytes() + m.HotBytes - m.CheckpointBytes + m.VerifiedMetadataBytes
	if added > 16<<20 {
		return fmt.Errorf("keeper added runtime bytes=%d: %w", added, runtimecheckpoint.ErrCapacity)
	}
	return nil
}
func (o *runtimeOwner) frozen(op runtimecheckpoint.Operation) (Operation, error) {
	raw, err := o.files.ReadStateObject(op.Payload)
	if err != nil {
		return Operation{}, err
	}
	cp, err := runtimeDecode(raw, o.files, false)
	if err != nil {
		return Operation{}, err
	}
	for _, pending := range cp.State.Ops {
		if op.ID == "capture:"+pending.Batch.Capture.CaptureId.String() {
			if op.Kind != "keeper_operation" || !pending.Frozen || pending.RuntimeApplied {
				return pending, runtimecheckpoint.ErrConflict
			}
			if err = Validate(pending.Batch); err != nil {
				return pending, err
			}
			return pending, nil
		}
	}
	return Operation{}, errors.New("keeper_runtime_frozen_operation_missing")
}
func sameRuntimeOperation(a, b Operation) bool {
	a.NotBefore, b.NotBefore = time.Time{}, time.Time{}
	a.RuntimeApplied, b.RuntimeApplied = false, false
	// The typed roundtrip strips process-only time data and preserves known zero
	// pointers that ordinary gob omitted in the legacy checkpoint encoding.
	raw, err := runtimecheckpoint.MarshalTyped(b)
	if err != nil {
		return false
	}
	var copy Operation
	if runtimecheckpoint.UnmarshalTyped(raw, &copy) != nil {
		return false
	}
	return reflect.DeepEqual(a, copy)
}
func (c *Collector) runtimeBegin(o *Operation) (runtimecheckpoint.Operation, error) {
	owner := c.runtime
	if owner.poison != nil {
		return runtimecheckpoint.Operation{}, owner.poison
	}
	if !o.Frozen || o.Batch.Capture.ConfigHash != c.Config.Hash() {
		return runtimecheckpoint.Operation{}, owner.fail(runtimecheckpoint.ErrConflict)
	}
	if err := Validate(o.Batch); err != nil {
		return runtimecheckpoint.Operation{}, owner.fail(err)
	}
	id := "capture:" + o.Batch.Capture.CaptureId.String()
	if op, ok := owner.files.Find(id); ok {
		frozen, err := owner.frozen(op)
		if err != nil {
			return op, owner.fail(err)
		}
		if !sameRuntimeOperation(frozen, *o) {
			return op, owner.fail(runtimecheckpoint.ErrConflict)
		}
		return op, nil
	}
	// prepareIndexPage may augment a legacy frozen operation. Persist that exact
	// existing Ops entry before linking the checkpoint and admitting SQL.
	if err := c.Save(); err != nil {
		return runtimecheckpoint.Operation{}, err
	}
	ref, err := owner.files.LinkCurrent()
	if err != nil {
		return runtimecheckpoint.Operation{}, owner.fail(err)
	}
	op, err := owner.files.Begin(id, "keeper_operation", ref, 0, 0)
	if err != nil {
		return op, owner.fail(err)
	}
	frozen, err := owner.frozen(op)
	if err == nil && !sameRuntimeOperation(frozen, *o) {
		err = runtimecheckpoint.ErrConflict
	}
	return op, owner.fail(err)
}
func (c *Collector) runtimeCommitted(b Batch) error {
	if err := c.State.Runtime.parent(b.Capture, b.IndexPage, c.State.EvidenceCursors); err != nil {
		return c.runtime.fail(err)
	}
	return c.runtime.fail(c.State.Runtime.child(b.Capture))
}
func (c *Collector) runtimeApplied(o *Operation, op runtimecheckpoint.Operation) error {
	if err := c.runtime.capacity(c.State); err != nil {
		return c.runtime.fail(err)
	}
	if err := c.runtime.files.Applied(op); err != nil {
		return c.runtime.fail(err)
	}
	o.RuntimeApplied = true
	return nil
}
func (c *Collector) OpenRuntime(ctx context.Context, dir string, binding runtimecheckpoint.Binding) error {
	if c.runtime != nil {
		return errors.New("keeper_runtime_already_open")
	}
	files, err := runtimecheckpoint.OpenContext(ctx, dir, binding, maxCheckpointBytes)
	if err != nil {
		return err
	}
	cp, err := runtimeDecode(files.State(), files, true)
	if err != nil {
		files.Close()
		return err
	}
	if cp.State.Version != 1 || cp.State.Target != c.State.Target || cp.State.ConfigHash != Hex(c.Config.Hash()) || cp.State.Sources == nil {
		files.Close()
		return runtimecheckpoint.ErrRecovery
	}
	if cp.State.Runtime.Conversion.Version != 0 {
		if err := cp.State.Runtime.Conversion.InputInventory.Verify(files); err != nil {
			files.Close()
			return err
		}
	}
	if err = cp.State.Runtime.open(files); err == nil {
		err = cp.State.Runtime.validate(binding, c.Config.Hash())
	}
	if err != nil {
		files.Close()
		return err
	}
	*c.State = cp.State
	c.runtime = &runtimeOwner{files: files, metricsAt: time.Now()}
	fail := func(err error) error { files.Close(); c.runtime = nil; return err }
	// Every uncovered SQL attempt has a link to the original frozen Ops entry.
	for after := uint64(0); ; {
		pending := files.Pending(after, 16)
		if len(pending) == 0 {
			break
		}
		for _, record := range pending {
			if record.Kind == "keeper_import" {
				if err = c.applyRuntimeImport(ctx, record); err != nil {
					return fail(err)
				}
				after = record.Sequence
				continue
			}
			original, e := c.runtime.frozen(record)
			if e != nil {
				return fail(e)
			}
			found := false
			for i := range c.State.Ops {
				current := &c.State.Ops[i]
				if current.Batch.Capture.CaptureId == original.Batch.Capture.CaptureId {
					if current.Frozen && !sameRuntimeOperation(original, *current) {
						return fail(runtimecheckpoint.ErrConflict)
					}
					if current.NotBefore.After(original.NotBefore) {
						original.NotBefore = current.NotBefore
					}
					*current = original
					found = true
					break
				}
			}
			if !found {
				c.State.Ops = append(c.State.Ops, original)
			}
			after = record.Sequence
		}
	}
	// Resolve original frozen operations before admitting new source work. The
	// database writer retries the same transaction identity; a missing read alone
	// is never used to declare an uncertain commit rolled back.
	for i := 0; i < len(c.State.Ops); {
		if !c.State.Ops[i].Frozen {
			i++
			continue
		}
		if err = c.Finish(ctx, &c.State.Ops[i]); err != nil {
			return fail(err)
		}
		c.State.Ops = append(c.State.Ops[:i], c.State.Ops[i+1:]...)
		if err = c.Save(); err != nil {
			return fail(err)
		}
	}
	if err = c.runtime.capacity(c.State); err != nil {
		return fail(err)
	}
	return nil
}
func (c *Collector) CloseRuntime() error {
	if c.runtime == nil {
		return nil
	}
	err := c.Save()
	closeErr := c.runtime.files.Close()
	c.runtime = nil
	return errors.Join(err, closeErr)
}
func (c *Collector) runtimeMetrics() {
	if c.runtime == nil {
		return
	}
	o := c.runtime
	now := c.Now()
	if now.Sub(o.metricsAt) < 10*time.Second {
		return
	}
	m := o.files.Metrics()
	elapsed := now.Sub(o.metricsAt)
	rate := uint64(0)
	if elapsed > 0 {
		rate = (m.WrittenBytes - o.written) * uint64(time.Second) / uint64(elapsed)
	}
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	var rss uint64
	if raw, err := os.ReadFile("/proc/self/statm"); err == nil {
		parts := strings.Fields(string(raw))
		if len(parts) > 1 {
			pages, _ := strconv.ParseUint(parts[1], 10, 64)
			rss = pages * uint64(os.Getpagesize())
		}
	}
	c.Log(fmt.Sprintf("runtime_state %s checkpoint_age_ms=%d checkpoint_bytes=%d recovery_bytes=%d recovery_peak_bytes=%d heap_bytes=%d rss_bytes=%d event_progress=%v evidence_progress=%v error=%v working_hot_bytes=%d pending_ops=%d uncheckpointed=%d written_bytes_per_second=%d file_syncs=%d directory_syncs=%d", c.State.Runtime.summary(), now.Sub(m.CheckpointAt).Milliseconds(), m.CheckpointBytes, m.RecoveryBytes, m.PeakRecoveryBytes, memory.HeapAlloc, rss, c.State.EventCursors, c.State.EvidenceCursors, o.poison, c.State.Runtime.memoryBytes(), len(c.State.Ops), m.Uncheckpointed, rate, m.FileSyncs-o.fileSyncs, m.DirectorySyncs-o.dirSyncs))
	o.metricsAt, o.written, o.fileSyncs, o.dirSyncs = now, m.WrittenBytes, m.FileSyncs, m.DirectorySyncs
}
