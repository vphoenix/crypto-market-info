package runtimecheckpoint

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
)

var ErrHandoff = errors.New("runtime maintenance handoff incomplete; resume the frozen plan before collection")

type maintenanceContextKey struct{}

// MaintenanceContext is used only by the explicit stopped-service command.
func MaintenanceContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, maintenanceContextKey{}, true)
}
func OpenContext(ctx context.Context, dir string, b Binding, limit int64) (*Store, error) {
	allow, _ := ctx.Value(maintenanceContextKey{}).(bool)
	return open(dir, b, limit, allow, false)
}

type HandoffPayload struct{ File, SHA256 string }

type HandoffInput[T any] struct {
	Version    uint32
	Binding    Binding
	Reason     string
	Operations []T
}

func ReadHandoffInput[T any](path string, b Binding) (HandoffInput[T], []byte, error) {
	var in HandoffInput[T]
	f, err := os.Open(path)
	if err != nil {
		return in, nil, err
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, (32<<20)+1))
	if err != nil {
		return in, nil, err
	}
	if len(raw) > 32<<20 {
		return in, nil, ErrCapacity
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&in); err != nil {
		return in, nil, err
	}
	var extra any
	if err = decoder.Decode(&extra); err != io.EOF {
		return in, nil, ErrConflict
	}
	if in.Version != 1 || in.Binding != b || in.Reason == "" || len(in.Reason) > 4096 || len(in.Operations) == 0 || len(in.Operations) > 4096 {
		return in, nil, ErrConflict
	}
	return in, raw, nil
}

// HandoffWork carries the domain's ORIGINAL frozen operation, never a new SQL
// identity. The exact finite list is checkpointed before the first SQL write.
type HandoffWork struct {
	Task                          string
	SourcePath                    string
	ID, Kind                      string
	Payload                       []byte
	ExpectedRevision, NewRevision uint64
}
type HandoffItem struct {
	Task                          string
	ID, Kind                      string
	Payload                       Reference
	ExpectedRevision, NewRevision uint64
}
type handoffPlan struct {
	Version uint32
	Binding Binding
	Source  Reference
	Items   []HandoffItem
}
type HandoffStatus struct {
	Plan, Source Reference
	Next, Total  uint32
	Complete     bool
}

func (s *Store) Handoff() HandoffStatus { return s.handoff }
func (s *Store) HandoffNext() (HandoffItem, bool) {
	if s.handoff.Complete || s.handoffPlan == nil || s.handoff.Next >= s.handoff.Total {
		return HandoffItem{}, false
	}
	return s.handoffPlan.Items[s.handoff.Next], true
}
func (s *Store) handoffRefs(status HandoffStatus) ([]Reference, error) {
	if status.Plan.Hash == "" {
		if status != (HandoffStatus{}) {
			return nil, ErrRecovery
		}
		return nil, nil
	}
	raw, err := s.ReadObject(status.Plan)
	if err != nil {
		return nil, err
	}
	var p handoffPlan
	if err = UnmarshalTyped(raw, &p); err != nil {
		return nil, err
	}
	if p.Version != 1 || p.Binding != s.current.Binding || len(p.Items) == 0 || len(p.Items) > 4096 || uint32(len(p.Items)) != status.Total || status.Next > status.Total || status.Complete != (status.Next == status.Total) || p.Source != status.Source {
		return nil, ErrRecovery
	}
	refs := []Reference{status.Plan, status.Source}
	if !status.Complete {
		for _, item := range p.Items {
			refs = append(refs, item.Payload)
		}
	}
	return refs, nil
}
func (s *Store) loadHandoff(allow, seal bool) error {
	for _, cp := range []checkpoint{s.current, s.previous} {
		refs, err := s.handoffRefs(cp.Handoff)
		if err != nil {
			return err
		}
		for _, ref := range refs {
			if !slices.Contains(cp.References, ref) {
				return ErrRecovery
			}
		}
	}
	s.handoff = s.current.Handoff
	if s.handoff.Plan.Hash != "" {
		raw, err := s.ReadObject(s.handoff.Plan)
		if err != nil {
			return err
		}
		var p handoffPlan
		if err = UnmarshalTyped(raw, &p); err != nil {
			return err
		}
		s.handoffPlan = &p
		s.metrics.HotBytes += EstimateTypedBytes(p)
		if s.metrics.HotBytes > s.hotLimit {
			return ErrCapacity
		}
	}
	if !allow && s.handoff.Plan.Hash != "" && !s.handoff.Complete {
		return ErrHandoff
	}
	if seal && allow && s.handoff.Plan.Hash != "" && !s.handoff.Complete && s.previous.Handoff.Plan != s.handoff.Plan {
		// A crash may have interrupted the initial two-root barrier. Complete
		// it before an explicit maintenance resume can issue the first SQL.
		if err := s.sealHandoffRoot(); err != nil {
			return err
		}
	}
	return nil
}
func (s *Store) StartHandoff(source []byte, work []HandoffWork) error {
	if err := s.ready(); err != nil {
		return err
	}
	digest := sha256.Sum256(source)
	sourceRef := Reference{hex.EncodeToString(digest[:]), int64(len(source))}
	if s.handoff.Source == sourceRef {
		return nil
	}
	if s.handoff.Plan.Hash != "" && !s.handoff.Complete {
		return ErrHandoff
	}
	if len(work) == 0 || len(work) > 4096 || len(source) > 32<<20 || s.Metrics().Pending != 0 || s.Metrics().Uncheckpointed != 0 {
		return ErrHandoff
	}
	p := handoffPlan{Version: 1, Binding: s.Binding(), Source: sourceRef}
	seen := map[string]bool{}
	for _, w := range work {
		if w.ID == "" || len(w.ID) > 1024 || w.Kind == "" || len(w.Kind) > 160 || seen[w.ID] || len(w.Payload) == 0 || len(w.Payload) > maxObjectBytes {
			return ErrConflict
		}
		seen[w.ID] = true
		var ref Reference
		var err error
		if w.SourcePath != "" {
			ref, err = s.LinkPending(w.SourcePath)
		} else {
			ref, err = s.PutObject(w.Payload)
		}
		if err != nil {
			return err
		}
		sum := sha256.Sum256(w.Payload)
		if ref != (Reference{hex.EncodeToString(sum[:]), int64(len(w.Payload))}) {
			return ErrConflict
		}
		p.Items = append(p.Items, HandoffItem{w.Task, w.ID, w.Kind, ref, w.ExpectedRevision, w.NewRevision})
	}
	if _, err := s.PutObject(source); err != nil {
		return err
	}
	raw, err := MarshalTyped(p)
	if err != nil {
		return err
	}
	ref, err := s.PutObject(raw)
	if err != nil {
		return err
	}
	refs := slices.Clone(s.current.References)
	oldRefs, err := s.handoffRefs(s.current.Handoff)
	if err != nil {
		return err
	}
	refs = slices.DeleteFunc(refs, func(ref Reference) bool { return slices.Contains(oldRefs, ref) })
	if s.handoffPlan != nil {
		s.metrics.HotBytes -= EstimateTypedBytes(*s.handoffPlan)
	}
	s.handoffPlan = &p
	s.metrics.HotBytes += EstimateTypedBytes(p)
	s.handoff = HandoffStatus{Plan: ref, Source: sourceRef, Total: uint32(len(work))}
	if err = s.Checkpoint(s.State(), refs); err != nil {
		return err
	}
	// Both recovery roots must know the entire finite plan BEFORE admitting
	// SQL. Otherwise RecoverPrevious could retain the first issued operation
	// but forget every not-yet-issued operation and allow normal collection.
	// Admission required a drained, checkpointed tail, so this does not retire
	// any older unresolved work.
	return s.sealHandoffRoot()
}

func (s *Store) sealHandoffRoot() error {
	if err := s.writeCheckpoint("checkpoint.previous", s.current); err != nil {
		return s.fail(err)
	}
	s.previous = s.current
	s.previous.State = nil
	return nil
}
func (s *Store) checkHandoff(op Operation) error {
	item, ok := s.HandoffNext()
	if !ok {
		return nil
	}
	if op.Task != item.Task || op.ID != item.ID || op.Kind != item.Kind || op.Payload != item.Payload || op.ExpectedRevision != item.ExpectedRevision || op.NewRevision != item.NewRevision {
		return ErrHandoff
	}
	return nil
}
func (s *Store) applyHandoff(op Operation) error {
	if _, ok := s.HandoffNext(); !ok {
		return nil
	}
	if err := s.checkHandoff(op); err != nil {
		return err
	}
	s.handoff.Next++
	s.handoff.Complete = s.handoff.Next == s.handoff.Total
	return nil
}
func (s *Store) checkpointHandoff(refs []Reference) ([]Reference, error) {
	refs = slices.Clone(refs)
	extra, err := s.handoffRefs(s.handoff)
	if err != nil {
		return nil, err
	}
	for _, ref := range extra {
		if !slices.Contains(refs, ref) {
			refs = append(refs, ref)
		}
	}
	return refs, nil
}
func (s *Store) handoffUnchanged() bool { return reflect.DeepEqual(s.current.Handoff, s.handoff) }

// ExecuteHandoff runs only while the service is stopped and target/process locks
// are held. access executes on the existing domain owner; commit performs SQL
// outside that owner and applies the verified result through its original path.
func ExecuteHandoff(ctx context.Context, path string, b Binding, prepare func([]byte) (HandoffWork, error), access func(func(*Store) (any, error)) (any, error), checkpoint func() error, commit func(Operation) error) (HandoffStatus, error) {
	in, source, err := ReadHandoffInput[HandoffPayload](path, b)
	if err != nil {
		return HandoffStatus{}, err
	}
	value, err := access(func(files *Store) (any, error) { return files.Handoff(), nil })
	if err != nil {
		return HandoffStatus{}, err
	}
	previous := value.(HandoffStatus)
	digest := sha256.Sum256(source)
	same := previous.Source == (Reference{hex.EncodeToString(digest[:]), int64(len(source))})
	if !same && previous.Plan.Hash != "" && !previous.Complete {
		return HandoffStatus{}, ErrHandoff
	}
	work := make([]HandoffWork, 0, len(in.Operations))
	var total int64
	for _, entry := range in.Operations {
		if same {
			break
		}
		if entry.File == "" || len(entry.SHA256) != 64 {
			return HandoffStatus{}, ErrConflict
		}
		file := entry.File
		if !filepath.IsAbs(file) {
			file = filepath.Join(filepath.Dir(path), file)
		}
		info, e := os.Stat(file)
		if e != nil {
			return HandoffStatus{}, e
		}
		if !info.Mode().IsRegular() || info.Size() > maxObjectBytes {
			return HandoffStatus{}, ErrCapacity
		}
		total += info.Size()
		if total > 64<<20 {
			return HandoffStatus{}, ErrCapacity
		}
		raw, e := os.ReadFile(file)
		if e != nil {
			return HandoffStatus{}, e
		}
		sum := sha256.Sum256(raw)
		if hex.EncodeToString(sum[:]) != entry.SHA256 {
			return HandoffStatus{}, ErrConflict
		}
		w, e := prepare(raw)
		w.SourcePath = file
		if e == nil && !bytes.Equal(w.Payload, raw) {
			return HandoffStatus{}, ErrConflict
		}
		if e != nil {
			return HandoffStatus{}, e
		}
		work = append(work, w)
	}
	if err = checkpoint(); err != nil {
		return HandoffStatus{}, err
	}
	if _, err = access(func(files *Store) (any, error) { return nil, files.StartHandoff(source, work) }); err != nil {
		return HandoffStatus{}, err
	}
	for {
		if err = ctx.Err(); err != nil {
			return HandoffStatus{}, err
		}
		value, e := access(func(files *Store) (any, error) {
			item, ok := files.HandoffNext()
			if !ok {
				return Operation{}, nil
			}
			return files.Begin(item.ID, item.Kind, item.Payload, item.ExpectedRevision, item.NewRevision, item.Task)
		})
		if e != nil {
			return HandoffStatus{}, e
		}
		op := value.(Operation)
		if op.ID == "" {
			break
		}
		if err = commit(op); err != nil {
			return HandoffStatus{}, err
		}
	}
	if err = checkpoint(); err != nil {
		return HandoffStatus{}, err
	}
	value, err = access(func(files *Store) (any, error) { return files.Handoff(), nil })
	if err != nil {
		return HandoffStatus{}, err
	}
	return value.(HandoffStatus), nil
}
