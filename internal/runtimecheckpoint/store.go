// Package runtimecheckpoint provides local durability, not business progress or
// proof of a database commit. A domain's single state owner supplies typed state
// and only calls Applied after the original database/verification protocol.
package runtimecheckpoint

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/gob"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
)

var (
	ErrBusy          = errors.New("runtime task has an unresolved operation")
	ErrRecovery      = errors.New("runtime state requires explicit recovery")
	ErrConflict      = errors.New("runtime operation content conflict")
	ErrCapacity      = errors.New("runtime hot state budget exceeded; durable work retained")
	ErrCheckpointDue = errors.New("checkpoint required before accepting more operations")
)

const (
	formatVersion   = 1
	segmentBytes    = 4 << 20
	maxObjectBytes  = 64 << 20
	defaultHotBytes = 16 << 20
)

// RecoveryEpoch is supplied by the explicit database restore/maintenance
// protocol. A server UUID or producer generation alone does not detect restore.
type Binding struct {
	Service, Database, ServerUUID, ScopeHash, SchemaHash string
	Producer, Holder, RecoveryEpoch                      string
	ProducerGeneration                                   uint64
	StateVersion                                         uint32
}

func (b Binding) validate() error {
	if b.Service == "" || b.Database == "" || b.ServerUUID == "" || b.ScopeHash == "" || b.SchemaHash == "" || b.Producer == "" || b.Holder == "" || b.RecoveryEpoch == "" || b.ProducerGeneration == 0 || b.StateVersion == 0 {
		return fmt.Errorf("incomplete runtime binding: %w", ErrRecovery)
	}
	for _, v := range []string{b.Service, b.Database, b.ServerUUID, b.ScopeHash, b.SchemaHash, b.Producer, b.Holder, b.RecoveryEpoch} {
		if len(v) > 1024 {
			return fmt.Errorf("oversized runtime binding: %w", ErrRecovery)
		}
	}
	return nil
}

// Reference names an immutable local object. Domain codecs preserve their
// pointer presence, exact decimal/integer values and original observation time.
type Reference struct {
	Hash  string
	Bytes int64
}

type Operation struct {
	Task                          string
	Sequence                      uint64
	Generation                    string
	ID, Kind                      string
	Payload                       Reference
	ExpectedRevision, NewRevision uint64
}

type record struct {
	Version    uint32
	Generation string
	Binding    Binding
	Operation  Operation
}

type checkpoint struct {
	Handoff         HandoffStatus
	Version         uint32
	Generation      string
	Binding         Binding
	Number, Through uint64
	Pending         []uint64
	State           []byte
	References      []Reference
	WrittenAt       time.Time
}

type entry struct {
	Record  record
	Path    string
	Applied bool
	Bytes   int64
}

type Metrics struct {
	WrittenBytes, FileSyncs, DirectorySyncs, Checkpoints, Intents uint64
	HotBytes                                                      int64
	Pending, Uncheckpointed                                       uint64
	CheckpointBytes                                               int64
	CheckpointAt                                                  time.Time
	RecoveryBytes, PeakRecoveryBytes, VerifiedMetadataBytes       int64
}

// Store is deliberately single-owner. Network/SQL workers receive immutable
// Operations and return results to the domain owner through bounded channels.
// This package never holds a state mutex while doing I/O or calls a database.
type Store struct {
	handoff           HandoffStatus
	handoffPlan       *handoffPlan
	dir               string
	lock              *os.File
	current, previous checkpoint
	entries           map[uint64]*entry
	issued            uint64
	segment           string
	segmentSize       int64
	hotLimit          int64
	poison            error
	metrics           Metrics
	now               func() time.Time
	fault             func(string) error
	verified          map[string]os.FileInfo
}

func acquire(dir string, limit int64) (*Store, error) {
	if limit <= 0 {
		limit = defaultHotBytes
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, "runtime.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("runtime state already owned: %w", err)
	}
	return &Store{dir: dir, lock: f, entries: map[uint64]*entry{}, verified: map[string]os.FileInfo{}, hotLimit: limit, now: func() time.Time { return time.Now().UTC() }}, nil
}

func (s *Store) Close() error {
	if s.lock == nil {
		return nil
	}
	err := s.lock.Close()
	s.lock = nil
	return err
}

func encode(v any) ([]byte, error) {
	var b bytes.Buffer
	err := gob.NewEncoder(&b).Encode(v)
	return b.Bytes(), err
}
func decode(b []byte, v any) error {
	d := gob.NewDecoder(bytes.NewReader(b))
	if err := d.Decode(v); err != nil {
		return err
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return fmt.Errorf("trailing state data: %w", ErrRecovery)
	}
	return nil
}

// Each final record is atomically installed after a full file sync. Incomplete
// .tmp files have never crossed the permission-to-write-SQL barrier. Damage to
// a final file is never treated as an ignorable interrupted tail.
func frame(kind byte, body []byte) []byte {
	out := make([]byte, 48+len(body))
	copy(out[:4], "CMIR")
	out[4] = formatVersion
	out[5] = kind
	binary.BigEndian.PutUint64(out[8:16], uint64(len(body)))
	copy(out[48:], body)
	h := sha256.Sum256(append(append([]byte(nil), out[:16]...), body...))
	copy(out[16:48], h[:])
	return out
}

func unframe(raw []byte, kind byte, limit int64) ([]byte, error) {
	if len(raw) < 48 || int64(len(raw)) > limit || string(raw[:4]) != "CMIR" || raw[4] != formatVersion || raw[5] != kind || raw[6] != 0 || raw[7] != 0 || binary.BigEndian.Uint64(raw[8:16]) != uint64(len(raw)-48) {
		return nil, fmt.Errorf("invalid runtime frame: %w", ErrRecovery)
	}
	h := sha256.Sum256(append(append([]byte(nil), raw[:16]...), raw[48:]...))
	if !bytes.Equal(h[:], raw[16:48]) {
		return nil, fmt.Errorf("runtime checksum: %w", ErrRecovery)
	}
	return raw[48:], nil
}

func readFrame(path string, kind byte, limit int64, v any) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() > limit {
		return fmt.Errorf("runtime file bounds: %w", ErrRecovery)
	}
	raw, err := io.ReadAll(io.LimitReader(f, limit+1))
	if err != nil {
		return err
	}
	body, err := unframe(raw, kind, limit)
	if err != nil {
		return err
	}
	return decode(body, v)
}

func (s *Store) syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	err = f.Sync()
	s.metrics.DirectorySyncs++
	return err
}
func (s *Store) point(stage string) error {
	if s.fault != nil {
		return s.fault(stage)
	}
	return nil
}
func (s *Store) fail(err error) error {
	if err != nil {
		s.poison = err
	}
	return err
}
func (s *Store) ready() error {
	if s.lock == nil {
		return errors.New("runtime store closed")
	}
	if s.poison != nil {
		return fmt.Errorf("runtime store must reopen after I/O failure: %w", s.poison)
	}
	return nil
}

func (s *Store) atomic(path string, raw []byte, stage string) error {
	var oldSize int64
	if old, err := os.Stat(path); err == nil {
		oldSize = old.Size()
	} else if !os.IsNotExist(err) {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".runtime-tmp-")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	if _, err = f.Write(raw); err == nil {
		s.metrics.PeakRecoveryBytes = max(s.metrics.PeakRecoveryBytes, s.metrics.RecoveryBytes+int64(len(raw)))
		s.metrics.WrittenBytes += uint64(len(raw))
		err = f.Sync()
		s.metrics.FileSyncs++
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = s.point(stage + "_before_replace"); err != nil {
		return err
	}
	if err = os.Rename(name, path); err != nil {
		return err
	}
	s.metrics.RecoveryBytes += int64(len(raw)) - oldSize
	if err = s.point(stage + "_after_replace"); err != nil {
		return err
	}
	return s.syncDir(filepath.Dir(path))
}

func (s *Store) writeCheckpoint(name string, c checkpoint) error {
	b, err := encode(c)
	if err != nil {
		return err
	}
	return s.atomic(filepath.Join(s.dir, name), frame(1, b), name)
}

// Create is only for a verified explicit conversion. Open never creates an
// empty checkpoint or falls back to scanning domain history.
func Create(dir string, b Binding, state []byte, refs []Reference, hotLimit int64) (*Store, error) {
	if err := b.validate(); err != nil {
		return nil, err
	}
	s, err := acquire(dir, hotLimit)
	if err != nil {
		return nil, err
	}
	ok := false
	defer func() {
		if !ok {
			s.Close()
		}
	}()
	for _, name := range []string{"checkpoint.current", "checkpoint.previous", "operations", "issued"} {
		if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
			return nil, fmt.Errorf("runtime conversion destination not empty: %w", ErrRecovery)
		}
	}
	if int64(len(state)) > s.hotLimit {
		return nil, ErrCapacity
	}
	c := checkpoint{Version: formatVersion, Generation: uuid.NewString(), Binding: b, Number: 1, State: bytes.Clone(state), References: slices.Clone(refs), WrittenAt: s.now()}
	if err = s.checkReferences(refs); err != nil {
		return nil, err
	}
	mark, _ := encode(checkpoint{Version: formatVersion, Generation: c.Generation, Binding: b})
	if err = s.atomic(filepath.Join(dir, "issued"), frame(3, mark), "issued"); err != nil {
		return nil, err
	}
	if err = s.writeCheckpoint("checkpoint.previous", c); err != nil {
		return nil, err
	}
	if err = s.writeCheckpoint("checkpoint.current", c); err != nil {
		return nil, err
	}
	s.current, s.previous = c, c
	s.metrics.HotBytes = int64(len(state))
	s.metrics.CheckpointAt = c.WrittenAt
	s.metrics.CheckpointBytes = int64(len(state))
	if err = s.measureRecoveryFiles(); err != nil {
		return nil, err
	}
	ok = true
	return s, nil
}

func Open(dir string, b Binding, hotLimit int64) (*Store, error) {
	return open(dir, b, hotLimit, false, false)
}
func open(dir string, b Binding, hotLimit int64, maintenance, inspection bool) (*Store, error) {
	if err := b.validate(); err != nil {
		return nil, err
	}
	if info, err := os.Stat(dir); err != nil || !info.IsDir() {
		return nil, fmt.Errorf("runtime directory missing or invalid: %w: %v", ErrRecovery, err)
	}
	s, err := acquire(dir, hotLimit)
	if err != nil {
		return nil, err
	}
	ok := false
	defer func() {
		if !ok {
			s.Close()
		}
	}()
	for name, dst := range map[string]*checkpoint{"checkpoint.current": &s.current, "checkpoint.previous": &s.previous} {
		if err = readFrame(filepath.Join(dir, name), 1, s.hotLimit+1<<20, dst); err != nil {
			return nil, fmt.Errorf("%s: %w: %w", name, ErrRecovery, err)
		}
		if dst.Version != formatVersion || dst.Binding != b || dst.Generation == "" || dst.Number == 0 {
			return nil, fmt.Errorf("runtime binding/version changed: %w", ErrRecovery)
		}
		if err = s.checkReferences(dst.References); err != nil {
			return nil, err
		}
	}
	if s.previous.Generation != s.current.Generation || s.previous.Number > s.current.Number || s.previous.Through > s.current.Through {
		return nil, fmt.Errorf("checkpoint chain: %w", ErrRecovery)
	}
	// Only the current business snapshot is hot. The previous snapshot stays on
	// disk for explicit recovery; its references/boundaries remain available to GC.
	s.previous.State = nil
	s.issued = s.current.Through
	s.metrics.HotBytes = int64(len(s.current.State))
	s.metrics.CheckpointAt = s.current.WrittenAt
	s.metrics.CheckpointBytes = int64(len(s.current.State))
	pending := map[uint64]bool{}
	for _, seq := range s.current.Pending {
		if seq == 0 || seq > s.current.Through || pending[seq] {
			return nil, fmt.Errorf("pending checkpoint boundary: %w", ErrRecovery)
		}
		pending[seq] = true
	}
	err = s.walkRecords(func(path string, r record, n int64) error {
		if r.Binding != b || r.Generation != s.current.Generation || r.Version != formatVersion || r.Operation.Sequence == 0 {
			return fmt.Errorf("operation binding: %w", ErrRecovery)
		}
		op := r.Operation
		if op.Generation != s.current.Generation {
			return fmt.Errorf("operation generation: %w", ErrRecovery)
		}
		if _, exists := s.entries[op.Sequence]; exists {
			return fmt.Errorf("duplicate local sequence: %w", ErrRecovery)
		}
		if err := s.checkReferences([]Reference{op.Payload}); err != nil {
			return err
		}
		e := &entry{Record: r, Path: path, Bytes: n, Applied: op.Sequence <= s.current.Through && !pending[op.Sequence]}
		s.entries[op.Sequence] = e
		s.issued = max(s.issued, op.Sequence)
		s.metrics.HotBytes += entryBytes(e)
		if s.metrics.HotBytes > s.hotLimit {
			return ErrCapacity
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	for _, seq := range s.previous.Pending {
		if seq == 0 || seq > s.previous.Through || s.entries[seq] == nil {
			return nil, fmt.Errorf("previous checkpoint pending reference missing: %w", ErrRecovery)
		}
	}
	for seq := range pending {
		if s.entries[seq] == nil {
			return nil, fmt.Errorf("pending operation reference missing: %w", ErrRecovery)
		}
	}
	// Every issued operation after the checkpoint must be represented. A missing
	// middle/final file is corruption, not evidence that SQL was never executed.
	for seq := s.previous.Through + 1; seq != 0 && seq <= s.issued; seq++ {
		if s.entries[seq] == nil {
			return nil, fmt.Errorf("operation sequence gap: %w", ErrRecovery)
		}
		if seq == ^uint64(0) {
			break
		}
	}
	// A durable high-water mark also detects removal of the last intent.
	var issued checkpoint
	if err = readFrame(filepath.Join(dir, "issued"), 3, 1<<20, &issued); err == nil {
		if issued.Version != formatVersion || issued.Binding != b || issued.Generation != s.current.Generation || issued.Through > s.issued || issued.Through < s.current.Through {
			return nil, fmt.Errorf("issued operation missing: %w", ErrRecovery)
		}
	} else {
		return nil, fmt.Errorf("issued boundary missing or invalid: %w: %w", ErrRecovery, err)
	}
	// An installed intent may precede a crash while installing its high-water
	// mark. Seal that boundary before recovery can return permission to retry SQL.
	if issued.Through < s.issued && !inspection {
		issued.Through = s.issued
		mark, err := encode(issued)
		if err != nil {
			return nil, err
		}
		if err = s.atomic(filepath.Join(dir, "issued"), frame(3, mark), "issued"); err != nil {
			return nil, err
		}
	}
	if err = s.measureRecoveryFiles(); err != nil {
		return nil, err
	}
	if err = s.loadHandoff(maintenance, !inspection); err != nil {
		return nil, err
	}
	ok = true
	return s, nil
}

// Only initial open/conversion enumerates disk usage. Live metrics are updated
// by the owner's writes/removals, never by a periodic filesystem/database scan.
func (s *Store) measureRecoveryFiles() error {
	var total int64
	err := filepath.WalkDir(s.dir, func(_ string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		total += info.Size()
		return nil
	})
	s.metrics.RecoveryBytes = total
	s.metrics.PeakRecoveryBytes = max(s.metrics.PeakRecoveryBytes, total)
	return err
}

func entryBytes(e *entry) int64 {
	b := e.Record.Binding
	op := e.Record.Operation
	return int64(1024 + len(op.ID) + len(op.Kind) + len(op.Generation) + len(e.Path) + len(b.Service) + len(b.Database) + len(b.ServerUUID) + len(b.ScopeHash) + len(b.SchemaHash) + len(b.Producer) + len(b.Holder) + len(b.RecoveryEpoch))
}

func (s *Store) walkRecords(fn func(string, record, int64) error) error {
	root := filepath.Join(s.dir, "operations")
	segments, err := os.ReadDir(root)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, segment := range segments {
		if !segment.IsDir() || !strings.HasPrefix(segment.Name(), "segment-") {
			return fmt.Errorf("unexpected recovery segment: %w", ErrRecovery)
		}
		dir := filepath.Join(root, segment.Name())
		files, err := os.ReadDir(dir)
		if err != nil {
			return err
		}
		var size int64
		for _, file := range files {
			if strings.HasPrefix(file.Name(), ".runtime-tmp-") {
				continue
			}
			if file.IsDir() || !strings.HasSuffix(file.Name(), ".op") {
				return fmt.Errorf("unexpected recovery record: %w", ErrRecovery)
			}
			path := filepath.Join(dir, file.Name())
			var r record
			if err = readFrame(path, 2, segmentBytes, &r); err != nil {
				return err
			}
			if file.Name() != fmt.Sprintf("%020d.op", r.Operation.Sequence) {
				return fmt.Errorf("operation filename: %w", ErrRecovery)
			}
			info, err := file.Info()
			if err != nil {
				return err
			}
			size += info.Size()
			if size > segmentBytes {
				return fmt.Errorf("oversized recovery segment: %w", ErrRecovery)
			}
			if err = fn(path, r, info.Size()); err != nil {
				return err
			}
		}
		s.segment, s.segmentSize = dir, size
	}
	return nil
}

func (s *Store) objectPath(ref Reference) (string, error) {
	if len(ref.Hash) != 64 || ref.Bytes < 0 || ref.Bytes > maxObjectBytes {
		return "", fmt.Errorf("runtime object reference: %w", ErrRecovery)
	}
	if _, err := hex.DecodeString(ref.Hash); err != nil {
		return "", err
	}
	return filepath.Join(s.dir, "objects", ref.Hash+".blob"), nil
}

func (s *Store) checkReferences(refs []Reference) error {
	for _, ref := range refs {
		path, err := s.objectPath(ref)
		if err != nil {
			return err
		}
		info, err := os.Stat(path)
		if err != nil {
			return err
		}
		if old := s.verified[ref.Hash]; old != nil && info.Mode().IsRegular() && info.Size() == ref.Bytes && os.SameFile(old, info) && old.ModTime().Equal(info.ModTime()) {
			previous, pok := old.Sys().(*syscall.Stat_t)
			current, cok := info.Sys().(*syscall.Stat_t)
			if pok && cok && previous.Ctim == current.Ctim {
				continue
			}
		}
		if _, err := s.ReadObject(ref); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) ReadObject(ref Reference) ([]byte, error) {
	path, err := s.objectPath(ref)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("runtime payload missing: %w: %w", ErrRecovery, err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() != ref.Bytes {
		return nil, fmt.Errorf("runtime object size: %w", ErrRecovery)
	}
	b, err := io.ReadAll(io.LimitReader(f, ref.Bytes+1))
	if err != nil {
		return nil, err
	}
	h := sha256.Sum256(b)
	if hex.EncodeToString(h[:]) != ref.Hash {
		return nil, fmt.Errorf("runtime payload checksum: %w", ErrRecovery)
	}
	if s.verified != nil {
		// Bound validation metadata separately from domain pages and payloads.
		if len(s.verified) >= 4096 {
			clear(s.verified)
		}
		s.verified[ref.Hash] = info
	}
	return b, nil
}

func (s *Store) PutObject(body []byte) (Reference, error) {
	if err := s.ready(); err != nil {
		return Reference{}, err
	}
	if len(body) > maxObjectBytes {
		return Reference{}, ErrCapacity
	}
	h := sha256.Sum256(body)
	ref := Reference{Hash: hex.EncodeToString(h[:]), Bytes: int64(len(body))}
	path, _ := s.objectPath(ref)
	if _, err := os.Stat(path); err == nil {
		_, err = s.ReadObject(ref)
		return ref, err
	} else if !os.IsNotExist(err) {
		return ref, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return ref, s.fail(err)
	}
	if err := s.syncDir(s.dir); err != nil {
		return ref, s.fail(err)
	}
	return ref, s.fail(s.atomic(path, body, "payload"))
}

// LinkPending reuses a domain's already frozen pending file without a second
// full payload copy. Subsequent domain writes must use atomic replacement.
func (s *Store) LinkPending(path string) (Reference, error) {
	if err := s.ready(); err != nil {
		return Reference{}, err
	}
	f, err := os.Open(path)
	if err != nil {
		return Reference{}, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return Reference{}, err
	}
	if !info.Mode().IsRegular() || info.Size() > maxObjectBytes {
		f.Close()
		return Reference{}, ErrCapacity
	}
	h := sha256.New()
	_, err = io.Copy(h, io.LimitReader(f, maxObjectBytes+1))
	f.Close()
	if err != nil {
		return Reference{}, err
	}
	ref := Reference{Hash: hex.EncodeToString(h.Sum(nil)), Bytes: info.Size()}
	dst, _ := s.objectPath(ref)
	if err = os.MkdirAll(filepath.Dir(dst), 0700); err != nil {
		return ref, s.fail(err)
	}
	if err = os.Link(path, dst); err != nil && !os.IsExist(err) {
		return ref, s.fail(err)
	}
	if err == nil {
		s.metrics.RecoveryBytes += info.Size()
		s.metrics.PeakRecoveryBytes = max(s.metrics.PeakRecoveryBytes, s.metrics.RecoveryBytes)
	}
	if _, err = s.ReadObject(ref); err != nil {
		return ref, s.fail(err)
	}
	if err = s.syncDir(filepath.Dir(dst)); err != nil {
		return ref, s.fail(err)
	}
	return ref, s.fail(s.syncDir(s.dir))
}

// Begin returns only after payload, intent and non-reusable local sequence are
// durable. No caller may send the database write before this method succeeds.
func (s *Store) Begin(id, kind string, payload Reference, expected, newRevision uint64, taskIdentity ...string) (Operation, error) {
	if err := s.ready(); err != nil {
		return Operation{}, err
	}
	task := ""
	if len(taskIdentity) > 1 {
		return Operation{}, ErrConflict
	}
	if len(taskIdentity) == 1 {
		task = taskIdentity[0]
	}
	if len(task) > 1024 {
		return Operation{}, ErrConflict
	}
	for _, e := range s.entries {
		if task != "" && !e.Applied && e.Record.Operation.Task == task && e.Record.Operation.ID != id {
			return Operation{}, ErrBusy
		}
	}
	if id == "" || kind == "" || len(id) > 1024 || len(kind) > 160 {
		return Operation{}, errors.New("invalid runtime operation identity")
	}
	if err := s.checkHandoff(Operation{Task: task, ID: id, Kind: kind, Payload: payload, ExpectedRevision: expected, NewRevision: newRevision}); err != nil {
		return Operation{}, err
	}
	for _, e := range s.entries {
		op := e.Record.Operation
		if op.ID == id {
			if op.Task != task || op.Kind != kind || op.Payload != payload || op.ExpectedRevision != expected || op.NewRevision != newRevision {
				return Operation{}, ErrConflict
			}
			return op, nil
		}
	}
	if s.Metrics().Uncheckpointed >= 128 {
		return Operation{}, ErrCheckpointDue
	}
	if err := s.checkReferences([]Reference{payload}); err != nil {
		return Operation{}, s.fail(err)
	}
	if s.issued == ^uint64(0) {
		return Operation{}, ErrCapacity
	}
	op := Operation{Task: task, Sequence: s.issued + 1, Generation: s.current.Generation, ID: id, Kind: kind, Payload: payload, ExpectedRevision: expected, NewRevision: newRevision}
	r := record{Version: formatVersion, Generation: s.current.Generation, Binding: s.current.Binding, Operation: op}
	body, err := encode(r)
	if err != nil {
		return Operation{}, err
	}
	raw := frame(2, body)
	if len(raw) > segmentBytes {
		return Operation{}, ErrCapacity
	}
	e := &entry{Record: r, Bytes: int64(len(raw))}
	if s.metrics.HotBytes+entryBytes(e)+256 > s.hotLimit {
		return Operation{}, ErrCapacity
	}
	if s.segment == "" || s.segmentSize+int64(len(raw)) > segmentBytes {
		root := filepath.Join(s.dir, "operations")
		s.segment = filepath.Join(root, fmt.Sprintf("segment-%020d", op.Sequence))
		s.segmentSize = 0
		if err = os.MkdirAll(s.segment, 0700); err != nil {
			return Operation{}, s.fail(err)
		}
		if err = s.syncDir(root); err != nil {
			return Operation{}, s.fail(err)
		}
		if err = s.syncDir(s.dir); err != nil {
			return Operation{}, s.fail(err)
		}
	}
	e.Path = filepath.Join(s.segment, fmt.Sprintf("%020d.op", op.Sequence))
	if err = s.atomic(e.Path, raw, "intent"); err != nil {
		return Operation{}, s.fail(err)
	}
	issued := checkpoint{Version: formatVersion, Generation: s.current.Generation, Binding: s.current.Binding, Through: op.Sequence}
	mark, _ := encode(issued)
	if err = s.atomic(filepath.Join(s.dir, "issued"), frame(3, mark), "issued"); err != nil {
		return Operation{}, s.fail(err)
	}
	s.issued = op.Sequence
	s.segmentSize += int64(len(raw))
	s.entries[op.Sequence] = e
	s.metrics.HotBytes += entryBytes(e)
	s.metrics.Intents++
	return op, nil
}

func (s *Store) State() []byte      { return bytes.Clone(s.current.State) }
func (s *Store) Binding() Binding   { return s.current.Binding }
func (s *Store) Generation() string { return s.current.Generation }

// SealConversion installs the verified initial domain image as both recovery
// checkpoints before its directory is published. No SQL operation may have
// been admitted; a temporary conversion placeholder must not become rollback.
func (s *Store) SealConversion() error {
	if err := s.ready(); err != nil {
		return err
	}
	if s.issued != 0 || len(s.entries) != 0 || len(s.current.Pending) != 0 {
		return ErrConflict
	}
	if err := s.writeCheckpoint("checkpoint.previous", s.current); err != nil {
		return s.fail(err)
	}
	s.previous = s.current
	s.previous.State = nil
	return s.collect()
}

// Find only searches the bounded retained recovery tail. A miss is never
// interpreted as proof that a historical database identity was not committed.
func (s *Store) Find(id string) (Operation, bool) {
	for _, e := range s.entries {
		if e.Record.Operation.ID == id {
			return e.Record.Operation, true
		}
	}
	return Operation{}, false
}

// Retained reports only the current/previous checkpoint recovery tail, for
// pruning a domain's short-lived revision deduplication metadata.
func (s *Store) Retained() []Operation {
	out := make([]Operation, 0, len(s.entries))
	for _, e := range s.entries {
		out = append(out, e.Record.Operation)
	}
	return out
}

func (s *Store) IsApplied(op Operation) (bool, error) {
	if op.Generation != s.current.Generation {
		return false, ErrConflict
	}
	if e := s.entries[op.Sequence]; e != nil {
		if e.Record.Operation != op {
			return false, ErrConflict
		}
		return e.Applied, nil
	}
	return false, fmt.Errorf("trimmed or unknown result requires exact domain identity verification: %w", ErrRecovery)
}

// Pending returns bounded metadata, never payloads. Completed records retained
// solely for the previous checkpoint are excluded from normal scheduling.
func (s *Store) Pending(after uint64, limit int) []Operation {
	if limit <= 0 {
		return nil
	}
	seqs := make([]uint64, 0, len(s.entries))
	for seq, e := range s.entries {
		if !e.Applied && seq > after {
			seqs = append(seqs, seq)
		}
	}
	slices.Sort(seqs)
	if len(seqs) > limit {
		seqs = seqs[:limit]
	}
	out := make([]Operation, 0, len(seqs))
	for _, seq := range seqs {
		out = append(out, s.entries[seq].Record.Operation)
	}
	return out
}

// Applied is called by the owner after the typed state transition (including
// child work) succeeds. A crash before Checkpoint replays the original intent.
func (s *Store) Applied(op Operation) error {
	if err := s.ready(); err != nil {
		return err
	}
	e := s.entries[op.Sequence]
	if e == nil {
		return fmt.Errorf("unknown operation result: %w", ErrConflict)
	}
	if e.Record.Operation != op {
		return ErrConflict
	}
	if err := s.applyHandoff(op); err != nil {
		return err
	}
	e.Applied = true
	return nil
}

func (s *Store) Due() bool {
	if !s.handoffUnchanged() {
		return true
	}
	var count int
	for seq, e := range s.entries {
		if e.Applied && (seq > s.current.Through || slices.Contains(s.current.Pending, seq)) {
			count++
		}
	}
	return count >= 128 || (count > 0 && s.now().Sub(s.current.WrittenAt) >= 30*time.Second)
}

func (s *Store) Checkpoint(state []byte, refs []Reference) error {
	if err := s.ready(); err != nil {
		return err
	}
	if s.current.Number == ^uint64(0) {
		return ErrCapacity
	}
	var err error
	refs, err = s.checkpointHandoff(refs)
	if err != nil {
		return s.fail(err)
	}
	if s.handoffUnchanged() && bytes.Equal(state, s.current.State) && s.issued == s.current.Through && slices.Equal(refs, s.current.References) {
		unchanged := true
		for _, seq := range s.current.Pending {
			if e := s.entries[seq]; e != nil && e.Applied {
				unchanged = false
				break
			}
		}
		if unchanged {
			return nil
		}
	}
	if int64(len(state))-int64(len(s.current.State))+s.metrics.HotBytes > s.hotLimit {
		return ErrCapacity
	}
	if err := s.checkReferences(refs); err != nil {
		return s.fail(err)
	}
	next := checkpoint{Handoff: s.handoff, Version: formatVersion, Generation: s.current.Generation, Binding: s.current.Binding, Number: s.current.Number + 1, Through: s.issued, State: bytes.Clone(state), References: slices.Clone(refs), WrittenAt: s.now()}
	for seq, e := range s.entries {
		if !e.Applied {
			next.Pending = append(next.Pending, seq)
		}
	}
	slices.Sort(next.Pending)
	if err := s.writeCheckpoint("checkpoint.previous", s.current); err != nil {
		return s.fail(err)
	}
	if err := s.writeCheckpoint("checkpoint.current", next); err != nil {
		return s.fail(err)
	}
	// Validate installed bytes before recycling any recovery reference.
	var verified checkpoint
	if err := readFrame(filepath.Join(s.dir, "checkpoint.current"), 1, s.hotLimit+1<<20, &verified); err != nil {
		return s.fail(err)
	}
	if verified.Number != next.Number || !bytes.Equal(verified.State, next.State) {
		return s.fail(ErrRecovery)
	}
	s.metrics.HotBytes += int64(len(state) - len(s.current.State))
	s.previous, s.current = s.current, next
	s.previous.State = nil
	s.metrics.Checkpoints++
	s.metrics.CheckpointAt = next.WrittenAt
	s.metrics.CheckpointBytes = int64(len(state))
	return s.collect()
}

func (s *Store) collect() error {
	keep := map[string]bool{}
	dirtyDirs := map[string]bool{}
	for _, cp := range []checkpoint{s.previous, s.current} {
		for _, r := range cp.References {
			keep[r.Hash] = true
		}
	}
	for seq, e := range s.entries {
		covered := seq <= s.previous.Through && !slices.Contains(s.previous.Pending, seq) && e.Applied
		if !covered {
			keep[e.Record.Operation.Payload.Hash] = true
			continue
		}
		if err := s.point("cleanup_before_remove"); err != nil {
			return s.fail(err)
		}
		if err := os.Remove(e.Path); err != nil {
			return s.fail(err)
		}
		s.metrics.RecoveryBytes -= e.Bytes
		dirtyDirs[filepath.Dir(e.Path)] = true
		s.metrics.HotBytes -= entryBytes(e)
		delete(s.entries, seq)
	}
	for dir := range dirtyDirs {
		if err := s.syncDir(dir); err != nil {
			return s.fail(err)
		}
	}
	objects, err := os.ReadDir(filepath.Join(s.dir, "objects"))
	if err != nil && !os.IsNotExist(err) {
		return s.fail(err)
	}
	for _, f := range objects {
		if strings.HasPrefix(f.Name(), ".runtime-tmp-") {
			continue
		}
		if !strings.HasSuffix(f.Name(), ".blob") || f.IsDir() {
			return s.fail(ErrRecovery)
		}
		if !keep[strings.TrimSuffix(f.Name(), ".blob")] {
			info, statErr := f.Info()
			if statErr != nil {
				return s.fail(statErr)
			}
			if err = os.Remove(filepath.Join(s.dir, "objects", f.Name())); err != nil {
				return s.fail(err)
			}
			s.metrics.RecoveryBytes -= info.Size()
			delete(s.verified, strings.TrimSuffix(f.Name(), ".blob"))
		}
	}
	if len(objects) > 0 {
		if err = s.syncDir(filepath.Join(s.dir, "objects")); err != nil {
			return s.fail(err)
		}
	}
	root := filepath.Join(s.dir, "operations")
	segments, err := os.ReadDir(root)
	if err != nil && !os.IsNotExist(err) {
		return s.fail(err)
	}
	for _, segment := range segments {
		dir := filepath.Join(root, segment.Name())
		files, err := os.ReadDir(dir)
		if err != nil {
			return s.fail(err)
		}
		if len(files) == 0 {
			if err = os.Remove(dir); err != nil {
				return s.fail(err)
			}
			if dir == s.segment {
				s.segment = ""
				s.segmentSize = 0
			}
		}
	}
	if len(segments) > 0 {
		if err = s.syncDir(root); err != nil {
			return s.fail(err)
		}
	}
	return nil
}

func (s *Store) Metrics() Metrics {
	m := s.metrics
	m.VerifiedMetadataBytes = int64(len(s.verified)) * 320
	for seq, e := range s.entries {
		if !e.Applied {
			m.Pending++
		} else if seq > s.current.Through || slices.Contains(s.current.Pending, seq) {
			m.Uncheckpointed++
		}
	}
	return m
}

// LinkCurrent reuses an already durable domain checkpoint as an intent payload.
// Keeper's frozen Ops remain in its original checkpoint; no second serialized
// copy of those pending batches is introduced before SQL.
func (s *Store) LinkCurrent() (Reference, error) {
	if err := s.ready(); err != nil {
		return Reference{}, err
	}
	return s.LinkPending(filepath.Join(s.dir, "checkpoint.current"))
}

// ReadStateObject returns only the state in a linked checkpoint. Domain recovery
// may extract its frozen original operation without loading unrelated cold
// caches referenced by that old snapshot. Current/previous references are still
// verified by Open, independently of this extraction.
func (s *Store) ReadStateObject(ref Reference) ([]byte, error) {
	raw, err := s.ReadObject(ref)
	if err != nil {
		return nil, err
	}
	body, err := unframe(raw, 1, maxObjectBytes)
	if err != nil {
		return nil, err
	}
	var cp checkpoint
	if err = decode(body, &cp); err != nil {
		return nil, err
	}
	if cp.Version != formatVersion || cp.Binding != s.current.Binding || cp.Generation != s.current.Generation {
		return nil, ErrRecovery
	}
	return bytes.Clone(cp.State), nil
}
