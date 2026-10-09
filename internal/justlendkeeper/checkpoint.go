package justlendkeeper

import (
	"bytes"
	"compress/gzip"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
)

// The primary checkpoint is the commit point. Its immutable candidate cache
// must be durable first; the previously referenced cache survives until then.
// This is local recovery state, not a source-response archive.
const checkpointMagic = "jl-keeper-checkpoint-v2\x00"
const maxCheckpointBytes = 64 << 20

type checkpoint struct {
	State         State // Candidates live in the content-addressed cache below.
	CandidateHash string
}

// Used only by the single writer under the command's existing process lock.
// Retain a deep, gob-normalized copy: maps have nondeterministic gob order, and
// a shallow copy would miss in-place changes to cursors, pending rows or money.
type stateWriter struct {
	path       string
	last       *checkpoint
	candidates []Candidate
	write      func(string, []byte) error
}

func (w *stateWriter) Save(path string, s State) error {
	write := w.write
	if write == nil {
		write = Atomic
	}
	raw, err := Freeze(s)
	if err != nil {
		return err
	}
	var normalized State
	if err = Thaw(raw, &normalized); err != nil {
		return err
	}
	candidates := normalized.Candidates
	normalized.Candidates = nil
	next := checkpoint{State: normalized}
	coldChanged := w.path != path || w.last == nil || !reflect.DeepEqual(candidates, w.candidates)
	if coldChanged {
		cold, err := compressCheckpoint(candidates)
		if err != nil {
			return err
		}
		next.CandidateHash = Hash(cold)
		if err = write(candidatePath(path, next.CandidateHash), cold); err != nil {
			return err
		}
	} else {
		next.CandidateHash = w.last.CandidateHash
	}
	if w.path == path && w.last != nil && reflect.DeepEqual(next, *w.last) {
		return nil
	}
	body, err := compressCheckpoint(next)
	if err != nil {
		return err
	}
	body = append([]byte(checkpointMagic), body...)
	if err = write(path, append([]byte(Hash(body)), body...)); err != nil {
		return err
	}
	// Only after the commit point is durable may obsolete/orphan caches go.
	if coldChanged {
		if err = pruneCandidateCaches(path, next.CandidateHash); err != nil {
			return err
		}
	}
	w.path, w.last, w.candidates = path, &next, candidates
	return nil
}

func candidatePath(path, hash string) string { return path + ".candidates-" + Hex(hash) }

func compressCheckpoint(v any) ([]byte, error) {
	raw, err := Freeze(v)
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

func expandCheckpoint(b []byte, v any) error {
	z, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		return err
	}
	defer z.Close()
	raw, err := io.ReadAll(io.LimitReader(z, maxCheckpointBytes+1))
	if err != nil {
		return err
	}
	if len(raw) > maxCheckpointBytes {
		return errors.New("checkpoint_size_limit")
	}
	return Thaw(raw, v)
}

func decodeState(path string, b []byte, s *State) error {
	if !bytes.HasPrefix(b, []byte(checkpointMagic)) {
		return Thaw(b, s) // Read the original checksum + gob format unchanged.
	}
	var cp checkpoint
	if err := expandCheckpoint(b[len(checkpointMagic):], &cp); err != nil {
		return err
	}
	if len(cp.CandidateHash) != 32 || len(cp.State.Candidates) != 0 {
		return errors.New("invalid_checkpoint_candidate_reference")
	}
	cold, err := os.ReadFile(candidatePath(path, cp.CandidateHash))
	if err != nil {
		return err
	}
	if Hash(cold) != cp.CandidateHash {
		return errors.New("candidate_cache_checksum_failed")
	}
	if err = expandCheckpoint(cold, &cp.State.Candidates); err != nil {
		return err
	}
	*s = cp.State
	return nil
}

func pruneCandidateCaches(path, keepHash string) error {
	dir := filepath.Dir(path)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	prefix := filepath.Base(path) + ".candidates-"
	keep := filepath.Base(candidatePath(path, keepHash))
	removed := false
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || name == keep || !strings.HasPrefix(name, prefix) {
			continue
		}
		suffix := strings.TrimPrefix(name, prefix)
		if len(suffix) != 64 {
			continue
		}
		if _, err := hex.DecodeString(suffix); err != nil {
			continue
		}
		if err := os.Remove(filepath.Join(dir, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		removed = true
	}
	if !removed {
		return nil
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
