package justlendkeeper

import (
	"bytes"
	"errors"
	"io"
	"os"
	"time"
)

type PendingStatus struct {
	ScheduledOperations int       `json:"scheduled_operations"`
	FrozenWrites        int       `json:"frozen_writes"`
	OldestFrozen        time.Time `json:"oldest_frozen"`
	ModifiedAt          time.Time `json:"modified_at"`
	CheckpointBytes     int64     `json:"checkpoint_bytes"`
}

// ReadPendingStatus reads only the atomic checkpoint, never creates recovery
// state or expands the cold candidate cache. This is a pending-work projection,
// not a replacement for LoadState's complete recovery validation.
func ReadPendingStatus(path string) (PendingStatus, error) {
	var out PendingStatus
	f, err := os.Open(path)
	if err != nil {
		return out, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return out, err
	}
	if st.Size() > maxCheckpointBytes+32 {
		return out, errors.New("checkpoint_size_limit")
	}
	raw, err := io.ReadAll(io.LimitReader(f, maxCheckpointBytes+33))
	if err != nil {
		return out, err
	}
	if len(raw) < 32 || len(raw) > maxCheckpointBytes+32 || !bytes.Equal(raw[:32], []byte(Hash(raw[32:]))) {
		return out, errors.New("state_checksum_failed")
	}
	var state State
	body := raw[32:]
	if bytes.HasPrefix(body, []byte(checkpointMagic)) || bytes.HasPrefix(body, []byte(typedCheckpointMagic)) {
		var cp checkpoint
		expand, magic := expandCheckpoint, checkpointMagic
		if bytes.HasPrefix(body, []byte(typedCheckpointMagic)) {
			expand, magic = runtimeExpand, typedCheckpointMagic
		}
		if err = expand(body[len(magic):], &cp); err != nil {
			return out, err
		}
		if len(cp.CandidateHash) != 32 || len(cp.State.Candidates) != 0 {
			return out, errors.New("invalid_checkpoint_candidate_reference")
		}
		state = cp.State
	} else if err = Thaw(body, &state); err != nil {
		return out, err
	}
	if state.Version != 1 {
		return out, errors.New("state_version_mismatch")
	}
	out.ModifiedAt, out.CheckpointBytes = st.ModTime().UTC(), st.Size()
	out.ScheduledOperations = len(state.Ops)
	for _, op := range state.Ops {
		if !op.Frozen {
			continue
		}
		out.FrozenWrites++
		if out.OldestFrozen.IsZero() || (!op.Scheduled.IsZero() && op.Scheduled.Before(out.OldestFrozen)) {
			out.OldestFrozen = op.Scheduled
		}
	}
	return out, nil
}
