package runtimecheckpoint

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
)

// Epoch is kept outside database backups. Every restore or destructive manual
// maintenance operation must rotate it before any collector may resume.
type Epoch struct {
	Version                      uint32
	Target, ID, Previous, Reason string
	ChangedAt                    time.Time
}

func ReadEpoch(path, target string) (Epoch, error) {
	var e Epoch
	if err := readFrame(path, 4, 64<<10, &e); err != nil {
		return e, fmt.Errorf("data recovery epoch: %w: %w", ErrRecovery, err)
	}
	if e.Version != formatVersion || e.Target != target || e.ID == "" || e.ChangedAt.IsZero() {
		return e, fmt.Errorf("data recovery epoch target/version: %w", ErrRecovery)
	}
	return e, nil
}

// RotateEpoch is an explicit compare-and-swap maintenance operation. expected
// is empty only for first installation. It cannot silently create a missing
// epoch during normal startup or replace an epoch changed by another operator.
func RotateEpoch(path, target, expected, reason string) (Epoch, error) {
	if target == "" || strings.TrimSpace(reason) == "" || len(target)+len(reason) > 8192 {
		return Epoch{}, errors.New("epoch target and maintenance reason required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return Epoch{}, err
	}
	lock, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return Epoch{}, err
	}
	defer lock.Close()
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return Epoch{}, err
	}
	old, err := ReadEpoch(path, target)
	if expected == "" {
		if !errors.Is(err, os.ErrNotExist) {
			return Epoch{}, fmt.Errorf("epoch initialization requires absent file: %w", ErrConflict)
		}
	} else if err != nil {
		return Epoch{}, err
	} else if old.ID != expected {
		return Epoch{}, ErrConflict
	}
	e := Epoch{Version: formatVersion, Target: target, ID: uuid.NewString(), Previous: expected, Reason: reason, ChangedAt: time.Now().UTC()}
	body, err := encode(e)
	if err != nil {
		return Epoch{}, err
	}
	s := &Store{}
	if err = s.atomic(path, frame(4, body), "epoch"); err != nil {
		return Epoch{}, err
	}
	return ReadEpoch(path, target)
}

type Inspection struct {
	Handoff                   HandoffStatus
	Binding                   Binding
	Generation                string
	CheckpointNumber, Through uint64
	Pending                   uint64
	StateBytes                int
	RecoveryBytes             int64
	Metrics                   Metrics
}

func (s *Store) Inspect() (Inspection, error) {
	if err := s.ready(); err != nil {
		return Inspection{}, err
	}
	out := Inspection{Handoff: s.handoff, Binding: s.current.Binding, Generation: s.current.Generation, CheckpointNumber: s.current.Number, Through: s.current.Through, StateBytes: len(s.current.State), Metrics: s.Metrics()}
	out.Pending = out.Metrics.Pending
	err := filepath.WalkDir(s.dir, func(path string, d os.DirEntry, err error) error {
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
		if !info.Mode().IsRegular() {
			return fmt.Errorf("unexpected state file: %w", ErrRecovery)
		}
		out.RecoveryBytes += info.Size()
		return nil
	})
	return out, err
}

// Export preserves the current checkpoint and all required recovery objects in
// a separate, verified directory. It does not declare pending SQL committed or
// convert domain state to a legacy format. Domain rollback must resolve these
// operations before handing control to a legacy collector.
func (s *Store) Export(destination string) error {
	if err := s.ready(); err != nil {
		return err
	}
	return s.export(destination, false)
}

// RecoverPrevious never edits the damaged source. It copies the previous
// checkpoint and complete retained tail to a new directory and validates the
// result. Domain recovery must still verify/replay the original operations.
func RecoverPrevious(source, destination string, binding Binding, hotLimit int64) error {
	if err := binding.validate(); err != nil {
		return err
	}
	s, err := acquire(source, hotLimit)
	if err != nil {
		return err
	}
	defer s.Close()
	if err = readFrame(filepath.Join(source, "checkpoint.previous"), 1, s.hotLimit+1<<20, &s.previous); err != nil {
		return err
	}
	if s.previous.Binding != binding || s.previous.Version != formatVersion || s.previous.Number == 0 || s.previous.Generation == "" {
		return ErrRecovery
	}
	return s.export(destination, true)
}

func (s *Store) export(destination string, previous bool) error {
	source, err := filepath.Abs(s.dir)
	if err != nil {
		return err
	}
	destination, err = filepath.Abs(destination)
	if err != nil {
		return err
	}
	if destination == source || strings.HasPrefix(destination, source+string(os.PathSeparator)) {
		return errors.New("export destination must be outside source")
	}
	if _, err = os.Lstat(destination); !os.IsNotExist(err) {
		return errors.New("export destination must not exist")
	}
	parent := filepath.Dir(destination)
	if err = os.MkdirAll(parent, 0700); err != nil {
		return err
	}
	staging, err := os.MkdirTemp(parent, ".runtime-export-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(staging)
	err = filepath.WalkDir(source, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		rel, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		if d.Name() == "runtime.lock" || strings.HasPrefix(d.Name(), ".runtime-tmp-") {
			return nil
		}
		if previous && rel == "checkpoint.current" {
			return nil
		}
		dst := filepath.Join(staging, rel)
		if d.IsDir() {
			return os.Mkdir(dst, 0700)
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("export refuses nonregular file: %w", ErrRecovery)
		}
		in, err := os.Open(path)
		if err != nil {
			return err
		}
		defer in.Close()
		out, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			return err
		}
		_, err = io.Copy(out, in)
		if err == nil {
			err = out.Sync()
		}
		if closeErr := out.Close(); err == nil {
			err = closeErr
		}
		return err
	})
	if err != nil {
		return err
	}
	binding := s.current.Binding
	if previous {
		binding = s.previous.Binding
		body, err := encode(s.previous)
		if err != nil {
			return err
		}
		if err = s.atomic(filepath.Join(staging, "checkpoint.current"), frame(1, body), "recovery"); err != nil {
			return err
		}
	}
	verified, err := open(staging, binding, s.hotLimit, true, false)
	if err != nil {
		return err
	}
	verified.Close()
	// Sync directories bottom-up before publishing the copy to its final name.
	var dirs []string
	if err = filepath.WalkDir(staging, func(path string, d os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if d.IsDir() {
			dirs = append(dirs, path)
		}
		return nil
	}); err != nil {
		return err
	}
	for i := len(dirs) - 1; i >= 0; i-- {
		if err = s.syncDir(dirs[i]); err != nil {
			return err
		}
	}
	if err = os.Rename(staging, destination); err != nil {
		return err
	}
	return s.syncDir(parent)
}

// InspectDirectory validates stopped-service recovery files without admitting
// domain work or modifying the source, including an unfinished handoff. It does
// not return a Store that could be used to bypass the collector startup gate.
// An optional export is verified in its own new directory.
func InspectDirectory(dir string, binding Binding, hotLimit int64, exportDir string) (Inspection, error) {
	s, err := open(dir, binding, hotLimit, true, true)
	if err != nil {
		return Inspection{}, err
	}
	defer s.Close()
	if exportDir != "" {
		if err = s.Export(exportDir); err != nil {
			return Inspection{}, err
		}
	}
	return s.Inspect()
}
