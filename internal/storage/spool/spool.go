// Package spool implements bounded, crash-recoverable disk delivery. Files do
// not expire. Only the caller's verified database ACK authorizes removal.
package spool

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
	"golang.org/x/sys/unix"
)

const MaxPayload = 32 << 20

var label = regexp.MustCompile(`^[a-zA-Z0-9_.:-]{1,160}$`)
var magic = []byte{'C', 'M', 'I', 'S', 1}

func Validate(d contracts.Delivery) error {
	for _, s := range []string{d.Database, d.Kind, d.ID, d.Service} {
		if !label.MatchString(s) {
			return fmt.Errorf("invalid delivery identity")
		}
	}
	if d.Generation == 0 || d.Codec == 0 || d.Members == 0 || len(d.Payload) == 0 || len(d.Payload) > MaxPayload || d.Minute.IsZero() || !d.Minute.Equal(d.Minute.UTC().Truncate(time.Minute)) {
		return fmt.Errorf("invalid delivery envelope")
	}
	if sha256.Sum256(d.Payload) != d.Digest {
		return fmt.Errorf("delivery digest mismatch")
	}
	return nil
}
func Name(d contracts.Delivery) string {
	h := sha256.Sum256([]byte(d.Database + "\x00" + d.Kind + "\x00" + d.ID))
	return hex.EncodeToString(h[:]) + ".batch"
}
func Marshal(d contracts.Delivery) ([]byte, error) {
	if e := Validate(d); e != nil {
		return nil, e
	}
	b := new(bytes.Buffer)
	b.Write(magic)
	for _, s := range []string{d.Database, d.Kind, d.ID, d.Service} {
		binary.Write(b, binary.BigEndian, uint16(len(s)))
		b.WriteString(s)
	}
	binary.Write(b, binary.BigEndian, d.Generation)
	binary.Write(b, binary.BigEndian, d.Minute.UTC().Unix())
	binary.Write(b, binary.BigEndian, d.Codec)
	binary.Write(b, binary.BigEndian, d.Members)
	binary.Write(b, binary.BigEndian, uint32(len(d.Payload)))
	b.Write(d.Digest[:])
	b.Write(d.Payload)
	h := sha256.Sum256(b.Bytes())
	b.Write(h[:])
	return b.Bytes(), nil
}
func Unmarshal(raw []byte) (contracts.Delivery, error) {
	var d contracts.Delivery
	if len(raw) < 5+32 || len(raw) > MaxPayload+1024 || !bytes.Equal(raw[:5], magic) {
		return d, fmt.Errorf("invalid spool header")
	}
	h := sha256.Sum256(raw[:len(raw)-32])
	if !bytes.Equal(h[:], raw[len(raw)-32:]) {
		return d, fmt.Errorf("spool checksum mismatch")
	}
	r := bytes.NewReader(raw[5 : len(raw)-32])
	for _, s := range []*string{&d.Database, &d.Kind, &d.ID, &d.Service} {
		var n uint16
		if e := binary.Read(r, binary.BigEndian, &n); e != nil {
			return d, e
		}
		if n == 0 || n > 160 || int(n) > r.Len() {
			return d, fmt.Errorf("invalid spool string")
		}
		a := make([]byte, n)
		io.ReadFull(r, a)
		*s = string(a)
	}
	var sec int64
	var n uint32
	for _, v := range []any{&d.Generation, &sec, &d.Codec, &d.Members, &n} {
		if e := binary.Read(r, binary.BigEndian, v); e != nil {
			return d, e
		}
	}
	if _, e := io.ReadFull(r, d.Digest[:]); e != nil {
		return d, e
	}
	if n > MaxPayload || int(n) != r.Len() {
		return d, fmt.Errorf("invalid spool payload length")
	}
	d.Payload = make([]byte, n)
	io.ReadFull(r, d.Payload)
	d.Minute = time.Unix(sec, 0).UTC()
	return d, Validate(d)
}

type Queue struct {
	Directory string
	MaxBytes  int64
}

func Open(dir string, limit int64) (*Queue, error) {
	if !filepath.IsAbs(dir) || limit <= 0 {
		return nil, fmt.Errorf("spool needs absolute path and positive quota")
	}
	if e := os.MkdirAll(dir, 0700); e != nil {
		return nil, e
	}
	return &Queue{dir, limit}, nil
}
func (q *Queue) locked(ctx context.Context, fn func() error) error {
	f, e := os.OpenFile(filepath.Join(q.Directory, "writer.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return e
	}
	defer f.Close()
	for {
		e = unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if e == nil {
			break
		}
		if !errors.Is(e, unix.EWOULDBLOCK) {
			return e
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	defer unix.Flock(int(f.Fd()), unix.LOCK_UN)
	return fn()
}
func syncDir(dir string) error {
	f, e := os.Open(dir)
	if e != nil {
		return e
	}
	defer f.Close()
	return f.Sync()
}
func same(a, b contracts.Delivery) bool {
	x, e := Marshal(a)
	if e != nil {
		return false
	}
	y, e := Marshal(b)
	return e == nil && bytes.Equal(x, y)
}
func read(path string) (contracts.Delivery, error) {
	f, e := os.Open(path)
	if e != nil {
		return contracts.Delivery{}, e
	}
	defer f.Close()
	s, e := f.Stat()
	if e != nil {
		return contracts.Delivery{}, e
	}
	if s.Size() > MaxPayload+1024 {
		return contracts.Delivery{}, fmt.Errorf("spool file too large")
	}
	a, e := io.ReadAll(io.LimitReader(f, MaxPayload+1025))
	if e != nil {
		return contracts.Delivery{}, e
	}
	return Unmarshal(a)
}
func (q *Queue) Accept(ctx context.Context, d contracts.Delivery) error {
	raw, e := Marshal(d)
	if e != nil {
		return e
	}
	return q.locked(ctx, func() error {
		if e := ctx.Err(); e != nil {
			return e
		}
		path := filepath.Join(q.Directory, Name(d))
		if old, e := read(path); e == nil {
			if !same(old, d) {
				return contracts.ErrConflict
			}
			return syncDir(q.Directory)
		} else if !errors.Is(e, os.ErrNotExist) {
			return e
		}
		directory, e := os.Open(q.Directory)
		if e != nil {
			return e
		}
		defer directory.Close()
		used := int64(0)
		entries := 0
		for {
			files, err := directory.ReadDir(64)
			if err != nil && err != io.EOF {
				return err
			}
			for _, file := range files {
				entries++
				if entries > 100000 {
					return contracts.ErrCapacity
				}
				if e := ctx.Err(); e != nil {
					return e
				}
				if !file.Type().IsRegular() {
					return fmt.Errorf("unexpected spool file type")
				}
				stat, e := file.Info()
				if e != nil {
					return e
				}
				used += stat.Size()
			}
			if err == io.EOF {
				break
			}
		}

		if used+int64(len(raw)) > q.MaxBytes {
			return contracts.ErrCapacity
		}
		f, e := os.CreateTemp(q.Directory, ".pending-")
		if e != nil {
			return e
		}
		tmp := f.Name()
		defer os.Remove(tmp)
		if _, e = f.Write(raw); e != nil {
			f.Close()
			return e
		}
		if e = f.Sync(); e != nil {
			f.Close()
			return e
		}
		if e = f.Close(); e != nil {
			return e
		}
		if e = os.Rename(tmp, path); e != nil {
			return e
		}
		return syncDir(q.Directory)
	})
}
func (q *Queue) List(ctx context.Context) ([]contracts.Delivery, error) {
	var out []contracts.Delivery
	var total int64
	e := q.locked(ctx, func() error {
		files, e := os.ReadDir(q.Directory)
		if e != nil {
			return e
		}
		for _, f := range files {
			if filepath.Ext(f.Name()) != ".batch" {
				continue
			}
			s, e := f.Info()
			if e != nil {
				return e
			}
			total += s.Size()
			if total > MaxPayload+1024 || len(out) >= 128 {
				return fmt.Errorf("spool listing exceeds memory limit; consume using Next")
			}
			d, e := read(filepath.Join(q.Directory, f.Name()))
			if e != nil {
				return fmt.Errorf("%s: %w", f.Name(), e)
			}
			if f.Name() != Name(d) {
				return fmt.Errorf("spool identity mismatch")
			}
			out = append(out, d)
		}
		return nil
	})
	sort.Slice(out, func(i, j int) bool {
		if out[i].Minute.Equal(out[j].Minute) {
			return Name(out[i]) < Name(out[j])
		}
		return out[i].Minute.Before(out[j].Minute)
	})
	return out, e
}

// Next scans bounded directory pages and reads only one payload. Even a full
// 20 GiB spool never needs to be loaded into memory by a writer.
func (q *Queue) Next(ctx context.Context) (contracts.Delivery, bool, error) {
	var out contracts.Delivery
	var found bool
	err := q.locked(ctx, func() error {
		dir, err := os.Open(q.Directory)
		if err != nil {
			return err
		}
		defer dir.Close()
		var chosen string
		var oldest time.Time
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			entries, err := dir.ReadDir(64)
			if err != nil && err != io.EOF {
				return err
			}
			for _, ent := range entries {
				if filepath.Ext(ent.Name()) != ".batch" {
					continue
				}
				info, err := ent.Info()
				if err != nil {
					return err
				}
				if chosen == "" || info.ModTime().Before(oldest) || (info.ModTime().Equal(oldest) && ent.Name() < chosen) {
					chosen = ent.Name()
					oldest = info.ModTime()
				}
			}
			if err == io.EOF {
				break
			}
		}
		if chosen == "" {
			return nil
		}
		out, err = read(filepath.Join(q.Directory, chosen))
		if err != nil {
			return err
		}
		if chosen != Name(out) {
			return fmt.Errorf("spool identity mismatch")
		}
		found = true
		return nil
	})
	return out, found, err
}

// Obtain the immutable commit proof before taking the directory lock. Recheck
// the file identity and digest under the short lock before deleting it.
func (q *Queue) Ack(ctx context.Context, d contracts.Delivery, verify func(context.Context, contracts.Delivery) (bool, error)) error {
	if verify == nil {
		return fmt.Errorf("spool ACK requires database verifier")
	}
	ok, e := verify(ctx, d)
	if e != nil {
		return e
	}
	if !ok {
		return fmt.Errorf("batch is not committed")
	}
	return q.locked(ctx, func() error {
		path := filepath.Join(q.Directory, Name(d))
		old, e := read(path)
		if errors.Is(e, os.ErrNotExist) {
			return nil
		}
		if e != nil {
			return e
		}
		if !old.SameHeader(d) || (len(d.Payload) > 0 && !same(old, d)) {
			return contracts.ErrConflict
		}
		if e = os.Remove(path); e != nil {
			return e
		}
		return syncDir(q.Directory)
	})
}

var _ contracts.DurableQueue = (*Queue)(nil)

// Recover promotes complete, checksummed staging files left by a process crash.
// Partial files remain for diagnosis and consume quota; they were never ACKed.
func (q *Queue) Recover(ctx context.Context) error {
	return q.locked(ctx, func() error {
		dir, e := os.Open(q.Directory)
		if e != nil {
			return e
		}
		defer dir.Close()
		for {
			files, err := dir.ReadDir(64)
			if err != nil && err != io.EOF {
				return err
			}
			for _, file := range files {
				if e = ctx.Err(); e != nil {
					return e
				}
				if !strings.HasPrefix(file.Name(), ".pending-") {
					continue
				}
				path := filepath.Join(q.Directory, file.Name())
				d, e := read(path)
				if e != nil {
					return fmt.Errorf("incomplete spool staging file retained: %s: %w", file.Name(), e)
				}
				target := filepath.Join(q.Directory, Name(d))
				old, e := read(target)
				if e == nil {
					if !old.SameHeader(d) || (len(d.Payload) > 0 && !same(old, d)) {
						return contracts.ErrConflict
					}
					if e = os.Remove(path); e != nil {
						return e
					}
					continue
				}
				if !errors.Is(e, os.ErrNotExist) {
					return e
				}
				f, e := os.OpenFile(path, os.O_RDWR, 0600)
				if e != nil {
					return e
				}
				e = f.Sync()
				closeError := f.Close()
				if e != nil {
					return e
				}
				if closeError != nil {
					return closeError
				}
				if e = os.Rename(path, target); e != nil {
					return e
				}
			}
			if err == io.EOF {
				break
			}
		}
		return syncDir(q.Directory)
	})
}

// Inspect counts retained files without decoding the entire queue into memory.
type Health struct {
	Files      uint64     `json:"files"`
	Bytes      int64      `json:"bytes"`
	QuotaBytes int64      `json:"quota_bytes"`
	Oldest     *time.Time `json:"oldest_utc"`
}

func (q *Queue) Inspect(ctx context.Context) (Health, error) {
	out := Health{QuotaBytes: q.MaxBytes}
	e := q.locked(ctx, func() error {
		dir, e := os.Open(q.Directory)
		if e != nil {
			return e
		}
		defer dir.Close()
		for {
			entries, e := dir.ReadDir(64)
			if e != nil && e != io.EOF {
				return e
			}
			for _, entry := range entries {
				if e := ctx.Err(); e != nil {
					return e
				}
				if entry.Name() == "writer.lock" {
					continue
				}
				info, e := entry.Info()
				if e != nil {
					return e
				}
				if !info.Mode().IsRegular() {
					return fmt.Errorf("unexpected spool file")
				}
				out.Files++
				out.Bytes += info.Size()
				at := info.ModTime().UTC()
				if out.Oldest == nil || at.Before(*out.Oldest) {
					out.Oldest = &at
				}
			}
			if e == io.EOF {
				return nil
			}
		}
	})
	return out, e
}

func (q *Queue) ServicePending(ctx context.Context, service string, generation uint64) (bool, error) {
	pending := false
	e := q.locked(ctx, func() error {
		dir, e := os.Open(q.Directory)
		if e != nil {
			return e
		}
		defer dir.Close()
		for {
			files, e := dir.ReadDir(64)
			if e != nil && e != io.EOF {
				return e
			}
			for _, file := range files {
				if e := ctx.Err(); e != nil {
					return e
				}
				if file.Name() == "writer.lock" {
					continue
				}
				if strings.HasPrefix(file.Name(), ".pending-") {
					return fmt.Errorf("recover spool temporary files before producer activation")
				}
				d, e := read(filepath.Join(q.Directory, file.Name()))
				if e != nil {
					return e
				}
				if d.Service == service && d.Generation == generation {
					pending = true
					return nil
				}
			}
			if e == io.EOF {
				return nil
			}
		}
	})
	return pending, e
}
