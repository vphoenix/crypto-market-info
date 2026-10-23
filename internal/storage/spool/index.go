package spool

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
	"golang.org/x/sys/unix"
)

// ConsumerLock is separate from the short directory mutation lock. Producers
// can durably enqueue while exactly one writer process holds this lock.
func (q *Queue) ConsumerLock() (*os.File, error) {
	f, e := os.OpenFile(filepath.Join(q.Directory, "consumer.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return nil, e
	}
	if e = unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB); e != nil {
		f.Close()
		return nil, fmt.Errorf("another spool consumer is active: %w", e)
	}
	return f, nil
}

func readHeader(path string) (contracts.Candidate, error) {
	var out contracts.Candidate
	f, e := os.Open(path)
	if e != nil {
		return out, e
	}
	defer f.Close()
	// Rebuilding metadata touches hundreds of tiny headers per page. Sequential
	// read-ahead would also fetch unused payload pages and evict active data.
	// This best-effort hint applies only to this header-only open description;
	// a later Load opens its own descriptor for complete checksum validation.
	_ = unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_RANDOM)
	var prefix [5]byte
	if _, e = io.ReadFull(f, prefix[:]); e != nil {
		return out, e
	}
	if !bytes.Equal(prefix[:], magic) && !bytes.Equal(prefix[:], compressedMagic) {
		return out, fmt.Errorf("invalid spool header")
	}
	d := &out.Header
	for _, field := range []*string{&d.Database, &d.Kind, &d.ID, &d.Service} {
		var n uint16
		if e = binary.Read(f, binary.BigEndian, &n); e != nil {
			return out, e
		}
		if n == 0 || n > 160 {
			return out, fmt.Errorf("invalid spool identity length")
		}
		buf := make([]byte, n)
		if _, e = io.ReadFull(f, buf); e != nil {
			return out, e
		}
		*field = string(buf)
		if !label.MatchString(*field) {
			return out, fmt.Errorf("invalid spool identity")
		}
	}
	var sec int64
	var size uint32
	for _, v := range []any{&d.Generation, &sec, &d.Codec, &d.Members, &size} {
		if e = binary.Read(f, binary.BigEndian, v); e != nil {
			return out, e
		}
	}
	if _, e = io.ReadFull(f, d.Digest[:]); e != nil {
		return out, e
	}
	d.Minute = time.Unix(sec, 0).UTC()
	out.Bytes = int64(size)
	stored := size
	if bytes.Equal(prefix[:], compressedMagic) {
		if e = binary.Read(f, binary.BigEndian, &stored); e != nil {
			return out, e
		}
		if stored == 0 || stored >= size {
			return out, fmt.Errorf("invalid compressed spool length")
		}
	}
	pos, e := f.Seek(0, io.SeekCurrent)
	if e != nil {
		return out, e
	}
	st, e := f.Stat()
	if e != nil {
		return out, e
	}
	if size == 0 || size > MaxPayload || st.Size() != pos+int64(stored)+32 || d.Generation == 0 || d.Codec == 0 || d.Members == 0 || !d.Minute.Equal(d.Minute.Truncate(time.Minute)) {
		return out, fmt.Errorf("invalid spool envelope length")
	}
	if filepath.Base(path) != Name(*d) {
		return out, contracts.ErrConflict
	}
	return out, nil
}

func (q *Queue) Load(ctx context.Context, candidate contracts.Candidate) (contracts.Delivery, error) {
	if e := ctx.Err(); e != nil {
		return contracts.Delivery{}, e
	}
	d, e := read(filepath.Join(q.Directory, Name(candidate.Header)))
	if e == nil && !d.SameHeader(candidate.Header) {
		e = contracts.ErrConflict
	}
	return d, e
}

// Index contains rebuildable headers, never payloads. inotify observes atomic
// renames; a paged reconciliation repairs missed notifications and deletions.
type Index struct {
	q                       *Queue
	mu                      sync.Mutex
	entries                 map[string]contracts.Candidate
	maxBytes, metadataBytes int64
	scan                    *os.File
	seen                    map[string]bool
	fd                      int
	complete                bool
}

func (q *Queue) NewIndex(ctx context.Context, maxMetadataBytes int64) (*Index, error) {
	if maxMetadataBytes < 1024 {
		return nil, fmt.Errorf("invalid spool metadata budget")
	}
	fd, e := unix.InotifyInit1(unix.IN_NONBLOCK | unix.IN_CLOEXEC)
	if e != nil {
		return nil, e
	}
	if _, e = unix.InotifyAddWatch(fd, q.Directory, unix.IN_MOVED_TO|unix.IN_DELETE|unix.IN_CLOSE_WRITE|unix.IN_Q_OVERFLOW); e != nil {
		unix.Close(fd)
		return nil, e
	}
	i := &Index{q: q, entries: map[string]contracts.Candidate{}, maxBytes: maxMetadataBytes, fd: fd}
	if _, e = i.reconcile(ctx, 1); e != nil {
		i.Close()
		return nil, e
	}
	return i, nil
}
func (i *Index) Close() error {
	if i.scan != nil {
		i.scan.Close()
		i.scan = nil
	}
	return unix.Close(i.fd)
}
func candidateCost(c contracts.Candidate) int64 {
	return 512 + int64(len(c.Header.Database)+len(c.Header.Kind)+len(c.Header.ID)+len(c.Header.Service))
}
func (i *Index) add(name string) error {
	c, e := readHeader(filepath.Join(i.q.Directory, name))
	if errors.Is(e, os.ErrNotExist) {
		return nil
	}
	if e != nil {
		return fmt.Errorf("spool header %s: %w", name, e)
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if old, ok := i.entries[name]; ok {
		if !old.Header.SameHeader(c.Header) {
			return contracts.ErrConflict
		}
		return nil
	}
	cost := candidateCost(c)
	if i.metadataBytes+cost > i.maxBytes {
		return fmt.Errorf("spool metadata budget exhausted: %w", contracts.ErrCapacity)
	}
	i.entries[name] = c
	i.metadataBytes += cost
	return nil
}
func (i *Index) Remove(d contracts.Delivery) {
	i.mu.Lock()
	defer i.mu.Unlock()
	name := Name(d)
	if old, ok := i.entries[name]; ok && old.Header.SameHeader(d) {
		delete(i.entries, name)
		i.metadataBytes -= candidateCost(old)
	}
}
func (i *Index) Snapshot() []contracts.Candidate {
	i.mu.Lock()
	defer i.mu.Unlock()
	out := make([]contracts.Candidate, 0, len(i.entries))
	for _, c := range i.entries {
		out = append(out, c)
	}
	return out
}
func (i *Index) Complete() bool { i.mu.Lock(); defer i.mu.Unlock(); return i.complete }
func (i *Index) reconcile(ctx context.Context, pages int) (bool, error) {
	if i.scan == nil {
		f, e := os.Open(i.q.Directory)
		if e != nil {
			return false, e
		}
		i.scan = f
		i.seen = map[string]bool{}
	}
	for range pages {
		if e := ctx.Err(); e != nil {
			return false, e
		}
		entries, e := i.scan.ReadDir(64)
		if e != nil && e != io.EOF {
			return false, e
		}
		for _, ent := range entries {
			if filepath.Ext(ent.Name()) != ".batch" {
				continue
			}
			i.seen[ent.Name()] = true
			i.mu.Lock()
			_, known := i.entries[ent.Name()]
			i.mu.Unlock()
			if known {
				continue
			} // Immutable filenames need not be reopened.
			if err := i.add(ent.Name()); err != nil {
				return false, err
			}
		}
		if e == io.EOF {
			i.scan.Close()
			i.scan = nil
			i.mu.Lock()
			for name, c := range i.entries {
				if !i.seen[name] {
					delete(i.entries, name)
					i.metadataBytes -= candidateCost(c)
				}
			}
			i.mu.Unlock()
			i.mu.Lock()
			i.complete = true
			i.mu.Unlock()
			i.seen = nil
			return true, nil
		}
	}
	return false, nil
}
func (i *Index) Refresh(ctx context.Context) error {
	buf := make([]byte, 64<<10)
	for range 4 {
		n, e := unix.Read(i.fd, buf)
		if errors.Is(e, unix.EAGAIN) {
			break
		}
		if e != nil {
			return e
		}
		for pos := 0; pos+16 <= n; {
			mask := binary.NativeEndian.Uint32(buf[pos+4 : pos+8])
			size := int(binary.NativeEndian.Uint32(buf[pos+12 : pos+16]))
			if pos+16+size > n {
				return fmt.Errorf("invalid spool notification")
			}
			name := strings.TrimRight(string(buf[pos+16:pos+16+size]), "\x00")
			pos += 16 + size
			if filepath.Ext(name) != ".batch" {
				continue
			}
			if mask&unix.IN_DELETE != 0 {
				i.mu.Lock()
				if c, ok := i.entries[name]; ok {
					delete(i.entries, name)
					i.metadataBytes -= candidateCost(c)
				}
				i.mu.Unlock()
			} else if e := i.add(name); e != nil {
				return e
			} else if i.seen != nil {
				i.seen[name] = true
			}
		}
	}
	_, e := i.reconcile(ctx, 4)
	return e
}
