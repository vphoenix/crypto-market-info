package writer

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
	"github.com/vphoenix/crypto-market-info/internal/storage/spool"
)

// FrameArchive captures a finite validation window in the existing .batch
// format. It never acknowledges or removes an accepted production delivery.
type FrameArchive struct {
	mu       sync.Mutex
	dir      string
	from, to time.Time
	used     int64
	files    map[string]int64
}

func OpenFrameArchive(dir string, from, to time.Time) (*FrameArchive, error) {
	if !filepath.IsAbs(dir) || from.IsZero() || !from.Before(to) || to.Sub(from) > 2*time.Hour || !from.Equal(from.Truncate(time.Minute)) || !to.Equal(to.Truncate(time.Minute)) {
		return nil, fmt.Errorf("fixture capture requires an explicit UTC minute window of at most two hours")
	}
	if e := os.MkdirAll(dir, 0700); e != nil {
		return nil, e
	}
	a := &FrameArchive{dir: dir, from: from, to: to, files: map[string]int64{}}
	entries, e := os.ReadDir(dir)
	if e != nil {
		return nil, e
	}
	for _, entry := range entries {
		if filepath.Ext(entry.Name()) != ".batch" {
			continue
		}
		st, e := entry.Info()
		if e != nil {
			return nil, e
		}
		a.files[entry.Name()] = st.Size()
		a.used += st.Size()
	}
	return a, nil
}
func (a *FrameArchive) Capture(ctx context.Context, d contracts.Delivery) error {
	if d.Minute.Before(a.from) || !d.Minute.Before(a.to) {
		return nil
	}
	if e := ctx.Err(); e != nil {
		return e
	}
	raw, e := spool.Marshal(d)
	if e != nil {
		return e
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	name := spool.Name(d)
	path := filepath.Join(a.dir, name)
	if _, ok := a.files[name]; ok {
		old, e := os.ReadFile(path)
		if e != nil {
			return e
		}
		got, e := spool.Unmarshal(old)
		if e != nil {
			return e
		}
		if !got.SameHeader(d) {
			return contracts.ErrConflict
		}
		return nil
	}
	if a.used+int64(len(raw)) > 8<<30 || len(a.files) >= 20000 {
		return fmt.Errorf("frozen fixture capacity reached")
	}
	f, e := os.CreateTemp(a.dir, ".capture-")
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
	dir, e := os.Open(a.dir)
	if e != nil {
		return e
	}
	e = dir.Sync()
	dir.Close()
	if e != nil {
		return e
	}
	a.files[name] = int64(len(raw))
	a.used += int64(len(raw))
	return nil
}
