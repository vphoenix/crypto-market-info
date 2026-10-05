package ethereum

import (
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/vphoenix/crypto-market-info/internal/dex"
	"io"
	"os"
	"path/filepath"
)

type Archive struct {
	Dir string
	// HashOnly retains source/member digests without writing response bodies.
	// Opt-in only: other collectors keep their existing archive behavior.
	HashOnly bool
}

func (a Archive) Put(raw []byte) (dex.Hash, error) {
	h := dex.Digest(raw)
	if a.HashOnly {
		return h, nil
	}
	if a.Dir == "" {
		return h, fmt.Errorf("evidence directory required")
	}
	dir := filepath.Join(a.Dir, h.String()[2:4])
	if e := os.MkdirAll(dir, 0700); e != nil {
		return h, e
	}
	f, e := os.CreateTemp(dir, ".pending-")
	if e != nil {
		return h, e
	}
	name := f.Name()
	defer os.Remove(name)
	z := gzip.NewWriter(f)
	if _, e = z.Write(raw); e != nil {
		f.Close()
		return h, e
	}
	if e = z.Close(); e != nil {
		f.Close()
		return h, e
	}
	if e = f.Sync(); e != nil {
		f.Close()
		return h, e
	}
	if e = f.Close(); e != nil {
		return h, e
	}
	if e = os.Rename(name, filepath.Join(dir, h.String()[2:]+".json.gz")); e != nil {
		return h, e
	}
	d, e := os.Open(dir)
	if e != nil {
		return h, e
	}
	defer d.Close()
	return h, d.Sync()
}
func (a Archive) PutObject(v any) (dex.Hash, error) {
	b, e := json.Marshal(v)
	if e != nil {
		return dex.Hash{}, e
	}
	return a.Put(b)
}
func (a Archive) Get(h dex.Hash) ([]byte, error) {
	if a.HashOnly {
		return nil, errors.New("raw_response_not_retained")
	}
	f, e := os.Open(filepath.Join(a.Dir, h.String()[2:4], h.String()[2:]+".json.gz"))
	if e != nil {
		return nil, e
	}
	defer f.Close()
	z, e := gzip.NewReader(f)
	if e != nil {
		return nil, e
	}
	defer z.Close()
	b, e := io.ReadAll(io.LimitReader(z, 32<<20))
	if e != nil {
		return nil, e
	}
	if dex.Digest(b) != h {
		return nil, fmt.Errorf("evidence hash mismatch")
	}
	return b, nil
}
