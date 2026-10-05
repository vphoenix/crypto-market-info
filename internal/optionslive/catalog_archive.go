package optionslive

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"github.com/vphoenix/crypto-market-info/internal/options"
	"io"
	"os"
	"path/filepath"
)

func archiveCatalog(dir string, raw []byte, hash string) error {
	if len(raw) == 0 {
		if hash != "" {
			return fmt.Errorf("evidence hash without payload")
		}
		return nil
	}
	if options.PayloadHash(raw) != hash {
		return fmt.Errorf("payload hash mismatch")
	}
	if e := os.MkdirAll(dir, 0700); e != nil {
		return e
	}
	path := filepath.Join(dir, hash+".json.gz")
	if f, e := os.Open(path); e == nil {
		defer f.Close()
		g, e := gzip.NewReader(f)
		if e != nil {
			return e
		}
		defer g.Close()
		b, e := io.ReadAll(io.LimitReader(g, int64(len(raw))+1))
		if e != nil {
			return e
		}
		if !bytes.Equal(b, raw) {
			return fmt.Errorf("conflicting evidence archive")
		}
		return nil
	}
	f, e := os.CreateTemp(dir, ".options-")
	if e != nil {
		return e
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	g := gzip.NewWriter(f)
	if _, e = g.Write(raw); e != nil {
		f.Close()
		return e
	}
	if e = g.Close(); e != nil {
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
	d, e := os.Open(dir)
	if e != nil {
		return e
	}
	defer d.Close()
	return d.Sync()
}
