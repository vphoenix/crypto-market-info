package writer

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
	"github.com/vphoenix/crypto-market-info/internal/storage/spool"
)

func TestFiniteFrameArchivePreservesOriginalBytesAndConflict(t *testing.T) {
	from := time.Now().UTC().Truncate(time.Minute)
	dir := t.TempDir()
	a, e := OpenFrameArchive(dir, from, from.Add(time.Hour))
	if e != nil {
		t.Fatal(e)
	}
	d := contracts.Delivery{Database: "crypto_market_info", Kind: "cex", ID: "fixture", Service: "collector", Generation: 1, Minute: from, Members: 1, Codec: 1, Payload: []byte{1, 2, 3}}
	d.Digest = sha256.Sum256(d.Payload)
	for range 2 {
		if e = a.Capture(context.Background(), d); e != nil {
			t.Fatal(e)
		}
	}
	raw, e := os.ReadFile(filepath.Join(dir, spool.Name(d)))
	if e != nil {
		t.Fatal(e)
	}
	want, _ := spool.Marshal(d)
	if !bytes.Equal(raw, want) {
		t.Fatal("fixture bytes changed")
	}
	changed := d
	changed.Payload = []byte{4, 5, 6}
	changed.Digest = sha256.Sum256(changed.Payload)
	if e = a.Capture(context.Background(), changed); !errors.Is(e, contracts.ErrConflict) {
		t.Fatal("fixture conflict accepted", e)
	}
	outside := d
	outside.Minute = from.Add(time.Hour)
	outside.ID = "outside"
	if e = a.Capture(context.Background(), outside); e != nil {
		t.Fatal(e)
	}
	if _, e = os.Stat(filepath.Join(dir, spool.Name(outside))); !os.IsNotExist(e) {
		t.Fatal("window capture did not stop")
	}
}
