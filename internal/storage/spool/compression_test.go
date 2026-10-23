package spool

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
	"os"
	"path/filepath"
	"testing"
)

func TestCompressedEnvelopeCompatibilityRecoveryAndBudget(t *testing.T) {
	ctx := context.Background()
	d := delivery()
	d.Payload = bytes.Repeat([]byte{1, 0, 5, 8}, 1<<18)
	d.Digest = sha256.Sum256(d.Payload)
	old, _ := Marshal(d)
	packed, e := MarshalCompressed(d)
	if e != nil || packed[4] != 2 || len(packed)*10 >= len(old) {
		t.Fatal("compression", len(old), len(packed), e)
	}
	for _, raw := range [][]byte{old, packed} {
		got, e := Unmarshal(raw)
		if e != nil || !same(d, got) {
			t.Fatal("roundtrip", e)
		}
		q, _ := Open(t.TempDir(), 4<<20)
		if e = os.WriteFile(filepath.Join(q.Directory, ".pending-crash"), raw, 0600); e != nil {
			t.Fatal(e)
		}
		if e = q.Recover(ctx); e != nil {
			t.Fatal(e)
		}
		candidate, e := readHeader(filepath.Join(q.Directory, Name(d)))
		if e != nil || candidate.Bytes != int64(len(d.Payload)) {
			t.Fatal("scheduler must reserve decoded bytes", candidate, e)
		}
		if e = q.Accept(ctx, d); e != nil {
			t.Fatal("retry across envelope versions", e)
		}
		got, e = q.Load(ctx, candidate)
		if e != nil || !same(d, got) {
			t.Fatal("load", e)
		}
		if e = q.Ack(ctx, d, func(context.Context, contracts.Delivery) (bool, error) { return false, nil }); e == nil {
			t.Fatal("unproved delete")
		}
		if e = q.Ack(ctx, d, func(context.Context, contracts.Delivery) (bool, error) { return true, nil }); e != nil {
			t.Fatal(e)
		}
	}
	q, _ := Open(t.TempDir(), 4<<20)
	d.Payload = bytes.Repeat([]byte{7}, 17<<20)
	d.Digest = sha256.Sum256(d.Payload)
	if e = q.Accept(ctx, d); e != nil {
		t.Fatal(e)
	}
	d.ID = "second"
	if e = q.Accept(ctx, d); e != nil {
		t.Fatal(e)
	}
	if _, e = q.List(ctx); e == nil {
		t.Fatal("compressed listing bypasses decoded memory cap")
	}
}

func TestCompressedEnvelopeRejectsCorruptionBombAndTrailingStream(t *testing.T) {
	d := delivery()
	d.Payload = bytes.Repeat([]byte{3}, 8192)
	d.Digest = sha256.Sum256(d.Payload)
	raw, _ := MarshalCompressed(d)
	for n := range raw {
		if _, e := Unmarshal(raw[:n]); e == nil {
			t.Fatalf("truncated %d accepted", n)
		}
	}
	// Locate decoded length and stored length in the versioned envelope.
	pos := 5
	for range 4 {
		n := int(binary.BigEndian.Uint16(raw[pos:]))
		pos += 2 + n
	}
	lengthAt := pos + 8 + 8 + 2 + 4
	storedAt := lengthAt + 4 + 32
	checksum := func(b []byte) []byte { h := sha256.Sum256(b); return append(b, h[:]...) }
	for _, n := range []uint32{0, 1, 8191, 8193, MaxPayload + 1} {
		bad := bytes.Clone(raw[:len(raw)-32])
		binary.BigEndian.PutUint32(bad[lengthAt:], n)
		if _, e := Unmarshal(checksum(bad)); e == nil {
			t.Fatalf("decoded length %d accepted", n)
		}
	}
	bad := bytes.Clone(raw[:len(raw)-32])
	bad = append(bad, 0)
	binary.BigEndian.PutUint32(bad[storedAt:], binary.BigEndian.Uint32(bad[storedAt:])+1)
	if _, e := Unmarshal(checksum(bad)); e == nil {
		t.Fatal("trailing compressed bytes accepted")
	}
	bad = bytes.Clone(raw[:len(raw)-32])
	bad[len(bad)-1] ^= 1
	if _, e := Unmarshal(checksum(bad)); e == nil {
		t.Fatal("inner checksum not verified")
	}
}
