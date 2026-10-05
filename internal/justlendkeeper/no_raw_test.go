package justlendkeeper

import (
	"context"
	"io"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

type noRawTransport struct{ status int }

func (tr noRawTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: tr.status, Body: io.NopCloser(strings.NewReader(`{"public":"data"}`)), Header: http.Header{}, Request: r}, nil
}

func TestSendKeepsHashesWithoutArchiveEvenOnHTTPError(t *testing.T) {
	for _, status := range []int{200, 503} {
		c, _, now := testCollector(t)
		// A regular file cannot contain an archive, so any archive I/O fails.
		path := filepath.Join(t.TempDir(), "not-a-directory")
		if err := os.WriteFile(path, []byte("untouched"), 0600); err != nil {
			t.Fatal(err)
		}
		c.API.Archive.Dir = path
		c.API.HTTP.Transport = noRawTransport{status}
		*now = now.Add(5 * time.Second)
		r := Post("publicnode", "/wallet/getaccount", "account", "", map[string]any{"address": c.Config.Callers[0]}, false)
		ev, raw, err := c.API.Send(context.Background(), r)
		if err != nil || ev.RequestHash != Hex(Hash(r.Body)) || ev.ResponseHash != Hex(Hash(raw)) || ev.Status != status {
			t.Fatal("response/hash collection requires files", err, ev)
		}
		if status != 200 && ev.Error != "http_503" {
			t.Fatal("HTTP failure lost", ev)
		}
		content, _ := os.ReadFile(path)
		if string(content) != "untouched" {
			t.Fatal("archive path modified")
		}
	}
}

type migrationStore struct {
	batches []Batch
	pages   map[uuid.UUID]IndexPage
}

func (s *migrationStore) KeeperCaptures(context.Context, any, any) ([]Capture, error) {
	var caps []Capture
	for _, b := range s.batches {
		caps = append(caps, b.Capture)
	}
	return caps, nil
}
func (s *migrationStore) KeeperBatch(_ context.Context, cap Capture) (Batch, error) {
	for _, b := range s.batches {
		if b.Capture.CaptureId == cap.CaptureId {
			if p, ok := s.pages[cap.CaptureId]; ok {
				b.IndexPage = &p
			}
			return b, Validate(b)
		}
	}
	return Batch{}, os.ErrNotExist
}
func (s *migrationStore) KeeperIndexPages(context.Context, []Capture) (map[uuid.UUID]IndexPage, error) {
	return s.pages, nil
}
func (s *migrationStore) WriteKeeperIndexPages(_ context.Context, pages []IndexPage) error {
	for _, p := range pages {
		s.pages[p.CaptureId] = p
	}
	return nil
}
func (s *migrationStore) KeeperParentCaptures(_ context.Context, ids []uuid.UUID) ([]Capture, error) {
	var caps []Capture
	for _, id := range ids {
		for _, b := range s.batches {
			if b.Capture.CaptureId == id {
				caps = append(caps, b.Capture)
			}
		}
	}
	return caps, nil
}

func TestLegacyPageMigrationAndExportWithoutAnyArchive(t *testing.T) {
	c, _, now := testCollector(t)
	s, body, ev := collectionPage(t, c)
	c.State.Scans = []Scan{s}
	c.AddPage(s)
	o := &c.State.Ops[0]
	req := o.Requests[0]
	o.Requests = nil
	o.Evidence = []Evidence{ev}
	if err := c.Process(o, req, ev, body); err != nil {
		t.Fatal(err)
	}
	if err := c.Finish(context.Background(), o); err != nil {
		t.Fatal(err)
	}
	legacy := o.Batch
	legacy.IndexPage = nil
	m := Manifest{Version: "keeper-v1", DigestEncoding: "jl-keeper-fact-v1", Capture: legacy.Capture.CaptureId.String(), Kind: "index_page", ScanID: s.ID, FingerprintIn: s.Fingerprint, FingerprintOut: o.NextFingerprint, Requests: []Evidence{ev}}
	legacy.Capture.EvidenceManifestHash, _ = c.API.Archive.Put(JSONEvidence(m))
	bad := legacy
	badManifest := m
	badManifest.Requests = append([]Evidence(nil), m.Requests...)
	badManifest.Requests[0].ResponseHash = Hex(Hash([]byte("wrong response")))
	bad.Capture.EvidenceManifestHash, _ = c.API.Archive.Put(JSONEvidence(badManifest))
	badStore := &migrationStore{batches: []Batch{bad}, pages: map[uuid.UUID]IndexPage{}}
	if count, err := MigrateIndexPages(context.Background(), badStore, c.API.Archive, badStore); err == nil || count != 0 || len(badStore.pages) != 0 {
		t.Fatal("migration accepted summary inconsistent with DB source hash", count, err)
	}
	before, _ := FactBytes(legacy.Capture)
	store := &migrationStore{batches: []Batch{legacy}, pages: map[uuid.UUID]IndexPage{}}
	count, err := MigrateIndexPages(context.Background(), store, c.API.Archive, store)
	if err != nil || count != 1 {
		t.Fatal("legacy progress migration", count, err)
	}
	after, _ := FactBytes(store.batches[0].Capture)
	if string(before) != string(after) {
		t.Fatal("migration rewrote frozen capture")
	}
	if err := os.RemoveAll(c.API.Archive.Dir); err != nil {
		t.Fatal(err)
	}
	count, err = MigrateIndexPages(context.Background(), store, c.API.Archive, store)
	if err != nil || count != 0 {
		t.Fatal("migration is not idempotent without archive", count, err)
	}
	out := filepath.Join(t.TempDir(), "export")
	if err := Export(context.Background(), store, Archive{Dir: "/nonexistent/archive"}, c.Config, now.Add(-time.Hour), now.Add(time.Hour), out); err != nil {
		t.Fatal("export depends on archive", err)
	}
	meta, _ := os.ReadFile(filepath.Join(out, "metadata.json"))
	if !strings.Contains(string(meta), `"raw_responses_retained":false`) || strings.Contains(string(meta), "evidence_directory") {
		t.Fatal("export promises raw evidence it no longer has")
	}
	if _, err := os.Stat(filepath.Join(out, "index_pages.csv")); err != nil {
		t.Fatal(err)
	}
	store.batches[0].IndexedEvents[0].AddedAmountSun = big.NewInt(1)
	if err := Export(context.Background(), store, Archive{}, c.Config, now.Add(-time.Hour), now.Add(time.Hour), filepath.Join(t.TempDir(), "bad")); err == nil {
		t.Fatal("member digest checks disappeared")
	}
}

func TestIndexedRestoreRejectsMissingFieldsAndPreservesOrdinals(t *testing.T) {
	c, _, _ := testCollector(t)
	s, body, ev := collectionPage(t, c)
	p, _, err := ParsePage(body, ev, c.Config, s)
	if err != nil {
		t.Fatal(err)
	}
	for _, source := range p.Data {
		row, err := indexedRow(source, c.NewOperation("index_page", "live", "trongrid").Batch.Capture, source.Ordinal)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := rawFromIndexed(row)
		if err != nil || raw.Ordinal != row.Ordinal || raw.Evidence.ResponseHash != Hex(row.PayloadHash) {
			t.Fatal("database row restore lost provenance", err)
		}
		row.AddedAmountSun = nil
		if _, err := rawFromIndexed(row); err == nil {
			t.Fatal("missing required field accepted")
		}
	}
}

func TestEmptyIndexPageWithoutResponseFiles(t *testing.T) {
	c, _, now := testCollector(t)
	from, to := now.Add(-time.Hour), now.Add(-30*time.Minute)
	s := Scan{ID: "watch:v2:empty", Kind: "RentResource", Mode: "live", From: from, To: to}
	body := []byte(`{"success":true,"data":[],"meta":{}}`)
	ev := Evidence{Source: "trongrid", Path: EventPath(s.Kind, from, to, ""), Started: *now, Available: *now, ResponseHash: Hex(Hash(body)), Status: 200}
	c.State.Scans = []Scan{s}
	c.AddPage(s)
	o := &c.State.Ops[0]
	r := o.Requests[0]
	o.Requests = nil
	o.Evidence = []Evidence{ev}
	if err := c.Process(o, r, ev, body); err != nil {
		t.Fatal(err)
	}
	if err := c.Finish(context.Background(), o); err != nil {
		t.Fatal(err)
	}
	if o.Batch.IndexPage == nil || o.Batch.IndexPage.PayloadHash != Hash(body) || len(o.Batch.IndexedEvents) != 0 || !o.Batch.Capture.PaginationExhausted {
		t.Fatal("empty page lost source progress")
	}
	if _, err := os.Stat(c.API.Archive.Dir); !os.IsNotExist(err) {
		t.Fatal("collection created archive", err)
	}
	store := &migrationStore{batches: []Batch{o.Batch}, pages: map[uuid.UUID]IndexPage{o.Batch.Capture.CaptureId: *o.Batch.IndexPage}}
	if err := Export(context.Background(), store, Archive{}, c.Config, from, now.Add(time.Hour), filepath.Join(t.TempDir(), "empty")); err != nil {
		t.Fatal(err)
	}
}
