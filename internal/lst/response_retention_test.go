package lst

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
)

func retainedFixture(t *testing.T) (*Collector, Batch, []string) {
	t.Helper()
	c := &Collector{StateDir: t.TempDir(), Archive: ethereum.Archive{Dir: t.TempDir()}, Store: &runnerMemoryStore{}, PruneCommittedResponses: true}
	b := reportGoodMarket(t)
	for _, raw := range []string{`{"method":"eth_call"}`, `{"result":"0x01"}`} {
		h, err := c.Archive.Put([]byte(raw))
		if err != nil {
			t.Fatal(err)
		}
		b.RawEvidenceHashes = append(b.RawEvidenceHashes, h.String())
	}
	return c, b, append([]string(nil), b.RawEvidenceHashes...)
}

func requireRawPresence(t *testing.T, c *Collector, hashes []string, want bool) {
	t.Helper()
	for _, h := range hashes {
		p, err := rawEvidencePath(c.Archive.Dir, h)
		if err != nil {
			t.Fatal(err)
		}
		_, err = os.Stat(p)
		if want && err != nil || !want && !os.IsNotExist(err) {
			t.Fatalf("raw presence want=%t: %s: %v", want, h, err)
		}
	}
}

func TestResponseRetentionWaitsForDatabaseAckAndSurvivesRestart(t *testing.T) {
	c, b, hashes := retainedFixture(t)
	s := c.Store.(*runnerMemoryStore)
	s.fail = true
	before := CanonicalHash(b)
	if err := c.Commit(context.Background(), b); err == nil {
		t.Fatal("write failure ignored")
	}
	requireRawPresence(t, c, hashes, true)
	var frozen Batch
	if err := readGob(filepath.Join(c.StateDir, "pending-batch.gob"), &frozen); err != nil {
		t.Fatal(err)
	}
	if CanonicalHash(frozen) != before {
		t.Fatal("retry identity, NULL/zero or cleanup list changed")
	}
	s.fail = false
	restarted := *c
	if err := restarted.FlushPending(context.Background()); err != nil {
		t.Fatal(err)
	}
	requireRawPresence(t, c, hashes, false)
	if CanonicalHash(s.batches[b.Capture.CaptureId]) != before {
		t.Fatal("stored typed batch changed")
	}
	if _, err := os.Stat(filepath.Join(c.StateDir, "pending-batch.gob")); !os.IsNotExist(err) {
		t.Fatal("pending not removed after success", err)
	}
}

func TestResponseRetentionNormalCommitKeepsManifestAndReport(t *testing.T) {
	c, b, hashes := retainedFixture(t)
	m, err := LoadManifest("../../config/lst-lido-ethereum.json")
	if err != nil {
		t.Fatal(err)
	}
	c.Manifest = m
	h, err := c.Archive.Put(m.Raw)
	if err != nil {
		t.Fatal(err)
	}
	b.RawEvidenceHashes = append(b.RawEvidenceHashes, h.String())
	if err := c.Commit(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	requireRawPresence(t, c, hashes, false)
	requireRawPresence(t, c, []string{h.String()}, true)
	if _, err := os.Stat(filepath.Join(c.Archive.Dir, "diagnostics", "market.json.gz")); !os.IsNotExist(err) {
		t.Fatal("success created raw diagnostic")
	}
	// Report never needs raw files, including for historical mixed retention.
	if err := Report(context.Background(), c.Store, Manifest{Hash: b.Capture.ManifestHash}, b.Capture.StartedAt, b.Capture.StartedAt.Add(time.Minute), t.TempDir()); err != nil {
		t.Fatal(err)
	}
}

func TestResponseRetentionPartialIsNotAParsingFailure(t *testing.T) {
	b := reportGoodMarket(t)
	b.Capture.Status = "partial"
	q := b.Quotes[0]
	q.HedgeStatus = "unknown"
	q.HedgeReason = "cex_ten_level_capacity_insufficient"
	b.Quotes = append(b.Quotes, q, Quote{TimingStatus: "not_scheduled"})
	if needsRawDiagnostic(b) {
		t.Fatal("capacity or unrequested member retained every successful body")
	}
	b.Quotes[0].ExitStatus = "unknown"
	b.Quotes[0].ExitReason = "abi_malformed"
	if !needsRawDiagnostic(b) {
		t.Fatal("malformed decoded response lost diagnostic")
	}
}

func TestResponseRetentionBoundedSampleAndIdempotentCleanup(t *testing.T) {
	c, b, hashes := retainedFixture(t)
	b.Protocols[0].StateStatus = "unknown"
	b.Protocols[0].Reason = "abi_malformed"
	if err := b.Seal(); err != nil {
		t.Fatal(err)
	}
	if err := c.Commit(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	requireRawPresence(t, c, hashes, false)
	path := filepath.Join(c.Archive.Dir, "diagnostics", "market.json.gz")
	first, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err = c.pruneRawEvidence(b); err != nil {
		t.Fatal(err)
	}
	second, _ := os.ReadFile(path)
	if string(first) != string(second) {
		t.Fatal("cleanup retry overwrote useful sample with missing objects")
	}
	for i := 0; i < 3; i++ {
		h, err := c.Archive.Put([]byte(strings.Repeat("a", 2<<20) + string(rune('0'+i))))
		if err != nil {
			t.Fatal(err)
		}
		if err = c.saveRawDiagnostic("identity", "failed", []string{h.String()}); err != nil {
			t.Fatal(err)
		}
		if err = c.removeRawEvidence([]string{h.String()}); err != nil {
			t.Fatal(err)
		}
	}
	f, err := os.Open(filepath.Join(c.Archive.Dir, "diagnostics", "identity.json.gz"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	z, err := gzip.NewReader(f)
	if err != nil {
		t.Fatal(err)
	}
	defer z.Close()
	var sample struct {
		Objects []struct {
			Raw       []byte
			Truncated bool
		}
	}
	if err = json.NewDecoder(z).Decode(&sample); err != nil {
		t.Fatal(err)
	}
	if len(sample.Objects) != 1 || len(sample.Objects[0].Raw) != 1<<20 || !sample.Objects[0].Truncated {
		t.Fatal("raw sample limit not explicit or exceeded")
	}
	files, _ := filepath.Glob(filepath.Join(c.Archive.Dir, "diagnostics", "*.json.gz"))
	if len(files) != 2 {
		t.Fatal("same task kind accumulated samples")
	}
}

func TestResponseRetentionTracksRequestsAndMaintenanceAndRejectsPaths(t *testing.T) {
	tr, _ := transportFixture(t, func(_ *http.Request) (*http.Response, error) {
		return transportReply(200, `{"jsonrpc":"2.0","id":1,"result":"0x1"}`), nil
	})
	tr.cfg.PruneCommittedResponses = true
	res, err := tr.Do(context.Background(), "rpc", "POST", "https://rpc.example/secret", transportRPCBody, "normal")
	if err != nil {
		t.Fatal(err)
	}
	c := &Collector{RPC: &RPC{Transport: tr}, Archive: tr.archive, PruneCommittedResponses: true}
	b := reportGoodMarket(t)
	if err = c.seal(&b, []Response{res}); err != nil {
		t.Fatal(err)
	}
	if len(b.RawEvidenceHashes) != 3 || len(c.takeRawEvidence()) != 0 {
		t.Fatal("requests, responses or proof not bound to frozen batch")
	}
	for _, bad := range []string{"../../state/live-logs.gob", "0x../state", strings.Repeat("a", 64)} {
		if _, err := rawEvidencePath(c.Archive.Dir, bad); err == nil {
			t.Fatal("unsafe deletion path accepted", bad)
		}
	}
}

func TestResponseRetentionAuxiliaryFailureProtectsPendingAndKeepsSample(t *testing.T) {
	c, b, hashes := retainedFixture(t)
	c.Store.(*runnerMemoryStore).fail = true
	if err := c.Commit(context.Background(), b); err == nil {
		t.Fatal("database failure ignored")
	}
	if err := c.saveUncommittedDiagnostic("maintenance", errors.New("db_failed")); err != nil {
		t.Fatal(err)
	}
	requireRawPresence(t, c, hashes, true)
	if _, err := os.Stat(filepath.Join(c.StateDir, "pending-batch.gob")); err != nil {
		t.Fatal(err)
	}
	c.Store.(*runnerMemoryStore).fail = false
	if err := c.FlushPending(context.Background()); err != nil {
		t.Fatal(err)
	}
	tr, _ := transportFixture(t, func(_ *http.Request) (*http.Response, error) {
		return transportReply(200, `{"jsonrpc":"2.0","id":1,"result":"bad_header"}`), nil
	})
	tr.cfg.PruneCommittedResponses = true
	c.RPC = &RPC{Transport: tr}
	c.Archive = tr.archive
	res, err := tr.Do(context.Background(), "rpc", "POST", "https://rpc.example", transportRPCBody, "normal")
	if err != nil {
		t.Fatal(err)
	}
	if err = c.saveUncommittedDiagnostic("maintenance", errors.New("header_malformed")); err != nil {
		t.Fatal(err)
	}
	requireRawPresence(t, c, []string{res.RequestHash, res.PayloadHash}, false)
	if _, err = os.Stat(filepath.Join(c.Archive.Dir, "diagnostics", "maintenance.json.gz")); err != nil {
		t.Fatal("auxiliary diagnostic lost", err)
	}
	if needsRawDiagnostic(Batch{Capture: Capture{Reason: "gas_receipt_enrichment"}, Requests: []WithdrawalRequest{{}}}) != true {
		t.Fatal("incomplete gas parse lost sample")
	}
}
