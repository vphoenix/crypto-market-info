package lst

import (
	"compress/gzip"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/vphoenix/crypto-market-info/internal/dex"
)

// Source bodies remain temporary until the frozen typed batch is acknowledged
// by the database. No report, followup or finality check reads these bodies.
func (t *Transport) putEvidence(v any) (dex.Hash, error) {
	h, err := t.archive.PutObject(v)
	if err == nil && t.cfg.PruneCommittedResponses {
		t.mu.Lock()
		t.rawEvidence = append(t.rawEvidence, h.String())
		t.mu.Unlock()
	}
	return h, err
}

func (c *Collector) takeRawEvidence() []string {
	if c.RPC == nil || c.RPC.Transport == nil {
		return nil
	}
	t := c.RPC.Transport
	t.mu.Lock()
	defer t.mu.Unlock()
	hashes := t.rawEvidence
	t.rawEvidence = nil
	return hashes
}

func rawEvidencePath(dir, hash string) (string, error) {
	if !strings.HasPrefix(hash, "0x") {
		return "", errors.New("raw_evidence_hash_invalid")
	}
	if _, err := ParseHex(hash, 32); err != nil {
		return "", errors.New("raw_evidence_hash_invalid")
	}
	return filepath.Join(dir, hash[2:4], hash[2:]+".json.gz"), nil
}

func needsRawDiagnostic(b Batch) bool {
	if b.Capture.Status == "failed" {
		return true
	}
	if b.Capture.Reason == "gas_receipt_enrichment" {
		for _, r := range b.Requests {
			if r.GasUsed == nil {
				return true
			}
		}
		for _, r := range b.Claims {
			if r.GasUsed == nil {
				return true
			}
		}
	}
	for _, p := range b.Protocols {
		if p.StateStatus != "ok" {
			return true
		}
	}
	for _, q := range b.Quotes {
		if q.TimingStatus == "not_scheduled" || q.TimingStatus == "missed" {
			continue
		}
		if q.QuoteRole == "entry" && (q.BuyStatus != "ok" || q.ConversionStatus != "ok") || q.ExitStatus != "ok" {
			return true
		}
		if q.HedgeStatus != "ok" && q.HedgeReason != "cex_ten_level_capacity_insufficient" {
			return true
		}
	}
	return false
}

func (c *Collector) pruneRawEvidence(b Batch) error {
	if len(b.RawEvidenceHashes) == 0 {
		return nil // old pending encoding remains readable
	}
	if needsRawDiagnostic(b) {
		reasons := []string{b.Capture.Reason}
		for _, p := range b.Protocols {
			reasons = append(reasons, p.Reason)
		}
		for _, q := range b.Quotes {
			if q.TimingStatus != "not_scheduled" {
				reasons = append(reasons, q.Reason, q.BuyReason, q.ConversionReason, q.ExitReason, q.HedgeReason)
			}
		}
		if err := c.saveRawDiagnostic(b.Capture.CaptureKind, strings.Join(reasons, "\n"), b.RawEvidenceHashes); err != nil {
			return err
		}
	}
	return c.removeRawEvidence(b.RawEvidenceHashes)
}

func (c *Collector) removeRawEvidence(hashes []string) error {
	// Check the whole allowlist before deleting anything. Manifest is useful
	// configuration, not a disposable source response.
	paths := make(map[string]bool)
	for _, h := range hashes {
		if h == Hex(c.Manifest.Hash) {
			continue
		}
		p, err := rawEvidencePath(c.Archive.Dir, h)
		if err != nil {
			return err
		}
		paths[p] = true
	}
	for p := range paths {
		if err := os.Remove(p); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

// At most one diagnostic per task kind, with a 1 MiB total raw-body budget.
// Truncation/missing objects are explicit; samples never establish coverage.
func (c *Collector) saveRawDiagnostic(kind, reason string, hashes []string) error {
	switch kind {
	case "market", "logs", "funding", "identity", "maintenance":
	default:
		return errors.New("raw_diagnostic_kind_invalid")
	}
	if len(hashes) == 0 {
		return nil
	}
	if len(reason) > 8192 {
		reason = reason[:8192]
	}
	dir := filepath.Join(c.Archive.Dir, "diagnostics")
	identity := Hex(CanonicalHash(hashes))
	// A retry after acknowledged writes/partial cleanup must not replace the
	// original sample with a bundle of missing bodies.
	if f, err := os.Open(filepath.Join(dir, kind+".json.gz")); err == nil {
		z, ze := gzip.NewReader(f)
		if ze != nil {
			f.Close()
			return ze
		}
		var old struct{ Identity string }
		de := json.NewDecoder(io.LimitReader(z, 2<<20)).Decode(&old)
		z.Close()
		f.Close()
		if de != nil {
			return de
		}
		if old.Identity == identity {
			return nil
		}
	} else if !os.IsNotExist(err) {
		return err
	}
	type object struct {
		Hash               string
		Raw                []byte
		Truncated, Missing bool
	}
	sample := struct {
		Version, Kind, Reason, Identity string
		Objects                         []object
	}{Version: "lst-diagnostic-v1", Kind: kind, Reason: reason, Identity: identity}
	budget := 1 << 20
	seen := map[string]bool{}
	for i := len(hashes) - 1; i >= 0; i-- {
		h := hashes[i]
		if seen[h] {
			continue
		}
		seen[h] = true
		if _, err := rawEvidencePath(c.Archive.Dir, h); err != nil {
			return err
		}
		if budget == 0 {
			sample.Objects = append(sample.Objects, object{Hash: h, Truncated: true})
			continue
		}
		rawHash, _ := ParseHex(h, 32)
		var digest dex.Hash
		copy(digest[:], rawHash)
		raw, err := c.Archive.Get(digest)
		if err != nil && !os.IsNotExist(err) {
			return err
		}
		x := object{Hash: h, Missing: os.IsNotExist(err)}
		if len(raw) > budget {
			raw = raw[:budget]
			x.Truncated = true
		}
		x.Raw = raw
		budget -= len(raw)
		sample.Objects = append(sample.Objects, x)
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".pending-")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	z := gzip.NewWriter(f)
	if err = json.NewEncoder(z).Encode(sample); err != nil {
		z.Close()
		return err
	}
	if err = z.Close(); err != nil {
		return err
	}
	if err = f.Sync(); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(f.Name(), filepath.Join(dir, kind+".json.gz")); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// An initialization failure has no business batch. Retain a bounded sample
// before the existing retry, rather than accumulating every retry's bodies.
func (c *Collector) SaveInitializationDiagnostic(cause error) error {
	return c.saveUncommittedDiagnostic("identity", cause)
}

func (c *Collector) saveUncommittedDiagnostic(kind string, cause error) error {
	if !c.PruneCommittedResponses {
		return nil
	}
	// An auxiliary error may itself be a database failure. The frozen batch
	// and all of its raw bodies must survive until acknowledgement.
	if _, err := os.Stat(filepath.Join(c.StateDir, "pending-batch.gob")); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}
	hashes := c.takeRawEvidence()
	if err := c.saveRawDiagnostic(kind, cause.Error(), hashes); err != nil {
		return err
	}
	return c.removeRawEvidence(hashes)
}
