package justlendkeeper

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/gob"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"
)

func Hash(b []byte) string { h := sha256.Sum256(b); return string(h[:]) }
func Hex(s string) string  { return hex.EncodeToString([]byte(s)) }
func Freeze(v any) ([]byte, error) {
	var b bytes.Buffer
	e := gob.NewEncoder(&b).Encode(v)
	return b.Bytes(), e
}
func Thaw(b []byte, v any) error { return gob.NewDecoder(bytes.NewReader(b)).Decode(v) }
func Atomic(path string, b []byte) error {
	if e := os.MkdirAll(filepath.Dir(path), 0700); e != nil {
		return e
	}
	f, e := os.CreateTemp(filepath.Dir(path), ".keeper-*")
	if e != nil {
		return e
	}
	name := f.Name()
	defer os.Remove(name)
	if e = f.Chmod(0600); e == nil {
		_, e = f.Write(b)
	}
	if e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e == nil {
		e = ce
	}
	if e != nil {
		return e
	}
	if e = os.Rename(name, path); e != nil {
		return e
	}
	d, e := os.Open(filepath.Dir(path))
	if e != nil {
		return e
	}
	defer d.Close()
	return d.Sync()
}

type Archive struct{ Dir string }

func (a Archive) Put(b []byte) (string, error) {
	h := Hash(b)
	p := filepath.Join(a.Dir, Hex(h)[:2], Hex(h)+".gz")
	if old, e := a.Get(h); e == nil {
		if !bytes.Equal(old, b) {
			return "", errors.New("archive_conflict")
		}
		return h, nil
	} else if !errors.Is(e, os.ErrNotExist) {
		return "", e
	}
	var out bytes.Buffer
	z := gzip.NewWriter(&out)
	if _, e := z.Write(b); e != nil {
		return "", e
	}
	if e := z.Close(); e != nil {
		return "", e
	}
	return h, Atomic(p, out.Bytes())
}
func (a Archive) Get(h string) ([]byte, error) {
	if len(h) != 32 {
		return nil, errors.New("invalid_archive_hash")
	}
	f, e := os.Open(filepath.Join(a.Dir, Hex(h)[:2], Hex(h)+".gz"))
	if e != nil {
		return nil, e
	}
	defer f.Close()
	z, e := gzip.NewReader(f)
	if e != nil {
		return nil, e
	}
	defer z.Close()
	b, e := io.ReadAll(io.LimitReader(z, 64*1024*1024+1))
	if e != nil {
		return nil, e
	}
	if len(b) > 64*1024*1024 || Hash(b) != h {
		return nil, errors.New("archive_hash_or_size")
	}
	return b, nil
}
func digest[T any](rows []T) string {
	members := make([]string, len(rows))
	for i, r := range rows {
		b, e := FactBytes(r)
		if e != nil {
			panic(e)
		}
		members[i] = Hash(b)
	}
	sort.Strings(members)
	return Hash([]byte(join(members)))
}
func join(ss []string) string {
	var b bytes.Buffer
	for _, s := range ss {
		b.WriteString(s)
	}
	return b.String()
}
func Seal(b *Batch) {
	c := &b.Capture
	c.EventRows = uint32(len(b.Events))
	c.EventDigest = digest(b.Events)
	c.ReceiptRows = uint32(len(b.Receipts))
	c.ReceiptDigest = digest(b.Receipts)
	c.ProbeRows = uint32(len(b.Probes))
	c.ProbeDigest = digest(b.Probes)
	c.CostRows = uint32(len(b.Costs))
	c.CostDigest = digest(b.Costs)
	if c.CaptureKind == "event_index" {
		c.IndexedRows = uint32(len(b.IndexedEvents))
		c.IndexedDigest = Ptr(digest(b.IndexedEvents))
	}
	c.Committed = true
}
func Validate(b Batch) error {
	c := b.Capture
	if c.CaptureId == [16]byte{} || c.CaptureStartedAt.IsZero() || len(c.ConfigHash) != 32 || len(c.EvidenceManifestHash) != 32 || len(c.ContractAddress) != 21 || c.Network != Network || !c.Committed {
		return errors.New("invalid_capture_identity")
	}
	if c.CompletedTasks > c.ExpectedTasks || c.SelectedCandidates > c.DiscoveredCandidates {
		return errors.New("invalid_capture_counts")
	}
	if int(c.EventRows+c.ReceiptRows+c.ProbeRows+c.CostRows+c.IndexedRows) > 500 {
		return errors.New("batch_exceeds_500")
	}
	if c.EventRows != uint32(len(b.Events)) || c.EventDigest != digest(b.Events) || c.ReceiptRows != uint32(len(b.Receipts)) || c.ReceiptDigest != digest(b.Receipts) || c.ProbeRows != uint32(len(b.Probes)) || c.ProbeDigest != digest(b.Probes) || c.CostRows != uint32(len(b.Costs)) {
		return errors.New("capture_members_mismatch")
	}
	if c.CostDigest != digest(b.Costs) {
		return errors.New("capture_members_mismatch")
	}
	if err := validateIndexed(b); err != nil {
		return err
	}
	for _, r := range b.Events {
		if r.CaptureId != c.CaptureId || !r.CaptureStartedAt.Equal(c.CaptureStartedAt) || r.Finality != "solid" || len(r.BlockHash) != 32 || len(r.TxId) != 32 || len(r.PayloadHash) != 32 || r.ResourceType != 1 || r.AmountSun == nil || r.AmountSun.Sign() < 0 || r.AmountSun.BitLen() > 256 {
			return errors.New("invalid_event")
		}
		if r.PositionStatus == "receipt_verified" && (r.ReceiptLogIndex == nil || r.TransactionIndex == nil) {
			return errors.New("missing_event_position")
		}
	}
	for _, r := range b.Receipts {
		if r.CaptureId != c.CaptureId || !r.CaptureStartedAt.Equal(c.CaptureStartedAt) || r.Finality != "solid" || len(r.BlockHash) != 32 || len(r.ReceiptPayloadHash) != 32 {
			return errors.New("invalid_receipt")
		}
	}
	for _, r := range b.Probes {
		if r.CaptureId != c.CaptureId || !r.CaptureStartedAt.Equal(c.CaptureStartedAt) || r.StateBinding != "node_latest_unpinned" || r.CallerAddress == r.Renter || r.CallerAddress == r.Receiver {
			return errors.New("invalid_probe")
		}
		if r.Status == "success_reward" {
			return errors.New("success_fixture_not_certified")
		}
	}
	for _, r := range b.Costs {
		if r.CaptureId != c.CaptureId || !r.CaptureStartedAt.Equal(c.CaptureStartedAt) || r.AvailableAt.IsZero() {
			return errors.New("invalid_cost")
		}
		if r.Status == "ok" && (r.PayloadHash == nil || len(*r.PayloadHash) != 32) {
			return errors.New("cost_missing_hash")
		}
	}
	return nil
}

func JSONEvidence(v any) []byte {
	b, e := json.Marshal(v)
	if e != nil {
		panic(e)
	}
	return b
}

type CohortMember struct {
	Renter       string    `json:"renter"`
	Receiver     string    `json:"receiver"`
	Stratum      uint8     `json:"stratum"`
	Lifecycle    string    `json:"lifecycle_id"`
	OriginTx     string    `json:"origin_tx"`
	OriginIndex  uint32    `json:"origin_provider_index"`
	LastActivity time.Time `json:"last_activity"`
	Verified     bool      `json:"verified"`
	Closed       bool      `json:"closed"`
	Ambiguous    bool      `json:"same_block_ambiguous"`
	SeedRevision string    `json:"seed_revision,omitempty"`
	SeedAttempts uint8     `json:"seed_attempts"`
	SeedNext     time.Time `json:"seed_next_at"`
}
type CohortChange struct {
	At      time.Time    `json:"at"`
	Slot    int          `json:"slot"`
	Exited  CohortMember `json:"exited"`
	Entered CohortMember `json:"entered"`
	Reason  string       `json:"reason"`
}
type Manifest struct {
	ParentCapture       string         `json:"parent_capture_id,omitempty"`
	ParentManifestHash  string         `json:"parent_manifest_hash,omitempty"`
	ExcludedResource    uint32         `json:"excluded_resource_events,omitempty"`
	EventFilterRevision string         `json:"event_filter_revision,omitempty"`
	QueryFrom           *time.Time     `json:"query_from,omitempty"`
	QueryTo             *time.Time     `json:"query_to,omitempty"`
	ExcludedBoundary    uint32         `json:"excluded_boundary_events,omitempty"`
	Version             string         `json:"version"`
	DigestEncoding      string         `json:"member_digest_encoding,omitempty"`
	Capture             string         `json:"capture_id"`
	Kind                string         `json:"kind"`
	ScanID              string         `json:"scan_id"`
	FingerprintIn       string         `json:"fingerprint_in"`
	FingerprintOut      string         `json:"fingerprint_out"`
	Requests            []Evidence     `json:"requests"`
	CohortID            string         `json:"cohort_id"`
	Selection           string         `json:"selection"`
	Rotation            uint32         `json:"rotation"`
	Cohort              []CohortMember `json:"cohort"`
	Changes             []CohortChange `json:"cohort_changes,omitempty"`
	ProbeLifecycle      string         `json:"probe_lifecycle,omitempty"`
}

func Snapshot(c Candidate) CohortMember {
	return CohortMember{Hex(c.Renter), Hex(c.Receiver), c.Stratum, c.Lifecycle, c.Origin.Transaction, c.Origin.Index, c.LastActivity, c.Verified, c.Closed, c.Ambiguous, c.SeedRevision, c.SeedAttempts, c.SeedNext}
}
