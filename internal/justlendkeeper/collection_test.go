package justlendkeeper

import (
	"context"
	"encoding/json"
	"github.com/google/uuid"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

type collectionTestStore struct {
	memoryStore
	parent Batch
}

type completeBacklogStore struct {
	collectionTestStore
	calls int
}

func (s *completeBacklogStore) KeeperNextEnrichment(context.Context, string, time.Time) (*Capture, error) {
	return nil, nil
}
func (s *completeBacklogStore) KeeperEvidenceFrontier(_ context.Context, _ string, _ string, from, to time.Time, _ func([]Capture) (bool, error)) (time.Time, error) {
	if from.IsZero() || !from.Before(to) {
		return from, nil
	}
	s.calls++
	at := from.Add(time.Second)
	if at.After(to) {
		at = to
	}
	return at, nil
}
func TestCompletedBacklogAdvancesWithoutNewChild(t *testing.T) {
	c, _, now := testCollector(t)
	base := now.Add(-time.Hour)
	c.State.EventCursors["RentResource"] = base
	if err := c.enableIndexing(); err != nil {
		t.Fatal(err)
	}
	c.State.EventCursors["RentResource"] = base.Add(273 * time.Second)
	store := &completeBacklogStore{}
	c.Store = store
	for i := 0; i < 10; i++ {
		if err := c.restoreEnrichment(context.Background()); err != nil {
			t.Fatal(err)
		}
		*now = now.Add(5 * time.Second)
	}
	if !c.State.EvidenceCursors["RentResource"].Equal(c.State.EventCursors["RentResource"]) || store.calls != 273 {
		t.Fatal("completed backlog became stranded without new children", store.calls)
	}
}

func (s *collectionTestStore) KeeperNextEnrichment(context.Context, string, time.Time) (*Capture, error) {
	return &s.parent.Capture, nil
}
func (s *collectionTestStore) KeeperBatch(context.Context, Capture) (Batch, error) {
	return s.parent, Validate(s.parent)
}
func (s *collectionTestStore) KeeperIndexPages(_ context.Context, caps []Capture) (map[uuid.UUID]IndexPage, error) {
	out := map[uuid.UUID]IndexPage{}
	if s.parent.IndexPage != nil {
		out[s.parent.Capture.CaptureId] = *s.parent.IndexPage
	}
	return out, nil
}

type pageTestStore struct {
	memoryStore
	pages map[uuid.UUID]IndexPage
}

func (s *pageTestStore) KeeperIndexPages(context.Context, []Capture) (map[uuid.UUID]IndexPage, error) {
	return s.pages, nil
}

func (s *collectionTestStore) KeeperEvidenceFrontier(_ context.Context, _ string, _ string, from, to time.Time, check func([]Capture) (bool, error)) (time.Time, error) {
	ok, err := check([]Capture{s.parent.Capture})
	if ok {
		return to, err
	}
	return from, err
}

func TestBackgroundRestoresOneChildAndAuthenticatesParent(t *testing.T) {
	c, _, now := testCollector(t)
	s, body, ev := collectionPage(t, c)
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
	store := &collectionTestStore{parent: o.Batch}
	c.Store = store
	c.State.Ops = nil
	if err := c.restoreEnrichment(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := c.restoreEnrichment(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(c.State.Ops) != 1 || c.State.Ops[0].Parent == nil || c.State.Ops[0].Batch.Capture.ParentCaptureId == nil || len(c.State.Ops[0].Requests) != 0 || c.State.Ops[0].Batch.Capture.CoverageScope != "sampled_rental_events" {
		t.Fatal("unbounded child queue or fabricated full rental coverage")
	}
	if err := c.Finish(context.Background(), &c.State.Ops[0]); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(c.API.Archive.Dir); err != nil {
		t.Fatal(err)
	}
	c.State.Ops = nil
	*now = now.Add(6 * time.Second)
	if err := c.restoreEnrichment(context.Background()); err != nil {
		t.Fatal("background depends on archive", err)
	}
	c.State.Ops = nil
	*now = now.Add(6 * time.Second)
	store.parent.IndexedEvents[0].AmountSun = big.NewInt(42)
	if err := c.restoreEnrichment(context.Background()); err == nil {
		t.Fatal("mutated database members accepted")
	}

}

func TestDataExportExactFieldsNoProfitAndRefusesOverwrite(t *testing.T) {
	c, _, now := testCollector(t)
	s, body, ev := collectionPage(t, c)
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
	out := filepath.Join(t.TempDir(), "export")
	retracted := o.Batch
	retracted.Capture.CaptureId = uuid.New()
	retracted.Capture.Committed = false
	retracted.Capture.Reason = "legacy_retracted"
	if err := Export(context.Background(), testReportReader{batches: []Batch{o.Batch, retracted}}, c.API.Archive, c.Config, now.Add(-time.Hour), now.Add(time.Hour), out); err != nil {
		t.Fatal(err)
	}
	audit, _ := os.ReadFile(filepath.Join(out, "captures.csv"))
	if !strings.Contains(string(audit), "legacy_retracted") {
		t.Fatal("retracted audit capture disappeared")
	}
	raw, err := os.ReadFile(filepath.Join(out, "indexed_events.csv"))
	if err != nil || !strings.Contains(string(raw), "provider_claimed_confirmed") || !strings.Contains(string(raw), Hex(o.Batch.IndexedEvents[0].TxId)) {
		t.Fatal("typed export lost status or binary identity", err)
	}
	if _, err = os.Stat(filepath.Join(out, "resource_break_even.csv")); !os.IsNotExist(err) {
		t.Fatal("data export contains analysis")
	}
	if err = Export(context.Background(), testReportReader{}, c.API.Archive, c.Config, now.Add(-time.Hour), *now, out); err == nil {
		t.Fatal("existing export overwritten")
	}
	max := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))
	if exportValue(reflect.ValueOf(max), "amount_sun") != max.String() || exportValue(reflect.ValueOf((*big.Int)(nil)), "amount_sun") != "unknown" {
		t.Fatal("export changed large integers or unknown costs")
	}
}

func collectionPage(t *testing.T, c *Collector) (Scan, []byte, Evidence) {
	t.Helper()
	h, _ := BinaryHex("57f4f50ce618dde5c4bf1485d3205f9b0757a752159dbd111460caf90bafd827", 32)
	body, err := (Archive{Dir: "../../research/2026-10-03-keeper-repair/fixtures"}).Get(h)
	if err != nil {
		t.Fatal(err)
	}
	from, _ := time.Parse(time.RFC3339Nano, "2026-10-02T20:54:03Z")
	s := Scan{ID: "watch:v2:RentResource:" + from.Format(time.RFC3339Nano), Kind: "RentResource", Mode: "catchup", From: from, To: from.Add(30 * time.Second)}
	h, err = c.API.Archive.Put(body)
	if err != nil {
		t.Fatal(err)
	}
	ev := Evidence{Source: "trongrid", Path: EventPath(s.Kind, s.From, s.To, ""), Started: c.Now(), Available: c.Now(), ResponseHash: Hex(h), Status: 200}
	return s, body, ev
}

func TestSourcePageCommitsAllRowsBeforeProofAndRetriesFrozen(t *testing.T) {
	c, store, _ := testCollector(t)
	s, body, ev := collectionPage(t, c)
	c.State.Scans = []Scan{s}
	c.State.EventCursors[s.Kind] = s.From
	if err := c.enableIndexing(); err != nil {
		t.Fatal(err)
	}
	c.AddPage(s)
	o := &c.State.Ops[0]
	if len(o.Requests) != 1 || o.Requests[0].Source != "trongrid" || o.Requests[0].Background {
		t.Fatal("source fetching depends on a proof request")
	}
	c.State.Sources["publicnode"] = SourceLimit{Blocked: true}
	r := o.Requests[0]
	o.Requests = nil
	o.Evidence = []Evidence{ev}
	if err := c.Process(o, r, ev, body); err != nil {
		t.Fatal(err)
	}
	if len(o.Requests) != 0 || len(o.Batch.IndexedEvents) != 9 || len(o.Batch.Events) != 0 || len(c.State.Cohort) != 0 {
		t.Fatal("source rows were restricted to cohort or proofs")
	}
	store.Fail = true
	if err := c.Finish(context.Background(), o); err == nil || !o.Frozen || !c.State.EventCursors[s.Kind].Equal(s.From) {
		t.Fatal("failed write advanced source cursor", err)
	}
	frozen, _ := Freeze(o.Batch)
	restored, err := LoadState(c.Path, "test", c.Config)
	if err != nil {
		t.Fatal(err)
	}
	c.State = &restored
	store.Fail = false
	o = &c.State.Ops[0]
	if err = c.Finish(context.Background(), o); err != nil {
		t.Fatal(err)
	}
	after, _ := Freeze(o.Batch)
	if string(frozen) != string(after) || !c.State.EventCursors[s.Kind].Equal(s.To) || !c.State.EvidenceCursors[s.Kind].Equal(s.From) {
		t.Fatal("retry mutated identity or conflated source/proof progress")
	}
	for _, row := range o.Batch.IndexedEvents {
		if row.BlockHash != nil || row.Finality != "provider_claimed_confirmed" || row.PositionStatus != "indexed_only" {
			t.Fatal("index observation fabricated verified finality")
		}
	}
}

func TestIndexMissingIndexAndRepeatedProviderPosition(t *testing.T) {
	c, _, _ := testCollector(t)
	s, body, ev := collectionPage(t, c)
	var page Page
	if err := Decode(body, &page); err != nil {
		t.Fatal(err)
	}
	page.Data = append(page.Data, page.Data[0])
	body = JSONEvidence(page)
	h, _ := c.API.Archive.Put(body)
	ev.ResponseHash = Hex(h)
	p, next, err := ParsePage(body, ev, c.Config, s)
	if err != nil {
		t.Fatal(err)
	}
	c.AddPage(s)
	o := &c.State.Ops[0]
	if err = c.indexRows(o, p, next); err != nil {
		t.Fatal(err)
	}
	Seal(&o.Batch)
	o.Batch.Capture.EvidenceManifestHash = Hash([]byte("manifest"))
	if err = Validate(o.Batch); err != nil || len(o.Batch.IndexedEvents) != 10 || o.Batch.IndexedEvents[0].Ordinal == o.Batch.IndexedEvents[9].Ordinal {
		t.Fatal("repeated provider position collapsed", err)
	}
	var bad map[string]json.RawMessage
	json.Unmarshal(body, &bad)
	var rows []map[string]json.RawMessage
	json.Unmarshal(bad["data"], &rows)
	for _, null := range []bool{false, true} {
		delete(rows[0], "event_index")
		if null {
			rows[0]["event_index"] = json.RawMessage("null")
		}
		bad["data"], _ = json.Marshal(rows)
		raw, _ := json.Marshal(bad)
		if _, _, err = ParsePage(raw, ev, c.Config, s); err == nil {
			t.Fatal("absent source index became index zero")
		}
	}
}

func TestIndexWindowRequiresWholeTokenChain(t *testing.T) {
	c, _, _ := testCollector(t)
	store := &pageTestStore{pages: map[uuid.UUID]IndexPage{}}
	c.Store = store
	page := func(in, out string) Capture {
		o := c.NewOperation("index_page", "live", "trongrid")
		o.Batch.Capture.CaptureKind = "event_index"
		o.Batch.Capture.PaginationExhausted = out == ""
		o.Batch.Capture.AvailableAt = c.Now()
		o.Batch.Capture.EvidenceManifestHash = Hash([]byte("summary"))
		Seal(&o.Batch)
		store.pages[o.Batch.Capture.CaptureId] = IndexPage{o.Batch.Capture.CaptureId, o.Batch.Capture.CaptureStartedAt, "watch:v2:test", in, out, c.Now(), c.Now(), Hash([]byte("source"))}
		return o.Batch.Capture
	}
	first, middle, last := page("", "a"), page("a", "b"), page("b", "")
	for _, tc := range []struct {
		caps []Capture
		want bool
	}{{[]Capture{first, last}, false}, {[]Capture{last}, false}, {[]Capture{first, middle, last}, true}, {[]Capture{first, first, middle, last}, false}} {
		ok, err := c.indexWindowComplete(context.Background(), tc.caps)
		if err != nil || ok != tc.want {
			t.Fatal("incorrect pagination certification", err, ok)
		}
	}
}

func TestLegacyFrozenAndUnfrozenPrefixMigration(t *testing.T) {
	for _, next := range []string{"", "next"} {
		t.Run("frozen_"+next, func(t *testing.T) {
			c, _, now := testCollector(t)
			from := now.Add(-4 * time.Minute)
			s := Scan{ID: "watch:v2:RentResource:test", Kind: "RentResource", Mode: "live", From: from, To: from.Add(30 * time.Second)}
			c.State.Scans = []Scan{s}
			c.State.EventCursors[s.Kind] = from
			o := c.NewOperation("page", "live", "trongrid")
			o.ScanID = s.ID
			o.NextFingerprint = next
			o.Batch.Capture.CaptureKind = "events"
			o.Batch.Capture.EvidenceManifestHash = Hash([]byte("legacy"))
			o.Batch.Capture.PaginationExhausted = next == ""
			Seal(&o.Batch)
			o.Frozen = true
			c.State.Ops = []Operation{o}
			before, _ := Freeze(o)
			if err := c.enableIndexing(); err != nil {
				t.Fatal(err)
			}
			after, _ := Freeze(c.State.Ops[0])
			if string(before) != string(after) {
				t.Fatal("legacy frozen attempt mutated")
			}
			if err := c.Finish(context.Background(), &c.State.Ops[0]); err != nil {
				t.Fatal(err)
			}
			if next == "" {
				if !c.State.EvidenceCursors[s.Kind].Equal(s.To) {
					t.Fatal("verified legacy last page left progress gap")
				}
			} else {
				if c.State.Scans[0].Fingerprint != "" || c.State.Scans[0].Pages != 0 || c.State.Scans[0].Done || !c.State.EvidenceCursors[s.Kind].Equal(from) {
					t.Fatal("legacy prefix not reindexed from first page")
				}
			}
		})
	}
	c, _, _ := testCollector(t)
	s, _, ev := collectionPage(t, c)
	s.Pages = 1
	s.Fingerprint = "next"
	c.State.Scans = []Scan{s}
	c.State.EventCursors[s.Kind] = s.From
	o := c.NewOperation("page", "live", "trongrid")
	o.ScanID = s.ID
	o.Evidence = []Evidence{ev}
	c.State.Ops = []Operation{o}
	if err := c.enableIndexing(); err != nil {
		t.Fatal(err)
	}
	if c.State.Ops[0].Batch.Capture.Reason != "legacy_prefix_reindexed" {
		t.Fatal("unfrozen legacy prefix was certified as whole index window")
	}
	if err := c.Finish(context.Background(), &c.State.Ops[0]); err != nil {
		t.Fatal(err)
	}
	if c.State.Scans[0].Fingerprint != "" || c.State.Scans[0].Done {
		t.Fatal("reindex scan did not reopen at first page")
	}
}

func TestGenuineOldFrozenGobAndBootstrapBaseline(t *testing.T) {
	raw, err := os.ReadFile("../../research/2026-10-03-keeper-collection-split/fixtures/legacy-frozen-state.gob")
	if err != nil {
		t.Fatal(err)
	}
	var old State
	if err = Thaw(raw, &old); err != nil {
		t.Fatal(err)
	}
	if len(old.Ops) != 1 || !old.Ops[0].Frozen || old.Ops[0].Batch.Capture.IndexedDigest != nil || old.Ops[0].Batch.Capture.ParentCaptureId != nil {
		t.Fatal("old gob defaults changed")
	}
	if err = Validate(old.Ops[0].Batch); err != nil {
		t.Fatal(err)
	}
	c, _, _ := testCollector(t)
	c.State = &old
	before, _ := Freeze(old.Ops[0])
	if err = c.enableIndexing(); err != nil {
		t.Fatal(err)
	}
	after, _ := Freeze(old.Ops[0])
	if string(before) != string(after) {
		t.Fatal("old frozen content changed")
	}
	c, _, now := testCollector(t)
	if err = c.enableIndexing(); err != nil {
		t.Fatal(err)
	}
	c.SeedBootstrap()
	if !c.State.EvidenceCursors["RentResource"].Equal(now.Add(-120 * time.Second)) {
		t.Fatal("initial increment baseline absent")
	}
	c.State.EvidenceCursors["RentResource"] = now.Add(-10 * time.Minute)
	c.State.EventCursors["RentResource"] = now.Add(-120 * time.Second)
	if c.lifecycleCurrent(*now) {
		t.Fatal("source progress falsely certified cohort lifecycle")
	}
	c, _, now = testCollector(t)
	from := now.Add(-time.Hour)
	c.State.Scans = []Scan{{ID: "watch:v2:RentResource:between-pages", Mode: "live", Kind: "RentResource", From: from, To: from.Add(30 * time.Second), Pages: 2, Fingerprint: "old-next"}}
	c.State.EventCursors["RentResource"] = from
	if err = c.enableIndexing(); err != nil {
		t.Fatal(err)
	}
	if c.State.Scans[0].Pages != 0 || c.State.Scans[0].Fingerprint != "" || !c.State.EvidenceCursors["RentResource"].Equal(from) {
		t.Fatal("between-page legacy restart retained partial source prefix")
	}
}

func TestEnrichmentAvailabilityDoesNotPrecedeProofCompletion(t *testing.T) {
	c, _, now := testCollector(t)
	sourceTime := now.Add(-time.Hour)
	var p Page
	body := fixture(t, "tron_energy_7d.raw.json")
	if err := Decode(body, &p); err != nil {
		t.Fatal(err)
	}
	re := p.Data[0]
	re.Evidence = Evidence{Started: sourceTime, Available: sourceTime, ResponseHash: Hex(Hash(body))}
	rawReceipt := fixture(t, "tron_receipt_0.raw.json")
	rr, logs, err := ParseReceipt(rawReceipt, Evidence{Started: sourceTime.Add(30 * time.Minute), Available: sourceTime.Add(30 * time.Minute), ResponseHash: Hex(Hash(rawReceipt))})
	if err != nil {
		t.Fatal(err)
	}
	o := c.NewOperation("enrichment", "live", "publicnode")
	o.Batch.Capture.ParentCaptureId = Ptr(uuid.New())
	rr.CaptureId = o.Batch.Capture.CaptureId
	rr.CaptureStartedAt = o.Batch.Capture.CaptureStartedAt
	o.RawEvents = []RawEvent{re}
	o.Headers = []Header{{Number: rr.BlockNumber, Hash: Hash([]byte("block")), Time: rr.BlockTime, Transactions: []string{rr.TxId}}}
	o.Receipts = []ReceiptLogs{{rr, logs}}
	if err = c.Finish(context.Background(), &o); err != nil {
		t.Fatal(err)
	}
	if len(o.Batch.Events) != 1 || len(o.Batch.Receipts) != 1 || !o.Batch.Events[0].AvailableAt.Equal(*now) || !o.Batch.Receipts[0].AvailableAt.Equal(*now) || !o.Batch.Events[0].RequestStartedAt.Equal(sourceTime) {
		t.Fatal("proof data acquired in the future was backdated to source page")
	}
	before, _ := Freeze(o.Batch)
	*now = now.Add(time.Hour)
	if err = c.Finish(context.Background(), &o); err != nil {
		t.Fatal(err)
	}
	after, _ := Freeze(o.Batch)
	if string(before) != string(after) {
		t.Fatal("frozen enrichment timestamps changed during database retry")
	}
}
