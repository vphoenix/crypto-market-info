package justlendkeeper

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	runtimeinputtest "github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint/testutil"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint"
)

func keeperRuntimeBinding(c *Collector) runtimecheckpoint.Binding {
	return runtimecheckpoint.Binding{Service: "keeper", Database: "test_keeper", ServerUUID: "server", ScopeHash: Hex(c.Config.Hash()), SchemaHash: "schema", Producer: "keeper", Holder: "holder", RecoveryEpoch: "restore-1", ProducerGeneration: 1, StateVersion: 1}
}
func keeperRuntimeFixture(t *testing.T, c *Collector) (string, runtimecheckpoint.Binding) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "runtime")
	binding := keeperRuntimeBinding(c)
	files, err := runtimecheckpoint.Create(dir, binding, []byte("preparing"), nil, maxCheckpointBytes)
	if err != nil {
		t.Fatal(err)
	}
	work := &runtimeWork{Version: 1, Binding: binding, Verified: true}
	if err = work.open(files); err != nil {
		t.Fatal(err)
	}
	c.State.Runtime = work
	c.runtime = &runtimeOwner{files: files, metricsAt: c.Now()}
	if err = c.Save(); err != nil {
		t.Fatal(err)
	}
	if err = files.SealConversion(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if c.runtime != nil {
			c.runtime.files.Close()
		}
	})
	return dir, binding
}
func keeperRuntimePage(c *Collector, from, to time.Time, in, out, mode string) Batch {
	o := c.NewOperation("index_page", mode, "trongrid")
	o.Batch.Capture.CaptureKind = "event_index"
	o.Batch.Capture.CoverageScope = "indexed_energy_events"
	o.Batch.Capture.EventKind = "RentResource"
	o.Batch.Capture.RequestedFrom = Ptr(from)
	o.Batch.Capture.RequestedTo = Ptr(to)
	o.Batch.Capture.PageCount = 1
	o.Batch.Capture.AvailableAt = c.Now()
	o.Batch.Capture.EvidenceManifestHash = Hash([]byte("summary"))
	o.Batch.Capture.PaginationExhausted = out == ""
	p := IndexPage{o.Batch.Capture.CaptureId, o.Batch.Capture.CaptureStartedAt, "watch:v2:test", in, out, c.Now(), c.Now(), Hash([]byte("source"))}
	o.Batch.IndexPage = &p
	Seal(&o.Batch)
	return o.Batch
}
func keeperRuntimeChild(c *Collector, parent Capture, status string) Capture {
	o := c.NewOperation("enrichment", parent.CaptureMode, "publicnode")
	o.Batch.Capture.ParentCaptureId = Ptr(parent.CaptureId)
	o.Batch.Capture.Status = status
	o.Batch.Capture.AvailableAt = c.Now()
	o.Batch.Capture.EventKind = parent.EventKind
	o.Batch.Capture.RequestedFrom = parent.RequestedFrom
	o.Batch.Capture.RequestedTo = parent.RequestedTo
	o.Batch.Capture.EvidenceManifestHash = Hash([]byte("child"))
	Seal(&o.Batch)
	return o.Batch.Capture
}
func TestRuntimeKeeperWholeChainAcrossPageBoundaryAndRestart(t *testing.T) {
	c, _, now := testCollector(t)
	dir, binding := keeperRuntimeFixture(t, c)
	from := now.Add(-time.Hour)
	to := from.Add(time.Minute)
	c.State.IndexingEnabled = true
	c.State.EvidenceCursors = map[string]time.Time{"RentResource": from}
	c.State.EventCursors["RentResource"] = to
	const n = 300
	parents := make([]Batch, n)
	for i := range parents {
		in, out := "", ""
		if i > 0 {
			in = fmt.Sprint(i)
		}
		if i < n-1 {
			out = fmt.Sprint(i + 1)
		}
		parents[i] = keeperRuntimePage(c, from, to, in, out, "live")
	}
	// Reverse arrival must connect only after the original first page exists.
	for i := n - 1; i >= 0; i-- {
		b := parents[i]
		if err := c.runtimeCommitted(b); err != nil {
			t.Fatal(err)
		}
		if i > 0 {
			if err := c.State.Runtime.child(keeperRuntimeChild(c, b.Capture, "complete")); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := c.advanceEvidence(context.Background(), "RentResource"); err != nil {
		t.Fatal(err)
	}
	if !c.State.EvidenceCursors["RentResource"].Equal(from) {
		t.Fatal("advanced over missing child")
	}
	if err := c.Save(); err != nil {
		t.Fatal(err)
	}
	c.runtime.files.Close()
	c.runtime = nil
	c.State.Runtime = nil
	if err := c.OpenRuntime(context.Background(), dir, binding); err != nil {
		t.Fatal(err)
	}
	if err := c.State.Runtime.child(keeperRuntimeChild(c, parents[0].Capture, "complete")); err != nil {
		t.Fatal(err)
	}
	if err := c.advanceEvidence(context.Background(), "RentResource"); err != nil {
		t.Fatal(err)
	}
	if !c.State.EvidenceCursors["RentResource"].Equal(to) || c.State.Runtime.parents.Len() != 0 || c.State.Runtime.pages.Len() != 0 || c.State.Runtime.windows.Len() != 0 {
		t.Fatal("completed window not retired", c.State.Runtime.summary())
	}
}
func TestRuntimeKeeperBrokenChainsMatchOriginal(t *testing.T) {
	for _, tc := range []struct {
		name      string
		tokens    [][2]string
		otherScan bool
	}{
		{"missing_middle", [][2]string{{"", "a"}, {"b", ""}}, false},
		{"duplicate_input", [][2]string{{"", "a"}, {"", "a"}, {"a", ""}}, false},
		{"cycle", [][2]string{{"", "a"}, {"a", "b"}, {"b", "a"}}, false},
		{"disconnected_after_empty", [][2]string{{"", ""}, {"a", ""}}, false},
		{"multiple_scans", [][2]string{{"", "a"}, {"a", ""}}, true},
		{"complete_empty_page", [][2]string{{"", ""}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, _, now := testCollector(t)
			keeperRuntimeFixture(t, c)
			from := now.Add(-time.Hour)
			to := from.Add(time.Minute)
			var caps []Capture
			pages := map[uuid.UUID]IndexPage{}
			for i, tokens := range tc.tokens {
				b := keeperRuntimePage(c, from, to, tokens[0], tokens[1], "live")
				if tc.otherScan && i > 0 {
					b.IndexPage.ScanID = "other"
				}
				caps = append(caps, b.Capture)
				pages[b.Capture.CaptureId] = *b.IndexPage
				if err := c.State.Runtime.parent(b.Capture, b.IndexPage, map[string]time.Time{"RentResource": from}); err != nil {
					t.Fatal(err)
				}
				if err := c.State.Runtime.child(keeperRuntimeChild(c, b.Capture, "complete")); err != nil {
					t.Fatal(err)
				}
			}
			want, err := indexWindowCompletePages(caps, pages)
			if err != nil {
				t.Fatal(err)
			}
			got, err := c.State.Runtime.advance("RentResource", from, to)
			if err != nil || got.Equal(to) != want {
				t.Fatal("strict chain changed", got, want, err)
			}
		})
	}
}
func TestRuntimeKeeperPriorityCooldownAndThreeFailures(t *testing.T) {
	c, _, now := testCollector(t)
	keeperRuntimeFixture(t, c)
	from := now.Add(-time.Hour)
	to := from.Add(time.Minute)
	var parents []Capture
	for _, mode := range []string{"bootstrap", "history", "live", "catchup"} {
		b := keeperRuntimePage(c, from, to, "", "", mode)
		parents = append(parents, b.Capture)
		if err := c.State.Runtime.parent(b.Capture, b.IndexPage, nil); err != nil {
			t.Fatal(err)
		}
	}
	if c.State.Runtime.parents.Len() != 3 {
		t.Fatal("bootstrap must not be an enrichment parent")
	}
	first, err := c.State.Runtime.next(*now)
	if err != nil || first == nil || first.CaptureMode == "history" {
		t.Fatal(first, err)
	}
	for i := 0; i < 3; i++ {
		child := keeperRuntimeChild(c, *first, "partial")
		if err = c.State.Runtime.child(child); err != nil {
			t.Fatal(err)
		}
		again, e := c.State.Runtime.next(*now)
		if e != nil || again != nil && again.CaptureId == first.CaptureId {
			t.Fatal("cooldown lost", e)
		}
		*now = now.Add(30 * time.Minute)
	}
	p, ok, err := c.State.Runtime.parents.Get(first.CaptureId.String())
	if err != nil || !ok || p.Complete || p.Attempts != 3 {
		t.Fatal("failed attempts became evidence", p, err)
	}
	at, err := c.State.Runtime.advance("RentResource", from, to)
	if err != nil || !at.Equal(from) {
		t.Fatal("advanced over failed enrichment", err)
	}
	_ = parents
}

type runtimeKeeperStore struct {
	rows                   map[uuid.UUID]Batch
	writes, history, pages int
	lose                   bool
	failPage               int
}

func (s *runtimeKeeperStore) WriteKeeperBatch(_ context.Context, b Batch) error {
	if err := Validate(b); err != nil {
		return err
	}
	s.writes++
	if old, ok := s.rows[b.Capture.CaptureId]; ok && !captureEqual(old.Capture, b.Capture) {
		return runtimecheckpoint.ErrConflict
	}
	s.rows[b.Capture.CaptureId] = b
	if s.lose {
		s.lose = false
		return errors.New("lost COMMIT response")
	}
	return nil
}
func (s *runtimeKeeperStore) KeeperNextEnrichment(context.Context, string, time.Time) (*Capture, error) {
	s.history++
	return nil, errors.New("unexpected historical state query")
}
func (s *runtimeKeeperStore) KeeperEvidenceFrontier(_ context.Context, _ string, _ string, from, to time.Time, _ func([]Capture) (bool, error)) (time.Time, error) {
	s.history++
	return from, errors.New("unexpected historical frontier")
}
func (s *runtimeKeeperStore) KeeperBatch(_ context.Context, cap Capture) (Batch, error) {
	b, ok := s.rows[cap.CaptureId]
	if !ok {
		return b, errors.New("missing")
	}
	return b, Validate(b)
}
func (s *runtimeKeeperStore) KeeperIndexPages(_ context.Context, caps []Capture) (map[uuid.UUID]IndexPage, error) {
	out := map[uuid.UUID]IndexPage{}
	for _, cap := range caps {
		b := s.rows[cap.CaptureId]
		if b.IndexPage != nil {
			out[cap.CaptureId] = *b.IndexPage
		}
	}
	return out, nil
}
func (s *runtimeKeeperStore) KeeperCapturePage(_ context.Context, config string, after []byte, limit int) ([]Capture, []byte, error) {
	s.pages++
	if s.failPage == s.pages {
		return nil, nil, errors.New("interrupted conversion")
	}
	var rows []Capture
	for id, b := range s.rows {
		if b.Capture.ConfigHash == config && b.Capture.Committed && bytes.Compare(id[:], after) > 0 {
			rows = append(rows, b.Capture)
		}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].CaptureId.String() < rows[j].CaptureId.String() })
	rows = rows[:min(limit, len(rows))]
	if len(rows) == 0 {
		return nil, nil, nil
	}
	id := rows[len(rows)-1].CaptureId
	return rows, append([]byte(nil), id[:]...), nil
}
func TestRuntimeKeeperLostCommitRecoversOriginalOpsExactlyOnce(t *testing.T) {
	for _, applyBeforeCrash := range []bool{false, true} {
		t.Run(fmt.Sprint(applyBeforeCrash), func(t *testing.T) {
			c, _, now := testCollector(t)
			dir, binding := keeperRuntimeFixture(t, c)
			store := &runtimeKeeperStore{rows: map[uuid.UUID]Batch{}, lose: !applyBeforeCrash}
			c.Store = store
			from := now.Add(-time.Hour)
			to := from.Add(time.Minute)
			batch := keeperRuntimePage(c, from, to, "", "next", "live")
			c.State.Scans = []Scan{{ID: "watch:v2:test", Kind: "RentResource", Mode: "live", From: from, To: to}}
			c.State.IndexingEnabled = true
			c.State.EvidenceCursors = map[string]time.Time{"RentResource": from}
			c.State.EventCursors["RentResource"] = from
			c.State.Ops = []Operation{{Kind: "index_page", ScanID: "watch:v2:test", Batch: batch, NextFingerprint: "next", Frozen: true}}
			if err := c.Save(); err != nil {
				t.Fatal(err)
			}
			err := c.Finish(context.Background(), &c.State.Ops[0])
			if (err == nil) != applyBeforeCrash {
				t.Fatal(err)
			}
			if !applyBeforeCrash && (c.State.Scans[0].Pages != 0 || c.State.Runtime.parents.Len() != 0) {
				t.Fatal("unknown commit advanced state")
			}
			records := c.runtime.files.Retained()
			if len(records) != 1 {
				t.Fatal(records)
			}
			info, e := os.Stat(filepath.Join(dir, "objects", records[0].Payload.Hash+".blob"))
			if e != nil {
				t.Fatal(e)
			}
			current, e := os.Stat(filepath.Join(dir, "checkpoint.current"))
			if e != nil || !os.SameFile(info, current) {
				t.Fatal("Ops serialized twice", e)
			}
			c.runtime.files.Close()
			c.runtime = nil
			c.State.Runtime = nil
			if err = c.OpenRuntime(context.Background(), dir, binding); err != nil {
				t.Fatal(err)
			}
			if len(c.State.Ops) != 0 || c.State.Scans[0].Pages != 1 || c.State.Runtime.parents.Len() != 1 || len(store.rows) != 1 || store.writes != 2 || store.history != 0 {
				t.Fatal("recovery not exactly once", len(c.State.Ops), c.State.Scans, store.writes, store.history)
			}
			// Reload a completed checkpoint without any capture-history access or SQL retry.
			if err = c.CloseRuntime(); err != nil {
				t.Fatal(err)
			}
			if err = c.OpenRuntime(context.Background(), dir, binding); err != nil {
				t.Fatal(err)
			}
			if store.writes != 2 || store.history != 0 {
				t.Fatal("normal restart rebuilt history")
			}
		})
	}
}
func TestRuntimeKeeperConversionResumeFrozenInputAndIdle(t *testing.T) {
	c, _, now := testCollector(t)
	store := &runtimeKeeperStore{rows: map[uuid.UUID]Batch{}, failPage: 2}
	c.Store = store
	from := now.Add(-time.Hour)
	to := from.Add(time.Minute)
	c.State.IndexingEnabled = true
	c.State.EvidenceCursors = map[string]time.Time{"RentResource": from}
	c.State.EventCursors["RentResource"] = to
	c.State.Used = 99
	c.State.Sources["trongrid"] = SourceLimit{Blocked: true, Cooldown: now.Add(time.Hour)}
	for i := 0; i < 502; i++ {
		b := keeperRuntimePage(c, from, to, "", "", "history")
		child := keeperRuntimeChild(c, b.Capture, "complete")
		cb := Batch{Capture: child}
		Seal(&cb)
		store.rows[b.Capture.CaptureId] = b
		store.rows[child.CaptureId] = cb
	}
	if err := c.Save(); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(t.TempDir(), "runtime")
	binding := keeperRuntimeBinding(c)
	check := func(context.Context) (runtimecheckpoint.Binding, error) { return binding, nil }
	if _, err := c.ConvertRuntime(context.Background(), dir, binding, runtimeinputtest.Inventory(t, binding, "keeper"), check, nil); err == nil {
		t.Fatal("expected interruption")
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatal("partial conversion published", err)
	}
	store.failPage = 0
	report, err := c.ConvertRuntime(context.Background(), dir, binding, runtimeinputtest.Inventory(t, binding, "keeper"), check, nil)
	if err != nil || !report.Verified || report.Headers != 1004 {
		t.Fatal(report, err)
	}
	pages := store.pages
	if err = c.OpenRuntime(context.Background(), dir, binding); err != nil {
		t.Fatal(err)
	}
	defer c.CloseRuntime()
	if c.State.Runtime.parents.Len() != 0 || c.State.Used != 99 || !c.State.Sources["trongrid"].Blocked {
		t.Fatal("state conversion changed original budgets or retained completed history")
	}
	for i := 0; i < 3; i++ {
		if err = c.restoreEnrichment(context.Background()); err != nil {
			t.Fatal(err)
		}
		*now = now.Add(5 * time.Second)
	}
	if store.history != 0 || store.pages != pages || store.writes != 0 {
		t.Fatal("idle queried historical progress")
	}
}

func TestRuntimeKeeperKnownZeroPendingAndCurrentRollback(t *testing.T) {
	c, _, now := testCollector(t)
	dir, binding := keeperRuntimeFixture(t, c)
	store := &runtimeKeeperStore{rows: map[uuid.UUID]Batch{}}
	c.Store = store
	// An unfinished request retains known-zero presence in the extended State/Ops
	// checkpoint. It cannot be handed to an old gob reader by losing that fact.
	op := c.NewOperation("costs", "live", "publicnode")
	op.Batch.Capture.SolidHeight = Ptr(uint64(0))
	op.Requests = []Request{{Source: "publicnode", Path: "/pending", Role: "head"}}
	c.State.Ops = []Operation{op}
	c.State.Used = 81
	c.State.Sources["publicnode"] = SourceLimit{Blocked: true, Cooldown: now.Add(time.Hour)}
	if err := c.Save(); err != nil {
		t.Fatal(err)
	}
	c.runtime.files.Close()
	c.runtime = nil
	c.State.Runtime = nil
	if err := c.OpenRuntime(context.Background(), dir, binding); err != nil {
		t.Fatal(err)
	}
	if len(c.State.Ops) != 1 || c.State.Ops[0].Batch.Capture.SolidHeight == nil || *c.State.Ops[0].Batch.Capture.SolidHeight != 0 || store.writes != 0 {
		t.Fatal("known zero changed during native recovery")
	}
	before, err := os.ReadFile(c.Path)
	if !os.IsNotExist(err) && err != nil {
		t.Fatal(err)
	}
	if _, err = c.RollbackRuntime(context.Background(), filepath.Join(t.TempDir(), "lossy-export")); err == nil {
		t.Fatal("lossy old encoding accepted")
	}
	after, err := os.ReadFile(c.Path)
	if !os.IsNotExist(err) && err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("failed rollback changed legacy state")
	}
	c.State.Ops = nil
	c.State.Used = 92
	if _, err = c.RollbackRuntime(context.Background(), filepath.Join(t.TempDir(), "current-export")); err != nil {
		t.Fatal(err)
	}
	old, err := LoadState(c.Path, c.State.Target, c.Config)
	if err != nil || old.Used != 92 || !old.Sources["publicnode"].Blocked || !old.Sources["publicnode"].Cooldown.Equal(now.Add(time.Hour)) || old.Runtime != nil {
		t.Fatal("rollback did not preserve current state", err)
	}
}
