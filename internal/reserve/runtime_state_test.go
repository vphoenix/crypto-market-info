package reserve

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
	runtimeinputtest "github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint/testutil"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/dex"
	"github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint"
)

type runtimeTestDB struct {
	rows                   map[string]Batch
	history, writes, pages int
	lose                   bool
	failPage               int
}

func (d *runtimeTestDB) WriteReserveBatch(_ context.Context, b Batch) error {
	if err := Validate(b); err != nil {
		return err
	}
	d.writes++
	key := runtimeIdentity(b.Capture)
	if old, ok := d.rows[key]; ok {
		if runtimeFrozen(old.Capture) != runtimeFrozen(b.Capture) {
			return runtimecheckpoint.ErrConflict
		}
	} else {
		d.rows[key] = b
	}
	if d.lose {
		d.lose = false
		return context.DeadlineExceeded
	}
	return nil
}
func (d *runtimeTestDB) WriteReserveRevision(_ context.Context, c Capture) error {
	key := runtimeIdentity(c)
	b, ok := d.rows[key]
	if !ok {
		return errors.New("missing capture")
	}
	if c.Revision >= b.Capture.Revision {
		b.Capture = c
		d.rows[key] = b
	}
	return nil
}
func (d *runtimeTestDB) ReserveCaptures(_ context.Context, scope string) ([]Capture, error) {
	d.history++
	rows := []Capture{}
	for _, b := range d.rows {
		if b.Capture.ManifestHash == scope {
			rows = append(rows, b.Capture)
		}
	}
	sort.Slice(rows, func(i, j int) bool { return runtimeBefore(rows[i], rows[j]) })
	return rows, nil
}
func (d *runtimeTestDB) ReserveBatch(_ context.Context, c Capture) (Batch, error) {
	b, ok := d.rows[runtimeIdentity(c)]
	if !ok {
		return b, errors.New("missing batch")
	}
	return b, nil
}
func (d *runtimeTestDB) ReserveCapture(ctx context.Context, c Capture) (Capture, error) {
	b, err := d.ReserveBatch(ctx, c)
	return b.Capture, err
}
func (d *runtimeTestDB) ReserveReceipt(context.Context, dex.Anchor, dex.Hash) (dex.Receipt, bool, error) {
	return dex.Receipt{}, false, nil
}
func (d *runtimeTestDB) ReserveCapturePage(_ context.Context, scope string, after []byte, limit int) ([]Capture, []byte, error) {
	d.pages++
	if d.failPage == d.pages {
		return nil, nil, errors.New("interrupted page")
	}
	keys := []string{}
	for key, b := range d.rows {
		if b.Capture.ManifestHash == scope {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	start := 0
	if len(after) > 0 {
		start = int(binary.BigEndian.Uint64(after[56:]))
	}
	end := min(len(keys), start+limit)
	rows := []Capture{}
	for _, key := range keys[start:end] {
		rows = append(rows, d.rows[key].Capture)
	}
	if len(rows) == 0 {
		return rows, nil, nil
	}
	next := make([]byte, 64)
	binary.BigEndian.PutUint64(next[56:], uint64(end))
	return rows, next, nil
}
func runtimeBatch(scope dex.Hash, id string, from, to uint64, finality string) Batch {
	at := time.Date(2026, 10, 15, 0, 0, 0, 0, time.UTC)
	b := Batch{Capture: Capture{ChainId: 1, ManifestHash: Hash(scope), CaptureId: ID(id), BatchId: ID("attempt" + id), CaptureKind: "logs", CaptureMode: "live", FromBlock: from, ToBlock: to, FromHash: ID(fmt.Sprint(from)), ToHash: ID(fmt.Sprint(to)), FromTime: at, ToTime: at, ReceivedAt: at, Canonical: true, Finality: finality, LogCoverage: "complete", ReceiptCoverage: "complete", PlanHash: ID("plan"), PayloadHash: ID("payload")}}
	b.Seal()
	b.Capture.AvailableAt = at
	b.Capture.Revision = 1
	return b
}
func runtimeFixture(t *testing.T) (*runtimeOwner, *runtimeTestDB, string, runtimecheckpoint.Binding, Manifest) {
	t.Helper()
	m := Manifest{Hash: dex.Digest([]byte("runtime"))}
	binding := runtimecheckpoint.Binding{Service: "reserve", Database: "db", ServerUUID: "server", ScopeHash: RuntimeScopeHash(m, nil, 1), SchemaHash: "schema", Producer: "reserve", Holder: "holder", RecoveryEpoch: "epoch", ProducerGeneration: 1, StateVersion: 1}
	state := newRuntimeState(binding, runtimeScopeHashes(m, nil))
	state.Verified = true
	raw, err := runtimecheckpoint.MarshalTyped(state)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	files, err := runtimecheckpoint.Create(dir, binding, raw, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	db := &runtimeTestDB{rows: map[string]Batch{}}
	owner, err := newRuntimeOwner(context.Background(), files, binding, runtimeScopeHashes(m, nil), db, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		select {
		case <-owner.done:
		default:
			owner.close()
		}
	})
	return owner, db, dir, binding, m
}
func TestRuntimeReserveUnknownCommitAndPartialReceiptIdentity(t *testing.T) {
	o, db, dir, binding, m := runtimeFixture(t)
	b := runtimeBatch(m.Hash, "partial", 1, 10, "head")
	b.Capture.ReceiptCoverage = "partial"
	b.Capture.ExpectedReceipts = 1
	db.lose = true
	if err := o.write(context.Background(), db, runtimeWrite{Version: 1, Batch: &b}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	if err := o.close(); err != nil {
		t.Fatal(err)
	}
	files, err := runtimecheckpoint.Open(dir, binding, 0)
	if err != nil {
		t.Fatal(err)
	}
	next, err := newRuntimeOwner(context.Background(), files, binding, runtimeScopeHashes(m, nil), db, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer next.close()
	var pending Capture
	err = next.view(context.Background(), func(s *runtimeState) (any, error) {
		h, ok := s.receiptCandidate(Hash(m.Hash))
		if !ok {
			return nil, errors.New("lost pending receipt")
		}
		return h.Capture, s.err
	}, &pending)
	if err != nil || pending.BatchId != b.Capture.BatchId || pending.ReceiptCoverage != "partial" || db.writes != 2 || db.history != 0 {
		t.Fatal(pending, err, db.writes, db.history)
	}
	complete := b
	complete.Capture.BatchId = ID("complete-attempt")
	complete.Capture.AvailableAt = b.Capture.AvailableAt.Add(time.Second)
	complete.Capture.ReceiptCoverage = "complete"
	complete.Capture.ExpectedReceipts = 0
	if err = next.write(context.Background(), db, runtimeWrite{Version: 1, Batch: &complete}); err != nil {
		t.Fatal(err)
	}
	orphan := complete.Capture
	orphan.Revision++
	orphan.Canonical = false
	if err = next.write(context.Background(), db, runtimeWrite{Version: 1, Revision: &orphan, Expected: 1}); err != nil {
		t.Fatal(err)
	}
	err = next.view(context.Background(), func(s *runtimeState) (any, error) {
		h, ok := s.receiptCandidate(Hash(m.Hash))
		if !ok {
			return nil, errors.New("orphan lost prior attempt")
		}
		return h.Capture, s.err
	}, &pending)
	if err != nil || pending.BatchId != b.Capture.BatchId {
		t.Fatal("latest valid attempt semantics changed", err, pending)
	}
}
func TestRuntimeReserveFinalizedHistoryIsCompactedAndScopesRemainIndependent(t *testing.T) {
	o, _, _, _, m := runtimeFixture(t)
	other := dex.Digest([]byte("related"))
	var counts []uint64
	err := o.request(context.Background(), runtimeRequest{Change: true, Apply: func(o *runtimeOwner) (any, error) {
		s := &o.state
		s.Scopes[Hash(other)] = &runtimeScope{}
		for i := 0; i < 1000; i++ {
			if i == 25 {
				continue
			}
			b := runtimeBatch(m.Hash, fmt.Sprint(i), uint64(i*2+1), uint64(i*2+2), "finalized")
			if err := s.capture(b.Capture, runtimecheckpoint.Reference{}, false); err != nil {
				return nil, err
			}
		}
		related := runtimeBatch(other, "related", 9000, 9010, "finalized")
		if err := s.capture(related.Capture, runtimecheckpoint.Reference{}, false); err != nil {
			return nil, err
		}
		if s.Scopes[Hash(m.Hash)].Anchor.ToBlock != 2000 || s.Scopes[Hash(other)].Anchor.ToBlock != 9010 || s.cursor(Hash(m.Hash), 0, true) != 50 {
			return nil, errors.New("gap or scope was merged")
		}
		return []uint64{s.active.Len(), s.pending.Len(), s.final.Len()}, s.err
	}}, &counts)
	if err != nil || !reflect.DeepEqual(counts, []uint64{0, 0, 0}) {
		t.Fatal("historical tasks retained", counts, err)
	}
}
func TestRuntimeReserveConversionResumesCompletePagesAndMatchesLegacy(t *testing.T) {
	o, db, _, binding, m := runtimeFixture(t)
	o.close()
	for i := 0; i < 501; i++ {
		b := runtimeBatch(m.Hash, fmt.Sprint(i), uint64(i*2+1), uint64(i*2+2), "finalized")
		if i == 50 {
			b.Capture.ReceiptCoverage = "partial"
			b.Capture.ExpectedReceipts = 1
		}
		db.rows[runtimeIdentity(b.Capture)] = b
	}
	dir := t.TempDir() + "/runtime"
	c := Collector{Manifest: m, Store: db, StartBlock: 1}
	check := func(context.Context) (runtimecheckpoint.Binding, error) { return binding, nil }
	db.failPage = 2
	if _, err := c.ConvertRuntime(context.Background(), dir, binding, runtimeinputtest.Inventory(t, binding, "reserve"), check, nil); err == nil {
		t.Fatal("injected conversion interruption missed")
	}
	db.failPage = 0
	report, err := c.ConvertRuntime(context.Background(), dir, binding, runtimeinputtest.Inventory(t, binding, "reserve"), check, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !report.Verified || report.Headers != 501 || report.Pending != 1 {
		t.Fatal(report)
	}
	before := db.history
	if err = c.OpenRuntime(context.Background(), dir, binding, nil); err != nil {
		t.Fatal(err)
	}
	defer c.CloseRuntime()
	cap, ok, err := c.runtimeReceiptCandidate(context.Background())
	if err != nil || !ok || cap.FromBlock != 101 || db.history != before {
		t.Fatal(cap, ok, err, "normal startup read history")
	}
}
func TestRuntimeReserveStaleRevisionUsesActualStoredVersion(t *testing.T) {
	o, db, _, _, m := runtimeFixture(t)
	b := runtimeBatch(m.Hash, "stale", 1, 10, "head")
	ctx := context.Background()
	if err := o.write(ctx, db, runtimeWrite{Version: 1, Batch: &b}); err != nil {
		t.Fatal(err)
	}
	newer := b.Capture
	newer.Revision = 5
	newer.Finality = "finalized"
	db.WriteReserveRevision(ctx, newer)
	stale := b.Capture
	stale.Revision = 2
	if err := o.write(ctx, db, runtimeWrite{Version: 1, Revision: &stale, Expected: 1}); err != nil {
		t.Fatal(err)
	}
	var anchor Capture
	if err := o.view(ctx, func(s *runtimeState) (any, error) { return *s.Scopes[Hash(m.Hash)].Anchor, s.err }, &anchor); err != nil || anchor.Revision != 5 || anchor.Finality != "finalized" {
		t.Fatal(anchor, err)
	}
}

func TestRuntimeReserveWatchUsesOwnedStateWithoutHistory(t *testing.T) {
	o, db, _, _, m := runtimeFixture(t)
	r := testReader(t, func(method string, params []json.RawMessage) (any, error) {
		switch method {
		case "eth_chainId":
			return "0x1", nil
		case "eth_getCode":
			return "0x01", nil
		case "eth_getLogs":
			return []any{}, nil
		case "eth_getBlockByNumber":
			var tag string
			json.Unmarshal(params[0], &tag)
			n := uint64(10)
			if tag == "finalized" {
				n = 7
			} else if tag == "safe" {
				n = 8
			} else if strings.HasPrefix(tag, "0x") {
				n, _ = strconv.ParseUint(tag[2:], 16, 64)
			}
			return map[string]string{"number": fmt.Sprintf("0x%x", n), "hash": dex.ObjectHash(n).String(), "parentHash": dex.ObjectHash(n - 1).String(), "timestamp": "0x64", "baseFeePerGas": "0x1", "miner": testToken.String()}, nil
		default:
			return nil, fmt.Errorf("unexpected RPC %s", method)
		}
	})
	code, err := CodeHash(ethereum.Result{Raw: json.RawMessage(`"0x01"`)})
	if err != nil {
		t.Fatal(err)
	}
	m.ImplementationCodeHash, m.QuoterCodeHash = code, code
	c := Collector{RPC: r.RPC, Manifest: m, Store: db, StartBlock: 1, runtime: o}
	if err = c.Watch(context.Background(), true, func(string) {}); err != nil {
		t.Fatal(err)
	}
	if db.history != 0 || db.writes != 2 {
		t.Fatal("runtime Watch scanned history or lost capture", db.history, db.writes)
	}
}

func TestRuntimeBatchUnknownCommitRetriesInSameOwnerAndUsesActualRevision(t *testing.T) {
	o, db, _, _, m := runtimeFixture(t)
	b := runtimeBatch(m.Hash, "retry-actual", 1, 10, "latest")
	db.lose = true
	if err := o.write(context.Background(), db, runtimeWrite{Version: 1, Batch: &b}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	actual := b.Capture
	actual.Revision += 10
	actual.Canonical = false
	actual.Finality = "orphaned"
	if err := db.WriteReserveRevision(context.Background(), actual); err != nil {
		t.Fatal(err)
	}
	if err := o.write(context.Background(), db, runtimeWrite{Version: 1, Batch: &b}); err != nil {
		t.Fatal("same-owner original retry", err)
	}
	var active uint64
	if err := o.view(context.Background(), func(s *runtimeState) (any, error) { return s.active.Len(), nil }, &active); err != nil || active != 0 {
		t.Fatal("stale proposal installed", active, err)
	}
	if db.writes != 2 || db.history != 0 {
		t.Fatal(db.writes, db.history)
	}
}

func TestRuntimeCloseReleasesLockBeforeAcknowledgement(t *testing.T) {
	o, db, dir, binding, m := runtimeFixture(t)
	for i := 0; i < 64; i++ {
		if err := o.close(); err != nil {
			t.Fatal(err)
		}
		files, err := runtimecheckpoint.Open(dir, binding, 0)
		if err != nil {
			t.Fatal(i, err)
		}
		o, err = newRuntimeOwner(context.Background(), files, binding, runtimeScopeHashes(m, nil), db, nil)
		if err != nil {
			files.Close()
			t.Fatal(err)
		}
	}
	if err := o.close(); err != nil {
		t.Fatal(err)
	}
}
