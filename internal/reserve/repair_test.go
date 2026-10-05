package reserve

import (
	"context"
	"github.com/vphoenix/crypto-market-info/internal/dex"
	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
	"testing"
	"time"
)

func rangeCap(a, b uint64) Capture {
	return Capture{FromBlock: a, ToBlock: b, CaptureKind: "logs", CaptureMode: "live", Canonical: true, Committed: true, Finality: "finalized", LogCoverage: "complete", ReceiptCoverage: "complete"}
}
func TestResumeUsesContiguousCoverageAndRetriesPartialReceipts(t *testing.T) {
	caps := []Capture{rangeCap(11, 15), rangeCap(1, 5), rangeCap(3, 8)}
	if n := ContiguousLiveCursor(caps, 0, true); n != 8 {
		t.Fatal("gap skipped", n)
	}
	if n := CoveredThrough(caps, 4); n != 8 {
		t.Fatal("different chunk boundaries", n)
	}
	caps = append(caps, rangeCap(9, 10))
	if n := ContiguousLiveCursor(caps, 0, true); n != 15 {
		t.Fatal(n)
	}
	caps[0].ReceiptCoverage = "partial"
	if n := CoveredThrough(caps, 1); n != 10 {
		t.Fatal("partial receipts skipped", n)
	}
	caps[3].Canonical = false
	if n := ContiguousLiveCursor(caps, 0, true); n != 8 {
		t.Fatal("orphan revived", n)
	}
	if n := ContiguousLiveCursor(nil, 100, false); n != 100 {
		t.Fatal("migration seed lost")
	}
}
func TestReconcileCadenceAndImmediateReorg(t *testing.T) {
	now := time.Now()
	prev := dex.Block{Anchor: dex.Anchor{Number: 100, Hash: dex.Digest([]byte("a"))}}
	next := dex.Block{Anchor: dex.Anchor{Number: 101, Hash: dex.Digest([]byte("b"))}, Parent: prev.Hash}
	if ReconcileDue(prev, next, now, now.Add(6*time.Second)) {
		t.Fatal("every-head refetch")
	}
	if !ReconcileDue(prev, next, now, now.Add(61*time.Second)) {
		t.Fatal("finality not checked")
	}
	next.Parent = dex.Hash{}
	if !ReconcileDue(prev, next, now, now) {
		t.Fatal("reorg delayed")
	}
	next.Number = prev.Number
	next.Hash = prev.Hash
	if ReconcileDue(prev, next, now, now) {
		t.Fatal("unchanged head")
	}
	next.Hash = dex.Hash{}
	if !ReconcileDue(prev, next, now, now) {
		t.Fatal("same-height reorg")
	}
}
func TestSourceRestrictionsHaveSeparateClassification(t *testing.T) {
	rpc, _ := ethereum.NewClient("https://example.invalid", t.TempDir())
	if v := SourceError(rpc, ethereum.Result{Err: &ethereum.RPCError{Code: -32602, Message: "Archive requests require a personal token."}}); v != "rpc_archive_auth_required" {
		t.Fatal(v)
	}
	if v := SourceError(rpc, ethereum.Result{Err: &ethereum.RPCError{Code: -32016, Message: "Rate limit exceeded"}}); v != "rpc_rate_limited" {
		t.Fatal(v)
	}
	h, e := rpc.Archive.PutObject(struct{ Response []byte }{[]byte(`[{"error":{"code":-32602,"message":"Archive requests require a personal token."}}]`)})
	if e != nil {
		t.Fatal(e)
	}
	if v := SourceError(rpc, ethereum.Result{Err: &ethereum.HTTPError{Status: 403, RPCCode: -32602, RPCMessage: "Archive requests require a personal token."}, Payload: h}); v != "rpc_archive_auth_required" {
		t.Fatal(v)
	}
	if TransientRPC(ErrArchiveAuthorization) {
		t.Fatal("authorization retry")
	}
	if !TransientRPC(&ethereum.RPCError{Code: -32016, Message: "Rate limit exceeded"}) {
		t.Fatal("rate classification")
	}
	_ = context.Background()
}

func TestReconcileRetryMustRemainPendingAtUnchangedHead(t *testing.T) {
	now := time.Now()
	previous := dex.Block{Anchor: dex.Anchor{Number: 100, Hash: dex.Digest([]byte("a"))}}
	fork := dex.Block{Anchor: dex.Anchor{Number: 101, Hash: dex.Digest([]byte("fork"))}, Parent: dex.Hash{}}
	pending := false
	pending = pending || ReconcileDue(previous, fork, now, now.Add(time.Second))
	if !pending {
		t.Fatal("fork not queued")
	}
	// First header batch fails. The failure path keeps pending set.
	pending = pending || ReconcileDue(fork, fork, now, now.Add(7*time.Second))
	if !pending {
		t.Fatal("unchanged poll lost pending reorg")
	}
	// A successful reconciliation is the only transition which clears it.
	pending = false
	pending = pending || ReconcileDue(fork, fork, now.Add(7*time.Second), now.Add(13*time.Second))
	if pending {
		t.Fatal("successful check was not cleared")
	}
}
func TestCoverageDoesNotTurnFailedAttemptsIntoGapsOrHideTrueGaps(t *testing.T) {
	caps := []Capture{rangeCap(1, 5), rangeCap(4, 8), rangeCap(11, 15)}
	failed := rangeCap(6, 7)
	failed.LogCoverage = "missing"
	failed.ReceiptCoverage = "missing"
	caps = append(caps, failed)
	got := CoverageScopes(caps, false)
	if len(got) != 1 || got[0].CoveredBlocks != 13 || got[0].MissingBlocks != 2 || len(got[0].Gaps) != 1 || got[0].Gaps[0].From != 9 || got[0].Gaps[0].To != 10 {
		t.Fatal(got)
	}
}
