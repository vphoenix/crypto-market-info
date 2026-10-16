package mysql

import (
	"testing"

	"github.com/vphoenix/crypto-market-info/internal/reserve"
)

func TestReserveFinalityProofRevisionKeepsFrozenMembers(t *testing.T) {
	old := reserve.Capture{ChainId: 1, CaptureId: "capture", BatchId: "batch", PayloadHash: "old-proof", Finality: "head", Canonical: true, Revision: 1}
	next := old
	next.PayloadHash, next.Finality, next.Revision = "new-proof", "finalized", 2
	if reserveFrozen(old) != reserveFrozen(next) {
		t.Fatal("normal Reconcile finality evidence replacement changed frozen identity")
	}
	next.QuoteMembers = "changed-members"
	if reserveFrozen(old) == reserveFrozen(next) {
		t.Fatal("changed quote commitments were accepted")
	}
	next = old
	next.FromBlock++
	if reserveFrozen(old) == reserveFrozen(next) {
		t.Fatal("changed chain location was accepted")
	}
}
