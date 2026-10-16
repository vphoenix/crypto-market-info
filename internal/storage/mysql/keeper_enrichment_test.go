package mysql

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/google/uuid"
	keeper "github.com/vphoenix/crypto-market-info/internal/justlendkeeper"
)

func TestKeeperNextEnrichmentPreservesPriorityAndCompletedParents(t *testing.T) {
	c := familyClient(t, "crypto_market_info_justlend_keeper")
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Microsecond)
	config := keeper.DefaultConfig().Hash()
	address, err := keeper.HexAddress(keeper.ContractHex)
	if err != nil {
		t.Fatal(err)
	}
	digest := keeper.Hash([]byte("selection-only fixture"))
	from, to := now.Add(-time.Hour), now.Add(-time.Minute)
	makeParent := func(mode string) keeper.Capture {
		return keeper.Capture{CaptureId: uuid.New(), CaptureStartedAt: from, ConfigHash: config, Network: keeper.Network, ContractAddress: address, CaptureKind: "event_index", CaptureMode: mode, Status: "complete", Committed: true, RequestedFrom: &from, RequestedTo: &to, AvailableAt: from, EventDigest: digest, ReceiptDigest: digest, ProbeDigest: digest, CostDigest: digest, EvidenceManifestHash: digest}
	}
	history, live := makeParent("history"), makeParent("live")
	bootstrap := makeParent("bootstrap")
	insert := func(rows ...keeper.Capture) {
		t.Helper()
		if err := c.transaction(ctx, func(tx *sql.Tx) error { return putModels(ctx, c, tx, "jl_keeper_capture", rows) }); err != nil {
			t.Fatal(err)
		}
	}
	insert(history, live, bootstrap)
	parent, err := c.KeeperNextEnrichment(ctx, config, now)
	if err != nil || parent == nil || parent.CaptureId != live.CaptureId {
		t.Fatal("live work must precede history", parent, err)
	}
	child := makeParent("live")
	child.CaptureKind = "enrichment"
	child.ParentCaptureId = &live.CaptureId
	insert(child)
	parent, err = c.KeeperNextEnrichment(ctx, config, now)
	if err != nil || parent == nil || parent.CaptureId != history.CaptureId {
		t.Fatal("completed live parent must be excluded", parent, err)
	}
	child.CaptureId = uuid.New()
	child.ParentCaptureId = &history.CaptureId
	insert(child)
	parent, err = c.KeeperNextEnrichment(ctx, config, now)
	if err != nil || parent != nil {
		t.Fatal("completed and bootstrap parents must remain excluded", parent, err)
	}
	eligible, exhausted, cooling := makeParent("live"), makeParent("live"), makeParent("live")
	insert(eligible, exhausted, cooling)
	for range 3 {
		attempt := makeParent("live")
		attempt.CaptureKind, attempt.Status = "enrichment", "partial"
		attempt.ParentCaptureId = &exhausted.CaptureId
		insert(attempt)
	}
	recent := makeParent("live")
	recent.CaptureKind, recent.Status = "enrichment", "partial"
	recent.ParentCaptureId, recent.AvailableAt = &cooling.CaptureId, now.Add(-time.Minute)
	insert(recent)
	parent, err = c.KeeperNextEnrichment(ctx, config, now)
	if err != nil || parent == nil || parent.CaptureId != eligible.CaptureId {
		t.Fatal("retry limit and cooldown must exclude their parents", parent, err)
	}
	child.CaptureId, child.ParentCaptureId = uuid.New(), &eligible.CaptureId
	insert(child)
	parent, err = c.KeeperNextEnrichment(ctx, config, now)
	if err != nil || parent != nil {
		t.Fatal("all completed, exhausted and cooling parents must be excluded", parent, err)
	}
}
