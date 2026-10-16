package mysql

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/google/uuid"
	keeper "github.com/vphoenix/crypto-market-info/internal/justlendkeeper"
)

func TestKeeperFrontierDoesNotTreat256RowsAsCompleteWindow(t *testing.T) {
	c := familyClient(t, "crypto_market_info_justlend_keeper")
	ctx := context.Background()
	lo := time.Now().UTC().Truncate(time.Microsecond).Add(-time.Hour)
	hi := lo.Add(time.Minute)
	config := keeper.DefaultConfig().Hash()
	address, _ := keeper.HexAddress(keeper.ContractHex)
	digest := keeper.Hash([]byte("frontier fixture"))
	var parents, children []keeper.Capture
	for n := range 300 {
		p := keeper.Capture{CaptureId: uuid.New(), CaptureStartedAt: lo.Add(time.Duration(n) * time.Microsecond), ConfigHash: config, Network: keeper.Network, ContractAddress: address, CaptureKind: "event_index", CaptureMode: "live", EventKind: "RentResource", Status: "complete", Committed: true, RequestedFrom: &lo, RequestedTo: &hi, AvailableAt: lo, EventDigest: digest, ReceiptDigest: digest, ProbeDigest: digest, CostDigest: digest, EvidenceManifestHash: digest}
		parents = append(parents, p)
		child := p
		child.CaptureId = uuid.New()
		child.CaptureKind = "enrichment"
		child.ParentCaptureId = &parents[n].CaptureId
		children = append(children, child)
	}
	insert := func(rows []keeper.Capture) {
		t.Helper()
		err := c.transaction(ctx, func(tx *sql.Tx) error {
			table, _ := c.mapping("jl_keeper_capture")
			records := []record{}
			for _, cap := range rows {
				r, e := modelRecord(cap)
				if e != nil {
					return e
				}
				records = append(records, r)
			}
			return putImmutableRows(ctx, tx, c.table(table.Name), table, records)
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	insert(parents)
	insert(children[:256])
	called := false
	check := func(caps []keeper.Capture) (bool, error) {
		called = true
		if len(caps) != 300 {
			t.Fatalf("partial source window: %d", len(caps))
		}
		return true, nil
	}
	at, err := c.KeeperEvidenceFrontier(ctx, config, "RentResource", lo, hi, check)
	if err != nil || !at.Equal(lo) || called {
		t.Fatal("advanced past unenriched page", at, called, err)
	}
	insert(children[256:])
	at, err = c.KeeperEvidenceFrontier(ctx, config, "RentResource", lo, hi, check)
	if err != nil || !at.Equal(hi) || !called {
		t.Fatal("complete window did not advance", at, called, err)
	}
	at, err = c.KeeperEvidenceFrontier(ctx, config, "RentResource", lo, hi, func([]keeper.Capture) (bool, error) { return false, nil })
	if err != nil || !at.Equal(lo) {
		t.Fatal("broken pagination chain advanced", at, err)
	}
}
