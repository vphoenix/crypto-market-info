package mysql

import (
	"context"
	"errors"
	"testing"

	"github.com/vphoenix/crypto-market-info/internal/across"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
)

func TestMySQLAcrossBulkRecoveryPreservesMembersAndIntegrityErrors(t *testing.T) {
	c := familyClient(t, "crypto_market_info_across")
	ctx := context.Background()
	base := acrossFixture(t)
	caps := []across.Capture{}
	deposits := []across.Deposit{}
	updates := []across.DepositUpdate{}
	fills := []across.Fill{}
	refunds := []across.Refund{}
	receipts := []across.TxReceipt{}
	probes := []across.OrderProbe{}
	for i := range 2*acrossSQLCaptureLimit + 3 {
		b := base
		b.Capture.CaptureId = across.ID(i)
		d, u, f, r, tx, p := base.Deposits[0], base.Updates[0], base.Fills[0], base.Refunds[0], base.Receipts[0], base.Probes[0]
		d.CaptureId, u.CaptureId, f.CaptureId, r.CaptureId, tx.CaptureId, p.CaptureId = b.Capture.CaptureId, b.Capture.CaptureId, b.Capture.CaptureId, b.Capture.CaptureId, b.Capture.CaptureId, b.Capture.CaptureId
		b.Deposits, b.Updates, b.Fills, b.Refunds, b.Receipts, b.Probes = []across.Deposit{d}, []across.DepositUpdate{u}, []across.Fill{f}, []across.Refund{r}, []across.TxReceipt{tx}, []across.OrderProbe{p}
		across.Seal(&b)
		if i != 2*acrossSQLCaptureLimit+2 {
			// Include a seven-table declaration with an empty transfer set;
			// the final capture retains its original six-table declaration.
			b.Capture.TableIds = append(b.Capture.TableIds, 7)
			b.Capture.RowCounts = append(b.Capture.RowCounts, 0)
			b.Capture.RowDigests = append(b.Capture.RowDigests, across.ID(across.Members(b.Transfers)))
		}
		if e := across.Validate(b); e != nil {
			t.Fatal(e)
		}
		caps = append(caps, b.Capture)
		deposits = append(deposits, d)
		updates = append(updates, u)
		fills = append(fills, f)
		refunds = append(refunds, r)
		receipts = append(receipts, tx)
		probes = append(probes, p)
	}
	insertCaptureFixtures(t, c, "across_capture", caps)
	insertCaptureFixtures(t, c, "across_deposit", deposits)
	insertCaptureFixtures(t, c, "across_deposit_update", updates)
	insertCaptureFixtures(t, c, "across_fill", fills)
	insertCaptureFixtures(t, c, "across_refund", refunds)
	insertCaptureFixtures(t, c, "across_tx_receipt", receipts)
	insertCaptureFixtures(t, c, "across_order_probe", probes)
	loaded, e := across.LoadBatches(ctx, c, caps)
	if e != nil || len(loaded.Errors) != 0 || len(loaded.Batches) != len(caps) {
		t.Fatal("valid bulk recovery rejected", e, loaded.Errors)
	}
	if loaded.Stats.Chunks != 3 || loaded.Stats.SQLQueries == nil || *loaded.Stats.SQLQueries != 24 {
		t.Fatal("unbounded per-capture reads", loaded.Stats)
	}
	for _, cap := range caps {
		one, e := c.AcrossBatch(ctx, cap)
		if e != nil || across.ID(one) != across.ID(loaded.Batches[cap.CaptureId]) {
			t.Fatal("bulk changed typed fields or member multiplicity", e)
		}
	}
	seen := map[string]bool{}
	if e = c.walkMigrationRecords(ctx, "across_capture", "committed=1", func(r record) error {
		cap, e := migrationModel[across.Capture](r)
		if e != nil {
			return e
		}
		if seen[cap.CaptureId] {
			t.Fatal("hashed primary-key page repeated a capture")
		}
		seen[cap.CaptureId] = true
		return nil
	}); e != nil || len(seen) != len(caps) {
		t.Fatal("hashed primary-key paging lost captures", len(seen), e)
	}
	if e = c.VerifyMigratedDomain(ctx, "across_capture"); e != nil {
		t.Fatal("paged bulk domain verification rejected valid closure", e)
	}
	if _, e = c.AcrossBatches(ctx, []across.Capture{caps[0], caps[0]}); !errors.Is(e, contracts.ErrConflict) {
		t.Fatal("duplicate requested identity accepted", e)
	}
	if _, e = c.db.ExecContext(ctx, "DELETE FROM "+c.table("across_deposit")+" WHERE capture_id=?", []byte(caps[0].CaptureId)); e != nil {
		t.Fatal(e)
	}
	if e = c.VerifyMigratedDomain(ctx, "across_capture"); e == nil {
		t.Fatal("paged bulk domain verification accepted a missing member")
	}
	ambiguous := caps[1]
	ambiguous.ManifestHash = across.ID("other_manifest")
	insertCaptureFixtures(t, c, "across_capture", []across.Capture{ambiguous})
	requested := append([]across.Capture(nil), caps...)
	requested[2].Revision++
	loaded, e = across.LoadBatches(ctx, c, requested)
	if e != nil {
		t.Fatal(e)
	}
	for _, cap := range requested[:3] {
		if loaded.Errors[cap.CaptureId] == nil {
			t.Fatal("missing member, ambiguous declaration or stale revision accepted")
		}
		if _, exists := loaded.Batches[cap.CaptureId]; exists {
			t.Fatal("invalid batch published")
		}
	}
	if len(loaded.Batches) != len(caps)-3 {
		t.Fatal("one invalid capture hid valid neighbours")
	}
	if _, e = c.db.ExecContext(ctx, "DROP TABLE "+c.table("across_refund")); e != nil {
		t.Fatal(e)
	}
	if _, e = c.AcrossBatches(ctx, caps[3:5]); e == nil {
		t.Fatal("query failure became successful empty recovery")
	}
}
