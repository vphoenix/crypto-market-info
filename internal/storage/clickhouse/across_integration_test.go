package clickhouse

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/across"
)

func TestAcrossSchemaOnlySevenTypedTables(t *testing.T) {
	ss, e := AcrossSchemaStatements("crypto_across_test")
	if e != nil || len(ss) != 7 {
		t.Fatal(len(ss), e)
	}
	for _, s := range ss {
		if !strings.Contains(s, "`crypto_across_test`.`across_") || strings.Contains(s, "Float64") || strings.Contains(s, " JSON") {
			t.Fatal("unexpected schema", s)
		}
	}
	if _, e = AcrossSchemaStatements("bad`; DROP TABLE"); e == nil {
		t.Fatal("invalid DB accepted")
	}
}

func acrossIntegration(t *testing.T) *Client {
	t.Helper()
	if os.Getenv("CLICKHOUSE_INTEGRATION") != "1" {
		t.Skip("set CLICKHOUSE_INTEGRATION=1")
	}
	db := fmt.Sprintf("crypto_across_it_%d", time.Now().UnixNano())
	c, e := Open(context.Background(), Config{Database: db, MaxAttempts: 1})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = c.conn.Exec(context.Background(), "DROP DATABASE `"+db+"` SYNC"); _ = c.Close() })
	for range 2 {
		if e = c.InitAcrossSchema(context.Background()); e != nil {
			t.Fatal(e)
		}
	}
	return c
}

func acrossFixture(t *testing.T) across.Batch {
	t.Helper()
	now := across.Now()
	id := across.ID("capture")
	h := across.ID("block")
	num := new(big.Int).Lsh(big.NewInt(1), 220)
	initRow := func(out any) {
		x := reflect.ValueOf(out).Elem()
		for i := 0; i < x.NumField(); i++ {
			v := x.Field(i)
			tag := x.Type().Field(i).Tag.Get("dbtype")
			if strings.HasPrefix(tag, "FixedString(") {
				n, _ := strconv.Atoi(tag[12 : len(tag)-1])
				v.SetString(strings.Repeat("x", n))
			}
			if strings.HasPrefix(tag, "DateTime64") {
				v.Set(reflect.ValueOf(now))
			}
			if tag == "UInt256" {
				v.Set(reflect.ValueOf(new(big.Int).Set(num)))
			}
		}
		x.FieldByName("CaptureId").SetString(id)
		x.FieldByName("ChainId").SetUint(8453)
		if x.FieldByName("BlockNumber").Kind() != reflect.Pointer {
			x.FieldByName("BlockNumber").SetUint(7)
			x.FieldByName("BlockHash").SetString(h)
		}
	}
	b := across.Batch{Capture: across.Capture{ManifestHash: across.ID("manifest"), CaptureId: id, ChainId: 8453, CaptureKind: "logs", CaptureMode: "live", FromBlock: across.Ptr(uint64(7)), ToBlock: across.Ptr(uint64(7)), FromHash: &h, ToHash: &h, StartedAt: now, AvailableAt: now, SourceId: "fixture", Status: "complete", EvidenceHash: across.ID("evidence"), Canonical: true, Finality: "head"}}
	var d across.Deposit
	initRow(&d)
	d.DestinationChainId = 42161
	d.MessageHash = strings.Repeat("\x00", 32)
	d.LiveReceivedAt = &now
	b.Deposits = []across.Deposit{d}
	var u across.DepositUpdate
	initRow(&u)
	u.DepositorSignature = "public fixture"
	b.Updates = []across.DepositUpdate{u}
	var f across.Fill
	initRow(&f)
	f.OriginChainId = 42161
	f.FillType = 1
	b.Fills = []across.Fill{f}
	var r across.Refund
	initRow(&r)
	r.EventKind = "leaf_execution"
	r.RootBundleId = across.Ptr(uint32(1))
	r.LeafId = across.Ptr(uint32(2))
	r.RefundAddresses = []string{strings.Repeat("a", 20), strings.Repeat("b", 20)}
	r.RefundAmountsRaw = []*big.Int{new(big.Int).Set(num), big.NewInt(0)}
	r.DeferredRefunds = across.Ptr(true)
	b.Refunds = []across.Refund{r}
	var tx across.TxReceipt
	initRow(&tx)
	tx.Success = true
	tx.FeeComplete = false
	tx.FeeRule = "unknown"
	tx.Recipient = across.Ptr(strings.Repeat("t", 20))
	b.Receipts = []across.TxReceipt{tx}
	var p across.OrderProbe
	initRow(&p)
	p.OriginChainId = 42161
	p.ProbeStatus = "skipped_budget"
	p.ObservationOrigin = "live"
	p.TargetDelayMs = across.Ptr(uint32(2000))
	p.EthUsdtAsk = across.Ptr(decimal.RequireFromString("2001.000000000000000001"))
	b.Probes = []across.OrderProbe{p}
	across.Seal(&b)
	if e := across.Validate(b); e != nil {
		t.Fatal(e)
	}
	return b
}

func TestAcrossIntegrationUInt256NullDecimalRetryRevisionAndMembership(t *testing.T) {
	c := acrossIntegration(t)
	ctx := context.Background()
	b := acrossFixture(t)
	if e := c.dexInsert(ctx, "across_deposit", acrossColumns(reflect.TypeOf(across.Deposit{})), acrossRows(b.Deposits)); e != nil {
		t.Fatal(e)
	}
	caps, e := c.AcrossCaptures(ctx, b.Capture.ManifestHash)
	if e != nil || len(caps) != 0 {
		t.Fatal("uncommitted facts visible", caps, e)
	}
	for range 2 {
		if e = c.WriteAcrossBatch(ctx, b); e != nil {
			t.Fatal(e)
		}
	}
	caps, e = c.AcrossCaptures(ctx, b.Capture.ManifestHash)
	if e != nil || len(caps) != 1 {
		t.Fatal(caps, e)
	}
	got, e := c.AcrossBatch(ctx, caps[0])
	if e != nil {
		t.Fatal(e)
	}
	if across.ID(got) != across.ID(b) {
		t.Fatal("inexact binary/UInt256/Decimal/null roundtrip")
	}
	if got.Receipts[0].TotalFeeWei != nil || got.Probes[0].BlockNumber != nil {
		t.Fatal("unknown became zero")
	}
	orphan := b.Capture
	orphan.Revision++
	orphan.Canonical = false
	orphan.Finality = "orphaned"
	if e = c.WriteAcrossRevision(ctx, orphan); e != nil {
		t.Fatal(e)
	}
	if e = c.WriteAcrossRevision(ctx, b.Capture); e != nil {
		t.Fatal(e)
	}
	caps, e = c.AcrossCaptures(ctx, b.Capture.ManifestHash)
	if e != nil || len(caps) != 1 || caps[0].Canonical {
		t.Fatal("older success resurrected", caps, e)
	}
	mutated := orphan
	mutated.Revision++
	mutated.RowCounts = append([]uint32{}, mutated.RowCounts...)
	mutated.RowCounts[0]++
	if e = c.WriteAcrossRevision(ctx, mutated); e == nil {
		t.Fatal("revision changed frozen membership")
	}
	// Deleting a committed member is detected even after physical duplicate merges.
	if e = c.conn.Exec(ctx, "ALTER TABLE "+c.table("across_deposit")+" DELETE WHERE capture_id=? SETTINGS mutations_sync=2", b.Capture.CaptureId); e != nil {
		t.Fatal(e)
	}
	if _, e = c.AcrossBatch(ctx, caps[0]); e == nil {
		t.Fatal("committed missing member accepted")
	}
}
