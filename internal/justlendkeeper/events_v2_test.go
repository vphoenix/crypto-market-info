package justlendkeeper

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func archivedV2(t *testing.T, prefix string) (RawEvent, []byte, []byte, []byte) {
	t.Helper()
	var provenance struct {
		Tx     string            `json:"tx_id"`
		Index  uint32            `json:"provider_event_index"`
		Hashes map[string]string `json:"hashes"`
	}
	if e := json.Unmarshal(liveFixture(t, prefix+"-provenance.json"), &provenance); e != nil {
		t.Fatal(e)
	}
	var page Page
	raw := liveFixture(t, prefix+"-page.raw")
	if Hex(Hash(raw)) != provenance.Hashes["page"] {
		t.Fatal("changed source page")
	}
	if e := Decode(raw, &page); e != nil {
		t.Fatal(e)
	}
	var re RawEvent
	for _, ev := range page.Data {
		if ev.Transaction == provenance.Tx && ev.Index == provenance.Index {
			re = ev
			break
		}
	}
	if re.Transaction == "" {
		t.Fatal("source event missing")
	}
	re.Evidence = Evidence{ResponseHash: Hex(Hash(raw)), Started: time.Now().UTC(), Available: time.Now().UTC()}
	parts := map[string][]byte{}
	for _, role := range []string{"receipt", "block", "body"} {
		parts[role] = liveFixture(t, prefix+"-"+role+".raw")
		if Hex(Hash(parts[role])) != provenance.Hashes[role] {
			t.Fatal("changed source", role)
		}
	}
	return re, parts["receipt"], parts["block"], parts["body"]
}

func TestRealArchivedExtendedRentAndReturn(t *testing.T) {
	for _, prefix := range []string{"rent-v2", "return-v2"} {
		t.Run(prefix, func(t *testing.T) {
			re, receiptRaw, blockRaw, bodyRaw := archivedV2(t, prefix)
			row, e := EventRow(re)
			if e != nil || row.AbiRevision != seedRevision || row.SecurityDepositSun == nil || row.RentIndex == nil {
				t.Fatal(e, row)
			}
			rr, logs, e := ParseReceipt(receiptRaw, Evidence{ResponseHash: Hex(Hash(receiptRaw)), Available: re.Evidence.Available})
			if e != nil {
				t.Fatal(e)
			}
			if e = ApplyBody(&rr, bodyRaw, Evidence{ResponseHash: Hex(Hash(bodyRaw)), Available: re.Evidence.Available}); e != nil {
				t.Fatal(e)
			}
			header, e := ParseBlock(blockRaw, true)
			if e != nil {
				t.Fatal(e)
			}
			found := -1
			for i, log := range logs {
				d, e := DecodeLog(log)
				if e == nil && EventEqual(row, d) {
					found = i
				}
			}
			if found != 3 {
				t.Fatal("wrong exact log match", found)
			}
			if restored, e := EventRow(RawFromRow(row)); e != nil || !EventEqual(row, restored) {
				t.Fatal("source reconstruction lost extended fields", e)
			}
			mutated := row
			mutated.RentIndex = nil
			if EventEqual(row, mutated) {
				t.Fatal("extended index ignored")
			}
			bad := logs[found]
			bad.Data = bad.Data[:len(bad.Data)-64]
			if _, e = DecodeLog(bad); e == nil {
				t.Fatal("truncated extended log accepted")
			}
			delete(re.Result, "rentIndex")
			if _, e = EventRow(re); e == nil {
				t.Fatal("partial extended event accepted")
			}
			re, _, _, _ = archivedV2(t, prefix)
			c, store, _ := testCollector(t)
			can := Candidate{Renter: row.Renter, Receiver: row.Receiver, LastActivity: row.BlockTime, Origin: re}
			c.State.Cohort = []Candidate{can}
			o := c.NewOperation("seed", "catchup", "publicnode")
			o.RawEvents, o.Headers = []RawEvent{re}, []Header{header}
			rr.CaptureId, rr.CaptureStartedAt = o.Batch.Capture.CaptureId, o.Batch.Capture.CaptureStartedAt
			o.Receipts = []ReceiptLogs{{rr, logs}}
			if e = c.Finish(context.Background(), &o); e != nil {
				t.Fatal(e)
			}
			if len(store.Batches[0].Events) != 1 || !c.State.Cohort[0].Verified || c.State.Cohort[0].Closed {
				t.Fatal("valid partial rental was lost", store.Batches[0], c.State.Cohort)
			}
		})
	}
}

func TestLegacyRentReturnExactSignatures(t *testing.T) {
	for _, prefix := range []string{"rent-v2", "return-v2"} {
		re, receiptRaw, _, _ := archivedV2(t, prefix)
		delete(re.Result, "securityDeposit")
		delete(re.Result, "rentIndex")
		row, e := EventRow(re)
		if e != nil || row.AbiRevision != "energy-market-events-v1" {
			t.Fatal(e)
		}
		_, logs, e := ParseReceipt(receiptRaw, Evidence{})
		if e != nil {
			t.Fatal(e)
		}
		l := logs[3]
		l.Topics[0] = hex.EncodeToString([]byte(Topic(signatures[re.Name])))
		l.Data = l.Data[:len(l.Data)-128]
		d, e := DecodeLog(l)
		if e != nil || !EventEqual(row, d) {
			t.Fatal("legacy signature mismatch", e)
		}
		l.Data += strings.Repeat("0", 64)
		if _, e = DecodeLog(l); e == nil {
			t.Fatal("unknown words accepted")
		}
	}
}

func TestSeedFailureBoundedRecoveryPreservesState(t *testing.T) {
	c, _, now := testCollector(t)
	re, _, _, _ := archivedV2(t, "rent-v2")
	row, _ := EventRow(re)
	c.State.Cohort = []Candidate{{Renter: row.Renter, Receiver: row.Receiver, LastActivity: row.BlockTime, Origin: re}}
	c.State.BootstrapReady = true
	c.State.Used = 137
	c.State.Sources["trongrid"] = SourceLimit{Blocked: true, Cooldown: now.Add(time.Hour)}
	for i := 1; i <= 3; i++ {
		c.RetrySeed()
		if len(c.State.Ops) != 1 || c.State.Cohort[0].SeedAttempts != uint8(i) {
			t.Fatal("retry count", i, c.State.Ops)
		}
		c.RetrySeed()
		if len(c.State.Ops) != 1 {
			t.Fatal("duplicate seed")
		}
		o := &c.State.Ops[0]
		o.Failed, o.Requests = true, nil
		o.Batch.Capture.Status, o.Batch.Capture.Reason = "partial", "fixture_failure"
		if _, _, e := c.Step(context.Background()); e != nil {
			t.Fatal(e)
		}
		c.RetrySeed()
		if len(c.State.Ops) != 0 {
			t.Fatal("failure retried without cooldown")
		}
		*now = now.Add(30 * time.Minute)
	}
	c.RetrySeed()
	if len(c.State.Ops) != 0 || c.State.Cohort[0].Verified || c.State.Cohort[0].Closed || c.State.Used != 137 || !c.State.Sources["trongrid"].Blocked {
		t.Fatal("exhaustion changed data or budgets")
	}
	if e := c.Save(); e != nil {
		t.Fatal(e)
	}
	reloaded, e := LoadState(c.Path, "test", c.Config)
	if e != nil || reloaded.Cohort[0].SeedAttempts != 3 {
		t.Fatal("retry cap lost across restart", e)
	}
	c.State.Cohort[0].SeedRevision = "legacy-decoder"
	c.RetrySeed()
	if len(c.State.Ops) != 1 || c.State.Cohort[0].SeedAttempts != 1 {
		t.Fatal("decoder repair could not recover old failure")
	}
	// Verified members continue to receive probes even with one recovery in flight.
	c.State.Cohort[0].Verified = true
	c.State.CallersValid = []string{row.Renter} // conflict is recorded as a skipped observation
	for _, kind := range []string{"RentResource", "ReturnResource"} {
		c.State.EventCursors[kind] = now.Add(-2 * time.Minute)
	}
	c.Schedule(true)
	if !c.Active("probe") {
		t.Fatal("recovery seed blocked foreground schedule")
	}
}
