package lst

import (
	"fmt"
	"math/big"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint"
)

func runtimeGasFixture(t *testing.T) (runtimeGasState, *runtimecheckpoint.Store) {
	t.Helper()
	binding := runtimecheckpoint.Binding{Service: "lst", Database: "test", ServerUUID: "server", ScopeHash: "scope", SchemaHash: "schema", Producer: "lst", Holder: "holder", RecoveryEpoch: "epoch", ProducerGeneration: 1, StateVersion: 1}
	files, err := runtimecheckpoint.Create(t.TempDir(), binding, []byte("initial"), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { files.Close() })
	var state runtimeGasState
	if err = state.open(files); err != nil {
		t.Fatal(err)
	}
	return state, files
}
func TestRuntimeGasMatchesOriginalSelectionAndTenTransactionBudget(t *testing.T) {
	g, files := runtimeGasFixture(t)
	today := Now().Truncate(24 * time.Hour)
	now := today.Add(time.Hour)
	var history []Batch
	for i := 0; i < 9; i++ {
		day := today.AddDate(0, 0, -9+i)
		b := gasTestBatch(t, day)
		from, to := uint64(i*100+100), uint64(i*100+120)
		b.Capture.FromBlock, b.Capture.ToBlock = &from, &to
		b.Capture.FromBlockHash = Ptr(HashBytes([]byte(fmt.Sprint(from))))
		b.Capture.ToBlockHash = Ptr(HashBytes([]byte(fmt.Sprint(to))))
		for j := range b.Requests {
			r := &b.Requests[j]
			r.BlockNumber = from + 10
			r.BlockHash = HashBytes([]byte(fmt.Sprint(from + 10)))
			r.TransactionHash = HashBytes([]byte(fmt.Sprintf("%d:%d", i, j)))
		}
		history = append(history, b)
	}
	for i := len(history) - 1; i >= 0; i-- {
		if err := g.add(history[i], now); err != nil {
			t.Fatal(err)
		}
	}
	sort.Slice(history, func(i, j int) bool { return runtimeGasBefore(history[i].Capture, history[j].Capture) })
	compare := func(state *runtimeGasState) {
		t.Helper()
		want, err := gasSamples(history, now)
		if err != nil {
			t.Fatal(err)
		}
		want = want[:min(10, len(want))]
		got, err := state.selectSamples(now, 10)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != len(want) {
			t.Fatal(len(got), len(want))
		}
		for i, v := range want {
			if got[i].Hash != v.hash || got[i].Block != v.block || got[i].Parent.CaptureId != history[v.batch].Capture.CaptureId {
				t.Fatal("changed sample/parent", i)
			}
		}
	}
	compare(&g)
	refs, err := g.references()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := runtimecheckpoint.MarshalTyped(g)
	if err != nil {
		t.Fatal(err)
	}
	if err = files.Checkpoint(raw, refs); err != nil {
		t.Fatal(err)
	}
	var restored runtimeGasState
	if err = runtimecheckpoint.UnmarshalTyped(files.State(), &restored); err != nil {
		t.Fatal(err)
	}
	if err = restored.open(files); err != nil {
		t.Fatal(err)
	}
	compare(&restored)
}
func TestRuntimeGasCompletionNeverPromotesThirdHash(t *testing.T) {
	g, _ := runtimeGasFixture(t)
	today := Now().Truncate(24 * time.Hour)
	now := today.Add(time.Hour)
	b := gasTestBatch(t, today.AddDate(0, 0, -1))
	if err := g.add(b, now); err != nil {
		t.Fatal(err)
	}
	before, err := g.selectSamples(now, 10)
	if err != nil || len(before) != 2 {
		t.Fatal(before, err)
	}
	known := b
	known.Capture.CaptureId = uuid.New()
	known.Capture.StartedAt = b.Capture.StartedAt.Add(time.Second)
	known.Requests = append([]WithdrawalRequest(nil), b.Requests...)
	for i := range known.Requests {
		r := &known.Requests[i]
		r.CaptureId = known.Capture.CaptureId
		if r.TransactionHash == gasTestHash(1) {
			r.ReceiptStatus = Ptr(uint8(1))
			r.GasUsed = Ptr(uint64(1))
			r.EffectiveGasPriceWei = big.NewInt(2)
			r.ReceiptPayloadHash = Ptr(gasTestHash(33))
			r.ReceiptAvailableAt = Ptr(now)
		}
	}
	if err = g.add(known, now); err != nil {
		t.Fatal(err)
	}
	got, err := g.selectSamples(now, 10)
	want, legacyErr := gasSamples([]Batch{b, known}, now)
	if err != nil || legacyErr != nil || len(got) != 1 || len(want) != 1 || got[0].Hash != gasTestHash(2) {
		t.Fatal(got, want, err, legacyErr)
	}
	if !reflect.DeepEqual(got[0].Parent, b.Capture) {
		t.Fatal("changed original earliest batch")
	}
}
func TestRuntimeGasWaitsForCoverageAndPrunesOriginalWindow(t *testing.T) {
	g, _ := runtimeGasFixture(t)
	today := Now().Truncate(24 * time.Hour)
	now := today.Add(time.Hour)
	b := gasTestBatch(t, today.AddDate(0, 0, -1))
	b.Capture.ToBlockTime = Ptr(today.Add(-time.Hour))
	if err := g.add(b, now); err != nil {
		t.Fatal(err)
	}
	if rows, err := g.selectSamples(now, 10); err != nil || len(rows) != 0 {
		t.Fatal("sampled incomplete day", rows, err)
	}
	// A separate adjacent range completes the exact original UTC-day condition.
	bridge := gasTestBatch(t, today.AddDate(0, 0, -1))
	bridge.Requests = nil
	bridge.Capture.FromBlock = Ptr(uint64(121))
	bridge.Capture.ToBlock = Ptr(uint64(122))
	bridge.Capture.FromBlockHash = Ptr(gasTestHash(121))
	bridge.Capture.ToBlockHash = Ptr(gasTestHash(122))
	bridge.Capture.FromBlockTime = Ptr(today.Add(-time.Hour))
	bridge.Capture.ToBlockTime = &today
	if err := g.add(bridge, now); err != nil {
		t.Fatal(err)
	}
	if rows, err := g.selectSamples(now, 10); err != nil || len(rows) != 2 {
		t.Fatal(rows, err)
	}
	if err := g.prune(now.AddDate(0, 0, 31)); err != nil {
		t.Fatal(err)
	}
	if g.headers.Len() != 0 || g.boundaries.Len() != 0 || len(g.Buckets) != 0 {
		t.Fatal("expired business window retained")
	}
}

func TestRuntimeGasSameFactsNewerConfirmedRevisionPreservesSamples(t *testing.T) {
	g, _ := runtimeGasFixture(t)
	now := Now()
	b := gasTestBatch(t, now.UTC().Truncate(24*time.Hour).Add(-24*time.Hour))
	if err := g.add(b, now); err != nil {
		t.Fatal(err)
	}
	before, err := g.selectSamples(now, 10)
	if err != nil {
		t.Fatal(err)
	}
	b.Capture.Revision += 10
	if err = g.add(b, now); err != nil {
		t.Fatal("confirmed newer revision rejected", err)
	}
	after, err := g.selectSamples(now, 10)
	if err != nil || len(after) != len(before) {
		t.Fatal(after, err)
	}
	for i := range after {
		if after[i].Hash != before[i].Hash || after[i].Parent.Revision != b.Capture.Revision {
			t.Fatal("changed gas sample identity", after[i])
		}
	}
}
