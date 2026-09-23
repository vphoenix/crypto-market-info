package ethereum

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/vphoenix/crypto-market-info/internal/dex"
	"github.com/vphoenix/crypto-market-info/internal/dexarb"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type recordedQuoter struct{ values map[dex.Hash]dex.SwapResult }

func (r recordedQuoter) Quotes(_ context.Context, _ dex.Hash, requests []dex.SwapRequest) []dex.SwapResult {
	out := make([]dex.SwapResult, len(requests))
	for i, req := range requests {
		v, ok := r.values[dex.ObjectHash(req)]
		if !ok {
			v.Err = fmt.Errorf("requested v3 input differs from archived observation")
		}
		out[i] = v
	}
	return out
}
func TestRealArchivedSampleOfflineRecompute(t *testing.T) {
	dir := os.Getenv("DEX_SAMPLE_DIR")
	if dir == "" {
		t.Skip("set DEX_SAMPLE_DIR to an absolute archived public sample directory")
	}
	raw, e := os.ReadFile(filepath.Join(dir, "sample.json"))
	if e != nil {
		t.Fatal(e)
	}
	var b dex.Batch
	if e = json.Unmarshal(raw, &b); e != nil {
		t.Fatal(e)
	}
	if b.Block.LogCoverage != "complete" || b.Block.ReceiptCoverage != "complete" || b.Block.QuoteCoverage != "complete" || b.Sky == nil || !b.Sky.Complete || len(b.Quotes) != 58 {
		t.Fatal("sample is incomplete")
	}
	m := DefaultManifest()
	fees := map[string]uint32{}
	for _, r := range m.Routes() {
		fees[r.ID] = r.Fee
	}
	recorded := recordedQuoter{values: map[dex.Hash]dex.SwapResult{}}
	stored := map[dex.Hash]dex.Quote{}
	for _, q := range b.Quotes {
		if q.Role != "strategy" {
			continue
		}
		req := dex.SwapRequest{TokenIn: q.V3InToken, TokenOut: q.V3OutToken, Fee: fees[q.Route], Amount: q.V3In}
		recorded.values[dex.ObjectHash(req)] = dex.SwapResult{Amount: q.V3Out, SqrtAfter: q.SqrtAfter, Gas: q.GasEstimate, Ticks: q.TicksCrossed, Payload: q.Payload, At: q.AvailableAt}
		stored[q.ID] = q
	}
	recomputed := dexarb.Scan(context.Background(), recorded, b.Block.Anchor, *b.Sky, m.Routes(), m.Sizes(), m.Addresses["USDC"])
	for _, q := range recomputed {
		want := stored[q.ID]
		if q.Status != "ok" || q.AmountOut.Cmp(want.AmountOut) != 0 || q.DustDAI.Cmp(want.DustDAI) != 0 || q.DustUSDS.Cmp(want.DustUSDS) != 0 {
			t.Fatal("archived integer cashflow changed", q.Route, q.Reason)
		}
	}
	archive := Archive{filepath.Join(dir, "evidence")}
	count := 0
	e = filepath.WalkDir(archive.Dir, func(path string, entry os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if entry.IsDir() {
			return nil
		}
		name := entry.Name()
		if !strings.HasSuffix(name, ".json.gz") {
			return nil
		}
		h, e := dex.ParseHash("0x" + strings.TrimSuffix(name, ".json.gz"))
		if e != nil {
			return e
		}
		if _, e = archive.Get(h); e != nil {
			return e
		}
		count++
		return nil
	})
	if e != nil || count == 0 {
		t.Fatal("evidence integrity", e)
	}
	t.Logf("offline recomputed %d routes/sizes; %d logs, %d receipts; verified %d gzip artifacts", len(recomputed), len(b.Logs), len(b.Receipts), count)
}
