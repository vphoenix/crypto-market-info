package lst

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/google/uuid"
)

type gasRange struct {
	from, to   uint64
	start, end time.Time
}
type gasSample struct {
	batch       int
	hash, block string
	day         time.Time
}

// gasSamples selects only immutable, fully scanned UTC days. A day whose final
// blocks are still missing cannot keep replacing an earlier top-two selection.
func gasSamples(batches []Batch, now time.Time) ([]gasSample, error) {
	today := now.UTC().Truncate(24 * time.Hour)
	cut := today.AddDate(0, 0, -30)
	ranges := []gasRange{}
	type boundary struct {
		hash string
		at   time.Time
	}
	boundaries := map[uint64]boundary{}
	for _, b := range batches {
		c := b.Capture
		if c.FromBlock == nil || c.ToBlock == nil || c.FromBlockTime == nil || c.ToBlockTime == nil || c.FromBlockHash == nil || c.ToBlockHash == nil {
			return nil, errors.New("gas_coverage_anchor_missing")
		}
		for _, v := range []struct {
			n uint64
			h string
			t time.Time
		}{{*c.FromBlock, *c.FromBlockHash, *c.FromBlockTime}, {*c.ToBlock, *c.ToBlockHash, *c.ToBlockTime}} {
			if old, ok := boundaries[v.n]; ok && (old.hash != v.h || !old.at.Equal(v.t)) {
				return nil, errors.New("gas_finalized_anchor_conflict")
			}
			boundaries[v.n] = boundary{v.h, v.t}
		}
		ranges = append(ranges, gasRange{*c.FromBlock, *c.ToBlock, *c.FromBlockTime, *c.ToBlockTime})
	}
	sort.Slice(ranges, func(i, j int) bool {
		if ranges[i].from == ranges[j].from {
			return ranges[i].to < ranges[j].to
		}
		return ranges[i].from < ranges[j].from
	})
	merged := []gasRange{}
	for _, r := range ranges {
		if len(merged) == 0 {
			merged = append(merged, r)
			continue
		}
		last := &merged[len(merged)-1]
		if r.from > last.to && r.from-last.to > 1 {
			merged = append(merged, r)
			continue
		}
		if r.to > last.to {
			if r.end.Before(last.end) {
				return nil, errors.New("gas_coverage_time_order")
			}
			last.to = r.to
			last.end = r.end
		}
	}
	fullDay := func(day time.Time) bool {
		for _, r := range merged {
			if !r.start.After(day) && !r.end.Before(day.AddDate(0, 0, 1)) {
				return true
			}
		}
		return false
	}
	buckets := map[string]map[string]gasSample{}
	done := map[string]bool{}
	txBlock := map[string]string{}
	add := func(i int, kind, hash, block string, at time.Time, receipt bool) error {
		if old, ok := txBlock[hash]; ok && old != block {
			return errors.New("gas_finalized_transaction_block_conflict")
		}
		txBlock[hash] = block
		if receipt {
			done[block+hash] = true
		}
		day := at.UTC().Truncate(24 * time.Hour)
		if day.Before(cut) || !day.Before(today) || !fullDay(day) {
			return nil
		}
		key := day.Format("2006-01-02") + ":" + kind
		if buckets[key] == nil {
			buckets[key] = map[string]gasSample{}
		}
		if _, ok := buckets[key][hash]; !ok {
			buckets[key][hash] = gasSample{i, hash, block, day}
		}
		return nil
	}
	for i, b := range batches {
		for _, r := range b.Requests {
			known := r.ReceiptStatus != nil && *r.ReceiptStatus == 1 && r.GasUsed != nil && r.EffectiveGasPriceWei != nil && r.ReceiptPayloadHash != nil && len(*r.ReceiptPayloadHash) == 32 && r.ReceiptAvailableAt != nil
			if e := add(i, "request", r.TransactionHash, r.BlockHash, r.BlockTime, known); e != nil {
				return nil, e
			}
		}
		for _, r := range b.Claims {
			known := r.ReceiptStatus != nil && *r.ReceiptStatus == 1 && r.GasUsed != nil && r.EffectiveGasPriceWei != nil && r.ReceiptPayloadHash != nil && len(*r.ReceiptPayloadHash) == 32 && r.ReceiptAvailableAt != nil
			if e := add(i, "claim", r.TransactionHash, r.BlockHash, r.BlockTime, known); e != nil {
				return nil, e
			}
		}
	}
	selected := map[string]gasSample{}
	for _, bucket := range buckets {
		hashes := make([]string, 0, len(bucket))
		for h := range bucket {
			hashes = append(hashes, h)
		}
		sort.Strings(hashes)
		for _, h := range hashes[:min(2, len(hashes))] {
			s := bucket[h]
			if !done[s.block+s.hash] {
				selected[s.block+s.hash] = s
			}
		}
	}
	out := make([]gasSample, 0, len(selected))
	for _, s := range selected {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].day.Equal(out[j].day) {
			return out[i].hash < out[j].hash
		}
		return out[i].day.Before(out[j].day)
	})
	return out, nil
}

// GasPass is a bounded optional task after the main log pass. Public source
// failures produce no synthetic evidence and do not alter the log cursor.
func (c *Collector) GasPass(ctx context.Context, maxTransactions int) error {
	if maxTransactions <= 0 || maxTransactions > 10 {
		return errors.New("gas_transaction_budget_must_be_1_to_10")
	}
	if c.Store == nil || c.RPC == nil {
		return errors.New("gas_collector_unconfigured")
	}
	captures, e := c.Store.LSTCaptures(ctx, c.Manifest.Hash)
	if e != nil {
		return e
	}
	latest := map[uuid.UUID]Capture{}
	for _, cap := range captures {
		if old, ok := latest[cap.CaptureId]; !ok || cap.Revision > old.Revision {
			latest[cap.CaptureId] = cap
		}
	}
	now := Now()
	cut := now.UTC().Truncate(24*time.Hour).AddDate(0, 0, -30)
	eligible := []Capture{}
	for _, cap := range latest {
		if cap.ManifestHash != c.Manifest.Hash || cap.CaptureKind != "logs" || !cap.Committed || !cap.Canonical || cap.Finality != "finalized" || cap.Status != "complete" || cap.ToBlockTime == nil || cap.ToBlockTime.Before(cut) {
			continue
		}
		eligible = append(eligible, cap)
	}
	sort.Slice(eligible, func(i, j int) bool {
		if eligible[i].StartedAt.Equal(eligible[j].StartedAt) {
			return eligible[i].CaptureId.String() < eligible[j].CaptureId.String()
		}
		return eligible[i].StartedAt.Before(eligible[j].StartedAt)
	})
	batches := make([]Batch, 0, len(eligible))
	for _, cap := range eligible {
		b, e := c.Store.LSTBatch(ctx, cap)
		if e != nil {
			return e
		}
		if e = Validate(b); e != nil {
			return e
		}
		batches = append(batches, b)
	}
	samples, e := gasSamples(batches, now)
	if e != nil {
		return e
	}
	samples = samples[:min(maxTransactions, len(samples))]
	groups := map[int][]string{}
	order := []int{}
	for _, s := range samples {
		if len(groups[s.batch]) == 0 {
			order = append(order, s.batch)
		}
		groups[s.batch] = append(groups[s.batch], s.hash)
	}
	for _, i := range order {
		if e = ctx.Err(); e != nil {
			return e
		}
		original := batches[i]
		b := original
		// Copy the slices before changing capture IDs or adding receipt evidence.
		b.Requests = append([]WithdrawalRequest(nil), original.Requests...)
		b.Finalizations = append([]WithdrawalFinalization(nil), original.Finalizations...)
		b.Claims = append([]WithdrawalClaim(nil), original.Claims...)
		b.Capture = c.newCapture("logs", "restart")
		b.Capture.SourceId = original.Capture.SourceId
		b.Capture.ChainId = original.Capture.ChainId
		b.Capture.FromBlock = original.Capture.FromBlock
		b.Capture.ToBlock = original.Capture.ToBlock
		b.Capture.FromBlockHash = original.Capture.FromBlockHash
		b.Capture.ToBlockHash = original.Capture.ToBlockHash
		b.Capture.FromBlockTime = original.Capture.FromBlockTime
		b.Capture.ToBlockTime = original.Capture.ToBlockTime
		b.Capture.Finality = "finalized"
		b.Capture.Canonical = true
		b.Capture.Status = "complete"
		b.Capture.Reason = "gas_receipt_enrichment"
		responses, enrichErr := c.EnrichGas(ctx, &b, groups[i])
		if enrichErr != nil {
			return enrichErr
		}
		changed := false
		for n := range b.Requests {
			r := &b.Requests[n]
			r.CaptureId = b.Capture.CaptureId
			if r.ReceiptPayloadHash != nil && (original.Requests[n].ReceiptPayloadHash == nil || *r.ReceiptPayloadHash != *original.Requests[n].ReceiptPayloadHash) {
				changed = true
				r.AvailableAt = Now()
			}
		}
		for n := range b.Claims {
			r := &b.Claims[n]
			r.CaptureId = b.Capture.CaptureId
			if r.ReceiptPayloadHash != nil && (original.Claims[n].ReceiptPayloadHash == nil || *r.ReceiptPayloadHash != *original.Claims[n].ReceiptPayloadHash) {
				changed = true
				r.AvailableAt = Now()
			}
		}
		for n := range b.Finalizations {
			b.Finalizations[n].CaptureId = b.Capture.CaptureId
		}
		if !changed {
			continue
		}
		// Retain links to the original full log evidence as well as new receipts.
		responses = append(responses, Response{PayloadHash: original.Capture.EvidenceRootHash})
		for _, r := range b.Requests {
			responses = append(responses, Response{PayloadHash: r.EventPayloadHash})
		}
		for _, r := range b.Finalizations {
			responses = append(responses, Response{PayloadHash: r.EventPayloadHash})
		}
		for _, r := range b.Claims {
			responses = append(responses, Response{PayloadHash: r.EventPayloadHash})
		}
		if e = c.seal(&b, responses); e != nil {
			return e
		}
		if e = c.Commit(ctx, b); e != nil {
			return e
		}
	}
	return nil
}
