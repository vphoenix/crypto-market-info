package lst

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint"
)

type runtimeGasRange struct {
	From, To   uint64
	Start, End time.Time
}
type runtimeGasBoundary struct {
	Hash  string
	At    time.Time
	Users uint64
}
type runtimeGasTx struct {
	Block                     string
	At, Through, KnownThrough time.Time
}
type runtimeGasCandidate struct {
	Hash, Block string
	Day         time.Time
	Parent      Capture
}

// The fixed two candidates are retained even after receipt completion. Removing
// them would promote the third hash and change the original sampling rule.
type runtimeGasState struct {
	HeaderRoot, BoundaryRoot, TxRoot runtimecheckpoint.PageRoot
	Ranges                           []runtimeGasRange
	Buckets                          map[string][]runtimeGasCandidate
	Cut                              time.Time
	TxCleanupAfter                   string
	headers                          *runtimecheckpoint.PagedMap[Capture]
	boundaries                       *runtimecheckpoint.PagedMap[runtimeGasBoundary]
	transactions                     *runtimecheckpoint.PagedMap[runtimeGasTx]
	err                              error
}

func (g *runtimeGasState) open(files *runtimecheckpoint.Store) error {
	var err error
	g.headers, err = runtimecheckpoint.OpenPagedMap[Capture](files, g.HeaderRoot, 1<<20, func(Capture) bool { return true })
	if err != nil {
		return err
	}
	g.boundaries, err = runtimecheckpoint.OpenPagedMap[runtimeGasBoundary](files, g.BoundaryRoot, 1<<20, func(runtimeGasBoundary) bool { return true })
	if err != nil {
		return err
	}
	g.transactions, err = runtimecheckpoint.OpenPagedMap[runtimeGasTx](files, g.TxRoot, 1<<20, func(runtimeGasTx) bool { return true })
	if err != nil {
		return err
	}
	if g.Buckets == nil {
		g.Buckets = map[string][]runtimeGasCandidate{}
	}
	return nil
}
func runtimeGasHeaderKey(cap Capture) string {
	return cap.ToBlockTime.UTC().Format("2006-01-02T15:04:05.000000000Z") + "/" + cap.CaptureId.String()
}
func runtimeGasBefore(a, b Capture) bool {
	if !a.StartedAt.Equal(b.StartedAt) {
		return a.StartedAt.Before(b.StartedAt)
	}
	return a.CaptureId.String() < b.CaptureId.String()
}
func (g *runtimeGasState) eachHeader(fn func(Capture)) {
	for after := ""; g.err == nil; {
		keys, rows, err := g.headers.Scan(after, 128, false)
		if err != nil {
			g.err = err
			return
		}
		if len(keys) == 0 {
			return
		}
		for _, cap := range rows {
			fn(cap)
			if g.err != nil {
				return
			}
		}
		after = keys[len(keys)-1]
	}
}
func (g *runtimeGasState) addRange(cap Capture) error {
	g.Ranges = append(g.Ranges, runtimeGasRange{*cap.FromBlock, *cap.ToBlock, *cap.FromBlockTime, *cap.ToBlockTime})
	sort.Slice(g.Ranges, func(i, j int) bool {
		if g.Ranges[i].From != g.Ranges[j].From {
			return g.Ranges[i].From < g.Ranges[j].From
		}
		return g.Ranges[i].To < g.Ranges[j].To
	})
	merged := g.Ranges[:0]
	for _, r := range g.Ranges {
		if len(merged) == 0 || r.From > merged[len(merged)-1].To && r.From-merged[len(merged)-1].To > 1 {
			merged = append(merged, r)
			continue
		}
		last := &merged[len(merged)-1]
		if r.To > last.To {
			if r.End.Before(last.End) {
				return errors.New("gas_coverage_time_order")
			}
			last.To = r.To
			last.End = r.End
		}
	}
	g.Ranges = merged
	return nil
}
func (g *runtimeGasState) boundary(cap Capture, add bool) error {
	for _, v := range []struct {
		N    uint64
		Hash string
		At   time.Time
	}{{*cap.FromBlock, *cap.FromBlockHash, *cap.FromBlockTime}, {*cap.ToBlock, *cap.ToBlockHash, *cap.ToBlockTime}} {
		key := fmt.Sprintf("%020d", v.N)
		old, exists, err := g.boundaries.Get(key)
		if err != nil {
			return err
		}
		if exists && (old.Hash != v.Hash || !old.At.Equal(v.At)) {
			return errors.New("gas_finalized_anchor_conflict")
		}
		if add {
			old.Hash, old.At = v.Hash, v.At
			old.Users++
			err = g.boundaries.Set(key, old)
		} else {
			if !exists || old.Users == 0 {
				return runtimecheckpoint.ErrRecovery
			}
			old.Users--
			if old.Users == 0 {
				err = g.boundaries.Delete(key)
			} else {
				err = g.boundaries.Set(key, old)
			}
		}
		if err != nil {
			return err
		}
	}
	return nil
}
func (g *runtimeGasState) prune(now time.Time) error {
	cut := now.UTC().Truncate(24*time.Hour).AddDate(0, 0, -30)
	if !g.Cut.Equal(cut) {
		if !g.Cut.IsZero() && cut.Before(g.Cut) {
			return errors.New("gas_runtime_clock_moved_backwards")
		}
		// Only expired business-window metadata is removed. This scan happens at
		// a day boundary, not on every maintenance pass, and loads bounded pages.
		expired := false
		g.eachHeader(func(cap Capture) {
			if cap.ToBlockTime.Before(cut) {
				if err := g.boundary(cap, false); err != nil {
					g.err = err
					return
				}
				g.err = g.headers.Delete(runtimeGasHeaderKey(cap))
				expired = true
			}
		})
		if g.err != nil {
			return g.err
		}
		if expired {
			g.Ranges = nil
			g.eachHeader(func(cap Capture) { g.err = g.addRange(cap) })
		}
		for key, rows := range g.Buckets {
			if len(rows) == 0 || rows[0].Day.Before(cut) {
				delete(g.Buckets, key)
			}
		}
		g.Cut = cut
	}
	keys, rows, err := g.transactions.Scan(g.TxCleanupAfter, 128, false)
	if err != nil {
		return err
	}
	for i, key := range keys {
		if rows[i].Through.Before(cut) {
			if err = g.transactions.Delete(key); err != nil {
				return err
			}
		}
	}
	if len(keys) == 0 {
		g.TxCleanupAfter = ""
	} else {
		g.TxCleanupAfter = keys[len(keys)-1]
	}
	return g.err
}
func (g *runtimeGasState) add(b Batch, now time.Time) error {
	if g.err != nil {
		return g.err
	}
	if err := g.prune(now); err != nil {
		return err
	}
	cap := b.Capture
	if cap.CaptureKind != "logs" || !cap.Committed || !cap.Canonical || cap.Finality != "finalized" || cap.Status != "complete" || cap.ToBlockTime == nil || cap.ToBlockTime.Before(g.Cut) {
		return nil
	}
	if cap.FromBlock == nil || cap.ToBlock == nil || cap.FromBlockTime == nil || cap.FromBlockHash == nil || cap.ToBlockHash == nil {
		return errors.New("gas_coverage_anchor_missing")
	}
	key := runtimeGasHeaderKey(cap)
	old, exists, err := g.headers.Get(key)
	if err != nil {
		return err
	}
	if exists {
		if cap.Revision < old.Revision {
			return nil
		}
		if !reflect.DeepEqual(cap, old) {
			if cap.Revision <= old.Revision || CaptureIdentity(cap) != CaptureIdentity(old) {
				return runtimecheckpoint.ErrConflict
			}
			if err = g.headers.Set(key, cap); err != nil {
				return err
			}
			for bucket, rows := range g.Buckets {
				for i := range rows {
					if rows[i].Parent.CaptureId == cap.CaptureId {
						rows[i].Parent = cap
					}
				}
				g.Buckets[bucket] = rows
			}
		}
		return nil
	}
	if err = g.boundary(cap, true); err != nil {
		return err
	}
	if err = g.headers.Set(key, cap); err != nil {
		return err
	}
	if err = g.addRange(cap); err != nil {
		return err
	}
	add := func(kind, hash, block string, at time.Time, known bool) error {
		tx, exists, err := g.transactions.Get(hash)
		if err != nil {
			return err
		}
		if exists && tx.Through.Before(g.Cut) {
			tx = runtimeGasTx{}
			exists = false
		}
		if exists && tx.Block != block {
			return errors.New("gas_finalized_transaction_block_conflict")
		}
		if !exists {
			tx.Block, tx.At = block, at
		}
		if cap.ToBlockTime.After(tx.Through) {
			tx.Through = *cap.ToBlockTime
		}
		if known && cap.ToBlockTime.After(tx.KnownThrough) {
			tx.KnownThrough = *cap.ToBlockTime
		}
		if err = g.transactions.Set(hash, tx); err != nil {
			return err
		}
		day := at.UTC().Truncate(24 * time.Hour)
		if day.Before(g.Cut) {
			return nil
		}
		bucket := day.Format("2006-01-02") + ":" + kind
		rows := g.Buckets[bucket]
		for i, v := range rows {
			if v.Hash == hash {
				if runtimeGasBefore(cap, v.Parent) {
					rows[i].Parent = cap
				}
				g.Buckets[bucket] = rows
				return nil
			}
		}
		rows = append(rows, runtimeGasCandidate{hash, block, day, cap})
		sort.Slice(rows, func(i, j int) bool { return rows[i].Hash < rows[j].Hash })
		if len(rows) > 2 {
			rows = rows[:2]
		}
		g.Buckets[bucket] = rows
		return nil
	}
	for _, r := range b.Requests {
		known := r.ReceiptStatus != nil && *r.ReceiptStatus == 1 && r.GasUsed != nil && r.EffectiveGasPriceWei != nil && r.ReceiptPayloadHash != nil && len(*r.ReceiptPayloadHash) == 32 && r.ReceiptAvailableAt != nil
		if err = add("request", r.TransactionHash, r.BlockHash, r.BlockTime, known); err != nil {
			return err
		}
	}
	for _, r := range b.Claims {
		known := r.ReceiptStatus != nil && *r.ReceiptStatus == 1 && r.GasUsed != nil && r.EffectiveGasPriceWei != nil && r.ReceiptPayloadHash != nil && len(*r.ReceiptPayloadHash) == 32 && r.ReceiptAvailableAt != nil
		if err = add("claim", r.TransactionHash, r.BlockHash, r.BlockTime, known); err != nil {
			return err
		}
	}
	return nil
}
func (g *runtimeGasState) selectSamples(now time.Time, limit int) ([]runtimeGasCandidate, error) {
	if g.err != nil {
		return nil, g.err
	}
	if err := g.prune(now); err != nil {
		return nil, err
	}
	today := now.UTC().Truncate(24 * time.Hour)
	selected := map[string]runtimeGasCandidate{}
	for _, rows := range g.Buckets {
		for _, s := range rows {
			if !s.Day.Before(today) || s.Day.Before(g.Cut) {
				continue
			}
			full := false
			for _, r := range g.Ranges {
				if !r.Start.After(s.Day) && !r.End.Before(s.Day.AddDate(0, 0, 1)) {
					full = true
					break
				}
			}
			if !full {
				continue
			}
			tx, ok, err := g.transactions.Get(s.Hash)
			if err != nil {
				return nil, err
			}
			if !ok || tx.Block != s.Block {
				return nil, runtimecheckpoint.ErrRecovery
			}
			if !tx.KnownThrough.IsZero() && !tx.KnownThrough.Before(g.Cut) {
				continue
			}
			key := s.Block + s.Hash
			if old, ok := selected[key]; !ok || runtimeGasBefore(s.Parent, old.Parent) {
				selected[key] = s
			}
		}
	}
	out := make([]runtimeGasCandidate, 0, len(selected))
	for _, s := range selected {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Day.Equal(out[j].Day) {
			return out[i].Hash < out[j].Hash
		}
		return out[i].Day.Before(out[j].Day)
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}
func (g *runtimeGasState) references() ([]runtimecheckpoint.Reference, error) {
	if g.err != nil {
		return nil, g.err
	}
	var refs, more []runtimecheckpoint.Reference
	var err error
	g.HeaderRoot, refs, err = g.headers.Snapshot()
	if err != nil {
		return nil, err
	}
	g.BoundaryRoot, more, err = g.boundaries.Snapshot()
	if err != nil {
		return nil, err
	}
	refs = append(refs, more...)
	g.TxRoot, more, err = g.transactions.Snapshot()
	return append(refs, more...), err
}
