package reserve

import (
	"context"
	"errors"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/dex"
)

func (c *Collector) runtimeReconcile(ctx context.Context, finalized, safe dex.Block) error {
	c.Rewind = 0
	var anchors []Capture
	err := c.runtime.view(ctx, func(s *runtimeState) (any, error) {
		rows := []Capture{}
		for _, scope := range runtimeScopeHashes(c.Manifest, c.RelatedManifests) {
			if a := s.Scopes[scope].Anchor; a != nil {
				rows = append(rows, *a)
			}
		}
		return rows, s.err
	}, &anchors)
	if err != nil {
		return err
	}
	numbers := make([]uint64, 0, len(anchors))
	for _, cap := range anchors {
		numbers = append(numbers, cap.ToBlock)
	}
	checked, err := c.Headers(ctx, numbers)
	if err != nil {
		return err
	}
	for _, cap := range anchors {
		b, ok := checked[cap.ToBlock]
		if !ok {
			return errors.New("reconcile_header_missing")
		}
		if Hash(b.Hash) != cap.ToHash {
			return errors.New("finalized_hash_conflict")
		}
	}
	for after := ""; ; {
		type workPage struct {
			Keys []string
			Rows []runtimeHeader
		}
		var page workPage
		err = c.runtime.view(ctx, func(s *runtimeState) (any, error) {
			keys, rows, e := s.active.Scan(after, 64, false)
			return workPage{keys, rows}, e
		}, &page)
		if err != nil {
			return err
		}
		if len(page.Keys) == 0 {
			return nil
		}
		numbers = nil
		for _, h := range page.Rows {
			numbers = append(numbers, h.Capture.FromBlock, h.Capture.ToBlock)
		}
		checked, err = c.Headers(ctx, numbers)
		if err != nil {
			return err
		}
		for _, h := range page.Rows {
			v := h.Capture
			from, ok := checked[v.FromBlock]
			if !ok {
				return errors.New("reconcile_header_missing")
			}
			to, ok := checked[v.ToBlock]
			if !ok {
				return errors.New("reconcile_header_missing")
			}
			canonical := Hash(from.Hash) == v.FromHash && Hash(to.Hash) == v.ToHash
			finality := "head"
			if v.ToBlock <= safe.Number {
				finality = "safe"
			}
			if v.ToBlock <= finalized.Number {
				finality = "finalized"
			}
			if canonical && v.Finality == finality {
				continue
			}
			var full Capture
			if err = c.runtime.view(ctx, func(s *runtimeState) (any, error) { return s.full(h) }, &full); err != nil {
				return err
			}
			expected := full.Revision
			if !canonical {
				full.Canonical = false
				full.Reason = "orphaned_branch"
				if c.Rewind == 0 || full.FromBlock < c.Rewind {
					c.Rewind = full.FromBlock
				}
			} else {
				full.Finality = finality
			}
			proof, e := c.RPC.Archive.PutObject(struct {
				Previous              string
				From, To, Safe, Final dex.Hash
				At                    time.Time
			}{Hex(full.PayloadHash), from.Payload, to.Payload, safe.Payload, finalized.Payload, dex.Now()})
			if e != nil {
				return e
			}
			full.PayloadHash = Hash(proof)
			full.Revision = max(full.Revision+1, uint64(dex.Now().UnixMicro()))
			if err = c.writeRevision(ctx, expected, full); err != nil {
				return err
			}
		}
		after = page.Keys[len(page.Keys)-1]
	}
}
func (c *Collector) runtimeReceiptCandidate(ctx context.Context) (Capture, bool, error) {
	type result struct {
		Capture Capture
		Found   bool
	}
	var out result
	err := c.runtime.view(ctx, func(s *runtimeState) (any, error) {
		h, ok := s.receiptCandidate(Hash(c.Manifest.Hash))
		return result{h.Capture, ok}, s.err
	}, &out)
	return out.Capture, out.Found, err
}
