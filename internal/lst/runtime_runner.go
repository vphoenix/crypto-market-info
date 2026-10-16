package lst

import (
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint"
)

func (c *Collector) runtimeLogProgress(ctx context.Context) (logProgress, error) {
	var out logProgress
	err := c.runtime.view(ctx, func(s *runtimeState) (any, error) {
		p, ok := s.Cursors[c.LiveLogsFromBlock]
		if !ok {
			return nil, errors.New("lst_runtime_original_cursor_missing")
		}
		p.Next = s.continuous(p.Start)
		return p, s.err
	}, &out)
	return out, err
}
func (c *Collector) saveLogProgress(ctx context.Context, p logProgress) error {
	if c.runtime == nil {
		return writeGob(c.liveLogCursorPath(), p)
	}
	return c.runtime.request(ctx, runtimeRequest{Apply: func(o *runtimeOwner) (any, error) {
		old, ok := o.state.Cursors[c.LiveLogsFromBlock]
		if !ok || p.Manifest != o.state.Manifest || p.Start != old.Start || !p.CoverageStartedAt.Equal(old.CoverageStartedAt) || p.Next > o.state.continuous(p.Start) || p.RangeSize == 0 {
			return nil, runtimecheckpoint.ErrConflict
		}
		p.Next = o.state.continuous(p.Start)
		if !reflect.DeepEqual(old, p) {
			o.state.Cursors[c.LiveLogsFromBlock] = p
			o.dirty = true
		}
		return nil, nil
	}}, nil)
}
func (c *Collector) runtimeFinalizePending(ctx context.Context, finalized *Block) error {
	var count uint64
	if err := c.runtime.view(ctx, func(s *runtimeState) (any, error) { return s.active.Len(), s.err }, &count); err != nil {
		return err
	}
	if count == 0 {
		return nil
	}
	var end Block
	var err error
	if finalized != nil {
		end = *finalized
	} else {
		end, err = c.RPC.Header(ctx, "finalized")
		if err != nil {
			return err
		}
	}
	limit := 4
	if c.RPC.ProtocolMulticall {
		limit = 1
	}
	var captures []Capture
	err = c.runtime.view(ctx, func(s *runtimeState) (any, error) {
		selected := []Capture{}
		for after := ""; len(selected) < limit; {
			keys, rows, e := s.active.Scan(after, 128, false)
			if e != nil {
				return nil, e
			}
			if len(keys) == 0 {
				break
			}
			for _, cap := range rows {
				if cap.ToBlock != nil && *cap.ToBlock <= end.Number {
					selected = append(selected, cap)
					if len(selected) == limit {
						break
					}
				}
			}
			after = keys[len(keys)-1]
		}
		return selected, s.err
	}, &captures)
	if err != nil {
		return err
	}
	for _, cap := range captures {
		h, err := c.RPC.Header(ctx, fmt.Sprintf("0x%x", *cap.ToBlock))
		if err != nil {
			return err
		}
		expected := cap.Revision
		cap.Revision++
		if cap.ToBlockHash == nil || h.Hash != *cap.ToBlockHash {
			cap.Canonical = false
			cap.Finality = "orphaned"
		} else {
			cap.Canonical = true
			cap.Finality = "finalized"
		}
		if err = c.runtimeWriteRevision(ctx, expected, cap); err != nil {
			return err
		}
	}
	return nil
}
func (c *Collector) runtimeGasPass(ctx context.Context, limit int) error {
	var selected []runtimeGasCandidate
	if err := c.runtime.view(ctx, func(s *runtimeState) (any, error) { return s.Gas.selectSamples(Now(), limit) }, &selected); err != nil {
		return err
	}
	batches := []Batch{}
	samples := []gasSample{}
	indices := map[uuid.UUID]int{}
	for _, candidate := range selected {
		i, exists := indices[candidate.Parent.CaptureId]
		if !exists {
			b, err := c.Store.LSTBatch(ctx, candidate.Parent)
			if err != nil {
				return err
			}
			if err = Validate(b); err != nil {
				return err
			}
			if !b.Capture.Canonical || b.Capture.Finality != "finalized" {
				return errors.New("gas_runtime_finalized_parent_changed")
			}
			i = len(batches)
			indices[candidate.Parent.CaptureId] = i
			batches = append(batches, b)
		}
		samples = append(samples, gasSample{i, candidate.Hash, candidate.Block, candidate.Day})
	}
	return c.runGasSamples(ctx, batches, samples)
}
func (c *Collector) saveRuntimeSchedule(ctx context.Context, next runtimeSchedule) error {
	if c.runtime == nil {
		return nil
	}
	raw, err := runtimecheckpoint.MarshalTyped(next)
	if err != nil {
		return err
	}
	return c.runtime.request(ctx, runtimeRequest{Apply: func(o *runtimeOwner) (any, error) {
		var copy runtimeSchedule
		if err := runtimecheckpoint.UnmarshalTyped(raw, &copy); err != nil {
			return nil, err
		}
		if !reflect.DeepEqual(copy, o.state.Schedule) {
			o.state.Schedule = copy
			o.dirty = true
		}
		return nil, nil
	}}, nil)
}
