package across

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint"
)

func (c *Collector) runtimeOrLegacyCoverage(ctx context.Context, chain uint64, decoded bool) ([]blockInterval, error) {
	if c.runtime == nil {
		return historyIntervals(c.captures, chain, decoded), nil
	}
	var ranges []RuntimeRange
	err := c.runtime.view(ctx, func(s *runtimeState) (any, error) {
		rr := s.coverage(chain, decoded)
		out := make([]RuntimeRange, len(rr))
		for i, r := range rr {
			out[i] = RuntimeRange{r.from, r.to}
		}
		return out, nil
	}, &ranges)
	return intervalSlice(ranges), err
}
func (c *Collector) runtimeDiscoveryCursor(ctx context.Context, chain uint64, once bool) (uint64, error) {
	var cursor uint64
	err := c.runtime.view(ctx, func(s *runtimeState) (any, error) {
		if once {
			return s.Watch.Cursors[chain], nil
		}
		rr := s.coverage(chain, false)
		if len(rr) == 0 {
			return uint64(0), nil
		}
		return rr[len(rr)-1].to, nil
	}, &cursor)
	// This is the live discovery edge only. Continuous raw/decoded progress is
	// calculated from the full interval union; maintenance retains every hole.
	return cursor, err
}
func (c *Collector) runtimePickProbe(ctx context.Context) (runtimeProbeWork, error) {
	var work runtimeProbeWork
	err := c.runtime.view(ctx, func(s *runtimeState) (any, error) {
		v := s.selectProbe(c.Manifest, Now(), c.runtime.leased)
		if v.Exists {
			c.runtime.leased[v.Lease] = true
		}
		return v, nil
	}, &work)
	return work, err
}
func (c *Collector) runtimeReleaseProbe(work runtimeProbeWork) error {
	var ignored bool
	return c.runtime.view(context.Background(), func(s *runtimeState) (any, error) { delete(c.runtime.leased, work.Lease); return true, nil }, &ignored)
}
func (c *Collector) runtimeExecuteProbe(ctx, network context.Context, work runtimeProbeWork) error {
	meta := work.Transition
	c.runtimeProbe = &meta
	defer func() { c.runtimeProbe = nil }()
	budget := work.Budget
	if budget <= 0 {
		budget = time.Second
	}
	if work.Live {
		network = probeContext(ctx)
	}
	network, cancel := context.WithTimeout(network, budget)
	defer cancel()
	_, err := c.recordProbe(ctx, &work.Order, meta.Task, work.Forced, network)
	if releaseErr := c.runtimeReleaseProbe(work); err == nil {
		err = releaseErr
	}
	return err
}
func (c *Collector) runtimeRunTasks(ctx context.Context) error {
	used := 0
	for {
		work, err := c.runtimePickProbe(ctx)
		if err != nil {
			return err
		}
		if !work.Exists {
			return nil
		}
		if used > 0 && work.Forced == "" {
			return c.runtimeReleaseProbe(work)
		}
		if err = c.runtimeExecuteProbe(ctx, ctx, work); err != nil {
			return err
		}
		if work.Forced == "" {
			used++
		}
	}
}
func (c *Collector) runtimeProbeLoop(ctx context.Context) error {
	for {
		work, err := c.runtimePickProbe(ctx)
		if err != nil {
			return err
		}
		if !work.Exists {
			timer := time.NewTimer(work.Wait)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-c.runtime.wake:
				timer.Stop()
			case <-timer.C:
			}
			continue
		}
		background, cancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- c.runtimeExecuteProbe(ctx, background, work) }()
		for {
			select {
			case err = <-done:
				cancel()
				if err != nil {
					return err
				}
				goto next
			case <-c.runtime.wake:
				if !work.Live {
					var live bool
					err = c.runtime.view(ctx, func(s *runtimeState) (any, error) {
						candidate := s.selectProbe(c.Manifest, Now(), c.runtime.leased)
						return candidate.Exists && candidate.Live, nil
					}, &live)
					if err != nil {
						cancel()
						<-done
						return err
					}
					if live {
						cancel()
					}
				}
			case <-ctx.Done():
				cancel()
				<-done
				return ctx.Err()
			}
		}
	next:
	}
}

type runtimeMaintenanceView struct {
	Receipts             map[string]receiptTask
	Missing              map[string]bool
	Finalized            map[uint64]Block
	ReceiptAttempts      map[string]time.Time
	RawBlocks            map[uint64]uint64
	RawNext, PartialNext int
}

func (c *Collector) runtimeLoadMaintenance(ctx context.Context) error {
	var v runtimeMaintenanceView
	err := c.runtime.view(ctx, func(s *runtimeState) (any, error) {
		v := runtimeMaintenanceView{Receipts: map[string]receiptTask{}, Missing: map[string]bool{}, Finalized: s.Finalized, RawBlocks: s.RawRepairBlocks, RawNext: s.RawRepairNext, PartialNext: s.PartialNext, ReceiptAttempts: map[string]time.Time{}}
		return v, nil
	}, &v)
	if err != nil {
		return err
	}
	c.receiptQueue = v.Receipts
	c.receiptDataMissing = v.Missing
	c.finalized = v.Finalized
	c.rawRepairBlocks = v.RawBlocks
	c.rawRepairNext = v.RawNext
	c.partialRepair = &partialRepairState{attempted: map[string]bool{}, retryAt: map[string]time.Time{}, failures: map[string]uint{}, cursor: v.PartialNext, receiptAttemptAt: v.ReceiptAttempts}
	return nil
}
func (c *Collector) runtimeSaveMaintenance(ctx context.Context) error {
	v := runtimeMaintenanceView{RawBlocks: c.rawRepairBlocks, Finalized: c.finalized, ReceiptAttempts: c.partialRepair.receiptAttemptAt, PartialNext: c.partialRepair.cursor, RawNext: c.rawRepairNext}
	raw, err := runtimecheckpoint.MarshalTyped(v)
	if err != nil {
		return err
	}
	return c.runtime.change(ctx, func(s *runtimeState) (any, error) {
		var v runtimeMaintenanceView
		if err := runtimecheckpoint.UnmarshalTyped(raw, &v); err != nil {
			return false, err
		}
		for chain, next := range v.Finalized {
			if old, ok := s.Finalized[chain]; ok && old.Number == next.Number && old.Hash == next.Hash && old.ParentHash == next.ParentHash && old.Time.Equal(next.Time) {
				v.Finalized[chain] = old
			}
		}
		receiptChanged := false
		for id, at := range v.ReceiptAttempts {
			if r, ok := s.receiptGet(id); ok && r.Pending && !r.LastAttempt.Equal(at) {
				r.LastAttempt = at
				s.receiptSet(id, r)
				receiptChanged = true
			}
		}
		if s.receiptErr != nil {
			return false, s.receiptErr
		}
		if !receiptChanged && reflect.DeepEqual(s.RawRepairBlocks, v.RawBlocks) && reflect.DeepEqual(s.Finalized, v.Finalized) && s.PartialNext == v.PartialNext && s.RawRepairNext == v.RawNext {
			return false, nil
		}
		// Serialize the worker's private mutable buffers before handing them
		// back: the actor must not keep aliases subsequently changed by the worker.
		s.RawRepairBlocks = v.RawBlocks
		s.Finalized = v.Finalized
		s.PartialNext = v.PartialNext
		s.RawRepairNext = v.RawNext
		return true, nil
	}, false)
}
func (c *Collector) runtimeMaintenanceLoop(ctx context.Context, progress func(string)) error {
	for {
		if err := c.runtimeLoadMaintenance(ctx); err != nil {
			return err
		}
		for _, chain := range c.Manifest.Chains {
			budget, cancel := context.WithTimeout(ctx, 20*time.Second)
			err := c.Reconcile(ctx, chain.ChainID, budget)
			cancel()
			if fatalCollectorError(err) {
				return err
			}
			if err != nil && progress != nil {
				progress(fmt.Sprintf("chain=%d reconcile_error=%v", chain.ChainID, err))
			}
		}
		budget, cancel := context.WithTimeout(ctx, 12*time.Second)
		err := c.RepairRawGapStep(ctx, budget)
		cancel()
		if fatalCollectorError(err) {
			return err
		}
		if err != nil && progress != nil {
			progress(fmt.Sprintf("raw_gap_repair: %v", err))
		}
		budget, cancel = context.WithTimeout(ctx, 12*time.Second)
		err = c.RepairPartialStep(ctx, budget)
		cancel()
		if fatalCollectorError(err) {
			return err
		}
		if err != nil && progress != nil {
			progress(fmt.Sprintf("partial_repair: %v", err))
		}
		if c.Prices != nil {
			budget, cancel = context.WithTimeout(ctx, 5*time.Second)
			c.Prices.Refresh(budget)
			cancel()
		}
		// New discoveries may have committed during network maintenance. This
		// reads the owner's pending set, never historical capture/fact tables.
		var incoming runtimeMaintenanceView
		err = c.runtime.view(ctx, func(s *runtimeState) (any, error) {
			v := runtimeMaintenanceView{Receipts: map[string]receiptTask{}, Missing: map[string]bool{}, ReceiptAttempts: map[string]time.Time{}}
			if key, r, ok := s.selectReceipt(c.Readers, Now()); ok {
				v.Receipts[key] = r.Task
				v.Missing[key] = r.Refund && !r.Transfers
				v.ReceiptAttempts[key] = r.LastAttempt
			}
			return v, s.receiptErr
		}, &incoming)
		if err != nil {
			return err
		}
		c.receiptQueue = incoming.Receipts
		c.receiptDataMissing = incoming.Missing
		c.partialRepair.receiptAttemptAt = incoming.ReceiptAttempts
		budget, cancel = context.WithTimeout(ctx, 8*time.Second)
		err = c.RepairReceiptStep(ctx, budget)
		cancel()
		if err != nil {
			return err
		}
		if err = c.runtimeSaveMaintenance(ctx); err != nil {
			return err
		}
		if err = c.networkFaultError(); err != nil {
			return err
		}
		if err = sleepContext(ctx, time.Second); err != nil {
			return err
		}
	}
}
