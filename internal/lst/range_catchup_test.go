package lst

import (
	"context"
	"testing"
)

func TestRangeCatchupGrowsWithoutSkippingFailedIntervals(t *testing.T) {
	c := &Collector{Manifest: Manifest{MaxLogBlocks: 512}, LogMode: "range", LiveLogsFromBlock: 100}
	p := logProgress{Start: 100, Next: 108, RangeSize: 8}
	for range 30 {
		to := p.Next + p.RangeSize - 1
		p.result(c, to, nil)
		if p.Next != to+1 || p.RangeSize > 32 {
			t.Fatal("coverage/cap", p)
		}
	}
	if p.RangeSize != 32 {
		t.Fatal("range never reaches bounded catch-up size", p)
	}
	next := p.Next
	p.result(c, next+31, context.DeadlineExceeded)
	if p.Next != next || p.RangeSize != 16 {
		t.Fatal("timeout skipped unverified blocks", p)
	}
	p.result(c, next+15, ErrLogRange)
	if p.Next != next || p.RangeSize != 8 {
		t.Fatal("range error skipped unverified blocks", p)
	}
	c.LogMode = "receipts"
	if c.liveLogBlocks() != 8 {
		t.Fatal("receipts budget changed")
	}
	c.LogMode = "range"
	c.Manifest.MaxLogBlocks = 4
	if c.liveLogBlocks() != 4 {
		t.Fatal("manifest cap ignored")
	}
}
