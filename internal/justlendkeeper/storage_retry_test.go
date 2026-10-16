package justlendkeeper

import (
	"context"
	"testing"
	"time"
)

func TestStorageRetryIsBoundedAndKeepsPendingIdentity(t *testing.T) {
	c, _, now := testCollector(t)
	o := c.NewOperation("identity", "live", "publicnode")
	c.State.Ops = append(c.State.Ops, o)
	for n := range 9 {
		wait := c.deferStorageRetry(context.DeadlineExceeded)
		want := min(5*time.Second<<min(n, 4), time.Minute)
		if wait < want || wait > want+500*time.Millisecond {
			t.Fatal("unbounded retry", n, wait)
		}
		if c.State.Ops[0].Batch.Capture.CaptureId != o.Batch.Capture.CaptureId {
			t.Fatal("retry changed pending identity")
		}
		*now = now.Add(wait)
	}
}
