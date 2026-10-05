package optionslive

import (
	"fmt"
	"sync"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/exchange"
	"github.com/vphoenix/crypto-market-info/internal/exchange/deribit"
	"github.com/vphoenix/crypto-market-info/internal/options"
)

type event struct {
	Spec         *options.ContractSpec
	Index        *catalogIndex
	At           time.Time
	Boundary     bool
	Group        string
	Stream       deribit.StreamEvent
	Metadata     *options.MetadataObservation
	InstrumentID uint32
	State        *catalogState
	Gate         *catalogGate
	Plan         *options.CollectionPlan
	Swap         *catalogSwap
}
type ingress struct {
	budget     *exchange.BufferBudget
	mu         sync.Mutex
	queue      chan event
	failure    chan error
	next, last time.Time
	bytes      int
	failed     bool
}

func newIngress(now time.Time) *ingress {
	return &ingress{queue: make(chan event, 256), failure: make(chan error, 1), next: now.Truncate(time.Second).Add(time.Second), last: now}
}
func (q *ingress) offer(e event) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.offerLocked(time.Now(), e)
}
func (q *ingress) pulse() error { return q.offer(event{Group: "tick"}) }
func (q *ingress) offerLocked(now time.Time, e event) error {
	if q.failed {
		return fmt.Errorf("options ingress stopped")
	}
	// Both wall and monotonic elapsed times are used when available. No past
	// UTC interval is rewritten after a discontinuity.
	drift := now.Round(0).Sub(q.last.Round(0)) - now.Sub(q.last)
	if now.Before(q.last) || now.Round(0).Before(q.last.Round(0)) || drift > 250*time.Millisecond || drift < -250*time.Millisecond {
		return q.fail(fmt.Errorf("options clock discontinuity"))
	}
	q.last = now
	for q.next.Before(now) {
		if err := q.push(event{At: q.next.UTC(), Boundary: true}); err != nil {
			return err
		}
		q.next = q.next.Add(time.Second)
	}
	if e.Group == "tick" {
		return nil
	}
	e.At = now.UTC()
	return q.push(e)
}
func (q *ingress) push(e event) error {
	cost := len(e.Stream.Raw) + 512
	if q.bytes+cost > 16<<20 {
		return q.fail(fmt.Errorf("options ingress byte budget exceeded"))
	}
	if !q.budget.Reserve(int64(cost)) {
		return q.fail(fmt.Errorf("global options ingress byte budget exceeded"))
	}
	select {
	case q.queue <- e:
		q.bytes += cost
		return nil
	default:
		q.budget.Release(int64(cost))
		return q.fail(fmt.Errorf("options ingress queue overflow"))
	}
}
func (q *ingress) consumed(e event) {
	q.mu.Lock()
	q.bytes -= len(e.Stream.Raw) + 512
	q.budget.Release(int64(len(e.Stream.Raw) + 512))
	q.mu.Unlock()
}
func (q *ingress) fail(err error) error {
	q.failed = true
	select {
	case q.failure <- err:
	default:
	}
	return err
}
