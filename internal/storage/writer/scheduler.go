// Package writer drains immutable minute deliveries from both durable stores.
package writer

import (
	"time"

	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
)

type task struct {
	candidate             contracts.Candidate
	redis, disk, inflight bool
	retryAt               time.Time
	failures              uint32
	blocked               bool
	seen                  time.Time
	group                 int
	estimate              time.Duration
}
type scheduler struct {
	tasks                      map[string]*task
	spent, reserved            [2]float64
	worked                     [2]float64
	nextKind                   [2]int
	costPerMember, costPerByte map[string]float64
}

func newScheduler() *scheduler {
	return &scheduler{tasks: map[string]*task{}, costPerMember: map[string]float64{}, costPerByte: map[string]float64{}}
}
func groupOf(t *task, now time.Time) int {
	if t.candidate.Header.Minute.Before(now.UTC().Truncate(time.Minute).Add(-2 * time.Minute)) {
		return 1
	}
	return 0
}
func (s *scheduler) estimate(t *task) time.Duration {
	d := t.candidate.Header
	member, ok := s.costPerMember[d.Kind]
	if !ok {
		member = float64(time.Millisecond)
	}
	bytes, ok := s.costPerByte[d.Kind]
	if !ok {
		bytes = float64(time.Second) / (10 << 20)
	}
	return max(time.Millisecond, time.Duration(member*float64(d.Members)+bytes*float64(t.candidate.Bytes)))
}
func (s *scheduler) pick(now time.Time) *task {
	var best [2][2]*task
	for _, t := range s.tasks {
		if t.inflight || t.blocked || now.Before(t.retryAt) || (!t.redis && !t.disk) {
			continue
		}
		g := groupOf(t, now)
		k := 0
		if t.candidate.Header.Kind != "cex" {
			k = 1
		}
		old := best[g][k]
		if old == nil || t.candidate.Header.Minute.Before(old.candidate.Header.Minute) || (t.candidate.Header.Minute.Equal(old.candidate.Header.Minute) && t.candidate.Header.Identity() < old.candidate.Header.Identity()) {
			best[g][k] = t
		}
	}
	has := func(g int) bool { return best[g][0] != nil || best[g][1] != nil }
	if !has(0) && !has(1) {
		return nil
	}
	g := 0
	if !has(0) {
		g = 1
	} else if has(1) && (s.spent[1]+s.reserved[1])/0.3 < (s.spent[0]+s.reserved[0])/0.7 {
		g = 1
	}
	// Borrow idle capacity without accumulating an unlimited future debt.
	if !has(1 - g) {
		virtual := (s.spent[g] + s.reserved[g]) / []float64{0.7, 0.3}[g]
		s.spent[1-g] = virtual*[]float64{0.7, 0.3}[1-g] - s.reserved[1-g]
	}
	k := s.nextKind[g]
	if best[g][k] == nil {
		k = 1 - k
	}
	s.nextKind[g] = 1 - k
	t := best[g][k]
	t.group = g
	t.estimate = s.estimate(t)
	t.inflight = true
	s.reserved[g] += float64(t.estimate)
	return t
}
func (s *scheduler) finish(t *task, elapsed time.Duration) {
	s.reserved[t.group] -= float64(t.estimate)
	s.spent[t.group] += float64(elapsed)
	s.worked[t.group] += float64(elapsed)
	t.inflight = false
	d := t.candidate.Header
	// Split the observed cost between member count and bytes. Actual elapsed
	// time, rather than task count, is charged to the age group's allowance.
	if d.Members > 0 {
		v := float64(elapsed) / 2 / float64(d.Members)
		old := s.costPerMember[d.Kind]
		if old == 0 {
			old = v
		}
		s.costPerMember[d.Kind] = 0.8*old + 0.2*v
	}
	if t.candidate.Bytes > 0 {
		v := float64(elapsed) / 2 / float64(t.candidate.Bytes)
		old := s.costPerByte[d.Kind]
		if old == 0 {
			old = v
		}
		s.costPerByte[d.Kind] = 0.8*old + 0.2*v
	}
}
