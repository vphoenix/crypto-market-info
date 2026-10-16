package across

import (
	"errors"
	"sort"
	"strings"
	"time"
)

// runtimeProbeTransition records which scheduled task produced a frozen probe.
// It is part of the parent's recovery operation, not a notification-only hint.
type runtimeProbeTransition struct {
	Task              probeTask
	Terminal, Skipped bool
}
type runtimeProbeWork struct {
	Exists       bool
	Order        pendingOrder
	Transition   runtimeProbeTransition
	Forced       string
	Live         bool
	Budget, Wait time.Duration
	Lease        string
}

func runtimeProbeTaskID(t probeTask) string { return Hex(ID(t)) }

// selectProbe applies the existing priority/deadline rules to the owner's
// work set. Cold pending pages may be loaded; no database or network call is made.
func (s *runtimeState) selectProbe(m Manifest, at time.Time, leased map[string]bool) runtimeProbeWork {
	if len(s.Invalidations) > 0 {
		return runtimeProbeWork{Wait: time.Second}
	}
	for after := ""; s.pendingErr == nil; {
		keys, orders := s.skippedPage(after)
		if len(keys) == 0 {
			break
		}
		for i, key := range keys {
			o := orders[i]
			task := probeTask{Key: key, Planned: o.Deposit.AvailableAt, Kind: "skipped_budget"}
			lease := runtimeProbeTaskID(task)
			if !leased[lease] {
				return runtimeProbeWork{Exists: true, Order: o, Transition: runtimeProbeTransition{Task: task, Skipped: true}, Forced: "skipped_budget", Lease: lease}
			}
		}
		after = keys[len(keys)-1]
	}
	tasks := append([]probeTask(nil), s.Watch.Tasks...)
	rank := func(t probeTask) int {
		if t.Planned.After(at) {
			return 4
		}
		if priorityProbe(s.Watch.Orders[t.Key], t, at, m) {
			return 0
		}
		if o := s.Watch.Orders[t.Key]; o != nil && o.Origin == "live" && t.Kind != "open_check" {
			return 2
		}
		return 3
	}
	sort.SliceStable(tasks, func(i, j int) bool {
		if a, b := rank(tasks[i]), rank(tasks[j]); a != b {
			return a < b
		}
		if rank(tasks[i]) == 0 {
			deadline := func(t probeTask) time.Time {
				if t.Delay != nil {
					return t.Planned.Add(time.Second)
				}
				return t.Planned.Add(time.Duration(m.FreshMillis) * time.Millisecond)
			}
			if a, b := deadline(tasks[i]), deadline(tasks[j]); !a.Equal(b) {
				return a.Before(b)
			}
		}
		return tasks[i].Planned.Before(tasks[j].Planned)
	})
	wait := time.Hour
	for _, task := range tasks {
		o := s.Watch.Orders[task.Key]
		if o == nil {
			continue
		}
		lease := runtimeProbeTaskID(task)
		if leased[lease] {
			continue
		}
		terminal := o.Terminal || at.Unix() > int64(o.Deposit.FillDeadline)+2
		if admission := s.ProbeAdmission[task.Key]; !terminal && task.Delay == nil && task.Kind == "open_check" && admission.After(at) {
			wait = min(wait, admission.Sub(at))
			continue
		}
		if !terminal && task.Planned.After(at) {
			wait = min(wait, task.Planned.Sub(at))
			continue
		}
		work := runtimeProbeWork{Exists: true, Order: *o, Transition: runtimeProbeTransition{Task: task, Terminal: terminal}, Budget: 12 * time.Second, Lease: lease}
		if terminal {
			work.Forced = "cancelled_terminal"
			return work
		}
		work.Live = priorityProbe(o, task, at, m)
		if work.Live {
			work.Budget = 5 * time.Second
		} else if live, ok := nextLiveProbe(&s.Watch, tasks); ok && live.After(at) && live.Sub(at) < work.Budget {
			wait = min(wait, live.Sub(at))
			continue
		}
		return work
	}
	return runtimeProbeWork{Wait: max(wait, time.Millisecond)}
}

func (s *runtimeState) probeCommitted(meta runtimeProbeTransition, p OrderProbe) error {
	key := meta.Task.Key
	if meta.Skipped {
		if p.ProbeStatus != "skipped_budget" {
			return errors.New("runtime_skipped_probe_content")
		}
		s.skippedDelete(key)
		return s.ioError()
	}
	o := s.Watch.Orders[key]
	if o == nil {
		return errors.New("runtime_probe_missing_parent")
	}
	found := -1
	for i, t := range s.Watch.Tasks {
		if runtimeProbeTaskID(t) == runtimeProbeTaskID(meta.Task) {
			found = i
			break
		}
	}
	if found < 0 {
		return errors.New("runtime_probe_missing_task")
	}
	s.Watch.Tasks = append(s.Watch.Tasks[:found], s.Watch.Tasks[found+1:]...)
	if meta.Terminal || terminalProbe(o.Deposit, p) || p.ProbeStatus == "cancelled_terminal" && (strings.Contains(p.Reason, "source_orphaned") || strings.Contains(p.Reason, "local_deadline_elapsed")) {
		o.Terminal = true
	}
	if !o.Terminal {
		if o.Origin == "live" && !o.Followups && ProbeOpen(o.Deposit, p) {
			s.Watch.follow(key, p.AvailableAt)
			o.Followups = true
		}
		if meta.Task.Delay == nil {
			delay := time.Second
			if p.ProbeStatus == "rpc_error" || p.ProbeStatus == "stale_head" {
				delay = 10 * time.Second
			} else if o.Origin != "live" {
				delay = 5 * time.Second
			}
			if p.ContractTime != nil && exclusive(o.Deposit) && *p.ContractTime <= uint64(o.Deposit.ExclusivityDeadline) {
				delay = time.Duration(uint64(o.Deposit.ExclusivityDeadline)-*p.ContractTime+1) * time.Second
			}
			// Derive the child identity from frozen source observation time.
			// Admission also enforces the post-commit delay in the owner.
			s.Watch.Tasks = append(s.Watch.Tasks, probeTask{Key: key, Planned: p.AvailableAt.Add(delay), Kind: "open_check"})
			s.ProbeAdmission[key] = Now().Add(delay)
		}
	} else {
		pending := false
		for _, t := range s.Watch.Tasks {
			if t.Key == key {
				pending = true
				break
			}
		}
		if !pending {
			delete(s.Watch.Orders, key)
			delete(s.ProbeAdmission, key)
		}
	}
	return nil
}
