package ethereum

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/dex"
)

// Policy is opt-in. The host key coordinates cooperating CLI processes; it
// cannot account for an old binary or another application using the same IP.
type Policy struct {
	Directory                                 string
	MinInterval, PerMember, Timeout, Cooldown time.Duration
	BatchSize                                 int
}
type quotaState struct {
	Source          string
	Next, CoolUntil time.Time
	RateFailures    uint32
	LastLimited     time.Time
}
type sourceGate struct {
	Policy
	Source, path string
	mu           sync.Mutex
	writeErr     error
}

func (c *Client) ConfigurePolicy(p Policy) error {
	if p.Directory == "" || p.MinInterval <= 0 || p.PerMember <= 0 || p.Timeout <= 0 || p.Cooldown <= 0 || p.BatchSize < 1 || p.BatchSize > 20 {
		return errors.New("invalid_rpc_policy")
	}
	if e := os.MkdirAll(p.Directory, 0700); e != nil {
		return e
	}
	g := &sourceGate{Policy: p, Source: c.SourceID, path: filepath.Join(p.Directory, dex.Digest([]byte(c.SourceID)).String()[2:]+".json")}
	c.BeforeRequest = g.acquire
	c.AfterRequest = func(_ int, e error) {
		if IsRateLimited(e) {
			var h *HTTPError
			var retry time.Duration
			if errors.As(e, &h) {
				retry = h.RetryAfter
			}
			if err := g.limited(strconv.FormatUint(uint64(retry/time.Second), 10)); err != nil {
				g.mu.Lock()
				g.writeErr = err
				g.mu.Unlock()
			}
		}
	}
	c.WaitForSource = func(ctx context.Context) error {
		if e := g.checkError(); e != nil {
			return e
		}
		var at time.Time
		if e := g.update(func(s *quotaState) error { at = s.CoolUntil; return nil }); e != nil {
			return e
		}
		return waitUntil(ctx, at)
	}
	c.BatchSize = p.BatchSize
	c.Timeout = p.Timeout
	c.HTTP.Timeout = p.Timeout
	c.AuditFailures = true
	return nil
}
func (g *sourceGate) checkError() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.writeErr != nil {
		return errors.New("rpc_policy_state_failed")
	}
	return nil
}
func (g *sourceGate) update(f func(*quotaState) error) error {
	lock, e := os.OpenFile(g.path+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return e
	}
	defer lock.Close()
	if e = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); e != nil {
		return e
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	s := quotaState{Source: g.Source}
	if raw, e := os.ReadFile(g.path); e == nil {
		if json.Unmarshal(raw, &s) != nil || s.Source != g.Source {
			return errors.New("rpc_policy_state_invalid")
		}
	} else if !errors.Is(e, os.ErrNotExist) {
		return e
	}
	if e = f(&s); e != nil {
		return e
	}
	raw, e := json.Marshal(s)
	if e != nil {
		return e
	}
	tmp, e := os.CreateTemp(filepath.Dir(g.path), ".quota-")
	if e != nil {
		return e
	}
	defer os.Remove(tmp.Name())
	if _, e = tmp.Write(raw); e != nil {
		tmp.Close()
		return e
	}
	if e = tmp.Sync(); e != nil {
		tmp.Close()
		return e
	}
	if e = tmp.Close(); e != nil {
		return e
	}
	if e = os.Rename(tmp.Name(), g.path); e != nil {
		return e
	}
	d, e := os.Open(filepath.Dir(g.path))
	if e != nil {
		return e
	}
	defer d.Close()
	return d.Sync()
}
func waitUntil(ctx context.Context, at time.Time) error {
	d := time.Until(at)
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
func (g *sourceGate) acquire(ctx context.Context, members int) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if e := g.checkError(); e != nil {
			return e
		}
		var at time.Time
		admitted := false
		err := g.update(func(s *quotaState) error {
			now := dex.Now()
			at = s.Next
			if s.CoolUntil.After(at) {
				at = s.CoolUntil
			}
			if at.After(now) {
				return nil
			}
			s.Next = now.Add(max(g.MinInterval, time.Duration(members)*g.PerMember))
			admitted = true
			return nil
		})
		if err != nil {
			return errors.New("rpc_policy_state_failed")
		}
		if admitted {
			return nil
		}
		if deadline, ok := ctx.Deadline(); ok && !at.Before(deadline) {
			return errors.New("rpc_source_wait_exceeds_deadline")
		}
		if err = waitUntil(ctx, at); err != nil {
			return errors.New("deadline_exceeded")
		}
	}
}
func (c *Client) WaitReady(ctx context.Context) error {
	if e := c.ConfirmedNetworkFault(); e != nil {
		return e
	}
	if c.WaitForSource == nil {
		return nil
	}
	return c.WaitForSource(ctx)
}
func (g *sourceGate) limited(retryAfter string) error {
	return g.update(func(s *quotaState) error {
		now := dex.Now()
		if now.Sub(s.LastLimited) > 10*time.Minute {
			s.RateFailures = 0
		}
		s.RateFailures = min(s.RateFailures+1, 6)
		s.LastLimited = now
		delay := g.Cooldown * time.Duration(uint64(1)<<min(s.RateFailures-1, 4))
		until := now.Add(delay)
		if n, e := strconv.ParseUint(retryAfter, 10, 32); e == nil {
			until = maxTime(until, now.Add(time.Duration(n)*time.Second))
		} else if t, e := time.Parse(time.RFC1123, retryAfter); e == nil {
			until = maxTime(until, t)
		}
		s.CoolUntil = maxTime(s.CoolUntil, until)
		return nil
	})
}
func maxTime(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}
