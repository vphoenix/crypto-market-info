package exchange

import (
	"context"
	"math/rand/v2"
	"sync"
	"time"
)

type WaitGate interface {
	Wait(context.Context) error
}

// RequestGate reserves request start times so all callers sharing it observe the
// configured minimum interval. Cooldown can also push every caller's next start
// time forward after an exchange returns a rate-limit response.
type RequestGate struct {
	mu       sync.Mutex
	interval time.Duration
	next     time.Time
}

func NewRequestGate(interval time.Duration) *RequestGate {
	if interval < 0 {
		interval = 0
	}
	return &RequestGate{interval: interval}
}

func (g *RequestGate) Wait(ctx context.Context) error {
	if g == nil {
		return nil
	}
	g.mu.Lock()
	now := time.Now()
	ready := g.next
	if ready.Before(now) {
		ready = now
	}
	if g.interval > 0 {
		g.next = ready.Add(g.interval)
	}
	g.mu.Unlock()
	delay := time.Until(ready)
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (g *RequestGate) Cooldown(delay time.Duration) {
	if g == nil || delay <= 0 {
		return
	}
	until := time.Now().Add(delay)
	g.mu.Lock()
	if until.After(g.next) {
		g.next = until
	}
	g.mu.Unlock()
}

func AddJitter(delay time.Duration) time.Duration {
	if delay <= 0 {
		return 0
	}
	window := delay / 4
	if window <= 0 {
		return delay
	}
	return delay + time.Duration(rand.Int64N(int64(window)+1))
}

func Wait(ctx context.Context, delay time.Duration) bool {
	if delay <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
