package exchange

import (
	"context"
	"testing"
	"time"
)

func TestCooldownAppliesToAlreadyReservedWaiter(t *testing.T) {
	g := NewRequestGate(40 * time.Millisecond)
	if err := g.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	done := make(chan time.Time, 1)
	go func() { _ = g.Wait(context.Background()); done <- time.Now() }()
	time.Sleep(10 * time.Millisecond)
	before := time.Now()
	g.Cooldown(90 * time.Millisecond)
	select {
	case at := <-done:
		if at.Sub(before) < 85*time.Millisecond {
			t.Fatalf("reserved waiter ignored cooldown: %v", at.Sub(before))
		}
	case <-time.After(time.Second):
		t.Fatal("waiter stuck")
	}
}
