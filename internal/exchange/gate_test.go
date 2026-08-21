package exchange

import (
	"context"
	"testing"
	"time"
)

func TestRequestGateSpacesReservations(t *testing.T) {
	gate := NewRequestGate(30 * time.Millisecond)
	if err := gate.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if err := gate.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed < 25*time.Millisecond {
		t.Fatalf("second reservation waited only %s", elapsed)
	}
}

func TestAddJitterStaysInBoundedWindow(t *testing.T) {
	delay := 100 * time.Millisecond
	for range 100 {
		got := AddJitter(delay)
		if got < delay || got > delay+delay/4 {
			t.Fatalf("jittered delay %s is outside [%s,%s]", got, delay, delay+delay/4)
		}
	}
}
