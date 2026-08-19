package model

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

func TestExactTickAndLotConversion(t *testing.T) {
	tick, err := PriceTick("60538.10", decimal.RequireFromString("0.10"))
	if err != nil || tick != 605381 {
		t.Fatalf("tick=%d err=%v", tick, err)
	}
	lot, err := QuantityLot("1.230", decimal.RequireFromString("0.001"))
	if err != nil || lot != 1230 {
		t.Fatalf("lot=%d err=%v", lot, err)
	}
	if _, err := PriceTick("1.001", decimal.RequireFromString("0.01")); err == nil {
		t.Fatal("non-divisible price was accepted")
	}
	if _, err := QuantityLot("-1", decimal.NewFromInt(1)); err == nil {
		t.Fatal("negative quantity was accepted")
	}
}

func TestMinuteIDRoundTrip(t *testing.T) {
	want := mustTime(t, "2026-08-19T01:23:00Z")
	id, err := MinuteID(42, want)
	if err != nil {
		t.Fatal(err)
	}
	if uint32(id) != 42 || !MinuteTimeFromID(id).Equal(want) {
		t.Fatalf("id=%d time=%s", id, MinuteTimeFromID(id))
	}
}

func mustTime(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}
