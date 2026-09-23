package options

import "testing"

func TestExactNumbersAndBounds(t *testing.T) {
	for _, tc := range []struct {
		raw    string
		signed bool
		want   int64
	}{{"0", true, 0}, {"-1.25e-2", true, -1250000}, {"1e-8", false, 1}, {"92233720368.54775807", false, 9223372036854775807}, {"-92233720368.54775808", true, -9223372036854775808}} {
		got, err := Price(tc.raw, tc.signed)
		if err != nil || got != tc.want {
			t.Fatalf("Price(%s)=%d,%v", tc.raw, got, err)
		}
	}
	for _, raw := range []string{"0", "-1", "1e-9", "NaN", "Infinity", "01", "1.", " 1", "\"1\"", "1e9999999999999999999", "92233720368.54775808"} {
		if _, err := Price(raw, false); err == nil {
			t.Errorf("accepted %q", raw)
		}
	}
	q, err := Quantity("184467440737.09551615")
	if err != nil || q != ^uint64(0) {
		t.Fatalf("uint64 maximum: %d %v", q, err)
	}
	for _, raw := range []string{"-0.1", "184467440737.09551616", "0.000000001"} {
		if _, err := Quantity(raw); err == nil {
			t.Errorf("accepted quantity %s", raw)
		}
	}
}
