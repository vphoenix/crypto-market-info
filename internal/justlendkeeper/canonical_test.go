package justlendkeeper

import (
	"fmt"
	"math/big"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

func TestFactDigestDifferentProcesses(t *testing.T) {
	if mode := os.Getenv("KEEPER_CANONICAL_CHILD"); mode != "" {
		// Exercise different gob type-registration orders before computing facts.
		if mode == "state-first" {
			_, _ = Freeze(NewState("test", DefaultConfig()))
		} else {
			_, _ = Freeze(struct{ Names map[string][]string }{map[string][]string{"x": {"y"}}})
		}
		at := time.Date(2026, 1, 31, 23, 59, 59, 123456000, time.UTC)
		id := uuid.MustParse("11111111-2222-3333-4444-555555555555")
		d := decimal.RequireFromString("0.12345678")
		row := CostObservation{CaptureId: id, CaptureStartedAt: at, ObservationKind: "trx_usdt_bbo", SourceId: "binance", AvailableAt: at, BidPriceUsdt: &d, PayloadHash: Ptr(Hash([]byte("payload")))}
		receipt := TxReceipt{CaptureId: id, CaptureStartedAt: at, BlockNumber: 100, FeeSun: Ptr(uint64(0)), NativeTransfers: []NativeTransfer{{InternalIndex: 1, AmountSun: *new(big.Int).Lsh(big.NewInt(1), 255)}}}
		probe := Probe{CaptureId: id, CaptureStartedAt: at, AvailableAt: at, RewardReturnSun: big.NewInt(1)}
		ev := RentalEvent{CaptureId: id, CaptureStartedAt: at, BlockNumber: 100, AmountSun: big.NewInt(100)}
		fmt.Println("FACT_DIGEST=" + Hex(Hash([]byte(digest([]CostObservation{row})+digest([]TxReceipt{receipt})+digest([]Probe{probe})+digest([]RentalEvent{ev})))))
		return
	}
	var want string
	for _, mode := range []string{"state-first", "other-first"} {
		cmd := exec.Command(os.Args[0], "-test.run=^TestFactDigestDifferentProcesses$")
		cmd.Env = append(os.Environ(), "KEEPER_CANONICAL_CHILD="+mode)
		b, e := cmd.CombinedOutput()
		if e != nil {
			t.Fatal(e, string(b))
		}
		var got string
		for _, line := range strings.Split(string(b), "\n") {
			if strings.HasPrefix(line, "FACT_DIGEST=") {
				got = line
			}
		}
		if got == "" {
			t.Fatal("child digest missing", string(b))
		}
		if want == "" {
			want = got
		} else if want != got {
			t.Fatal("digest varies with process/type order", want, got)
		}
	}
}

func TestCanonicalUnknownZeroAndDecimalEquality(t *testing.T) {
	a := TxReceipt{}
	b := a
	b.FeeSun = Ptr(uint64(0))
	if digest([]TxReceipt{a}) == digest([]TxReceipt{b}) {
		t.Fatal("NULL and zero collapsed")
	}
	a.NativeTransfers = nil
	b = a
	b.NativeTransfers = []NativeTransfer{}
	if digest([]TxReceipt{a}) != digest([]TxReceipt{b}) {
		t.Fatal("empty source and database lists differ")
	}
	x, y := decimal.RequireFromString("0.1"), decimal.RequireFromString("0.100000000000000000")
	if digest([]CostObservation{{BidPriceUsdt: &x}}) != digest([]CostObservation{{BidPriceUsdt: &y}}) {
		t.Fatal("equal fixed-point values have different digests")
	}
}
