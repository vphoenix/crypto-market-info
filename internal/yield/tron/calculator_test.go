package tron

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

func TestAPROfficialWorkedExamplesAndRankBoundary(t *testing.T) {
	total := decimal.NewFromInt(28_978_895_254)
	srVotes := decimal.NewFromInt(1_233_278_454)
	rate, err := APR(1, srVotes, total, 10, 8_000_000, 128_000_000)
	if err != nil {
		t.Fatal(err)
	}
	dailyForTenMillion := rate.Div(decimal.NewFromInt(365)).Mul(decimal.NewFromInt(10_000_000))
	if dailyForTenMillion.Sub(decimal.RequireFromString("1206.4")).Abs().GreaterThan(decimal.NewFromInt(1)) {
		t.Fatalf("official SR example daily reward=%s", dailyForTenMillion)
	}
	partnerVotes := decimal.NewFromInt(82_830_160)
	partner, err := APR(28, partnerVotes, total, 20, 8_000_000, 128_000_000)
	if err != nil {
		t.Fatal(err)
	}
	partnerDaily := partner.Div(decimal.NewFromInt(365)).Mul(decimal.NewFromInt(10_000_000))
	if partnerDaily.Sub(decimal.RequireFromString("1017.3")).Abs().GreaterThan(decimal.NewFromInt(1)) {
		t.Fatalf("official partner example daily reward=%s", partnerDaily)
	}
	rank27, _ := APR(27, partnerVotes, total, 20, 8_000_000, 128_000_000)
	if !rank27.GreaterThan(partner) {
		t.Fatal("rank 27 did not receive block production reward")
	}
	if _, err = APR(28, partnerVotes, total, 101, 8_000_000, 128_000_000); err == nil {
		t.Fatal("out-of-range brokerage accepted")
	}
}

func TestValidBase58AddressChecksVersionAndChecksum(t *testing.T) {
	valid := "T9yD14Nj9j7xAB4dbGeiX9h8unkKHxuWwb"
	if !validBase58Address(valid) {
		t.Fatal("known TRON address was rejected")
	}
	last := byte('c')
	if valid[len(valid)-1] == last {
		last = 'd'
	}
	mutated := valid[:len(valid)-1] + string(last)
	if len(mutated) != len(valid) || validBase58Address(mutated) {
		t.Fatal("checksum mutation was accepted")
	}
}

type fakeTRONSource struct {
	snapshot Snapshot
	err      error
}

func (f fakeTRONSource) Fetch(context.Context) (Snapshot, error) { return f.snapshot, f.err }

func TestCollectorProducesComplete127RouteFinalizedAnchorBatch(t *testing.T) {
	s := validTRONSnapshot()
	b, err := (&Collector{Client: fakeTRONSource{snapshot: s}, Now: func() time.Time { return time.UnixMilli(50_000) }}).Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err = b.NormalizeAndValidate(); err != nil {
		t.Fatal(err)
	}
	if len(b.Items) != 127 {
		t.Fatalf("items=%d", len(b.Items))
	}
	if b.Items[26].Observation.Rate.Equal(*b.Items[27].Observation.Rate) {
		t.Fatal("rank 27 and 28 unexpectedly have same APR")
	}
	for _, item := range b.Items {
		if item.Observation.Finality == nil || *item.Observation.Finality != "finalized_anchor" || item.Observation.BlockHeight == nil || *item.Observation.BlockHeight != 101 {
			t.Fatalf("anchor=%+v", item.Observation)
		}
	}
}

func TestCollectorRejectsIncompleteUnsortedAndCrossMaintenanceSnapshots(t *testing.T) {
	for name, mutate := range map[string]func(*Snapshot){
		"incomplete":               func(s *Snapshot) { s.Witnesses = s.Witnesses[:126] },
		"unsorted":                 func(s *Snapshot) { s.Witnesses[1].VoteCount = json.Number("2000") },
		"duplicate":                func(s *Snapshot) { s.Witnesses[1].Address = s.Witnesses[0].Address },
		"maintenance":              func(s *Snapshot) { s.Parameters["getMaintenanceTimeInterval"] = 1 },
		"cross boundary":           func(s *Snapshot) { s.EndBlock.Time = time.UnixMilli(s.NextMaintenance) },
		"start in previous period": func(s *Snapshot) { s.StartBlock.Time = time.UnixMilli(-1) },
		"height went backwards":    func(s *Snapshot) { s.EndBlock.Number = s.StartBlock.Number - 1 },
		"time went backwards":      func(s *Snapshot) { s.EndBlock.Time = s.StartBlock.Time.Add(-time.Millisecond) },
		"zero votes":               func(s *Snapshot) { s.Witnesses[126].VoteCount = json.Number("0") },
		"missing parameter":        func(s *Snapshot) { delete(s.Parameters, "getWitness127PayPerBlock") },
		"negative parameter":       func(s *Snapshot) { s.Parameters["getWitnessPayPerBlock"] = -1 },
		"missing raw brokerage":    func(s *Snapshot) { delete(s.RawBrokerage, s.Witnesses[50].Address) },
	} {
		t.Run(name, func(t *testing.T) {
			s := validTRONSnapshot()
			mutate(&s)
			if _, err := (&Collector{Client: fakeTRONSource{snapshot: s}}).Collect(context.Background()); err == nil {
				t.Fatal("invalid snapshot accepted")
			}
		})
	}
}

func validTRONSnapshot() Snapshot {
	next := int64(21_600_000)
	s := Snapshot{NextMaintenance: next, StartBlock: Block{ID: strings.Repeat("a", 64), Number: 100, Time: time.UnixMilli(20_000_000)}, EndBlock: Block{ID: strings.Repeat("b", 64), Number: 101, Time: time.UnixMilli(20_000_100)}, Brokerage: map[string]int64{}, Parameters: map[string]int64{"getMaintenanceTimeInterval": 21_600_000, "getWitnessPayPerBlock": 8_000_000, "getWitness127PayPerBlock": 128_000_000, "getUnfreezeDelayDays": 14}, RawBrokerage: map[string][]byte{}, RawMaintenanceStart: []byte("m1"), RawStartBlock: []byte("b1"), RawWitnesses: []byte("w"), RawParameters: []byte("p"), RawEndBlock: []byte("b2"), RawMaintenanceEnd: []byte("m2")}
	for i := 0; i < 127; i++ {
		address := testAddress(i)
		s.Witnesses = append(s.Witnesses, Witness{Address: address, VoteCount: json.Number(fmt.Sprint(1000 - i)), URL: fmt.Sprintf("https://sr%d.invalid", i)})
		s.Brokerage[address] = int64(i % 101)
		s.RawBrokerage[address] = []byte(address)
	}
	return s
}

func testAddress(number int) string {
	const alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"
	payload := make([]byte, 21)
	payload[0] = 0x41
	payload[18] = byte(number >> 16)
	payload[19] = byte(number >> 8)
	payload[20] = byte(number)
	first := sha256.Sum256(payload)
	second := sha256.Sum256(first[:])
	raw := append(payload, second[:4]...)
	value := new(big.Int).SetBytes(raw)
	base := big.NewInt(58)
	remainder := new(big.Int)
	encoded := ""
	for value.Sign() > 0 {
		value.QuoRem(value, base, remainder)
		encoded = string(alphabet[remainder.Int64()]) + encoded
	}
	for _, item := range raw {
		if item != 0 {
			break
		}
		encoded = "1" + encoded
	}
	return encoded
}
