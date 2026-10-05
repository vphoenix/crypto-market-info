package clickhouse

import (
	"context"
	"math/big"
	"reflect"
	"testing"

	"github.com/vphoenix/crypto-market-info/internal/reserve"
)

func TestReserveSimulationIntegrationExactNullRetryAndCorruptRead(t *testing.T) {
	c := reserveIntegration(t)
	ctx := context.Background()
	b := reserveFixture(200)
	n := new(big.Int).Lsh(big.NewInt(1), 200)
	s := reserve.Simulation{ChainId: 1, ManifestHash: b.Capture.ManifestHash, QuoteId: b.Quotes[0].QuoteId, BlockNumber: b.Capture.ToBlock, BlockHash: b.Capture.ToHash, BlockTime: b.Capture.ToTime, AvailableAt: b.States[0].AvailableAt, Folio: b.Quotes[0].Folio, RouteKind: "mint", BudgetRaw: n, FundedRaw: new(big.Int).Lsh(big.NewInt(1), 230), AmountInRaw: new(big.Int).Sub(n, big.NewInt(1)), AmountOutRaw: new(big.Int).Set(n), SharesRaw: new(big.Int).Set(n), GasInternal: reserve.Ptr(uint64(1 << 62)), WithinBudget: reserve.Ptr(true), Status: "joint_call_simulated", SimulatorHash: reserve.ID("runtime"), PayloadHash: reserve.ID("simulation-proof")}
	s.SimulationId = reserve.ID(struct{ Quote, Payload string }{s.QuoteId, s.PayloadHash})
	for i := 0; i < 2; i++ {
		if e := c.WriteReserveSimulation(ctx, s); e != nil {
			t.Fatal(e)
		}
	}
	f := s
	f.Status, f.Reason = "failed", "rpc_rate_limited"
	f.AmountInRaw, f.AmountOutRaw, f.SharesRaw = nil, nil, nil
	f.GasInternal, f.WithinBudget = nil, nil
	f.PayloadHash = reserve.ID("failure-proof")
	f.SimulationId = reserve.ID(struct{ Quote, Payload string }{f.QuoteId, f.PayloadHash})
	if e := c.WriteReserveSimulation(ctx, f); e != nil {
		t.Fatal(e)
	}
	rows, e := c.ReserveSimulations(ctx, s.ManifestHash)
	if e != nil || len(rows) != 2 {
		t.Fatal("retry identity", len(rows), e)
	}
	for _, row := range rows {
		if row.Status == "failed" {
			if row.AmountInRaw != nil || row.GasInternal != nil || row.WithinBudget != nil {
				t.Fatal("NULL lost")
			}
		} else if row.AmountInRaw.Cmp(s.AmountInRaw) != 0 || row.AmountOutRaw.Cmp(n) != 0 || row.SharesRaw.Cmp(n) != 0 || *row.GasInternal != *s.GasInternal {
			t.Fatal("exact integer lost")
		}
	}
	bad := s
	bad.WithinBudget = reserve.Ptr(false)
	bad.PayloadHash = reserve.ID("corrupt-proof")
	bad.SimulationId = reserve.ID(struct{ Quote, Payload string }{bad.QuoteId, bad.PayloadHash})
	// Bypass the writer to represent a corrupted persisted row. Readers must fail
	// closed rather than returning a successful quote with a false budget flag.
	if e = c.dexInsert(ctx, "reserve_route_simulation", reserveColumns(reflect.TypeOf(bad)), reserveRows([]reserve.Simulation{bad})); e != nil {
		t.Fatal(e)
	}
	if _, e = c.ReserveSimulations(ctx, s.ManifestHash); e == nil {
		t.Fatal("corrupt simulation returned by reader")
	}
}
