package mysql

import (
	"context"
	"math/big"
	"reflect"
	"testing"

	"github.com/google/uuid"
	keeper "github.com/vphoenix/crypto-market-info/internal/justlendkeeper"
)

func keeperSelectionFixture(t *testing.T) keeper.Batch {
	t.Helper()
	b := keeperFixture(t)
	b.Events = nil
	b.Receipts = nil
	b.Probes = nil
	b.Costs = nil
	b.Capture.CaptureKind = "selection_round"
	now := b.Capture.CaptureStartedAt
	round := uuid.New()
	cfg := keeper.DefaultConfig()
	renter, _ := keeper.HexAddress(cfg.Callers[0])
	receiver, _ := keeper.HexAddress(cfg.Callers[1])
	m := keeper.SelectionMember{CaptureId: b.Capture.CaptureId, CaptureStartedAt: now, RoundId: round, SlotNo: 1, Renter: renter, Receiver: receiver, Generation: keeper.Hash([]byte("generation")), RequestedLane: "focus", ActualLane: "focus", SelectedAt: now, Reason: "pressure_verified", EstimatorRevision: "deposit-pressure-v1", AmountSun: new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1)), OriginTx: keeper.Hash([]byte("origin")), CoverageTime: now, Outcome: "unknown", RPCAttempts: 3}
	m.InputDigest = keeper.SelectionInputDigest(m)
	b.Selections = []keeper.SelectionMember{m}
	b.SelectionRound = &keeper.SelectionRound{CaptureId: b.Capture.CaptureId, CaptureStartedAt: now, RoundId: round, Policy: keeper.DynamicPolicy, StartedAt: now, FinishedAt: now, FocusTarget: 40, ExploreTarget: 10, FocusCount: 1, Reason: "no_eligible_orders", Population: 2000, CoverageTime: now, CoverageScope: "all_discovered_orders_history_incomplete"}
	keeper.Seal(&b)
	if err := keeper.Validate(b); err != nil {
		t.Fatal(err)
	}
	return b
}
func TestKeeperDynamicMappingAndFrozenCatalog(t *testing.T) {
	cat, err := Mappings()
	if err != nil || len(cat.Tables) != 125 {
		t.Fatal("frozen catalog changed", err)
	}
	for _, def := range keeper.DynamicTables() {
		m, err := Mapping(keeperRentalFamily, def.Name)
		if err != nil {
			t.Fatal(err)
		}
		typ := reflect.TypeOf(def.Model)
		if len(m.Columns) != typ.NumField() {
			t.Fatal("omitted column")
		}
		for i, col := range m.Columns {
			if col.Name != typ.Field(i).Tag.Get("ch") {
				t.Fatal("column order")
			}
		}
	}
}
func TestMySQLKeeperDynamicRoundTripFrozenAndBulk(t *testing.T) {
	c := familyClient(t, keeperRentalFamily)
	ctx := context.Background()
	if err := c.CheckKeeperDynamicSchema(ctx); err == nil {
		t.Fatal("missing schema accepted")
	}
	for range 2 {
		if err := InstallKeeperDynamicSchema(ctx, c.db, c.database); err != nil {
			t.Fatal(err)
		}
	}
	if err := InstallKeeperRentalStateSchema(ctx, c.db, c.database); err != nil {
		t.Fatal(err)
	}
	if err := c.CheckKeeperRentalStateSchema(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.CheckKeeperDynamicSchema(ctx); err != nil {
		t.Fatal(err)
	}
	b := keeperSelectionFixture(t)
	for range 2 {
		if err := c.WriteKeeperBatch(ctx, b); err != nil {
			t.Fatal(err)
		}
	}
	got, err := c.KeeperBatch(ctx, b.Capture)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.Selections, b.Selections) || !keeper.DynamicProofEqual(got, b) {
		t.Fatal("selection roundtrip changed")
	}
	changed := b
	changed.Selections = append([]keeper.SelectionMember(nil), b.Selections...)
	changed.Selections[0].Reason = "changed"
	changed.Selections[0].InputDigest = keeper.SelectionInputDigest(changed.Selections[0])
	p := *b.SelectionRound
	changed.SelectionRound = &p
	keeper.Seal(&changed)
	if err := c.WriteKeeperBatch(ctx, changed); err == nil {
		t.Fatal("frozen proof mutation accepted")
	}
	rate := keeperFixture(t)
	rate.Events = nil
	rate.Receipts = nil
	rate.Probes = nil
	rate.Costs = nil
	rate.Capture.CaptureKind = "rental_rate"
	rate.Rates = []keeper.RentalRateObservation{{CaptureId: rate.Capture.CaptureId, CaptureStartedAt: rate.Capture.CaptureStartedAt, ContractAddress: rate.Capture.ContractAddress, ResourceType: 1, AvailableAt: rate.Capture.AvailableAt, Status: "unknown", ApiSuccess: keeper.Ptr(false), Rate: big.NewInt(0), Scale: 1000000000000000000, StateBinding: "node_latest_unpinned"}}
	keeper.Seal(&rate)
	if err := c.WriteKeeperBatch(ctx, rate); err != nil {
		t.Fatal(err)
	}
	bulk, err := c.KeeperBatches(ctx, []keeper.Capture{b.Capture, rate.Capture})
	if err != nil {
		t.Fatal(err)
	}
	actualRate, _ := keeper.FactBytes(bulk[rate.Capture.CaptureId].Rates)
	expectedRate, _ := keeper.FactBytes(rate.Rates)
	if string(actualRate) != string(expectedRate) {
		t.Fatal("rate NULL/known zero/false changed")
	}

	if !keeper.DynamicProofEqual(bulk[b.Capture.CaptureId], b) {
		t.Fatal("bulk lost proof")
	}
	empty := keeperFixture(t)
	empty.Events = nil
	empty.Receipts = nil
	empty.Probes = nil
	empty.Costs = nil
	empty.Capture.CaptureKind = "rental_rate"
	keeper.Seal(&empty)
	if err := c.WriteKeeperBatch(ctx, empty); err != nil {
		t.Fatal(err)
	}
	if _, err := c.KeeperBatch(ctx, empty.Capture); err != nil {
		t.Fatal("empty proof", err)
	}
	extra := b.Selections[0]
	extra.SlotNo = 2
	extra.InputDigest = keeper.SelectionInputDigest(extra)
	if err := putModels(ctx, c, c.db, "jl_keeper_selection_member", []keeper.SelectionMember{extra}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.KeeperBatches(ctx, []keeper.Capture{b.Capture}); err == nil {
		t.Fatal("extra member accepted")
	}
}
