package clickhouse

import (
	"context"
	"math/big"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	keeper "github.com/vphoenix/crypto-market-info/internal/justlendkeeper"
)

func TestKeeperDynamicSchemaSeparateAndReplacing(t *testing.T) {
	ss, err := KeeperDynamicSchemaStatements("fixture")
	if err != nil || len(ss) != 4 {
		t.Fatal(err)
	}
	for _, s := range ss {
		if !strings.Contains(s, "ENGINE=ReplacingMergeTree") {
			t.Fatal("frozen retries require replacing semantics")
		}
	}
	if _, err := KeeperDynamicSchemaStatements("bad-name"); err == nil {
		t.Fatal("invalid identifier")
	}
}
func TestKeeperDynamicDatabaseRoundtrip(t *testing.T) {
	if os.Getenv("KEEPER_DB_TEST") != "1" {
		t.Skip("KEEPER_DB_TEST=1 enables private ClickHouse database")
	}
	ctx := context.Background()
	db := "crypto_market_info_justlend_keeper_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	c, err := Open(ctx, Config{Database: db})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	defer c.conn.Exec(ctx, "DROP DATABASE `"+db+"`")
	if err = c.InitKeeperSchema(ctx); err != nil {
		t.Fatal(err)
	}
	if err = c.InitKeeperRentalStateSchema(ctx); err != nil {
		t.Fatal(err)
	}
	if err = c.InitKeeperDynamicSchema(ctx); err != nil {
		t.Fatal(err)
	}
	cfg := keeper.DefaultConfig()
	contract, _ := keeper.HexAddress(keeper.ContractHex)
	now := keeper.UTC(time.Now())
	id := uuid.New()
	b := keeper.Batch{Capture: keeper.Capture{CaptureId: id, CaptureStartedAt: now, ConfigHash: cfg.Hash(), Network: keeper.Network, ContractAddress: contract, CaptureKind: "rental_rate", CaptureMode: "live", SourceId: "publicnode", CoverageScope: "selected_tasks", AvailableAt: now, Status: "partial", EvidenceManifestHash: keeper.Hash([]byte("summary"))}}
	b.Rates = []keeper.RentalRateObservation{{CaptureId: id, CaptureStartedAt: now, ContractAddress: contract, ResourceType: 1, AvailableAt: now, Status: "unknown", ApiSuccess: keeper.Ptr(false), Rate: big.NewInt(0), Scale: 1000000000000000000, StateBinding: "node_latest_unpinned"}}
	keeper.Seal(&b)
	// A member can pre-exist when the final capture commit did not happen.
	if err = c.writeKeeperDynamic(ctx, b); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err = c.WriteKeeperBatch(ctx, b); err != nil {
			t.Fatal(err)
		}
	}
	got, err := c.KeeperBatch(ctx, b.Capture)
	if err != nil {
		t.Fatal(err)
	}
	x, _ := keeper.FactBytes(got.Rates)
	y, _ := keeper.FactBytes(b.Rates)
	if string(x) != string(y) || !keeper.DynamicProofEqual(got, b) {
		t.Fatal("rate or nullable values changed")
	}
	id = uuid.New()
	round := uuid.New()
	renter, _ := keeper.HexAddress(cfg.Callers[0])
	receiver, _ := keeper.HexAddress(cfg.Callers[1])
	s := keeper.Batch{Capture: b.Capture}
	s.Capture.CaptureId = id
	s.Capture.CaptureKind = "selection_round"
	s.Capture.SourceId = "local"
	m := keeper.SelectionMember{CaptureId: id, CaptureStartedAt: now, RoundId: round, SlotNo: 1, Renter: renter, Receiver: receiver, Generation: keeper.Hash([]byte("generation")), RequestedLane: "focus", ActualLane: "explore", SelectedAt: now, Reason: "focus_empty_oldest_waiting", EstimatorRevision: "deposit-pressure-v1", OriginTx: keeper.Hash([]byte("origin")), CoverageTime: now, Outcome: "unknown", RPCAttempts: 3}
	m.InputDigest = keeper.SelectionInputDigest(m)
	s.Selections = []keeper.SelectionMember{m}
	s.SelectionRound = &keeper.SelectionRound{CaptureId: id, CaptureStartedAt: now, RoundId: round, Policy: keeper.DynamicPolicy, StartedAt: now, FinishedAt: now, FocusTarget: 40, ExploreTarget: 10, ExploreCount: 1, Reason: "no_eligible_orders", Population: 2000, CoverageTime: now, CoverageScope: "all_discovered_orders_history_incomplete"}
	keeper.Seal(&s)
	for range 2 {
		if err = c.WriteKeeperBatch(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	got, err = c.KeeperBatch(ctx, s.Capture)
	if err != nil {
		t.Fatal(err)
	}
	x, _ = keeper.FactBytes(got.Selections)
	y, _ = keeper.FactBytes(s.Selections)
	if string(x) != string(y) || !keeper.DynamicProofEqual(got, s) {
		t.Fatal("selection changed")
	}
	changed := s
	changed.Selections = append([]keeper.SelectionMember(nil), s.Selections...)
	changed.Selections[0].Outcome = "changed"
	p := *s.SelectionRound
	changed.SelectionRound = &p
	keeper.Seal(&changed)
	if err = c.WriteKeeperBatch(ctx, changed); err == nil {
		t.Fatal("mutated frozen selection accepted")
	}
}
