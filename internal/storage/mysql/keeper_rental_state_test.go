package mysql

import (
	"context"
	"database/sql"
	"github.com/google/uuid"
	keeper "github.com/vphoenix/crypto-market-info/internal/justlendkeeper"
	"github.com/vphoenix/crypto-market-info/internal/storage/codec"
	"math/big"
	"reflect"
	"strings"
	"testing"
)

func TestKeeperRentalStateMappingsOutsideFrozenCatalog(t *testing.T) {
	catalog, err := Mappings()
	if err != nil || len(catalog.Tables) != 125 {
		t.Fatal("historical mapping changed", err)
	}
	for _, name := range []string{"jl_keeper_rental_state_observation", "jl_keeper_rental_state_batch"} {
		if _, err := Mapping("crypto_market_info_justlend_keeper", name); err != nil {
			t.Fatal(err)
		}
		for _, old := range catalog.Tables {
			if old.Name == name {
				t.Fatal("runtime extension entered frozen migration", name)
			}
		}
	}
}

func TestKeeperRentalStateCodecHasEveryField(t *testing.T) {
	types := []any{keeper.RentalStateObservation{}, keeper.RentalStateBatchProof{}}
	for i, name := range KeeperRentalStateTableNames() {
		m, e := Mapping(keeperRentalFamily, name)
		if e != nil {
			t.Fatal(e)
		}
		typ := reflect.TypeOf(types[i])
		if len(m.Columns) != typ.NumField() {
			t.Fatal("mapping omits typed fields")
		}
		for n, col := range m.Columns {
			if col.Name != typ.Field(n).Tag.Get("ch") {
				t.Fatal("wrong field", col.Name)
			}
		}
		for _, col := range m.Columns {
			if col.Name == "liquidatable" {
				for _, tc := range []struct {
					value     *bool
					parameter any
					returned  any
				}{{nil, nil, nil}, {keeper.Ptr(false), false, int64(0)}, {keeper.Ptr(true), true, int64(1)}} {
					wire, err := codec.SQLValue(col.Type, tc.value)
					if err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(wire, tc.parameter) {
						t.Fatal("SQL parameter changed", wire)
					}
					// MySQL returns TINYINT as a numeric scalar, not the bool parameter.
					back, err := codec.FromSQL(col.Type, tc.returned)
					if err != nil {
						t.Fatal(err)
					}
					var got *bool
					if err = codec.Assign(reflect.ValueOf(&got).Elem(), back); err != nil || !reflect.DeepEqual(got, tc.value) {
						t.Fatal("NULL/false/true changed", err)
					}
				}
			}
			if col.Name == "rent_index" {
				value := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))
				wire, e := codec.SQLValue(col.Type, value)
				if e != nil {
					t.Fatal(e)
				}
				back, e := codec.FromSQL(col.Type, wire)
				if e != nil {
					t.Fatal(e)
				}
				var got *big.Int
				if e = codec.Assign(reflect.ValueOf(&got).Elem(), back); e != nil || got.Cmp(value) != 0 {
					t.Fatal("UInt256 changed", e)
				}
				nilWire, e := codec.SQLValue(col.Type, (*big.Int)(nil))
				if e != nil || nilWire != nil {
					t.Fatal("NULL changed", e)
				}
			}
		}
	}
}

func keeperRentalFixture(t *testing.T) keeper.Batch {
	b := keeperFixture(t)
	b.Events = nil
	b.Receipts = nil
	b.Probes = nil
	b.Costs = nil
	b.Capture.CaptureKind = "rental_state"
	b.RentalStates = []keeper.RentalStateObservation{{CaptureId: b.Capture.CaptureId, CaptureStartedAt: b.Capture.CaptureStartedAt, ContractAddress: b.Capture.ContractAddress, Renter: strings.Repeat("r", 21), Receiver: strings.Repeat("s", 21), CallerAddress: strings.Repeat("c", 21), ResourceType: 1, ScheduledAt: b.Capture.CaptureStartedAt, AvailableAt: b.Capture.AvailableAt, EndpointView: "wallet_latest", StateBinding: "node_latest_unpinned", Status: "unknown", RentIndex: new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))}}
	keeper.Seal(&b)
	return b
}

func TestMySQLKeeperRentalStateLegacySchemaAndFrozenRetry(t *testing.T) {
	c := familyClient(t, keeperRentalFamily)
	ctx := context.Background()
	legacy := keeperFixture(t)
	if e := c.WriteKeeperBatch(ctx, legacy); e != nil {
		t.Fatal(e)
	}
	if _, e := c.KeeperBatch(ctx, legacy.Capture); e != nil {
		t.Fatal("old seven-table read", e)
	}
	b := keeperRentalFixture(t)
	if e := c.CheckKeeperRentalStateSchema(ctx); e == nil {
		t.Fatal("missing extension accepted")
	}
	if e := c.WriteKeeperBatch(ctx, b); e == nil {
		t.Fatal("new states written without schema")
	}
	for range 2 {
		if e := InstallKeeperRentalStateSchema(ctx, c.db, c.database); e != nil {
			t.Fatal(e)
		}
	}
	if e := c.CheckKeeperRentalStateSchema(ctx); e != nil {
		t.Fatal(e)
	}
	for range 2 {
		if e := c.WriteKeeperBatch(ctx, b); e != nil {
			t.Fatal(e)
		}
	}
	got, e := c.KeeperBatch(ctx, b.Capture)
	if e != nil {
		t.Fatal(e)
	}
	if !reflect.DeepEqual(got.RentalStates, b.RentalStates) || !keeper.RentalStateProofEqual(got.RentalStateProof, b.RentalStateProof) {
		t.Fatal("roundtrip lost state or proof")
	}
	changed := b
	changed.RentalStates = append([]keeper.RentalStateObservation(nil), b.RentalStates...)
	changed.RentalStates[0].ErrorMessage = "changed"
	keeper.Seal(&changed)
	if e = c.WriteKeeperBatch(ctx, changed); e == nil {
		t.Fatal("same Capture accepted mutated state proof")
	}
	empty := b
	empty.Capture.CaptureId = uuid.New()
	empty.RentalStates = nil
	keeper.Seal(&empty)
	if e = c.WriteKeeperBatch(ctx, empty); e != nil {
		t.Fatal("empty proof", e)
	}
	if _, e = c.KeeperBatch(ctx, empty.Capture); e != nil {
		t.Fatal(e)
	}
	extra := b.RentalStates[0]
	extra.CaptureId = legacy.Capture.CaptureId
	if e = c.transaction(ctx, func(tx *sql.Tx) error {
		return putModels(ctx, c, tx, "jl_keeper_rental_state_observation", []keeper.RentalStateObservation{extra})
	}); e != nil {
		t.Fatal(e)
	}
	if _, e = c.KeeperBatch(ctx, legacy.Capture); e == nil {
		t.Fatal("expected-empty extension members ignored")
	}
	if _, e = c.db.ExecContext(ctx, "DELETE FROM "+c.table("jl_keeper_rental_state_batch")+" WHERE capture_id=?", b.Capture.CaptureId[:]); e != nil {
		t.Fatal(e)
	}
	if _, e = c.KeeperBatch(ctx, b.Capture); e == nil {
		t.Fatal("missing proof published")
	}
}
