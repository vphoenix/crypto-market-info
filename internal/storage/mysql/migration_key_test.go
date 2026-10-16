package mysql

import (
	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/storage/codec"
	"math/big"
	"testing"
)

func TestMigrationUInt256FullWidthOrdering(t *testing.T) {
	typ, _ := codec.Parse("UInt256")
	tab := Table{Key: []string{"deposit_id"}, Columns: []Column{{Name: "deposit_id", Type: typ}}}
	values := []*big.Int{big.NewInt(0), big.NewInt(1), new(big.Int).Lsh(big.NewInt(1), 64), new(big.Int).Lsh(big.NewInt(1), 255), new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))}
	for i, a := range values {
		for j, b := range values {
			ak, e := MigrationKey(tab, MigrationRow{"deposit_id": *a})
			if e != nil {
				t.Fatal(e)
			}
			bk, e := MigrationKey(tab, MigrationRow{"deposit_id": *b})
			if e != nil {
				t.Fatal(e)
			}
			got, e := CompareMigrationKeys(tab, ak, bk)
			if e != nil || got != a.Cmp(b) {
				t.Fatalf("%d vs %d: %d %v", i, j, got, e)
			}
		}
	}
}

func TestMigrationUUIDTrailingHalfThenLeadingHalf(t *testing.T) {
	typ, _ := codec.Parse("UUID")
	tab := Table{Key: []string{"run_id"}, Columns: []Column{{Name: "run_id", Type: typ}}}
	ids := []string{"00000000-0000-0000-0000-000000000000", "00000000-0000-0001-0000-000000000000", "ffffffff-ffff-ffff-0000-000000000000", "00000000-0000-0000-0000-000000000001", "00000000-0000-0000-0001-000000000000", "00000000-0000-0000-0100-000000000000"}
	for i := range ids {
		for j := range ids {
			a, _ := MigrationKey(tab, MigrationRow{"run_id": uuid.MustParse(ids[i])})
			b, _ := MigrationKey(tab, MigrationRow{"run_id": uuid.MustParse(ids[j])})
			got, e := CompareMigrationKeys(tab, a, b)
			if e != nil || (i < j && got >= 0) || (i == j && got != 0) || (i > j && got <= 0) {
				t.Fatalf("%s vs %s: %d %v", ids[i], ids[j], got, e)
			}
		}
	}
}
