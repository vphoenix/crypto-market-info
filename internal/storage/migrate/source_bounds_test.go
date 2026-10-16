package migrate

import (
	"math/big"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/storage/codec"
	"github.com/vphoenix/crypto-market-info/internal/storage/mysql"
)

func TestSourceBoundsPreserveCompositeTypesAndUTC(t *testing.T) {
	// Deliberately keep physical column order different from cursor key order.
	tab := mysql.Table{Key: []string{"deposit", "micro", "milli"}}
	for _, c := range []struct{ name, typ string }{{"milli", "DateTime64(3, 'UTC')"}, {"micro", "DateTime64(6, 'UTC')"}, {"deposit", "UInt256"}} {
		typ, e := codec.Parse(c.typ)
		if e != nil {
			t.Fatal(e)
		}
		tab.Columns = append(tab.Columns, mysql.Column{Name: c.name, SourceType: c.typ, Type: typ})
	}
	local := time.FixedZone("fixture-UTC+8", 8*3600)
	at := time.Date(2026, 10, 1, 8, 0, 0, 123456000, local)
	n := new(big.Int).Lsh(big.NewInt(1), 255)
	start, e := mysql.MigrationKey(tab, mysql.MigrationRow{"deposit": *n, "micro": at, "milli": at.Truncate(time.Millisecond)})
	if e != nil {
		t.Fatal(e)
	}
	n.Add(n, big.NewInt(1))
	upper, e := mysql.MigrationKey(tab, mysql.MigrationRow{"deposit": *n, "micro": at.Add(time.Microsecond), "milli": at.Truncate(time.Millisecond).Add(time.Millisecond)})
	if e != nil {
		t.Fatal(e)
	}
	where, args, e := sourceBounds(tab, start, upper)
	if e != nil {
		t.Fatal(e)
	}
	if strings.Count(where, "toUInt256(?)") != 2 || strings.Count(where, "toDateTime64(?, 6, 'UTC')") != 2 || strings.Count(where, "toDateTime64(?, 3, 'UTC')") != 2 {
		t.Fatal("both bounds must bind every type", where)
	}
	first := new(big.Int).Sub(n, big.NewInt(1))
	want := []any{first.String(), "2026-10-01 00:00:00.123456", "2026-10-01 00:00:00.123", n.String(), "2026-10-01 00:00:00.123457", "2026-10-01 00:00:00.124"}
	if !reflect.DeepEqual(args, want) {
		t.Fatal("composite cursor lost precision/order/UTC", args)
	}
	for _, bad := range [][]byte{nil, start[:len(start)-1], append(append([]byte{}, start...), 0)} {
		if _, _, e := sourceBounds(tab, nil, bad); e == nil {
			t.Fatal("malformed checkpoint accepted")
		}
	}
	if _, _, e := sourceBounds(tab, start[:len(start)-1], upper); e == nil {
		t.Fatal("malformed lower checkpoint accepted")
	}
	tab.Key = append(tab.Key, "missing")
	if _, _, e := sourceBounds(tab, nil, upper); e == nil {
		t.Fatal("unmapped key accepted")
	}
}
