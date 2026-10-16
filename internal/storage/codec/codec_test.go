package codec

import (
	"bytes"
	"encoding/json"
	"math"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

func fixture(t *Type) any {
	switch t.Name {
	case "Nullable", "LowCardinality":
		return fixture(t.Children[0])
	case "Array":
		return []any{fixture(t.Children[0]), fixture(t.Children[0])}
	case "Tuple":
		m := map[string]any{}
		for i, n := range t.Fields {
			m[n] = fixture(t.Children[i])
		}
		return m
	case "UInt8":
		return uint8(255)
	case "UInt16":
		return uint16(65535)
	case "UInt32":
		return uint32(math.MaxUint32)
	case "UInt64":
		return uint64(math.MaxUint64)
	case "Int32":
		return int32(math.MinInt32)
	case "Int64":
		return int64(math.MinInt64)
	case "UInt256":
		return *new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))
	case "Decimal":
		return decimal.RequireFromString("-99999999999999999999.999999999999999999")
	case "String":
		return "a\x00\xff"
	case "FixedString":
		return string(bytes.Repeat([]byte{0xff}, t.Size-1)) + "\x00"
	case "DateTime", "DateTime64":
		ts := time.Date(2026, 10, 12, 12, 13, 14, 0, time.UTC)
		if t.Scale == 3 {
			ts = ts.Add(123 * time.Millisecond)
		} else if t.Scale == 6 {
			ts = ts.Add(123456 * time.Microsecond)
		}
		return ts
	case "Enum8":
		for n := range t.Enums {
			return n
		}
	case "Bool":
		return true
	case "UUID":
		return uuid.MustParse("ffeeddcc-bbaa-9988-7766-554433221100")
	}
	panic(t.Text)
}

func TestAllInventoryTypesRoundTrip(t *testing.T) {
	data, e := os.ReadFile("../../../docs/redis-mysql-migration/source-columns.json")
	if e != nil {
		t.Fatal(e)
	}
	var cols []struct {
		Type    string `json:"type"`
		Default string `json:"default_kind"`
	}
	if e = json.Unmarshal(data, &cols); e != nil {
		t.Fatal(e)
	}
	seen := map[string]bool{}
	for _, c := range cols {
		if seen[c.Type] {
			continue
		}
		seen[c.Type] = true
		t.Run(c.Type, func(t *testing.T) {
			typ, e := Parse(c.Type)
			if e != nil {
				t.Fatal(e)
			}
			in, e := Encode(typ, fixture(typ))
			if e != nil {
				t.Fatal(e)
			}
			v, e := Decode(typ, in)
			if e != nil {
				t.Fatal(e)
			}
			out, e := Encode(typ, v)
			if e != nil || !bytes.Equal(in, out) {
				t.Fatalf("lossy round trip: %v", e)
			}
			for n := 0; n < len(in); n++ {
				if _, e = Decode(typ, in[:n]); e == nil {
					t.Fatalf("accepted truncation %d", n)
				}
			}
			if _, e = Decode(typ, append(in, 0)); e == nil {
				t.Fatal("accepted trailing byte")
			}
		})
	}
}

func TestRejectLossyValues(t *testing.T) {
	for _, v := range []struct {
		typ   string
		value any
	}{
		{"UInt8", uint16(256)}, {"Int32", int64(math.MaxInt64)}, {"UInt64", float64(1)},
		{"UInt256", *big.NewInt(-1)}, {"UInt256", *new(big.Int).Lsh(big.NewInt(1), 256)},
		{"Decimal(38, 18)", decimal.RequireFromString("0.0000000000000000001")},
		{"Decimal(38, 18)", decimal.RequireFromString("100000000000000000000")},
		{"FixedString(4)", "a"}, {"DateTime64(6, 'UTC')", time.Now().Truncate(time.Microsecond).Add(time.Nanosecond)},
		{"Bool", uint8(2)}, {"UInt64", nil},
	} {
		typ, e := Parse(v.typ)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = Encode(typ, v.value); e == nil {
			t.Fatalf("accepted %s %v", v.typ, v.value)
		}
	}
}

func TestNullEmptyBoundsAndHeader(t *testing.T) {
	typ, _ := Parse("Array(Nullable(UInt256))")
	z := *big.NewInt(0)
	a, e := Encode(typ, []any{nil, z})
	if e != nil {
		t.Fatal(e)
	}
	v, e := Decode(typ, a)
	if e != nil {
		t.Fatal(e)
	}
	n := v.([]any)[1].(big.Int)
	if v.([]any)[0] != nil || n.Sign() != 0 {
		t.Fatalf("NULL != zero: %v", e)
	}
	a, _ = Encode(typ, []any{})
	v, e = Decode(typ, a)
	if e != nil || len(v.([]any)) != 0 {
		t.Fatal(e)
	}
	bad := append([]byte(nil), a...)
	bad[0] = 2
	if _, e = Decode(typ, bad); e == nil {
		t.Fatal("version accepted")
	}
	bad = append([]byte(nil), a...)
	bad[1] ^= 1
	if _, e = Decode(typ, bad); e == nil {
		t.Fatal("type accepted")
	}
	copy(a[17:], []byte{0xff, 0xff, 0xff, 0xff})
	if _, e = Decode(typ, a); e == nil {
		t.Fatal("unbounded array")
	}
	if _, e = Parse("Array(Float64)"); e == nil {
		t.Fatal("float accepted")
	}
}

func FuzzDecode(f *testing.F) {
	typ, _ := Parse("Array(Nullable(UInt64))")
	seed, _ := Encode(typ, []any{nil, uint64(math.MaxUint64)})
	f.Add(seed)
	f.Fuzz(func(t *testing.T, b []byte) {
		v, e := Decode(typ, b)
		if e != nil {
			return
		}
		out, e := Encode(typ, v)
		if e != nil || !bytes.Equal(out, b) {
			t.Fatal("noncanonical round trip")
		}
	})
}

func TestExtremeDecimalExponentIsBounded(t *testing.T) {
	typ, e := Parse("Decimal(38,18)")
	if e != nil {
		t.Fatal(e)
	}
	for _, exponent := range []int32{-2147483648, 2147483647} {
		if _, e = Encode(typ, decimal.New(1, exponent)); e == nil {
			t.Fatal("extreme exponent accepted")
		}
	}
}
