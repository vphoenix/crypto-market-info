package migrate

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/vphoenix/crypto-market-info/internal/storage/codec"
	"github.com/vphoenix/crypto-market-info/internal/storage/mysql"
)

// Redirect only the source table and DDL lookup to literal SELECT fixtures.
// Freeze, the actual driver binding, source row decoding, and disk checkpoints
// all run normally. No ClickHouse table is created or changed.
type literalSourceConn struct {
	driver.Conn
	table        mysql.Table
	fixture, ddl string
	t            *testing.T
}

func (c literalSourceConn) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	if query != "SELECT create_table_query FROM system.tables WHERE database=? AND name=?" || len(args) != 2 || args[0] != c.table.Database || args[1] != c.table.Name {
		c.t.Fatalf("unexpected fixture metadata query: %s", query)
	}
	return c.Conn.QueryRow(ctx, "SELECT ?", c.ddl)
}
func (c literalSourceConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	from := " FROM " + qualified(c.table)
	if strings.Count(query, from) != 1 {
		return nil, fmt.Errorf("unexpected fixture source query")
	}
	query = strings.Replace(query, from, " FROM ("+c.fixture+")", 1)
	// Literal SELECTs have no MergeTree PREWHERE stage. Execute the unchanged
	// bounds and bindings as WHERE; real snapshot tests cover PREWHERE itself.
	query = strings.Replace(query, " PREWHERE ", " WHERE ", 1)
	return c.Conn.Query(ctx, query, args...)
}

func TestFreezeExactBoundsPaginationAndResume(t *testing.T) {
	if os.Getenv("CLICKHOUSE_SOURCE_TEST") != "1" {
		t.Skip("read-only ClickHouse literal fixtures")
	}
	ctx := context.Background()
	native, e := OpenSource(ctx, SourceConfig{Address: "127.0.0.1:9000"})
	if e != nil {
		t.Fatal(e)
	}
	defer native.Close()
	cases := []struct {
		name, typ string
		keys      []string
	}{
		{"uint256", "UInt256", []string{"toUInt256('18446744073709551615')", "toUInt256('18446744073709551616')", "toUInt256('115792089237316195423570985008687907853269984665640564039457584007913129639934')", "toUInt256('115792089237316195423570985008687907853269984665640564039457584007913129639935')"}},
		{"milliseconds", "DateTime64(3, 'UTC')", []string{"toDateTime64('2026-10-01 00:00:00.001',3,'UTC')", "toDateTime64('2026-10-01 00:00:00.002',3,'UTC')", "toDateTime64('2026-10-01 00:00:00.999',3,'UTC')", "toDateTime64('2026-10-01 00:00:01.000',3,'UTC')"}},
		{"microseconds", "DateTime64(6, 'UTC')", []string{"toDateTime64('2026-10-01 00:00:00.123001',6,'UTC')", "toDateTime64('2026-10-01 00:00:00.123002',6,'UTC')", "toDateTime64('2026-10-01 00:00:00.123999',6,'UTC')", "toDateTime64('2026-10-01 00:00:00.124000',6,'UTC')"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			typ, e := codec.Parse(tc.typ)
			if e != nil {
				t.Fatal(e)
			}
			tab := mysql.Table{Database: "literal_fixture", Name: "bounds", Engine: "MergeTree", Key: []string{"key"}, Columns: []mysql.Column{{Name: "key", SourceType: tc.typ, Type: typ}}}
			if tc.name == "microseconds" {
				wide, err := codec.Parse("UInt256")
				if err != nil {
					t.Fatal(err)
				}
				tab.Columns = append(tab.Columns, mysql.Column{Name: "deposit", SourceType: "UInt256", Type: wide})
				tab.Key = []string{"deposit", "key"}
			}
			ddl := "literal fixture " + tc.typ
			hash := sha256.Sum256([]byte(ddl))
			tab.SourceDDLHash = hex.EncodeToString(hash[:])
			// An identical pair tests WITH TIES and preservation of physical repeats.
			fixture := "SELECT arrayJoin([" + strings.Join([]string{tc.keys[0], tc.keys[0], tc.keys[1], tc.keys[2], tc.keys[3]}, ",") + "]) AS key"
			if tc.name == "microseconds" {
				fixture += ", toUInt256('115792089237316195423570985008687907853269984665640564039457584007913129639935') AS deposit"
			}
			source := &Source{conn: literalSourceConn{Conn: native.conn, table: tab, fixture: fixture, ddl: ddl, t: t}}
			want := []mysql.MigrationRow{}
			rows, e := native.conn.Query(ctx, fixture+" ORDER BY tuple(key)")
			if e != nil {
				t.Fatal(e)
			}
			for rows.Next() {
				row, e := readSourceRow(rows, tab)
				if e != nil {
					rows.Close()
					t.Fatal(e)
				}
				want = append(want, row)
			}
			e = rows.Err()
			rows.Close()
			if e != nil {
				t.Fatal(e)
			}
			if len(want) != 5 {
				t.Fatal("incomplete fixture")
			}
			// Upper bounds depend only on key presence, even with physical
			// replacement duplicates. Key-only planning must also work without
			// FINAL support (this literal fixture has no MergeTree FINAL stage).
			for _, engine := range []string{"MergeTree", "ReplacingMergeTree"} {
				boundaryTable := tab
				boundaryTable.Engine = engine
				boundary, err := source.UpperKey(ctx, boundaryTable)
				if err != nil {
					t.Fatal(engine, err)
				}
				expected, err := mysql.MigrationKey(tab, want[len(want)-1])
				if err != nil || !bytes.Equal(boundary, expected) {
					t.Fatal(engine, "upper boundary changed", err)
				}
			}
			// Freeze below the real maximum so upper exclusion is exercised too.
			upper, e := mysql.MigrationKey(tab, want[3])
			if e != nil {
				t.Fatal(e)
			}
			var start []byte
			observed := []mysql.MigrationRow{}
			directory := t.TempDir()
			for page := 0; page < 4; page++ {
				chunk, e := source.Freeze(ctx, tab, start, upper, 1, directory)
				if e != nil {
					t.Fatal(e)
				}
				next, close, e := chunk.Reader()
				if e != nil {
					t.Fatal(e)
				}
				got := []mysql.MigrationRow{}
				for {
					row, e := next()
					if e == io.EOF {
						break
					}
					if e != nil {
						close()
						t.Fatal(e)
					}
					got = append(got, row)
				}
				close()
				expected := uint64(1)
				if page == 0 {
					expected = 2
				}
				if page == 3 {
					expected = 0
				}
				if chunk.Rows != expected || uint64(len(got)) != expected {
					t.Fatalf("page %d repeated or omitted keys: %d", page, chunk.Rows)
				}
				if !bytes.Equal(chunk.Start, start) {
					t.Fatal("checkpoint start changed")
				}
				observed = append(observed, got...)
				// Repeating from the same saved checkpoint must produce the same chunk.
				retry, e := source.Freeze(ctx, tab, start, upper, 1, directory)
				if e != nil {
					t.Fatal(e)
				}
				if retry.Hash != chunk.Hash || retry.Rows != chunk.Rows || !bytes.Equal(retry.End, chunk.End) {
					t.Fatal("resume changed chunk identity")
				}
				if e = retry.Close(); e != nil {
					t.Fatal(e)
				}
				if page < 3 {
					start = bytes.Clone(chunk.End)
				}
				if e = chunk.Close(); e != nil {
					t.Fatal(e)
				}
			}
			if !bytes.Equal(start, upper) || len(observed) != 4 {
				t.Fatal("frozen range incomplete")
			}
			for i, row := range observed {
				got, e := mysql.PackMigrationRow(tab, row)
				if e != nil {
					t.Fatal(e)
				}
				expected, e := mysql.PackMigrationRow(tab, want[i])
				if e != nil {
					t.Fatal(e)
				}
				if !bytes.Equal(got, expected) {
					t.Fatalf("row %d changed", i)
				}
			}
		})
	}
}
