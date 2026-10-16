package mysql

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/dex"
)

func TestDEXGapStreamingEquivalentWithoutTemporaryTables(t *testing.T) {
	if os.Getenv("MYSQL_INTEGRATION") != "1" {
		t.Skip("isolated MySQL required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	db, err := Connect(ctx, Config{Socket: os.Getenv("CMI_MYSQL_TEST_SOCKET"), Username: os.Getenv("CMI_MYSQL_TEST_USER"), PasswordFile: os.Getenv("CMI_MYSQL_TEST_PASSWORD_FILE"), MaxConnections: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	target := "cmi_test_gap_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err = db.ExecContext(ctx, "CREATE DATABASE `"+target+"`"); err != nil {
		t.Fatal(err)
	}
	defer db.ExecContext(context.Background(), "DROP DATABASE `"+target+"`")
	c := &Client{db: db, database: target, family: "crypto_market_info"}
	table := c.table("dex_block")
	// Only the projection used by the gap query is needed. Distinct IDs allow
	// duplicate heights, as can occur with competing historical block hashes.
	_, err = db.ExecContext(ctx, "CREATE TABLE "+table+" (id INT AUTO_INCREMENT PRIMARY KEY,chain_id BIGINT UNSIGNED NOT NULL,manifest_hash BINARY(32) NOT NULL,canonical TINYINT NOT NULL,committed TINYINT NOT NULL,block_number BIGINT UNSIGNED NOT NULL,KEY cmi_dex_committed(chain_id,manifest_hash,canonical,committed,block_number)) ENGINE=InnoDB")
	if err != nil {
		t.Fatal(err)
	}
	m := dex.ObjectHash("streaming-gap")
	oldQuery := "SELECT COALESCE(MIN(block_number+1),0) FROM (SELECT block_number,LEAD(block_number) OVER(ORDER BY block_number) AS next_block FROM " + table + " FORCE INDEX (`cmi_dex_committed`) WHERE chain_id=1 AND manifest_hash=? AND canonical=1 AND committed=1) AS coverage WHERE next_block>block_number+1"
	tempTables := func() uint64 {
		t.Helper()
		var name string
		var count uint64
		if e := db.QueryRowContext(ctx, "SHOW SESSION STATUS LIKE 'Created_tmp_tables'").Scan(&name, &count); e != nil {
			t.Fatal(e)
		}
		return count
	}
	cases := []struct {
		name    string
		heights []uint64
		want    uint64
	}{
		{"empty", nil, 0}, {"one", []uint64{42}, 0},
		{"contiguous", []uint64{42, 43, 44}, 0},
		{"earliest_of_multiple", []uint64{42, 44, 49}, 43},
		{"duplicate_heights", []uint64{42, 42, 44, 44}, 43},
		{"unordered_insert", []uint64{44, 42, 43, 47}, 45},
	}
	large := make([]uint64, 5000)
	for i := range large {
		large[i] = uint64(i + 100)
	}
	cases = append(cases, struct {
		name    string
		heights []uint64
		want    uint64
	}{"large_contiguous", large, 0})
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, e := db.ExecContext(ctx, "DELETE FROM "+table); e != nil {
				t.Fatal(e)
			}
			for start := 0; start < len(tc.heights); start += 500 {
				part := tc.heights[start:min(start+500, len(tc.heights))]
				args := make([]any, 0, len(part)*5)
				values := make([]string, len(part))
				for i, h := range part {
					values[i] = "(?,?,?,?,?)"
					args = append(args, uint64(1), m[:], uint8(1), uint8(1), h)
				}
				if _, e := db.ExecContext(ctx, "INSERT INTO "+table+" (chain_id,manifest_hash,canonical,committed,block_number) VALUES "+strings.Join(values, ","), args...); e != nil {
					t.Fatal(e)
				}
			}
			// These rows would fill or introduce gaps if any scope predicate
			// were lost. They must not influence either implementation.
			other := dex.ObjectHash("other-manifest")
			if _, e := db.ExecContext(ctx, "INSERT INTO "+table+" (chain_id,manifest_hash,canonical,committed,block_number) VALUES (1,?,0,1,43),(1,?,1,0,43),(2,?,1,1,43),(1,?,1,1,43)", m[:], m[:], m[:], other[:]); e != nil {
				t.Fatal(e)
			}
			before := tempTables()
			var old uint64
			if e := db.QueryRowContext(ctx, oldQuery, m[:]).Scan(&old); e != nil {
				t.Fatal(e)
			}
			afterOld := tempTables()
			got, e := c.DEXGapStart(ctx, m)
			afterNew := tempTables()
			if e != nil || old != tc.want || got != old {
				t.Fatalf("old=%d new=%d want=%d error=%v", old, got, tc.want, e)
			}
			if afterOld <= before || afterNew != afterOld {
				t.Fatalf("temporary tables before=%d old=%d new=%d", before, afterOld, afterNew)
			}
		})
	}
}
