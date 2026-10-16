package migrate

import (
	"context"
	"github.com/vphoenix/crypto-market-info/internal/storage/codec"
	"github.com/vphoenix/crypto-market-info/internal/storage/mysql"
	"os"
	"testing"
)

func TestReviewUUIDComparatorMatchesSourceOrder(t *testing.T) {
	if os.Getenv("CLICKHOUSE_SOURCE_TEST") != "1" {
		t.Skip("requires read-only source SELECT")
	}
	ctx := context.Background()
	source, err := OpenSource(ctx, SourceConfig{Address: "127.0.0.1:9000"})
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	typ, err := codec.Parse("UUID")
	if err != nil {
		t.Fatal(err)
	}
	mapping := mysql.Table{Name: "fixture", Key: []string{"run_id"}, Columns: []mysql.Column{{Name: "run_id", SourceType: "UUID", Type: typ}}}
	rows, err := source.conn.Query(ctx, `SELECT arrayJoin([toUUID('00000001-0000-0000-0000-000000000000'),toUUID('00000000-0000-0000-0000-000000000001')]) AS run_id ORDER BY tuple(run_id)`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var prev []byte
	var previous any
	for rows.Next() {
		row, err := readSourceRow(rows, mapping)
		if err != nil {
			t.Fatal(err)
		}
		key, err := mysql.MigrationKey(mapping, row)
		if err != nil {
			t.Fatal(err)
		}
		if prev != nil {
			cmp, err := mysql.CompareMigrationKeys(mapping, prev, key)
			if err != nil || cmp >= 0 {
				t.Fatalf("source ordered %v before %v, comparator=%d err=%v", previous, row["run_id"], cmp, err)
			}
		}
		prev, previous = key, row["run_id"]
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
}
