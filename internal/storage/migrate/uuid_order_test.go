package migrate

import (
	"context"
	"crypto/sha256"
	"fmt"
	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/storage/codec"
	"github.com/vphoenix/crypto-market-info/internal/storage/mysql"
	"os"
	"strings"
	"testing"
)

func TestUUIDKeyOrderingMatchesClickHouseAcrossBothHalves(t *testing.T) {
	if os.Getenv("CLICKHOUSE_SOURCE_TEST") != "1" {
		t.Skip("read-only literal source test")
	}
	source, e := OpenSource(context.Background(), SourceConfig{Address: "127.0.0.1:9000"})
	if e != nil {
		t.Fatal(e)
	}
	defer source.Close()
	values := []string{}
	add := func(id uuid.UUID) { values = append(values, "toUUID('"+id.String()+"')") }
	add(uuid.Nil)
	for i := 0; i < 16; i++ {
		var id uuid.UUID
		id[i] = 1
		add(id)
		id[i] = 255
		add(id)
	}
	for i := 0; i < 64; i++ {
		hash := sha256.Sum256([]byte(fmt.Sprintf("uuid-order-%d", i)))
		var id uuid.UUID
		copy(id[:], hash[:])
		add(id)
	}
	typ, _ := codec.Parse("UUID")
	tab := mysql.Table{Key: []string{"run_id"}, Columns: []mysql.Column{{Name: "run_id", Type: typ, SourceType: "UUID"}}}
	rows, e := source.conn.Query(context.Background(), "SELECT arrayJoin(["+strings.Join(values, ",")+"]) AS run_id ORDER BY tuple(run_id)")
	if e != nil {
		t.Fatal(e)
	}
	defer rows.Close()
	var previous []byte
	count := 0
	for rows.Next() {
		row, e := readSourceRow(rows, tab)
		if e != nil {
			t.Fatal(e)
		}
		key, e := mysql.MigrationKey(tab, row)
		if e != nil {
			t.Fatal(e)
		}
		if previous != nil {
			cmp, e := mysql.CompareMigrationKeys(tab, previous, key)
			if e != nil || cmp >= 0 {
				t.Fatal("source/local ordering differ", row, cmp, e)
			}
		}
		previous = key
		count++
	}
	if e = rows.Err(); e != nil || count != len(values) {
		t.Fatal("incomplete source comparison", count, e)
	}
}
