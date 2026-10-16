package migrate

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/vphoenix/crypto-market-info/internal/storage/mysql"
)

func TestBootstrapSnapshotScopeAndRunIdentity(t *testing.T) {
	tab, e := mysql.Mapping("crypto_market_info", "instrument")
	if e != nil {
		t.Fatal(e)
	}
	ddl := sha256.Sum256([]byte("fixture snapshot DDL"))
	item := SnapshotTable{Family: tab.Database, Table: tab.Name, Database: "fixture_snapshot", Name: "b001", SourceDDLHash: tab.SourceDDLHash, DDLHash: hex.EncodeToString(ddl[:]), Rows: 1}
	path := filepath.Join(t.TempDir(), "snapshot.json")
	write := func(items []SnapshotTable) {
		data, e := json.Marshal(SnapshotManifest{Version: 1, Tables: items})
		if e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(path, data, 0600); e != nil {
			t.Fatal(e)
		}
	}
	write([]SnapshotTable{item})
	tables, _, e := loadSnapshot(path)
	if e != nil {
		t.Fatal(e)
	}
	s := Source{snapshot: tables}
	name, e := s.tableSQL(tab)
	if e != nil || name != "`fixture_snapshot`.`b001`" {
		t.Fatal(name, e)
	}
	main, e := configInventoryHash(Config{})
	if e != nil {
		t.Fatal(e)
	}
	snapshot, e := configInventoryHash(Config{Source: SourceConfig{SnapshotFile: path}})
	if e != nil || main == snapshot {
		t.Fatal("snapshot did not pin run identity", e)
	}
	for _, kind := range []string{"hot", "duplicate", "ddl", "source_database", "historical_family"} {
		bad := item
		items := []SnapshotTable{bad}
		switch kind {
		case "hot":
			hot, _ := mysql.Mapping("crypto_market_info", "order_book_second_delta")
			bad.Table = hot.Name
			bad.SourceDDLHash = hot.SourceDDLHash
			items = []SnapshotTable{bad}
		case "duplicate":
			items = append(items, bad)
		case "ddl":
			bad.SourceDDLHash = "wrong"
			items = []SnapshotTable{bad}
		case "source_database":
			bad.Database = bad.Family
			items = []SnapshotTable{bad}
		case "historical_family":
			archived, e := mysql.Mapping("crypto_market_info_across_livecheck", "across_capture")
			if e != nil {
				t.Fatal(e)
			}
			bad.Family = archived.Database
			bad.Table = archived.Name
			bad.SourceDDLHash = archived.SourceDDLHash
			items = []SnapshotTable{bad}
		}
		write(items)
		if _, _, e = loadSnapshot(path); e == nil {
			t.Fatal("unsafe snapshot accepted", kind)
		}
	}
}
