package migrate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"

	"github.com/vphoenix/crypto-market-info/internal/storage/mysql"
)

// SnapshotManifest pins a reviewed, independent bootstrap snapshot. It does not
// change the mapping family, source identity, or complete-history scope.
type SnapshotManifest struct {
	Version uint16          `json:"version"`
	Tables  []SnapshotTable `json:"tables"`
}
type SnapshotTable struct {
	Family        string `json:"family"`
	Table         string `json:"table"`
	Database      string `json:"snapshot_database"`
	Name          string `json:"snapshot_table"`
	SourceDDLHash string `json:"source_ddl_sha256"`
	DDLHash       string `json:"snapshot_ddl_sha256"`
	Rows          uint64 `json:"rows"`
}

var snapshotIdentifier = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]{0,63}$`)

func loadSnapshot(path string) (map[string]SnapshotTable, [32]byte, error) {
	if !filepath.IsAbs(path) {
		return nil, [32]byte{}, fmt.Errorf("absolute snapshot manifest path required")
	}
	data, e := os.ReadFile(path)
	if e != nil {
		return nil, [32]byte{}, e
	}
	if len(data) > 1<<20 {
		return nil, [32]byte{}, fmt.Errorf("snapshot manifest too large")
	}
	var manifest SnapshotManifest
	if e = json.Unmarshal(data, &manifest); e != nil {
		return nil, [32]byte{}, e
	}
	if manifest.Version != 1 || len(manifest.Tables) == 0 || len(manifest.Tables) > 59 {
		return nil, [32]byte{}, fmt.Errorf("invalid bootstrap snapshot manifest")
	}
	out := map[string]SnapshotTable{}
	families := map[string]bool{"crypto_market_info": true, "crypto_market_info_across": true, "crypto_market_info_reserve": true, "crypto_market_info_lst": true, "crypto_market_info_justlend_keeper": true}
	for _, v := range manifest.Tables {
		if !families[v.Family] {
			return nil, [32]byte{}, fmt.Errorf("snapshot family is outside production startup scope")
		}
		t, e := mysql.Mapping(v.Family, v.Table)
		if e != nil {
			return nil, [32]byte{}, e
		}
		allowed, e := BootstrapTables(v.Family)
		if e != nil {
			return nil, [32]byte{}, e
		}
		found := false
		for _, name := range allowed {
			if name == v.Table {
				found = true
			}
		}
		if !found || !snapshotIdentifier.MatchString(v.Database) || !snapshotIdentifier.MatchString(v.Name) || v.Database == v.Family || v.SourceDDLHash != t.SourceDDLHash {
			return nil, [32]byte{}, fmt.Errorf("snapshot outside bootstrap closure or identity mismatch")
		}
		h, e := hex.DecodeString(v.DDLHash)
		if e != nil || len(h) != 32 {
			return nil, [32]byte{}, fmt.Errorf("invalid snapshot DDL digest")
		}
		key := v.Family + "." + v.Table
		if _, ok := out[key]; ok {
			return nil, [32]byte{}, fmt.Errorf("duplicate snapshot table")
		}
		out[key] = v
	}
	return out, sha256.Sum256(data), nil
}
func (s *Source) tableSQL(t mysql.Table) (string, error) {
	if s.snapshot == nil {
		return qualified(t), nil
	}
	v, ok := s.snapshot[t.Database+"."+t.Name]
	if !ok {
		return "", fmt.Errorf("table absent from pinned bootstrap snapshot")
	}
	return "`" + v.Database + "`.`" + v.Name + "`", nil
}
func (s *Source) checkSnapshot(ctx context.Context, t mysql.Table) error {
	if s.snapshot == nil {
		return nil
	}
	v, ok := s.snapshot[t.Database+"."+t.Name]
	if !ok {
		return fmt.Errorf("table absent from pinned bootstrap snapshot")
	}
	var ddl string
	if e := s.conn.QueryRow(ctx, "SELECT create_table_query FROM system.tables WHERE database=? AND name=?", v.Database, v.Name).Scan(&ddl); e != nil {
		return e
	}
	h := sha256.Sum256([]byte(ddl))
	if hex.EncodeToString(h[:]) != v.DDLHash {
		return fmt.Errorf("bootstrap snapshot DDL changed")
	}
	return nil
}
func configInventoryHash(c Config) ([32]byte, error) {
	base, e := catalogHash()
	if e != nil {
		return base, e
	}
	if c.Source.SnapshotFile == "" {
		return base, nil
	}
	_, snapshot, e := loadSnapshot(c.Source.SnapshotFile)
	if e != nil {
		return [32]byte{}, e
	}
	return sha256.Sum256(append(base[:], snapshot[:]...)), nil
}
