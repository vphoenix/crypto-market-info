package migrate

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/vphoenix/crypto-market-info/internal/storage/mysql"
)

// Reads only the sealed preparation snapshot. Exercise large arrays, UInt256,
// UUID/time cursors and ordinary MergeTree multiplicity through the real driver.
func TestPreparedSnapshotBoundedFreeze(t *testing.T) {
	root := os.Getenv("CMI_BOOTSTRAP_PREPARATION")
	if root == "" {
		t.Skip("explicit sealed preparation directory required")
	}
	ctx := context.Background()
	s, e := OpenSource(ctx, SourceConfig{Address: "127.0.0.1:9000", SnapshotFile: filepath.Join(root, "snapshot-manifest.json")})
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	for _, name := range []string{"options_collection_plan", "source_publication_intents", "across_deposit"} {
		t.Run(name, func(t *testing.T) {
			family := "crypto_market_info"
			if name == "across_deposit" {
				family = "crypto_market_info_across"
			}
			table, e := mysql.Mapping(family, name)
			if e != nil {
				t.Fatal(e)
			}
			upper, e := s.UpperKey(ctx, table)
			if e != nil {
				t.Fatal(e)
			}
			if len(upper) == 0 {
				t.Fatal("representative snapshot table empty")
			}
			limit := 500
			if name == "options_collection_plan" {
				limit = 50
			}
			first, e := s.Freeze(ctx, table, nil, upper, limit, t.TempDir())
			if e != nil {
				t.Fatal(e)
			}
			defer first.Close()
			if first.Rows == 0 || first.Bytes > 64<<20 {
				t.Fatal("unbounded or empty freeze")
			}
			again, e := s.FreezeRange(ctx, table, nil, first.End, min(limit, 100), t.TempDir())
			if e != nil {
				t.Fatal(e)
			}
			defer again.Close()
			if first.Rows != again.Rows || first.Hash != again.Hash || !bytes.Equal(first.End, again.End) {
				t.Fatal("same complete key range changed")
			}
			next, e := s.Freeze(ctx, table, first.End, upper, limit, t.TempDir())
			if e != nil {
				t.Fatal(e)
			}
			defer next.Close()
			if next.Rows > 0 {
				cmp, e := mysql.CompareMigrationKeys(table, first.End, next.End)
				if e != nil || cmp >= 0 {
					t.Fatal("paging did not advance", e)
				}
			}
		})
	}
}

func TestPreparedSnapshotOldCheckpointRange(t *testing.T) {
	root := os.Getenv("CMI_BOOTSTRAP_PREPARATION")
	if root == "" {
		t.Skip("explicit sealed preparation directory required")
	}
	files, e := filepath.Glob(filepath.Join(root, "staging", "*.typed.manifest.json"))
	if e != nil {
		t.Fatal(e)
	}
	for _, file := range files {
		var saved struct {
			Table, SourceHash string
			Start, End        []byte
			Rows              uint64
		}
		raw, e := os.ReadFile(file)
		if e != nil {
			t.Fatal(e)
		}
		if e = json.Unmarshal(raw, &saved); e != nil {
			t.Fatal(e)
		}
		if saved.Table != "public_observation_provenance" || saved.Rows != 500 {
			continue
		}
		s, e := OpenSource(context.Background(), SourceConfig{Address: "127.0.0.1:9000", SnapshotFile: filepath.Join(root, "snapshot-manifest.json")})
		if e != nil {
			t.Fatal(e)
		}
		defer s.Close()
		table, e := mysql.Mapping("crypto_market_info", saved.Table)
		if e != nil {
			t.Fatal(e)
		}
		chunk, e := s.FreezeRange(context.Background(), table, saved.Start, saved.End, 50, t.TempDir())
		if e != nil {
			t.Fatal(e)
		}
		defer chunk.Close()
		if chunk.Rows != saved.Rows || hex.EncodeToString(chunk.Hash[:]) != saved.SourceHash || !bytes.Equal(chunk.End, saved.End) {
			t.Fatal("new block size redefined the old fixed checkpoint")
		}
		return
	}
	t.Fatal("original failed 500-row checkpoint evidence missing")
}
