package mysql

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/model"
)

func sourceBenchRows(first, count int) ([]publicationRecord, []record) {
	pubs := []publicationRecord{}
	proofs := []record{}
	for n := first; n < first+count; n++ {
		h := sha256.Sum256([]byte(fmt.Sprint(n)))
		id := uuid.NewSHA1(uuid.NameSpaceOID, h[:]).String()
		ms := int64(1792000000000 + n*10)
		p := publicationRecord{Kind: "funding_forecast_observation", ID: id, Hash: fmt.Sprintf("%x", h), Codec: model.DiscoveryCodec, SourceMS: ms, ObservedMS: ms, KnownMS: ms, InstrumentID: uint32(n%332 + 1), SourceCount: 4, ProvenanceHash: fmt.Sprintf("%x", sha256.Sum256(h[:]))}
		pubs = append(pubs, p)
		for j := 0; j < 4; j++ {
			r, _ := namedRecord("source_kind,source_id,source_order,source_url,payload_hash,time_basis,source_ms,observed_ms", []any{p.Kind, id, uint16(j), fmt.Sprintf("https://www.okx.com/api/v5/public/source-%d?instId=BTC-USDT-SWAP", j), p.Hash, "exchange_ts", ms, ms})
			proofs = append(proofs, r)
		}
	}
	return pubs, proofs
}

func sourceBenchWrite(ctx context.Context, c *Client, first, count int) error {
	pubs, proofs := sourceBenchRows(first, count)
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = c.discoveryIntents(ctx, tx, pubs); err != nil {
		return err
	}
	if err = c.putProvenanceRows(ctx, tx, proofs); err != nil {
		return err
	}
	if err = c.publishDiscoveryRows(ctx, tx, pubs); err != nil {
		return err
	}
	return tx.Commit()
}

func sourceBenchReadBytes(ctx context.Context, db *sql.DB) (uint64, error) {
	var n uint64
	err := db.QueryRowContext(ctx, "SELECT COALESCE(SUM(SUM_NUMBER_OF_BYTES_READ),0) FROM performance_schema.file_summary_by_instance WHERE EVENT_NAME='wait/io/file/innodb/innodb_data_file'").Scan(&n)
	return n, err
}

// Explicit, synthetic layout stress test. Its isolated 128 MiB buffer pool is
// intentionally smaller than the historical working set; it is not live TPS.
func TestSourceStoragePhysicalComparison(t *testing.T) {
	out := os.Getenv("CMI_SOURCE_BENCH_OUTPUT")
	if out == "" {
		t.Skip("explicit isolated comparison required")
	}
	if !strings.HasPrefix(out, "/home/ubuntu/crypto-market-info/var/storage/io-spike-fix-") || os.Getenv("MYSQL_INTEGRATION") != "1" || !strings.HasSuffix(os.Getenv("CMI_MYSQL_TEST_SOCKET"), "/write-backlog-improvement/bench-mysql/run/mysql.sock") {
		t.Fatal("explicit isolated target required")
	}
	ctx := context.Background()
	db, err := Connect(ctx, Config{Socket: os.Getenv("CMI_MYSQL_TEST_SOCKET"), Username: os.Getenv("CMI_MYSQL_TEST_USER"), PasswordFile: os.Getenv("CMI_MYSQL_TEST_PASSWORD_FILE"), MaxConnections: 2, ReadTimeout: 2 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var server, dwb string
	var pool uint64
	var binlog, sync int
	if err = db.QueryRowContext(ctx, "SELECT @@server_uuid,@@innodb_buffer_pool_size,@@log_bin,@@innodb_doublewrite,@@innodb_flush_log_at_trx_commit").Scan(&server, &pool, &binlog, &dwb, &sync); err != nil || server != "a93e84b2-c7c3-11f1-bf43-7ab521822a4d" || binlog != 0 || dwb != "ON" || sync != 1 {
		t.Fatal("isolated durability identity", err)
	}
	if _, err = db.ExecContext(ctx, "SET GLOBAL innodb_buffer_pool_size=134217728"); err != nil {
		t.Fatal(err)
	}
	defer db.ExecContext(context.Background(), fmt.Sprintf("SET GLOBAL innodb_buffer_pool_size=%d", pool))
	results := []map[string]any{}
	for _, variant := range []string{"legacy", "append"} {
		name := "cmi_source_bench_" + strings.ReplaceAll(uuid.NewString(), "-", "")
		if _, err = db.ExecContext(ctx, "CREATE DATABASE `"+name+"`"); err != nil {
			t.Fatal(err)
		}
		c := &Client{db: db, database: name, family: "crypto_market_info"}
		for _, table := range sourceStorageNames {
			def, _ := c.mapping(table)
			if _, err = db.ExecContext(ctx, strings.Replace(def.DDL, "`crypto_market_info`.", "`"+name+"`.", 1)); err != nil {
				t.Fatal(err)
			}
		}
		// Identical legacy history remains present in both cases. The candidate
		// still has to rule out legacy identity conflicts for every new row.
		for first := 0; first < 40000; first += 200 {
			if err = sourceBenchWrite(ctx, c, first, 200); err != nil {
				t.Fatal("seed", err)
			}
		}
		if variant == "append" {
			if err = InstallSourceStorageSchema(ctx, db, name); err != nil {
				t.Fatal(err)
			}
			c.sourceStorageV2 = true
			c.sourceStorageAvailable = true
		}
		if err = qualityBenchDrain(ctx, db); err != nil {
			t.Fatal(err)
		}
		before, err := qualityBenchCounters(ctx, db)
		if err != nil {
			t.Fatal(err)
		}
		readBefore, err := sourceBenchReadBytes(ctx, db)
		if err != nil {
			t.Fatal(err)
		}
		allocatedBefore, err := qualityBenchAllocated(ctx, db, name)
		if err != nil {
			t.Fatal(err)
		}
		latencies := []float64{}
		start := time.Now()
		for first := 40000; first < 52000; first += 32 {
			a := time.Now()
			if err = sourceBenchWrite(ctx, c, first, 32); err != nil {
				t.Fatal("measured write", err)
			}
			latencies = append(latencies, float64(time.Since(a))/float64(time.Millisecond))
		}
		elapsed := time.Since(start).Seconds()
		if err = qualityBenchDrain(ctx, db); err != nil {
			t.Fatal(err)
		}
		after, err := qualityBenchCounters(ctx, db)
		if err != nil {
			t.Fatal(err)
		}
		readAfter, err := sourceBenchReadBytes(ctx, db)
		if err != nil {
			t.Fatal(err)
		}
		allocated, err := qualityBenchAllocated(ctx, db, name)
		if err != nil {
			t.Fatal(err)
		}
		slices.Sort(latencies)
		readStart := time.Now()
		// Every logical field of every measured row is read back, not just counts.
		for first := 40000; first < 52000; first += 32 {
			pubs, proofs := sourceBenchRows(first, 32)
			for _, name := range sourceStorageNames {
				tab, _ := c.mapping(name)
				keys := proofs
				if name != "public_observation_provenance" {
					keys = nil
					for _, p := range pubs {
						keys = append(keys, record{"source_kind": p.Kind, "source_id": p.ID})
					}
				}
				where, args, e := sourceIdentityPredicate(tab, keys)
				if e != nil {
					t.Fatal(e)
				}
				stored, e := c.sourceRecords(ctx, db, name, where, args...)
				if e != nil || len(stored) != len(keys) {
					t.Fatal("read count", name, len(stored), e)
				}
				if name == "public_observation_provenance" {
					want := map[[32]byte][32]byte{}
					for _, r := range proofs {
						k, _ := keyHash(tab, r)
						h, _ := rowHash(tab, r)
						want[k] = h
					}
					for _, r := range stored {
						k, _ := keyHash(tab, r)
						h, _ := rowHash(tab, r)
						if want[k] != h {
							t.Fatal("proof changed")
						}
						delete(want, k)
					}
					if len(want) != 0 {
						t.Fatal("proof duplicated")
					}
				} else {
					want := map[string]publicationRecord{}
					for _, p := range pubs {
						want[p.ID] = p
					}
					for _, r := range stored {
						p, e := scanPublication(r)
						if e != nil || want[p.ID] != p {
							t.Fatal("publication changed", e)
						}
						delete(want, p.ID)
						if name == "source_batch_status" && (r["complete"] != uint8(1) || r["published_ms"].(int64) < p.KnownMS) {
							t.Fatal("invalid commit")
						}
					}
					if len(want) != 0 {
						t.Fatal("publication duplicated")
					}
				}
			}
		}
		r := map[string]any{"variant": variant, "seed_identities": 40000, "measured_identities": 12000, "logical_rows": 72000, "seconds": elapsed, "commit_p95_ms": latencies[len(latencies)*95/100], "commit_p99_ms": latencies[len(latencies)*99/100], "data_read_bytes": readAfter - readBefore, "data_write_bytes": after["datafile"] - before["datafile"], "redo_bytes": after["Innodb_os_log_written"] - before["Innodb_os_log_written"], "doublewrite_bytes": after["doublewrite"] - before["doublewrite"], "allocated_increment": allocated - allocatedBefore, "exact_read_seconds": time.Since(readStart).Seconds()}
		results = append(results, r)
		t.Logf("%+v", r)
		raw, _ := json.MarshalIndent(results, "", "  ")
		if err = os.WriteFile(out+"/source-layout-comparison.json", append(raw, '\n'), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err = db.ExecContext(ctx, "DROP DATABASE `"+name+"`"); err != nil {
			t.Fatal(err)
		}
		if _, err = db.ExecContext(ctx, "DELETE FROM cmi_storage.schema_version WHERE database_name=?", name); err != nil {
			t.Fatal(err)
		}
	}
}
