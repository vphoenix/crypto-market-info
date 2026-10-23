package mysql

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/options"
	"github.com/vphoenix/crypto-market-info/internal/storage/codec"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
	"github.com/vphoenix/crypto-market-info/internal/storage/spool"
)

type qualityBenchBatch struct {
	Minute            time.Time
	ID                string
	Run               uuid.UUID
	Quality, Evidence [][]byte
}

type qualityBenchResult struct {
	Variant                                                                                   string
	Batches, Members, ActiveMembers, InactiveMembers                                          int
	ElapsedSeconds, CommitP95MS, CommitP99MS, ReadMS                                          float64
	DataBytes, RedoBytes, DoublewriteBytes, TotalBytes, AllocatedBytes, InitialAllocatedBytes uint64
	PagesWritten, Profiles, References                                                        uint64
	DailyAllocatedIncrementEstimate                                                           uint64
}

func qualityBenchCounters(ctx context.Context, db *sql.DB) (map[string]uint64, error) {
	rows, err := db.QueryContext(ctx, "SELECT VARIABLE_NAME,VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME IN ('Innodb_data_written','Innodb_os_log_written','Innodb_dblwr_pages_written')")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]uint64{}
	for rows.Next() {
		var k string
		var v uint64
		if err = rows.Scan(&k, &v); err != nil {
			return nil, err
		}
		out[k] = v
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	for key, event := range map[string]string{"doublewrite": "wait/io/file/innodb/innodb_dblwr_file", "datafile": "wait/io/file/innodb/innodb_data_file"} {
		var value uint64
		if err = db.QueryRowContext(ctx, "SELECT COALESCE(SUM(SUM_NUMBER_OF_BYTES_WRITE),0) FROM performance_schema.file_summary_by_instance WHERE EVENT_NAME=?", event).Scan(&value); err != nil {
			return nil, err
		}
		out[key] = value
	}
	return out, nil
}

func qualityBenchDrain(ctx context.Context, db *sql.DB) error {
	var pct, lwm float64
	if err := db.QueryRowContext(ctx, "SELECT @@innodb_max_dirty_pages_pct,@@innodb_max_dirty_pages_pct_lwm").Scan(&pct, &lwm); err != nil {
		return err
	}
	defer db.ExecContext(context.Background(), fmt.Sprintf("SET GLOBAL innodb_max_dirty_pages_pct=%g,GLOBAL innodb_max_dirty_pages_pct_lwm=%g", pct, lwm))
	if _, err := db.ExecContext(ctx, "SET GLOBAL innodb_max_dirty_pages_pct=0,GLOBAL innodb_max_dirty_pages_pct_lwm=0"); err != nil {
		return err
	}
	for end := time.Now().Add(90 * time.Second); time.Now().Before(end); time.Sleep(time.Second) {
		var dirty uint64
		if err := db.QueryRowContext(ctx, "SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME='Innodb_buffer_pool_pages_dirty'").Scan(&dirty); err != nil {
			return err
		}
		if dirty == 0 {
			return nil
		}
	}
	return fmt.Errorf("isolated dirty pages did not drain")
}

func qualityBenchAllocated(ctx context.Context, db *sql.DB, name string) (uint64, error) {
	var n uint64
	err := db.QueryRowContext(ctx, "SELECT COALESCE(SUM(FILE_SIZE),0) FROM information_schema.INNODB_TABLESPACES WHERE NAME LIKE ?", name+"/%").Scan(&n)
	return n, err
}

// Replays 60 logical minutes of frozen real data. It cannot connect to production.
// No cadence/capacity claim is made: only quality storage I/O and exact reads.
func TestQualityStoragePhysicalComparison(t *testing.T) {
	frames := os.Getenv("CMI_QUALITY_BENCH_FRAMES")
	if frames == "" {
		t.Skip("explicit frozen fixture required")
	}
	if os.Getenv("MYSQL_INTEGRATION") != "1" || !strings.HasSuffix(os.Getenv("CMI_MYSQL_TEST_SOCKET"), "/write-backlog-improvement/bench-mysql/run/mysql.sock") {
		t.Fatal("benchmark requires the pinned isolated server")
	}
	outdir := os.Getenv("CMI_QUALITY_BENCH_OUTPUT")
	if !strings.HasPrefix(outdir, "/home/ubuntu/crypto-market-info/var/storage/quality-storage-") {
		t.Fatal("explicit private output directory required")
	}
	if err := os.MkdirAll(outdir, 0700); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	db, err := Connect(ctx, Config{Socket: os.Getenv("CMI_MYSQL_TEST_SOCKET"), Username: os.Getenv("CMI_MYSQL_TEST_USER"), PasswordFile: os.Getenv("CMI_MYSQL_TEST_PASSWORD_FILE"), MaxConnections: 2, ReadTimeout: 2 * time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var server string
	if err = db.QueryRowContext(ctx, "SELECT @@server_uuid").Scan(&server); err != nil || server != "a93e84b2-c7c3-11f1-bf43-7ab521822a4d" {
		t.Fatal("isolated identity", err)
	}
	var logBin, commitSync int
	var doublewrite string
	if err = db.QueryRowContext(ctx, "SELECT @@log_bin,@@innodb_flush_log_at_trx_commit,@@innodb_doublewrite").Scan(&logBin, &commitSync, &doublewrite); err != nil || logBin != 0 || commitSync != 1 || doublewrite != "ON" {
		t.Fatal("comparison must match production durability/binlog settings", logBin, commitSync, doublewrite, err)
	}
	fixture := filepath.Join(outdir, "quality-input.gob.gz")
	qualityTable, _ := Mapping("crypto_market_info", "derivative_book_quality_minute")
	evidenceTable, _ := Mapping("crypto_market_info", "options_catalog_quality_evidence_minute")
	active, inactive := 0, 0
	var cached struct {
		Frames                    string
		Minutes, Active, Inactive int
		SHA256                    string
	}
	manifestPath := filepath.Join(outdir, "quality-input.json")
	manifestRaw, manifestErr := os.ReadFile(manifestPath)
	if manifestErr == nil {
		if err = json.Unmarshal(manifestRaw, &cached); err != nil || cached.Frames != frames || cached.Minutes != 60 {
			t.Fatal("invalid cached quality fixture", err)
		}
		f, err := os.Open(fixture)
		if err != nil {
			t.Fatal(err)
		}
		h := sha256.New()
		_, err = io.Copy(h, f)
		f.Close()
		if err != nil || fmt.Sprintf("%x", h.Sum(nil)) != cached.SHA256 {
			t.Fatal("cached quality fixture changed", err)
		}
		active, inactive = cached.Active, cached.Inactive
	} else {
		q, err := spool.Open(frames, 20<<30)
		if err != nil {
			t.Fatal(err)
		}
		index, err := q.NewIndex(ctx, 32<<20)
		if err != nil {
			t.Fatal(err)
		}
		defer index.Close()
		for !index.Complete() {
			if err = index.Refresh(ctx); err != nil {
				t.Fatal(err)
			}
		}
		entries := []contracts.Candidate{}
		for _, e := range index.Snapshot() {
			if e.Header.Kind == "options_catalog" {
				entries = append(entries, e)
			}
		}
		slices.SortFunc(entries, func(a, b contracts.Candidate) int {
			if n := a.Header.Minute.Compare(b.Header.Minute); n != 0 {
				return n
			}
			return strings.Compare(a.Header.ID, b.Header.ID)
		})
		if len(entries) == 0 {
			t.Fatal("no catalog input")
		}
		from := entries[0].Header.Minute
		to := from.Add(time.Hour)
		minutes := map[time.Time]bool{}
		file, err := os.Create(fixture)
		if err != nil {
			t.Fatal(err)
		}
		file.Chmod(0600)
		zip, _ := gzip.NewWriterLevel(file, gzip.BestSpeed)
		enc := gob.NewEncoder(zip)
		for _, entry := range entries {
			if !entry.Header.Minute.Before(to) {
				continue
			}
			d, err := q.Load(ctx, entry)
			if err != nil {
				t.Fatal(err)
			}
			e, err := codec.DecodeOptionsCatalog(d.Payload)
			if err != nil {
				t.Fatal(err)
			}
			// Delivery ownership ID includes its scope/generation; the domain envelope
			// has a separate content ID. Both original checksums are validated by Load.
			if !e.Live.MinuteTime.Equal(d.Minute) || len(e.Live.Books) != int(d.Members) {
				t.Fatal("frozen batch membership changed")
			}
			b := qualityBenchBatch{Minute: e.Live.MinuteTime, ID: e.ID(), Run: e.Live.RunID}
			for _, book := range e.Live.Books {
				cols, values, err := compactInsertValues("derivative_book_quality_minute", derivativeQualityColumns, [][]any{derivativeQualityValues(book, b.ID)})
				if err != nil {
					t.Fatal(err)
				}
				r, err := namedRecord(cols, values[0])
				if err != nil {
					t.Fatal(err)
				}
				raw, err := PackMigrationRow(qualityTable, MigrationRow(r))
				if err != nil {
					t.Fatal(err)
				}
				b.Quality = append(b.Quality, raw)
				if book.Minute != nil && book.Minute.DeltaBitmap != 0 {
					active++
				} else {
					inactive++
				}
			}
			for _, evidence := range e.Evidence {
				evidence.BatchID = b.ID
				evidence.RowHash = evidence.Hash()
				r, err := modelRecord(evidence)
				if err != nil {
					t.Fatal(err)
				}
				if err = compactRecord("options_catalog_quality_evidence_minute", r); err != nil {
					t.Fatal(err)
				}
				raw, err := PackMigrationRow(evidenceTable, MigrationRow(r))
				if err != nil {
					t.Fatal(err)
				}
				b.Evidence = append(b.Evidence, raw)
			}
			if err = enc.Encode(b); err != nil {
				t.Fatal(err)
			}
			minutes[b.Minute] = true
		}
		if err = zip.Close(); err != nil {
			t.Fatal(err)
		}
		if err = file.Close(); err != nil {
			t.Fatal(err)
		}
		if len(minutes) != 60 {
			t.Fatalf("need 60 complete logical minutes, got %d", len(minutes))
		}
		f, err := os.Open(fixture)
		if err != nil {
			t.Fatal(err)
		}
		h := sha256.New()
		_, err = io.Copy(h, f)
		f.Close()
		if err != nil {
			t.Fatal(err)
		}
		cached.Frames, cached.Minutes, cached.Active, cached.Inactive, cached.SHA256 = frames, 60, active, inactive, fmt.Sprintf("%x", h.Sum(nil))
		raw, _ := json.MarshalIndent(cached, "", "  ")
		if err = os.WriteFile(manifestPath, append(raw, '\n'), 0600); err != nil {
			t.Fatal(err)
		}
	}
	t.Logf("prepared 60-minute quality fixture: active=%d inactive=%d", active, inactive)
	results := []qualityBenchResult{}
	for _, variant := range []string{"baseline", "minute-layout", "shared-profile"} {
		name := "cmi_quality_bench_" + strings.ReplaceAll(uuid.NewString(), "-", "")
		if _, err = db.ExecContext(ctx, "CREATE DATABASE `"+name+"`"); err != nil {
			t.Fatal(err)
		}
		for _, table := range []Table{qualityTable, evidenceTable} {
			ddl := storageDDL(strings.Replace(table.DDL, "`crypto_market_info`.", "`"+name+"`.", 1))
			if variant == "minute-layout" {
				if table.Name == qualityTable.Name {
					ddl = strings.Replace(ddl, "PRIMARY KEY (`instrument_id`,`minute_time`,`compact_batch_id`)", "PRIMARY KEY (`minute_time`,`instrument_id`,`compact_batch_id`), KEY instrument_minute(instrument_id,minute_time)", 1)
				} else {
					ddl = strings.Replace(ddl, "PRIMARY KEY (`run_id`,`instrument_id`,`minute_time`,`compact_batch_id`)", "PRIMARY KEY (`minute_time`,`run_id`,`instrument_id`,`compact_batch_id`), KEY run_instrument_minute(run_id,instrument_id,minute_time)", 1)
				}
			}
			if _, err = db.ExecContext(ctx, ddl); err != nil {
				t.Fatal(err)
			}
		}
		c := &Client{db: db, database: name, family: "crypto_market_info"}
		if variant == "shared-profile" {
			if err = InstallQualityStorageSchema(ctx, db, name); err != nil {
				t.Fatal(err)
			}
			c.qualityStorageAvailable = true
			c.qualityStorageV2 = true
		}
		if err = qualityBenchDrain(ctx, db); err != nil {
			t.Fatal(err)
		}
		before, err := qualityBenchCounters(ctx, db)
		if err != nil {
			t.Fatal(err)
		}
		initial, err := qualityBenchAllocated(ctx, db, name)
		if err != nil {
			t.Fatal(err)
		}
		r := qualityBenchResult{Variant: variant, ActiveMembers: active, InactiveMembers: inactive, InitialAllocatedBytes: initial}
		latency := []float64{}
		start := time.Now()
		f, err := os.Open(fixture)
		if err != nil {
			t.Fatal(err)
		}
		z, err := gzip.NewReader(f)
		if err != nil {
			t.Fatal(err)
		}
		dec := gob.NewDecoder(z)
		for {
			var b qualityBenchBatch
			err = dec.Decode(&b)
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			bookRows := []record{}
			evidence := []options.QualityEvidence{}
			for _, raw := range b.Quality {
				m, err := UnpackMigrationRow(qualityTable, raw)
				if err != nil {
					t.Fatal(err)
				}
				bookRows = append(bookRows, record(m))
			}
			for _, raw := range b.Evidence {
				m, err := UnpackMigrationRow(evidenceTable, raw)
				if err != nil {
					t.Fatal(err)
				}
				rec := record(m)
				if err = expandAliases(evidenceTable, rec); err != nil {
					t.Fatal(err)
				}
				e, err := evidenceFromRecord(rec)
				if err != nil {
					t.Fatal(err)
				}
				evidence = append(evidence, e)
			}
			target := qualityTable
			if c.qualityStorageV2 {
				target, _ = c.mapping(qualityMinuteV2)
			}
			at := time.Now()
			tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
			if err != nil {
				t.Fatal(err)
			}
			if err = putImmutableRows(context.WithValue(ctx, minuteInsertKey{}, true), tx, c.table(target.Name), target, bookRows); err == nil {
				err = c.putQualityEvidence(context.WithValue(ctx, minuteInsertKey{}, true), tx, evidence)
			}
			if err != nil {
				tx.Rollback()
				t.Fatal(err)
			}
			if err = tx.Commit(); err != nil {
				t.Fatal(err)
			}
			latency = append(latency, float64(time.Since(at))/float64(time.Millisecond))
			r.Batches++
			r.Members += len(bookRows)
			if r.Batches%1000 == 0 {
				t.Logf("%s batches=%d members=%d", variant, r.Batches, r.Members)
			}
		}
		z.Close()
		f.Close()
		if err = qualityBenchDrain(ctx, db); err != nil {
			t.Fatal(err)
		}
		r.ElapsedSeconds = time.Since(start).Seconds()
		after, err := qualityBenchCounters(ctx, db)
		if err != nil {
			t.Fatal(err)
		}
		r.DataBytes = after["datafile"] - before["datafile"]
		r.RedoBytes = after["Innodb_os_log_written"] - before["Innodb_os_log_written"]
		r.DoublewriteBytes = after["doublewrite"] - before["doublewrite"]
		r.TotalBytes = r.DataBytes + r.RedoBytes + r.DoublewriteBytes
		r.PagesWritten = after["Innodb_dblwr_pages_written"] - before["Innodb_dblwr_pages_written"]
		r.AllocatedBytes, err = qualityBenchAllocated(ctx, db, name)
		if err != nil {
			t.Fatal(err)
		}
		r.DailyAllocatedIncrementEstimate = (r.AllocatedBytes - r.InitialAllocatedBytes) * 24
		slices.Sort(latency)
		r.CommitP95MS = latency[(len(latency)-1)*95/100]
		r.CommitP99MS = latency[(len(latency)-1)*99/100]
		if c.qualityStorageV2 {
			if err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+c.table(qualityProfile)).Scan(&r.Profiles); err != nil {
				t.Fatal(err)
			}
			if err = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+c.table(qualityMember)).Scan(&r.References); err != nil {
				t.Fatal(err)
			}
		}
		// Exact reads after flushing include the original per-member content hash.
		f, err = os.Open(fixture)
		if err != nil {
			t.Fatal(err)
		}
		z, err = gzip.NewReader(f)
		if err != nil {
			t.Fatal(err)
		}
		dec = gob.NewDecoder(z)
		readStart := time.Now()
		for {
			var b qualityBenchBatch
			err = dec.Decode(&b)
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			rows, err := c.loadDerivativeQuality(ctx, b.Minute, b.ID)
			if err != nil || len(rows) != len(b.Quality) {
				t.Fatal("book quality count", err)
			}
			for n, raw := range b.Quality {
				want, err := UnpackMigrationRow(qualityTable, raw)
				if err != nil {
					t.Fatal(err)
				}
				a, err := rowHash(qualityTable, record(want))
				if err != nil {
					t.Fatal(err)
				}
				back, err := rowHash(qualityTable, rows[n])
				if err != nil || a != back {
					t.Fatal("book quality bytes changed", err)
				}
			}
			e, err := c.loadQualityEvidence(ctx, b.Run, b.Minute, b.ID)
			if err != nil || len(e) != len(b.Evidence) {
				t.Fatal("evidence count", err)
			}
			for n, raw := range b.Evidence {
				want, err := UnpackMigrationRow(evidenceTable, raw)
				if err != nil {
					t.Fatal(err)
				}
				if err = expandAliases(evidenceTable, record(want)); err != nil {
					t.Fatal(err)
				}
				original, err := evidenceFromRecord(record(want))
				if err != nil || original.Hash() != e[n].Hash() || original.RowHash != e[n].RowHash || original.BatchID != e[n].BatchID {
					t.Fatal("evidence bytes changed", err)
				}
			}
		}
		r.ReadMS = float64(time.Since(readStart)) / float64(time.Millisecond)
		z.Close()
		f.Close()
		results = append(results, r)
		report, _ := json.MarshalIndent(results, "", "  ")
		if err = os.WriteFile(filepath.Join(outdir, "comparison.json"), append(report, '\n'), 0600); err != nil {
			t.Fatal(err)
		}
		t.Logf("RESULT %s", report)
		if _, err = db.ExecContext(ctx, "DROP DATABASE `"+name+"`"); err != nil {
			t.Fatal(err)
		}
		if _, err = db.ExecContext(ctx, "DELETE FROM cmi_storage.schema_version WHERE database_name=?", name); err != nil {
			t.Fatal(err)
		}
	}
	if results[2].TotalBytes >= results[0].TotalBytes {
		t.Fatal("shared quality storage did not reduce physical write counters")
	}
}
