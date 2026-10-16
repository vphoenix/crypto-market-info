// bootstrap-prepare only imports the reviewed startup closure from a sealed
// independent snapshot. It never activates a producer or controls a service.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"syscall"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/storage/codec"
	migration "github.com/vphoenix/crypto-market-info/internal/storage/migrate"
	store "github.com/vphoenix/crypto-market-info/internal/storage/mysql"
)

type identity struct {
	Runs map[string]string `json:"runs"`
}
type preparation struct {
	root     string
	ids      identity
	manifest migration.SnapshotManifest
	target   store.Config
}

func readJSON(path string, v any) error {
	b, e := os.ReadFile(path)
	if e != nil {
		return e
	}
	return json.Unmarshal(b, v)
}
func writeJSON(path string, v any) error {
	b, e := json.MarshalIndent(v, "", "  ")
	if e != nil {
		return e
	}
	return os.WriteFile(path, append(b, '\n'), 0600)
}
func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: bootstrap-prepare register|copy|verify|check|recovery|recovery-lowfrequency ABSOLUTE_PREPARATION_DIR")
		os.Exit(2)
	}
	p := preparation{root: os.Args[2], target: store.Config{Socket: "/home/ubuntu/crypto-market-info/var/storage/mysql/run/mysql.sock", Username: "cmi_migrate", PasswordFile: "/home/ubuntu/.config/crypto-market-info-storage/cmi_migrate-password", MaxConnections: 1, ReadTimeout: 2 * time.Minute}}
	e := p.run(ctx, os.Args[1])
	if e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func (p *preparation) config(t migration.SnapshotTable) migration.Config {
	rows := 500
	if t.Table == "options_collection_plan" {
		rows = 50
	}
	if t.Table == "okx_public_loan_policy" || t.Table == "okx_public_capital_parameters" {
		rows = 100
	}
	return migration.Config{Source: migration.SourceConfig{Address: "127.0.0.1:9000", SnapshotFile: filepath.Join(p.root, "snapshot-manifest.json")}, Target: p.target, Run: uuid.MustParse(p.ids.Runs[t.Family]), Family: t.Family, Database: t.Family, Table: t.Table, Rows: rows, StagingDir: filepath.Join(p.root, "staging"), MinFreeBytes: 50 << 30}
}
func (p *preparation) run(ctx context.Context, mode string) error {
	if !filepath.IsAbs(p.root) {
		return fmt.Errorf("absolute preparation directory required")
	}
	if e := readJSON(filepath.Join(p.root, "snapshot-identity.json"), &p.ids); e != nil {
		return e
	}
	if e := readJSON(filepath.Join(p.root, "snapshot-manifest.json"), &p.manifest); e != nil {
		return e
	}
	if p.manifest.Version != 1 || len(p.manifest.Tables) != 59 {
		return fmt.Errorf("complete sealed 59-table bootstrap manifest required")
	}
	for _, t := range p.manifest.Tables {
		if _, e := uuid.Parse(p.ids.Runs[t.Family]); e != nil {
			return e
		}
		s, e := migration.OpenSource(ctx, p.config(t).Source)
		if e != nil {
			return e
		}
		m, e := store.Mapping(t.Family, t.Table)
		if e == nil {
			e = s.CheckMapping(ctx, m)
		}
		s.Close()
		if e != nil {
			return e
		}
	}
	if mode == "register" {
		return p.register(ctx)
	}
	if mode == "check" {
		return p.check(ctx)
	}
	if mode == "recovery" {
		return p.recovery(ctx, true)
	}
	if mode == "recovery-lowfrequency" {
		return p.recovery(ctx, false)
	}
	if mode != "copy" && mode != "verify" {
		return fmt.Errorf("unknown preparation phase")
	}
	if e := os.MkdirAll(filepath.Join(p.root, "staging"), 0700); e != nil {
		return e
	}
	tables := append([]migration.SnapshotTable(nil), p.manifest.Tables...)
	// Small tables first; one source connection and one MySQL connection only.
	sort.Slice(tables, func(i, j int) bool { return tables[i].Rows < tables[j].Rows })
	if mode == "copy" {
		for _, t := range tables {
			if e := migration.Plan(ctx, p.config(t)); e != nil {
				return fmt.Errorf("plan %s.%s: %w", t.Family, t.Table, e)
			}
		}
	}
	var total uint64
	for i, t := range tables {
		fmt.Printf("%s begin %s.%s rows=%d table=%d/59 at=%s\n", mode, t.Family, t.Table, t.Rows, i+1, time.Now().UTC().Format(time.RFC3339))
		var e error
		if mode == "copy" {
			var count uint64
			e = migration.Copy(ctx, p.config(t), func(r store.MigrationResult) {
				count += r.Rows
				total += r.Rows
				if count%10000 == 0 {
					fmt.Printf("copy progress %s.%s rows=%d total=%d\n", t.Family, t.Table, count, total)
				}
			})
		} else {
			e = migration.Verify(ctx, p.config(t))
		}
		if e != nil {
			return fmt.Errorf("%s %s.%s: %w", mode, t.Family, t.Table, e)
		}
		fmt.Printf("%s finished %s.%s at=%s\n", mode, t.Family, t.Table, time.Now().UTC().Format(time.RFC3339))
	}
	return nil
}
func (p *preparation) register(ctx context.Context) error {
	var remaining struct {
		Remaining struct {
			DEX []struct {
				Batch string `json:"batch_id"`
				Block uint64 `json:"block_number"`
			} `json:"dex_batches"`
			Keeper []struct {
				ID string `json:"capture_id"`
			} `json:"keeper_batches"`
		} `json:"remaining"`
	}
	evidence := "/home/ubuntu/crypto-market-info/research/2026-10-13-source-recovery/summary.json"
	if e := readJSON(evidence, &remaining); e != nil {
		return e
	}
	if len(remaining.Remaining.DEX) != 6 || len(remaining.Remaining.Keeper) != 2 {
		return fmt.Errorf("reviewed abandoned batch set changed")
	}
	// Bound registration to the exact IDs explicitly reviewed before bootstrap.
	want := map[string]bool{
		"56f63ae97a7130b39f50b54eb9f5079e35776a64ecc61fc70b9b3479042452ab": true, "87fbc2694571ecbd6aa39e2622692be6d202fa20c11eddf99b441e10595df79c": true,
		"9c5ad10d44521f20bc01e3780d7819daaca969a029efeed0cc2250270895a5c1": true, "e08e05238a56341c5b9b4badc55bb240dfb4d524863719a18bc48b81a5c58ee3": true,
		"6660055f74a6f8e58593a47226e8670bad70578b8eb14f313a568546c2bf4bb3": true, "0fcc5a0956dd2cdc14c2de99c5930b23ab00d49d6a61cbac8f1807c44f70cdb6": true,
		"b6a66037-7afc-4bc6-8bd9-660b5bd4640e": true, "d5c3b497-757d-4de6-be2f-0afc6a4bf823": true,
	}
	type item struct{ family, table, filter, id string }
	items := []item{}
	for _, v := range remaining.Remaining.DEX {
		if !want[v.Batch] {
			return fmt.Errorf("unexpected DEX gap")
		}
		delete(want, v.Batch)
		items = append(items, item{"crypto_market_info", "dex_block", fmt.Sprintf("block_number=%d AND hex(batch_id)='%s'", v.Block, strings.ToUpper(v.Batch)), v.Batch})
	}
	for _, v := range remaining.Remaining.Keeper {
		if !want[v.ID] {
			return fmt.Errorf("unexpected Keeper gap")
		}
		delete(want, v.ID)
		items = append(items, item{"crypto_market_info_justlend_keeper", "jl_keeper_capture", "capture_id=toUUID('" + v.ID + "')", v.ID})
	}
	if len(want) != 0 {
		return fmt.Errorf("reviewed gap set incomplete")
	}
	conn, e := ch.Open(&ch.Options{Addr: []string{"127.0.0.1:9000"}, MaxOpenConns: 1, MaxIdleConns: 1, Settings: ch.Settings{"readonly": 2, "max_threads": 1, "max_memory_usage": 128 << 20, "max_execution_time": 30}})
	if e != nil {
		return e
	}
	defer conn.Close()
	db, e := store.Connect(ctx, p.target)
	if e != nil {
		return e
	}
	defer db.Close()
	output := []map[string]any{}
	for _, v := range items {
		var snapshot migration.SnapshotTable
		for _, t := range p.manifest.Tables {
			if t.Family == v.family && t.Table == v.table {
				snapshot = t
			}
		}
		if snapshot.Database == "" {
			return fmt.Errorf("gap snapshot not present")
		}
		t, e := store.Mapping(v.family, v.table)
		if e != nil {
			return e
		}
		cols := []string{}
		for _, c := range t.Columns {
			cols = append(cols, "`"+c.Name+"`")
		}
		rs, e := conn.Query(ctx, "SELECT "+strings.Join(cols, ",")+" FROM `"+snapshot.Database+"`.`"+snapshot.Name+"` FINAL WHERE "+v.filter)
		if e != nil {
			return e
		}
		if !rs.Next() {
			rs.Close()
			return fmt.Errorf("original gap marker not found")
		}
		ct := rs.ColumnTypes()
		dest := make([]any, len(ct))
		for i, c := range ct {
			dest[i] = reflect.New(c.ScanType()).Interface()
		}
		if e = rs.Scan(dest...); e != nil {
			rs.Close()
			return e
		}
		row := store.MigrationRow{}
		for i, c := range t.Columns {
			raw, e := codec.Encode(c.Type, reflect.ValueOf(dest[i]).Elem().Interface())
			if e != nil {
				rs.Close()
				return e
			}
			row[c.Name], e = codec.Decode(c.Type, raw)
			if e != nil {
				rs.Close()
				return e
			}
		}
		if rs.Next() {
			rs.Close()
			return fmt.Errorf("ambiguous original gap marker")
		}
		if e = rs.Err(); e != nil {
			rs.Close()
			return e
		}
		rs.Close()
		if e = validateOriginalGap(v.table, v.id, row); e != nil {
			return e
		}
		gap := store.AcceptedBootstrapGap{Run: uuid.MustParse(p.ids.Runs[v.family]), Database: v.family, Family: v.family, Table: v.table, Row: row, Reason: "Historical members are unrecoverable; explicitly abandoned by user on 2026-10-13", Evidence: evidence}
		if e = store.RegisterBootstrapGap(ctx, db, gap); e != nil {
			return e
		}
		raw, e := store.PackMigrationRow(t, row)
		if e != nil {
			return e
		}
		h := sha256.Sum256(raw)
		if e = os.WriteFile(filepath.Join(p.root, "original-gap-"+v.id+".row"), raw, 0600); e != nil {
			return e
		}
		output = append(output, map[string]any{"family": v.family, "table": v.table, "id": v.id, "original_row_sha256": hex.EncodeToString(h[:]), "declaration_preserved": true, "query_valid": false})
	}
	return writeJSON(filepath.Join(p.root, "accepted-bootstrap-gaps.json"), output)
}

func validateOriginalGap(table, id string, row store.MigrationRow) error {
	path := "/home/ubuntu/crypto-market-info/research/2026-10-13-source-recovery/dex-original-declarations.json"
	var entries []map[string]any
	load := func(b []byte, v any) error {
		d := json.NewDecoder(strings.NewReader(string(b)))
		d.UseNumber()
		return d.Decode(v)
	}
	data, e := os.ReadFile(path)
	if e != nil {
		return e
	}
	keys := map[string]string{"quote_digest_hex": "quote_members", "log_digest_hex": "log_members", "receipt_digest_hex": "receipt_members", "actual_quotes": "actual_quotes", "log_count": "log_count", "receipt_count": "receipt_count", "revision": "revision", "committed": "committed"}
	identityKey := "batch_hex"
	if table == "jl_keeper_capture" {
		path = "/home/ubuntu/crypto-market-info/research/2026-10-13-source-integrity/keeper-incomplete-members-detail.json"
		data, e = os.ReadFile(path)
		if e != nil {
			return e
		}
		var wrapper struct {
			Captures []map[string]any `json:"captures"`
		}
		if e = load(data, &wrapper); e != nil {
			return e
		}
		entries = wrapper.Captures
		keys = map[string]string{"cost_digest_hex": "cost_digest", "cost_rows": "cost_rows", "committed": "committed", "capture_kind": "capture_kind", "status": "status"}
		identityKey = "capture_uuid"
	} else if e = load(data, &entries); e != nil {
		return e
	}
	for _, entry := range entries {
		if !strings.EqualFold(fmt.Sprint(entry[identityKey]), id) {
			continue
		}
		for original, current := range keys {
			value := fmt.Sprint(row[current])
			if strings.HasSuffix(original, "_hex") {
				switch v := row[current].(type) {
				case string:
					value = hex.EncodeToString([]byte(v))
				case []byte:
					value = hex.EncodeToString(v)
				default:
					return fmt.Errorf("original gap digest type changed")
				}
			}
			if !strings.EqualFold(value, fmt.Sprint(entry[original])) {
				return fmt.Errorf("reviewed original declaration changed: %s %s", id, current)
			}
		}
		return nil
	}
	return fmt.Errorf("reviewed original declaration missing: %s", id)
}
func (p *preparation) check(ctx context.Context) error {
	db, e := store.Connect(ctx, p.target)
	if e != nil {
		return e
	}
	defer db.Close()
	out := map[string]any{"checked_at_utc": time.Now().UTC(), "history_complete": false, "services_switched": false}
	tables := []map[string]any{}
	for _, t := range p.manifest.Tables {
		var n uint64
		var state uint8
		if e = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM `"+t.Family+"`.`"+t.Table+"`").Scan(&n); e != nil {
			return e
		}
		if n != t.Rows {
			return fmt.Errorf("bootstrap count differs %s.%s snapshot=%d target=%d", t.Family, t.Table, t.Rows, n)
		}
		run := uuid.MustParse(p.ids.Runs[t.Family])
		if e = db.QueryRowContext(ctx, "SELECT state FROM cmi_storage.migration_table WHERE run_id=? AND database_name=? AND table_name=?", run[:], t.Family, t.Table).Scan(&state); e != nil {
			return e
		}
		if state != 2 {
			return fmt.Errorf("dependency domain not verified: %s.%s", t.Family, t.Table)
		}
		tables = append(tables, map[string]any{"family": t.Family, "table": t.Table, "rows": n, "state": state})
	}
	var producers, gaps, books uint64
	if e = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM cmi_storage.producer_generation").Scan(&producers); e != nil {
		return e
	}
	if producers != 0 {
		return fmt.Errorf("unexpected activated producer")
	}
	if e = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM cmi_storage.bootstrap_source_gap").Scan(&gaps); e != nil {
		return e
	}
	if gaps != 8 {
		return fmt.Errorf("unexpected accepted gap count")
	}
	if e = db.QueryRowContext(ctx, "SELECT (SELECT COUNT(*) FROM crypto_market_info.order_book_minute)+(SELECT COUNT(*) FROM crypto_market_info.derivative_book_minute)").Scan(&books); e != nil {
		return e
	}
	if books != 0 {
		return fmt.Errorf("hot books unexpectedly imported")
	}
	out["tables"] = tables
	out["accepted_historical_batch_gaps"] = gaps
	out["active_producers"] = producers
	out["hot_book_rows"] = books
	for family, id := range p.ids.Runs {
		report, e := store.MigrationReport(ctx, p.target, uuid.MustParse(id))
		if e != nil {
			return e
		}
		if report.State == 3 || report.Conflicts != 0 {
			return fmt.Errorf("bootstrap unexpectedly finalized or has unresolved conflicts")
		}
		out[family] = report
	}
	return writeJSON(filepath.Join(p.root, "bootstrap-check.json"), out)
}
