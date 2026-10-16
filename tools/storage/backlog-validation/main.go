// backlog-validation seals real frames and replays them into a dedicated local
// server. Socket guards prevent its write mode from reaching production.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math/bits"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/options"
	"github.com/vphoenix/crypto-market-info/internal/replay"
	"github.com/vphoenix/crypto-market-info/internal/storage/codec"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
	"github.com/vphoenix/crypto-market-info/internal/storage/mysql"
	"github.com/vphoenix/crypto-market-info/internal/storage/redisbuffer"
	"github.com/vphoenix/crypto-market-info/internal/storage/spool"
	"github.com/vphoenix/crypto-market-info/internal/storage/writer"
)

const workspaceEvidence = "/home/ubuntu/crypto-market-info/var/storage/write-backlog-improvement"

var base = workspaceEvidence
var captureFrom = time.Date(2026, 10, 14, 11, 38, 0, 0, time.UTC)
var captureTo = time.Date(2026, 10, 14, 13, 8, 0, 0, time.UTC)

const secretBase = "/home/ubuntu/.config/crypto-market-info-storage/"

type entry struct {
	File   string
	Header contracts.Delivery
	Bytes  int64
}
type totals struct{ Batches, Members, Bytes int64 }
type manifest struct {
	From, To          time.Time
	SealedAt          time.Time
	Entries           []entry
	Minutes           map[string]map[string]totals
	ProductionProofs  int
	MissingCEXMinutes []time.Time
}

func save(path string, v any) error {
	b, e := json.MarshalIndent(v, "", "  ")
	if e != nil {
		return e
	}
	return os.WriteFile(path, append(b, '\n'), 0600)
}
func config(test bool) mysql.Config {
	socket := "/home/ubuntu/crypto-market-info/var/storage/mysql/run/mysql.sock"
	if test {
		socket = workspaceEvidence + "/bench-mysql/run/mysql.sock"
	}
	return mysql.Config{Socket: socket, Database: "crypto_market_info", Username: "cmi_query", PasswordFile: secretBase + "cmi_query-password", ReadOnly: true, ReadTimeout: 2 * time.Minute, SelectTimeout: time.Minute, MaxConnections: 1}
}
func loadFrame(dir string, e entry) (contracts.Delivery, error) {
	raw, err := os.ReadFile(filepath.Join(dir, e.File))
	if err != nil {
		return contracts.Delivery{}, err
	}
	d, err := spool.Unmarshal(raw)
	if err != nil {
		return d, err
	}
	if !d.SameHeader(e.Header) || int64(len(d.Payload)) != e.Bytes {
		return d, contracts.ErrConflict
	}
	return d, nil
}
func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
func run() error {
	mode := flag.String("mode", "seal", "seal, replay, verify")
	dir := flag.String("frames", base+"/frozen-real-90m", "immutable original .batch files")
	workers := flag.Int("workers", 2, "commit workers")
	evidence := flag.String("evidence-dir", workspaceEvidence, "private evidence directory")
	from := flag.String("from", captureFrom.Format(time.RFC3339), "inclusive capture UTC minute")
	to := flag.String("to", captureTo.Format(time.RFC3339), "exclusive capture UTC minute")
	speed := flag.Float64("speed", 1.5, "logical minutes per actual minute")
	flag.Parse()
	if *evidence != workspaceEvidence && *evidence != workspaceEvidence+"/full-scope-validation" && *evidence != workspaceEvidence+"/post-retry-validation" && *evidence != workspaceEvidence+"/dex-index-validation" && *evidence != workspaceEvidence+"/streaming-gap-validation" && *evidence != workspaceEvidence+"/discovery-bulk-validation" && *evidence != workspaceEvidence+"/cache-diagnostics-validation" && *evidence != workspaceEvidence+"/publication-prefetch-validation" {
		return fmt.Errorf("unreviewed evidence destination")
	}
	base = *evidence
	if e := os.MkdirAll(base, 0700); e != nil {
		return e
	}
	var err error
	captureFrom, err = time.Parse(time.RFC3339, *from)
	if err != nil {
		return err
	}
	captureTo, err = time.Parse(time.RFC3339, *to)
	if err != nil {
		return err
	}
	if captureTo.Sub(captureFrom) != 90*time.Minute || !captureFrom.Equal(captureFrom.Truncate(time.Minute)) {
		return fmt.Errorf("explicit 90-minute capture window required")
	}
	if (*dir != workspaceEvidence+"/frozen-real-90m" && *dir != workspaceEvidence+"/frozen-real-full-90m" && *dir != workspaceEvidence+"/frozen-real-post-retry-90m" && *dir != workspaceEvidence+"/frozen-real-dex-index-90m" && *dir != workspaceEvidence+"/frozen-real-streaming-gap-90m" && *dir != workspaceEvidence+"/frozen-real-discovery-bulk-90m" && *dir != workspaceEvidence+"/frozen-real-cache-diagnostics-90m" && *dir != workspaceEvidence+"/frozen-real-publication-prefetch-90m") || *speed <= 0 || *speed > 2 {
		return fmt.Errorf("invalid guarded fixture settings")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Hour)
	defer cancel()
	if *mode == "audit-runtime" {
		return runtimeAudit(ctx)
	}
	if *mode == "probe-dex-gap" {
		return probeDEXGap(ctx)
	}
	if *mode == "probe-statement-cost" {
		return probeStatementCost(ctx)
	}
	if *mode == "probe-current-work" {
		return probeCurrentWork(ctx)
	}
	if *mode == "probe-active-plans" {
		return probeActivePlans(ctx)
	}
	if *mode == "probe-file-io" {
		return probeFileIO(ctx)
	}
	if *mode == "probe-bulk-plans" {
		return probeBulkPlans(ctx)
	}
	if *mode == "probe-public-controls" {
		c, stop := context.WithTimeout(ctx, 2*time.Minute)
		defer stop()
		return probePublicControls(c)
	}
	mp := base + "/frozen-manifest.json"
	if *mode == "seal" {
		return seal(ctx, *dir, mp)
	}
	if *mode == "refs" {
		return references(*dir)
	}
	var m manifest
	raw, e := os.ReadFile(mp)
	if e != nil {
		return e
	}
	if e = json.Unmarshal(raw, &m); e != nil {
		return e
	}
	if *mode == "replay" {
		return feed(ctx, *dir, m, *workers, *speed)
	}
	if *mode == "cex-coverage" {
		return cexCoverage(*dir, m)
	}
	if *mode == "verify" {
		return verify(ctx, *dir, m)
	}
	return fmt.Errorf("unknown mode")
}

func cexCoverage(dir string, m manifest) error {
	perMinute := map[string]int{}
	streams := map[uint32]int{}
	validSeconds := uint64(0)
	for _, v := range m.Entries {
		if v.Header.Kind != "cex" {
			continue
		}
		d, e := loadFrame(dir, v)
		if e != nil {
			return e
		}
		c, e := codec.DecodeBookDelivery(d.Minute, d.Payload, d.Codec)
		if e != nil {
			return e
		}
		for _, b := range c.Batches {
			perMinute[d.Minute.Format(time.RFC3339)]++
			streams[b.Minute.InstrumentID]++
			validSeconds += uint64(bits.OnesCount64(b.Minute.ValidBitmap))
		}
	}
	out := map[string]any{"sample_minutes": 90, "members_by_minute": perMinute, "minutes_per_stream": streams, "distinct_streams": len(streams), "valid_member_seconds": validSeconds, "missing_cex_minutes": m.MissingCEXMinutes, "full_scope_capacity_demonstrated": false}
	if e := save(base+"/frozen-cex-coverage.json", out); e != nil {
		return e
	}
	fmt.Printf("CEX original sample: %d distinct streams; source gaps retained; full-scope capacity not demonstrated\n", len(streams))
	return nil
}

func references(dir string) error {
	sets := map[string]map[string]bool{}
	add := func(table, value string) {
		if sets[table] == nil {
			sets[table] = map[string]bool{}
		}
		sets[table][value] = true
	}
	uid := func(table string, id uuid.UUID) {
		if id != uuid.Nil {
			add(table, "UNHEX('"+strings.ReplaceAll(id.String(), "-", "")+"')")
		}
	}
	files, e := os.ReadDir(dir)
	if e != nil {
		return e
	}
	for _, f := range files {
		if filepath.Ext(f.Name()) != ".batch" {
			continue
		}
		raw, e := os.ReadFile(filepath.Join(dir, f.Name()))
		if e != nil {
			return e
		}
		d, e := spool.Unmarshal(raw)
		if e != nil {
			return e
		}
		if d.Kind == "cex" {
			v, e := codec.DecodeBookDelivery(d.Minute, d.Payload, d.Codec)
			if e != nil {
				return e
			}
			for _, b := range v.Batches {
				add("instrument", fmt.Sprint(b.Minute.InstrumentID))
			}
			continue
		}
		var live options.LiveEnvelope
		if d.Kind == "options_catalog" {
			v, e := codec.DecodeOptionsCatalog(d.Payload)
			if e != nil {
				return e
			}
			live = v.Live
			for _, q := range v.Evidence {
				for sec := range 60 {
					if q.RuleObservationIDs[sec] != nil {
						uid("options_catalog_scope_observation", *q.RuleObservationIDs[sec])
					}
					if q.StateIDs[sec] != nil {
						table := "options_lifecycle_observation"
						if q.StateKinds[sec] == 2 {
							table = "options_catalog_scope_observation"
						}
						uid(table, *q.StateIDs[sec])
					}
					for _, id := range []*uuid.UUID{q.PlatformIDs[sec], q.MaintenanceIDs[sec], q.LockIDs[sec]} {
						if id != nil {
							uid("options_lifecycle_observation", *id)
						}
					}
				}
			}
		} else {
			live, e = codec.DecodeOptionsLive(d.Payload)
			if e != nil {
				return e
			}
			uid("options_metadata_observation", live.RunID)
		}
		uid("options_live_run", live.RunID)
		for _, b := range live.Books {
			add("instrument", fmt.Sprint(b.InstrumentID))
			add("derivative_contract_spec", fmt.Sprint(b.InstrumentID))
			for _, q := range b.Quality {
				if q.TradingRuleID != "" {
					if !model.ValidDigest(q.TradingRuleID) {
						return fmt.Errorf("invalid rule digest")
					}
					add("derivative_trading_rule", "'"+q.TradingRuleID+"'")
				}
			}
		}
	}
	cols := map[string]string{"instrument": "instrument_id", "derivative_contract_spec": "instrument_id", "derivative_trading_rule": "trading_rule_id", "options_live_run": "run_id", "options_metadata_observation": "run_id", "options_catalog_scope_observation": "observation_id", "options_lifecycle_observation": "observation_id"}
	queries := map[string][]string{"options_collection_plan": {"1=1"}}
	for table, set := range sets {
		values := []string{}
		for v := range set {
			values = append(values, v)
		}
		sort.Strings(values)
		for i := 0; i < len(values); i += 128 {
			queries[table] = append(queries[table], cols[table]+" IN ("+strings.Join(values[i:min(i+128, len(values))], ",")+")")
		}
	}
	return save(base+"/fixture-reference-filters.json", queries)
}
func seal(ctx context.Context, dir, path string) error {
	m := manifest{From: captureFrom, To: captureTo, Minutes: map[string]map[string]totals{}}
	c, e := mysql.Open(ctx, config(false))
	if e != nil {
		return e
	}
	defer c.Close()
	files, e := os.ReadDir(dir)
	if e != nil {
		return e
	}
	seen := map[string]bool{}
	runs := map[string]map[uuid.UUID]bool{}
	db, e := mysql.Connect(ctx, config(false))
	if e != nil {
		return e
	}
	defer db.Close()
	proofs := map[string]contracts.Delivery{}
	proofRows, e := db.QueryContext(ctx, "SELECT kind,batch_id,service,generation,logical_time,codec,content_hash,member_count FROM cmi_storage.ingest_batch WHERE database_name='crypto_market_info' AND service='collector' AND generation=1 AND logical_time>=? AND logical_time<?", m.From, m.To)
	if e != nil {
		return e
	}
	for proofRows.Next() {
		var d contracts.Delivery
		var digest []byte
		d.Database = "crypto_market_info"
		if e = proofRows.Scan(&d.Kind, &d.ID, &d.Service, &d.Generation, &d.Minute, &d.Codec, &digest, &d.Members); e != nil {
			proofRows.Close()
			return e
		}
		if len(digest) != 32 {
			proofRows.Close()
			return fmt.Errorf("invalid durable digest")
		}
		copy(d.Digest[:], digest)
		proofs[d.Identity()] = d
	}
	e = proofRows.Err()
	proofRows.Close()
	if e != nil {
		return e
	}
	for _, f := range files {
		if filepath.Ext(f.Name()) != ".batch" {
			continue
		}
		raw, e := os.ReadFile(filepath.Join(dir, f.Name()))
		if e != nil {
			return e
		}
		d, e := spool.Unmarshal(raw)
		if e != nil {
			return fmt.Errorf("%s: %w", f.Name(), e)
		}
		if d.Database != "crypto_market_info" || d.Service != "collector" || d.Generation != 1 || d.Minute.Before(m.From) || !d.Minute.Before(m.To) || seen[d.Identity()] || f.Name() != spool.Name(d) {
			return fmt.Errorf("invalid fixture identity")
		}
		proof, ok := proofs[d.Identity()]
		if !ok || !proof.SameHeader(d) {
			return fmt.Errorf("missing or conflicting production proof %s", d.ID)
		}
		seen[d.Identity()] = true
		m.ProductionProofs++
		key := d.Minute.Format(time.RFC3339)
		if d.Kind == "options_catalog" {
			env, e := codec.DecodeOptionsCatalog(d.Payload)
			if e != nil {
				return e
			}
			if runs[key] == nil {
				runs[key] = map[uuid.UUID]bool{}
			}
			if runs[key][env.Live.RunID] {
				return fmt.Errorf("duplicate catalog run at %s", key)
			}
			runs[key][env.Live.RunID] = true
		}
		if m.Minutes[key] == nil {
			m.Minutes[key] = map[string]totals{}
		}
		t := m.Minutes[key][d.Kind]
		t.Batches++
		t.Members += int64(d.Members)
		t.Bytes += int64(len(d.Payload))
		m.Minutes[key][d.Kind] = t
		n := int64(len(d.Payload))
		d.Payload = nil
		m.Entries = append(m.Entries, entry{f.Name(), d, n})
	}
	// Compare all durable source batch identities, not just captured successes.
	rows, e := db.QueryContext(ctx, "SELECT logical_time,kind,COUNT(*),SUM(member_count) FROM cmi_storage.ingest_batch WHERE database_name='crypto_market_info' AND service='collector' AND generation=1 AND logical_time>=? AND logical_time<? GROUP BY logical_time,kind", m.From, m.To)
	if e != nil {
		return e
	}
	counts := 0
	for rows.Next() {
		var t time.Time
		var kind string
		var n, member int64
		if e = rows.Scan(&t, &kind, &n, &member); e != nil {
			rows.Close()
			return e
		}
		got := m.Minutes[t.UTC().Format(time.RFC3339)][kind]
		if got.Batches != n || got.Members != member {
			rows.Close()
			return fmt.Errorf("incomplete capture %s %s captured=%d durable=%d", t, kind, got.Batches, n)
		}
		counts++
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return e
	}
	if len(m.Minutes) != 90 || counts < 90 {
		return fmt.Errorf("incomplete 90-minute coverage: %d minutes, %d groups", len(m.Minutes), counts)
	}
	for t := m.From; t.Before(m.To); t = t.Add(time.Minute) {
		groups := m.Minutes[t.Format(time.RFC3339)]
		if groups["options_catalog"].Batches == 0 {
			return fmt.Errorf("missing catalog minute at %s", t)
		}
		// A source minute with no CEX anchor emits no CEX batch. Retain that
		// absence explicitly; never synthesize a replacement for capacity.
		if groups["cex"].Batches == 0 {
			m.MissingCEXMinutes = append(m.MissingCEXMinutes, t)
		}
	}
	// Every enabled catalog shard of each effective plan must be represented.
	for t := m.From; t.Before(m.To); t = t.Add(time.Minute) {
		p, e := c.OptionsPlanAt(ctx, t)
		if e != nil {
			return e
		}
		for _, id := range p.RunIDs {
			if !runs[t.Format(time.RFC3339)][id] {
				return fmt.Errorf("missing plan shard at %s", t)
			}
		}
	}
	sort.Slice(m.Entries, func(i, j int) bool {
		a, b := m.Entries[i].Header, m.Entries[j].Header
		if !a.Minute.Equal(b.Minute) {
			return a.Minute.Before(b.Minute)
		}
		if a.Kind != b.Kind {
			return a.Kind < b.Kind
		}
		return a.ID < b.ID
	})
	m.SealedAt = time.Now().UTC()
	if e = save(path, m); e != nil {
		return e
	}
	fmt.Printf("sealed %d original batches, 90 logical minutes, all durable proofs and catalog shards verified\n", len(m.Entries))
	return nil
}
func feed(ctx context.Context, dir string, m manifest, workers int, speed float64) error {
	cfg := config(true)
	cfg.ReadOnly = false
	cfg.ReadTimeout = 0
	cfg.SelectTimeout = 0
	cfg.MaxConnections = 2
	cfg.Username = "cmi_live"
	cfg.PasswordFile = secretBase + "cmi_live-password"
	// Test server must be fresh: repeats are not capacity evidence.
	db, e := mysql.Connect(ctx, cfg)
	if e != nil {
		return e
	}
	var n int
	if e = db.QueryRowContext(ctx, "SELECT COUNT(*) FROM cmi_storage.ingest_batch").Scan(&n); e != nil {
		db.Close()
		return e
	}
	db.Close()
	if n != 0 {
		return fmt.Errorf("test server already has committed batches; refusing idempotent replay")
	}
	rc := redisbuffer.Config{Socket: workspaceEvidence + "/bench-redis/run/redis.sock", Prefix: "cmi:backlog-validation", WaitTimeout: 2 * time.Second}
	b, e := redisbuffer.Open(ctx, rc)
	if e != nil {
		return e
	}
	defer b.Close()
	q, e := spool.Open(workspaceEvidence+"/bench-spool", 20<<30)
	if e != nil {
		return e
	}
	logfile, e := writer.OpenDiagnosticLog(base+"/bench-writer.log", 64<<20)
	if e != nil {
		return e
	}
	defer logfile.Close()
	wc, stop := context.WithCancel(ctx)
	defer stop()
	done := make(chan error, 1)
	go func() {
		done <- writer.Run(wc, writer.Config{MySQL: cfg, Redis: rc, Spool: q.Directory, Databases: []string{"crypto_market_info"}, Workers: workers, Preparers: 2, PrepareBytes: 64 << 20, CacheBytes: 32 << 20}, slog.New(slog.NewJSONHandler(logfile, nil)))
	}()
	start := time.Now()
	if e = save(base+"/bench-run-start.json", map[string]any{"started_at": start.UTC(), "logical_minutes": 90, "speed": speed, "workers": workers, "production_writes": 0}); e != nil {
		return e
	}
	sent := 0
	var bytes, members int64
	var maxEnqueueDelay time.Duration
	for _, v := range m.Entries {
		due := start.Add(time.Duration(float64(v.Header.Minute.Sub(m.From)) / speed))
		timer := time.NewTimer(time.Until(due))
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case err := <-done:
			timer.Stop()
			return fmt.Errorf("writer exited: %v", err)
		case <-timer.C:
		}
		d, e := loadFrame(dir, v)
		if e != nil {
			return e
		}
		// Exercise both durable locations, including duplicate-copy confirmation.
		// Redis's synchronous AOF confirmation can take one second. Spool
		// supplies the remaining durable ingress so the feeder itself does not
		// cap the requested 1.5x rate. Both locations remain continuously used.
		if sent%10 == 0 {
			e = b.Accept(ctx, d)
		} else {
			e = q.Accept(ctx, d)
		}
		if e != nil {
			return e
		}
		if sent%20 == 0 {
			if e = q.Accept(ctx, d); e != nil {
				return e
			}
			if e = b.Accept(ctx, d); e != nil {
				return e
			}
		}
		sent++
		bytes += v.Bytes
		members += int64(d.Members)
		maxEnqueueDelay = max(maxEnqueueDelay, time.Since(due))
		if maxEnqueueDelay > 30*time.Second {
			return fmt.Errorf("feeder missed cadence by %s; capacity is not demonstrated", maxEnqueueDelay)
		}
	}
	feedCompleted := time.Now()
	duration := time.Duration(float64(m.To.Sub(m.From)) / speed)
	timer := time.NewTimer(time.Until(start.Add(duration)))
	select {
	case <-timer.C:
	case err := <-done:
		timer.Stop()
		return fmt.Errorf("writer exited: %v", err)
	case <-ctx.Done():
		timer.Stop()
		return ctx.Err()
	}
	proof, e := mysql.Open(ctx, config(true))
	if e != nil {
		return e
	}
	defer proof.Close()
	deadline := time.Now().Add(10 * time.Minute)
	for _, v := range m.Entries {
		for {
			ok, e := proof.Committed(ctx, v.Header)
			if e != nil {
				return e
			}
			if ok {
				break
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("backlog did not drain within 10 minutes")
			}
			select {
			case err := <-done:
				return fmt.Errorf("writer exited: %v", err)
			case <-time.After(time.Second):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	// ACK is separate from commit; allow a bounded cleanup interval.
	for {
		fs, e := os.ReadDir(q.Directory)
		if e != nil {
			return e
		}
		count := 0
		for _, f := range fs {
			if filepath.Ext(f.Name()) == ".batch" {
				count++
			}
		}
		cs, e := b.Candidates(ctx, "crypto_market_info", 0, 256)
		if e != nil {
			return e
		}
		if count == 0 && len(cs) == 0 {
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("committed copies remain unacknowledged")
		}
		time.Sleep(time.Second)
	}
	stop()
	select {
	case e = <-done:
		if e != nil && !errors.Is(e, context.Canceled) {
			return e
		}
	case <-time.After(45 * time.Second):
		return fmt.Errorf("writer did not stop")
	}
	end := time.Now()
	if e = checkCapacityCadence(m, start, speed); e != nil {
		return e
	}
	fmt.Printf("replayed %d batches over %.1f actual minutes; all committed and both queues drained\n", sent, end.Sub(start).Minutes())
	return save(base+"/bench-run-complete.json", map[string]any{"started_at": start.UTC(), "ended_at": end.UTC(), "feed_seconds": duration.Seconds(), "feed_completed_at": feedCompleted.UTC(), "max_enqueue_delay_seconds": maxEnqueueDelay.Seconds(), "elapsed_seconds": end.Sub(start).Seconds(), "batches": sent, "members": members, "bytes": bytes, "speed": speed, "workers": workers, "all_durable_proofs": true, "both_queues_empty": true, "production_writes": 0})
}
func verify(ctx context.Context, dir string, m manifest) error {
	c, e := mysql.Open(ctx, config(true))
	if e != nil {
		return e
	}
	defer c.Close()
	if e = c.InitDiscoverySchema(ctx); e != nil {
		return e
	}
	books, seconds := 0, 0
	for _, v := range m.Entries {
		d, e := loadFrame(dir, v)
		if e != nil {
			return e
		}
		ok, e := c.Committed(ctx, d)
		if e != nil || !ok {
			return fmt.Errorf("missing bench proof: %v", e)
		}
		if d.Kind == "cex" {
			env, e := codec.DecodeBookDelivery(d.Minute, d.Payload, d.Codec)
			if e != nil {
				return e
			}
			for _, src := range env.Batches {
				minute, e := c.LoadMinute(ctx, src.Minute.InstrumentID, d.Minute)
				if e != nil {
					return e
				}
				ds, e := c.LoadDeltas(ctx, minute.ID, 59)
				if e != nil {
					return e
				}
				got := model.MinuteBatch{Minute: minute, Deltas: ds}
				if model.DiscoveryHash(model.TradingMinuteBody(src)) != model.DiscoveryHash(model.TradingMinuteBody(got)) {
					return fmt.Errorf("CEX payload mismatch")
				}
				for sec := uint8(0); sec < 60; sec++ {
					_, _, e = replay.AtSecond(minute, ds, sec)
					if e != nil {
						return e
					}
					seconds++
				}
				books++
			}
		} else {
			var src, got options.LiveEnvelope
			if d.Kind == "options_catalog" {
				s, e := codec.DecodeOptionsCatalog(d.Payload)
				if e != nil {
					return e
				}
				g, e := c.LoadOptionsCatalogMinute(ctx, s.Live.RunID, d.Minute)
				if e != nil {
					return e
				}
				if s.ID() != g.ID() {
					return fmt.Errorf("catalog payload mismatch")
				}
				src, got = s.Live, g.Live
			} else {
				src, e = codec.DecodeOptionsLive(d.Payload)
				if e != nil {
					return e
				}
				got, e = c.LoadOptionsMinute(ctx, src.RunID, d.Minute)
				if e != nil {
					return e
				}
				if src.ID() != got.ID() {
					return fmt.Errorf("live payload mismatch")
				}
			}
			for _, book := range got.Books {
				for sec := uint8(0); sec < 60; sec++ {
					_, e := replay.ReplayDerivative(book, sec)
					if e != nil {
						return e
					}
					seconds++
				}
				books++
			}
		}
	}
	fmt.Printf("exact stored content and 60-second replay checked: %d books, %d seconds\n", books, seconds)
	return save(base+"/bench-replay-verified.json", map[string]any{"checked_at": time.Now().UTC(), "batches": len(m.Entries), "books": books, "seconds": seconds, "all_domain_content_hashes_equal": true, "production_writes": 0})
}
